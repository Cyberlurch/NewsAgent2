from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from .collectors_youtube import list_recent_videos, fetch_transcript
from .summarizer import summarize, summarize_item_detail
from .reporter import to_markdown
from .emailer import send_markdown
from .state_manager import (
    load_state,
    save_state,
    prune_state,
    is_processed,
    mark_processed,
)

STO = ZoneInfo("Europe/Stockholm")


def _safe_int(env_name: str, default: int) -> int:
    raw = os.getenv(env_name, "")
    raw = (raw or "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        print(f"[warn] Invalid int in {env_name}={raw!r} -> using default {default}")
        return default


def load_channels_config(path: str) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]], Dict[str, float]]:
    """
    Liest data/channels.json (oder youtube_only.json) und flacht die Kanäle aus.

    Erwartet (tolerant):
    {
      "topic_buckets": [
        {
          "name": "..."   # oder: "topic": "...",
          "weight": 1.0,  # optional
          "channels": [
            {"name": "...", "url": "..."}
          ]
        }
      ]
    }
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    buckets = cfg.get("topic_buckets")
    if not isinstance(buckets, list):
        raise ValueError("topic_buckets must be a list")

    channels: List[Dict[str, Any]] = []
    channel_topics: Dict[str, List[str]] = {}  # channel_name -> [topic_name,...]
    topic_weights: Dict[str, float] = {}

    for b in buckets:
        if not isinstance(b, dict):
            continue

        # Support both 'name' and 'topic' (youtube_only.json uses 'topic')
        tname = (b.get("name") or b.get("topic") or "").strip()
        if not tname:
            continue

        w = b.get("weight", 1.0)
        try:
            w = float(w)
        except Exception:
            w = 1.0
        if w <= 0:
            w = 1.0

        topic_weights[tname] = w

        chs = b.get("channels") or []
        if not isinstance(chs, list):
            continue

        for c in chs:
            if not isinstance(c, dict):
                continue
            cname = (c.get("name") or "").strip()
            curl = (c.get("url") or "").strip()
            if not cname or not curl:
                continue

            channels.append({"name": cname, "url": curl})
            channel_topics.setdefault(cname, []).append(tname)

    # defensive: stable ordering for deterministic runs
    channels.sort(key=lambda x: (x.get("name") or "", x.get("url") or ""))

    return channels, channel_topics, topic_weights


def _dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicates within the same run by video id."""
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        vid = str(it.get("id") or "").strip()
        if not vid:
            continue
        if vid in seen:
            continue
        seen.add(vid)
        out.append(it)
    return out


def _allocate_detail_slots_by_topic(
    items: List[Dict[str, Any]],
    channel_topics: Dict[str, List[str]],
    topic_weights: Dict[str, float],
    total_slots: int,
) -> Dict[str, int]:
    """Allocate total detail slots across topics based on weights, only for active topics."""
    if total_slots <= 0:
        return {}

    topic_counts: Dict[str, int] = {}
    for it in items:
        ch = (it.get("channel") or "").strip()
        for t in channel_topics.get(ch, []):
            topic_counts[t] = topic_counts.get(t, 0) + 1

    active_topics = [t for t in topic_counts.keys() if topic_counts.get(t, 0) > 0]
    if not active_topics:
        return {}

    weights = {t: float(topic_weights.get(t, 1.0) or 1.0) for t in active_topics}
    wsum = sum(weights.values()) or 1.0

    alloc: Dict[str, int] = {t: int(round(total_slots * weights[t] / wsum)) for t in active_topics}

    drift = total_slots - sum(alloc.values())
    if drift != 0:
        order = sorted(active_topics, key=lambda t: (-weights[t], t))
        i = 0
        step = 1 if drift > 0 else -1
        for _ in range(abs(drift)):
            alloc[order[i % len(order)]] = max(0, alloc[order[i % len(order)]] + step)
            i += 1

    for t in list(alloc.keys()):
        alloc[t] = min(alloc[t], topic_counts.get(t, 0))

    return alloc


def _choose_detail_items(
    items: List[Dict[str, Any]],
    channel_topics: Dict[str, List[str]],
    topic_weights: Dict[str, float],
    detail_items_per_day: int,
    detail_items_per_channel_max: int,
) -> List[Dict[str, Any]]:
    """
    Pick items for detail sections:
      - Allocate slots per topic (weight-based).
      - Round-robin across channels within each topic.
      - Enforce per-channel cap.
    """
    if detail_items_per_day <= 0:
        return []

    items_sorted = sorted(
        items,
        key=lambda it: it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    slots_by_topic = _allocate_detail_slots_by_topic(items_sorted, channel_topics, topic_weights, detail_items_per_day)
    if not slots_by_topic:
        chosen: List[Dict[str, Any]] = []
        per_ch: Dict[str, int] = {}
        for it in items_sorted:
            ch = (it.get("channel") or "").strip()
            if per_ch.get(ch, 0) >= detail_items_per_channel_max:
                continue
            chosen.append(it)
            per_ch[ch] = per_ch.get(ch, 0) + 1
            if len(chosen) >= detail_items_per_day:
                break
        return chosen

    chosen: List[Dict[str, Any]] = []
    per_ch: Dict[str, int] = {}

    pools: Dict[str, List[Dict[str, Any]]] = {t: [] for t in slots_by_topic.keys()}
    for it in items_sorted:
        ch = (it.get("channel") or "").strip()
        for t in channel_topics.get(ch, []):
            if t in pools:
                pools[t].append(it)

    for t in sorted(slots_by_topic.keys(), key=lambda x: (-float(topic_weights.get(x, 1.0) or 1.0), x)):
        need = slots_by_topic.get(t, 0)
        if need <= 0:
            continue

        pool = pools.get(t, [])
        if not pool:
            continue

        by_channel: Dict[str, List[Dict[str, Any]]] = {}
        for it in pool:
            ch = (it.get("channel") or "").strip()
            by_channel.setdefault(ch, []).append(it)

        channels_order = sorted(by_channel.keys())

        picked_here = 0
        guard = 0
        while picked_here < need and guard < 10000:
            guard += 1
            progress = False
            for ch in channels_order:
                if picked_here >= need:
                    break
                if per_ch.get(ch, 0) >= detail_items_per_channel_max:
                    continue
                lst = by_channel.get(ch) or []
                while lst:
                    it = lst.pop(0)
                    if any(x.get("id") == it.get("id") for x in chosen):
                        continue
                    chosen.append(it)
                    per_ch[ch] = per_ch.get(ch, 0) + 1
                    picked_here += 1
                    progress = True
                    break
            if not progress:
                break

    if len(chosen) < detail_items_per_day:
        for it in items_sorted:
            if len(chosen) >= detail_items_per_day:
                break
            vid = it.get("id")
            if any(x.get("id") == vid for x in chosen):
                continue
            ch = (it.get("channel") or "").strip()
            if per_ch.get(ch, 0) >= detail_items_per_channel_max:
                continue
            chosen.append(it)
            per_ch[ch] = per_ch.get(ch, 0) + 1

    return chosen[:detail_items_per_day]


def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser(description="NewsAgent2 daily report")
    ap.add_argument("--channels", default="data/channels.json", help="Path to channels config JSON")
    ap.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    args = ap.parse_args()

    report_key = (os.getenv("REPORT_KEY", "cyberlurch") or "cyberlurch").strip()
    report_title = (os.getenv("REPORT_TITLE", "The Cyberlurch Report") or "The Cyberlurch Report").strip()
    report_subject = (os.getenv("REPORT_SUBJECT", report_title) or report_title).strip()

    report_dir = (os.getenv("REPORT_DIR", "reports") or "reports").strip()
    os.makedirs(report_dir, exist_ok=True)

    send_empty_email = (os.getenv("SEND_EMPTY_REPORT_EMAIL", "1") or "1").strip()

    max_items_per_channel = _safe_int("MAX_ITEMS_PER_CHANNEL", 5)
    detail_items_per_day = _safe_int("DETAIL_ITEMS_PER_DAY", 8)
    detail_items_per_channel_max = _safe_int("DETAIL_ITEMS_PER_CHANNEL_MAX", 3)

    state_path = (os.getenv("STATE_PATH", "state/processed_items.json") or "state/processed_items.json").strip()
    retention_days = _safe_int("STATE_RETENTION_DAYS", 20)

    print("=== NewsAgent2 run ===")
    print(f"[config] report_key={report_key!r}")
    print(f"[config] report_title={report_title!r}")
    print(f"[config] report_subject={report_subject!r}")
    print(f"[config] channels_file={args.channels!r} hours={args.hours}")
    print(f"[config] report_dir={report_dir!r}")
    print(
        f"[config] limits: MAX_ITEMS_PER_CHANNEL={max_items_per_channel}, "
        f"DETAIL_ITEMS_PER_DAY={detail_items_per_day}, DETAIL_ITEMS_PER_CHANNEL_MAX={detail_items_per_channel_max}"
    )
    print(f"[state] path={state_path!r} retention_days={retention_days}")

    state = load_state(state_path)
    try:
        removed_age, removed_cap = prune_state(state, retention_days=retention_days)
        if removed_age or removed_cap:
            print(f"[state] pruned: removed_by_age={removed_age} removed_by_cap={removed_cap}")
    except Exception as e:
        print(f"[state] WARN: prune_state failed (continuing): {e!r}")

    try:
        channels, channel_topics, topic_weights = load_channels_config(args.channels)
    except Exception as e:
        overview = f"## Kurzüberblick\n\n**Fehler:** Konnte Channels-Konfiguration nicht laden: `{e!r}`\n"
        out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
        md = to_markdown([], overview, {}, report_title=report_title)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"[report] Wrote {out_path}")

        if send_empty_email == "1":
            try:
                send_markdown(report_subject, md)
            except Exception as se:
                print(f"[email] WARN: failed to send error report email: {se!r}")
        else:
            print("[email] SEND_EMPTY_REPORT_EMAIL=0 -> not sending email.")

        try:
            save_state(state_path, state)
        except Exception as ee:
            print(f"[state] WARN: failed to save state after config error: {ee!r}")
        return

    print(f"[channels] Loaded channels: {len(channels)}")

    items: List[Dict[str, Any]] = []
    skipped_by_state = 0

    for ch in channels:
        cname = ch["name"]
        curl = ch["url"]

        try:
            vids = list_recent_videos(curl, hours=args.hours, max_items=max_items_per_channel)
        except Exception as e:
            print(f"[collect] ERROR channel={cname!r}: list_recent_videos failed: {e!r}")
            continue

        for v in vids:
            vid = str(v.get("id") or "").strip()
            if not vid:
                continue

            if is_processed(state, report_key, "youtube", vid):
                skipped_by_state += 1
                continue

            desc = (v.get("description") or "").strip()

            transcript = None
            try:
                transcript = fetch_transcript(vid)
            except Exception as e:
                print(f"[collect] WARN channel={cname!r} video={vid!r}: fetch_transcript failed: {e!r}")
                transcript = None

            text = (transcript or desc).strip()
            if not text:
                print(f"[collect] WARN channel={cname!r} video={vid!r}: no transcript/description -> skipping")
                continue

            items.append(
                {
                    "id": vid,
                    "channel": cname,
                    "title": (v.get("title") or "").strip(),
                    "url": (v.get("url") or "").strip(),
                    "published_at": v.get("published_at"),
                    "description": desc,
                    "text": text,
                }
            )

    items = _dedupe_items(items)
    print(f"[collect] Collected {len(items)} item(s). (skipped_by_state={skipped_by_state})")

    if not items:
        overview = "## Kurzüberblick\n\nKeine neuen Inhalte in den letzten 24 Stunden.\n"
        out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
        md = to_markdown([], overview, {}, report_title=report_title)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"[report] Wrote {out_path}")

        try:
            save_state(state_path, state)
            print(f"[state] Saved state to {state_path!r}")
        except Exception as e:
            print(f"[state] WARN: failed to save state: {e!r}")

        if send_empty_email == "1":
            try:
                send_markdown(report_subject, md)
            except Exception as e:
                print(f"[email] WARN: failed to send empty report email: {e!r}")
        else:
            print("[email] No new items and SEND_EMPTY_REPORT_EMAIL=0 -> not sending email.")
        return

    try:
        overview_body = summarize(items).strip()
    except Exception as e:
        print(f"[summarize] ERROR: summarize() failed: {e!r}")
        overview_body = "## Kurzüberblick\n\n**Fehler:** Konnte Kurzüberblick nicht erzeugen.\n"

    detail_items = _choose_detail_items(
        items=items,
        channel_topics=channel_topics,
        topic_weights=topic_weights,
        detail_items_per_day=detail_items_per_day,
        detail_items_per_channel_max=detail_items_per_channel_max,
    )

    details_by_id: Dict[str, str] = {}
    for it in detail_items:
        vid = str(it.get("id") or "").strip()
        if not vid:
            continue
        try:
            details_by_id[vid] = summarize_item_detail(it).strip()
        except Exception as e:
            print(f"[summarize] WARN: summarize_item_detail failed for {vid!r}: {e!r}")
            details_by_id[vid] = "Kernaussagen:\n- (Fehler beim Erzeugen der Detail-Zusammenfassung)\n"

    out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
    md = to_markdown(items, overview_body, details_by_id, report_title=report_title)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"[report] Wrote {out_path}")

    # Memory: mark processed BEFORE email to avoid duplicates on SMTP failures
    now_utc_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for it in items:
        vid = str(it.get("id") or "").strip()
        if not vid:
            continue
        try:
            meta = {
                "title": it.get("title"),
                "channel": it.get("channel"),
                "url": it.get("url"),
                "processed_at_utc": now_utc_iso,
            }
            mark_processed(state, report_key, "youtube", vid, meta=meta)
        except Exception as e:
            print(f"[state] WARN: mark_processed failed for {vid!r}: {e!r}")

    try:
        save_state(state_path, state)
        print(f"[state] Saved state to {state_path!r}")
    except Exception as e:
        print(f"[state] WARN: failed to save state: {e!r}")

    try:
        send_markdown(report_subject, md)
    except Exception as e:
        print(f"[email] WARN: failed to send email (report was generated and state saved): {e!r}")


if __name__ == "__main__":
    main()
