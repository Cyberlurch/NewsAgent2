from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
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
    make_item_key,
)

STO = ZoneInfo("Europe/Stockholm")


def _safe_int(env_name: str, default: int) -> int:
    raw = os.getenv(env_name, str(default)).strip()
    try:
        return int(raw)
    except Exception:
        print(f"[warn] Invalid int in {env_name}={raw!r} -> using default {default}")
        return default


def load_channels_config(path: str) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]], Dict[str, float]]:
    """
    Liest data/channels.json (oder youtube_only.json) und flacht die Kanäle aus.

    Erwartet:
    {
      "topic_buckets": [
        {
          "name": "...",
          "weight": 1.0,
          "channels": [
            {"name": "...", "url": "..."}
          ]
        }
      ]
    }

    Rückgabe:
      channels: List[{"name","url"}]
      channel_topics: {channel_name: [topic_name,...]}
      topic_weights: {topic_name: weight}
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    buckets = cfg.get("topic_buckets") or []
    channels: List[Dict[str, Any]] = []
    channel_topics: Dict[str, List[str]] = {}
    topic_weights: Dict[str, float] = {}

    if not isinstance(buckets, list):
        raise ValueError("topic_buckets must be a list")

    for b in buckets:
        if not isinstance(b, dict):
            continue
        tname = (b.get("name") or "").strip()
        if not tname:
            continue
        w = b.get("weight", 1.0)
        try:
            w = float(w)
        except Exception:
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

    return channels, channel_topics, topic_weights


def dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Defensive Dedupe innerhalb *eines* Runs.
    """
    seen: set[tuple[str, str]] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        key = (it.get("channel", ""), it.get("title", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def choose_detail_items(
    items: List[Dict[str, Any]],
    channel_topics: Dict[str, List[str]],
    topic_weights: Dict[str, float],
    detail_items_per_day: int,
    detail_items_per_channel_max: int,
) -> List[Dict[str, Any]]:
    """
    Themengewichtete Verteilung der Detail-Slots + Round-Robin über Kanäle.

    - Pro Topic wird ein Slotbudget anhand weight verteilt.
    - Innerhalb eines Topics werden Kanäle Round-Robin ausgewählt.
    - Zusätzlich: per-channel cap (DETAIL_ITEMS_PER_CHANNEL_MAX)
    """
    if detail_items_per_day <= 0 or not items:
        return []

    # Items nach Topic bucketen (ein Item kann mehrere Topics haben -> zählt zum ersten Topic, falls vorhanden)
    by_topic: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        topics = channel_topics.get(it["channel"]) or ["(misc)"]
        topic = topics[0] if topics else "(misc)"
        by_topic.setdefault(topic, []).append(it)

    # Weights normalisieren (nur Topics, die überhaupt Items haben)
    active_topics = [t for t in by_topic.keys()]
    weights: Dict[str, float] = {}
    for t in active_topics:
        weights[t] = float(topic_weights.get(t, 1.0))

    total_w = sum(weights.values()) or 1.0

    # Slotbudget per Topic (mind. 0, später Rest auffüllen)
    topic_budget: Dict[str, int] = {}
    allocated = 0
    for t, w in weights.items():
        b = int(round(detail_items_per_day * (w / total_w)))
        b = max(0, b)
        topic_budget[t] = b
        allocated += b

    # Budget-Korrektur (auf exakt detail_items_per_day)
    # Falls zu viele: schneide ab. Falls zu wenig: verteile Rest auf Topics mit Items.
    if allocated > detail_items_per_day:
        overflow = allocated - detail_items_per_day
        for t in sorted(topic_budget.keys(), key=lambda x: topic_budget[x], reverse=True):
            if overflow <= 0:
                break
            if topic_budget[t] > 0:
                take = min(topic_budget[t], overflow)
                topic_budget[t] -= take
                overflow -= take
    elif allocated < detail_items_per_day:
        missing = detail_items_per_day - allocated
        # verteile Rest auf Topics mit den meisten Items
        for t in sorted(active_topics, key=lambda x: len(by_topic[x]), reverse=True):
            if missing <= 0:
                break
            topic_budget[t] += 1
            missing -= 1

    # Round-robin pro Topic über Kanäle
    picked: List[Dict[str, Any]] = []
    picked_per_channel: Dict[str, int] = {}

    for topic in sorted(active_topics, key=lambda x: topic_budget[x], reverse=True):
        budget = topic_budget.get(topic, 0)
        if budget <= 0:
            continue

        # gruppiere Items dieses Topics pro Kanal
        per_channel: Dict[str, List[Dict[str, Any]]] = {}
        for it in sorted(by_topic[topic], key=lambda x: x["published_at"], reverse=True):
            per_channel.setdefault(it["channel"], []).append(it)

        channels_rr = list(per_channel.keys())
        idx = 0
        guard = 0
        while budget > 0 and channels_rr and guard < 10_000:
            guard += 1
            ch = channels_rr[idx % len(channels_rr)]
            idx += 1

            if picked_per_channel.get(ch, 0) >= detail_items_per_channel_max:
                continue

            if not per_channel.get(ch):
                continue

            it = per_channel[ch].pop(0)
            if it in picked:
                continue

            picked.append(it)
            picked_per_channel[ch] = picked_per_channel.get(ch, 0) + 1
            budget -= 1

    # Falls noch Slots übrig (z.B. wegen channel caps): fülle global mit neuesten Items
    if len(picked) < detail_items_per_day:
        remaining = [it for it in sorted(items, key=lambda x: x["published_at"], reverse=True) if it not in picked]
        for it in remaining:
            if len(picked) >= detail_items_per_day:
                break
            ch = it["channel"]
            if picked_per_channel.get(ch, 0) >= detail_items_per_channel_max:
                continue
            picked.append(it)
            picked_per_channel[ch] = picked_per_channel.get(ch, 0) + 1

    return picked[:detail_items_per_day]


def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--channels", default="data/channels.json")
    ap.add_argument("--hours", type=int, default=24)
    args = ap.parse_args()

    # Report-Parametrisierung (für Multi-Report-Läufe)
    report_key = (os.getenv("REPORT_KEY", "cyberlurch") or "cyberlurch").strip()
    report_title = (os.getenv("REPORT_TITLE", "The Cyberlurch Report") or "The Cyberlurch Report").strip()
    report_subject = (os.getenv("REPORT_SUBJECT", report_title) or report_title).strip()

    report_dir = (os.getenv("REPORT_DIR", "reports") or "reports").strip()
    send_empty_email = (os.getenv("SEND_EMPTY_REPORT_EMAIL", "1") or "1").strip()

    # Memory/State
    state_path = (os.getenv("STATE_PATH", "state/processed_items.json") or "state/processed_items.json").strip()
    retention_days = _safe_int("STATE_RETENTION_DAYS", 20)

    # Limits
    max_items_per_channel = _safe_int("MAX_ITEMS_PER_CHANNEL", 5)
    detail_items_per_day = _safe_int("DETAIL_ITEMS_PER_DAY", 8)
    detail_items_per_channel_max = _safe_int("DETAIL_ITEMS_PER_CHANNEL_MAX", 3)

    print("=== NewsAgent2 run ===")
    print(f"[config] report_key={report_key!r}")
    print(f"[config] report_title={report_title!r}")
    print(f"[config] report_subject={report_subject!r}")
    print(f"[config] channels_file={args.channels!r} hours={args.hours}")
    print(f"[config] report_dir={report_dir!r}")
    print(f"[config] limits: MAX_ITEMS_PER_CHANNEL={max_items_per_channel}, DETAIL_ITEMS_PER_DAY={detail_items_per_day}, DETAIL_ITEMS_PER_CHANNEL_MAX={detail_items_per_channel_max}")
    print(f"[state] path={state_path!r} retention_days={retention_days}")

    # Load/prune state
    state = load_state(state_path)
    state = prune_state(state, retention_days=retention_days)

    # Load channels
    try:
        channels, channel_topics, topic_weights = load_channels_config(args.channels)
    except Exception as e:
        print(f"[error] Failed to load channels config: {e!r}")
        overview = f"## Kurzüberblick\n\n**Fehler:** Konnte Channels-Konfiguration nicht laden: `{e!r}`\n"
        items: List[Dict[str, Any]] = []
        details_by_id: Dict[str, str] = {}
        os.makedirs(report_dir, exist_ok=True)
        out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
        md = to_markdown(items, overview, details_by_id, report_title=report_title)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"[report] Wrote {out_path}")

        if send_empty_email == "1":
            send_markdown(report_subject, md)
        else:
            print("[email] SEND_EMPTY_REPORT_EMAIL=0 -> not sending email.")
        return

    print(f"[channels] Loaded channels: {len(channels)}")

    # Collect items (YouTube)
    items: List[Dict[str, Any]] = []
    skipped_by_state = 0

    for ch in channels:
        cname = ch["name"]
        curl = ch["url"]

        try:
            vids = list_recent_videos(curl, hours=args.hours, max_items=max_items_per_channel)
        except Exception as e:
            print(f"[warn] list_recent_videos failed for {cname}: {e!r}")
            continue

        for v in vids:
            vid = v["id"]
            vurl = v["url"]
            vtitle = v.get("title") or ""
            item_state_key = make_item_key(
                report_key=report_key,
                source="youtube",
                item_id=vid,
                url=vurl,
                title=vtitle,
                channel=cname,
            )

            if is_processed(state, item_state_key):
                skipped_by_state += 1
                continue

            transcript = None
            try:
                transcript = fetch_transcript(vid)
            except Exception:
                transcript = None

            desc = (v.get("description") or "").strip()
            text = (transcript or desc).strip()

            if not text:
                print(f"[warn] No transcript/description -> skipping video {vid} ({cname})")
                continue

            published_at_sto = v["published_at"].astimezone(STO)

            items.append(
                {
                    "id": vid,
                    "title": vtitle,
                    "channel": cname,
                    "published_at": published_at_sto,
                    "url": vurl,
                    "text": text,
                    "_state_key": item_state_key,
                }
            )

    items = dedupe_items(items)

    print(f"[collect] Collected {len(items)} item(s).")
    if skipped_by_state:
        print(f"[memory] Skipped {skipped_by_state} already-processed item(s) for report_key={report_key!r}.")

    # Summarize
    if not items:
        overview = "## Kurzüberblick\n\nKeine neuen Inhalte in den letzten 24 Stunden."
        details_by_id = {}
    else:
        overview = summarize(items)

        # Details (themengewichtet)
        detail_candidates = choose_detail_items(
            items=items,
            channel_topics=channel_topics,
            topic_weights=topic_weights,
            detail_items_per_day=detail_items_per_day,
            detail_items_per_channel_max=detail_items_per_channel_max,
        )
        print(f"[details] Selected {len(detail_candidates)} item(s) for details.")

        details_by_id: Dict[str, str] = {}
        for it in detail_candidates:
            try:
                details_by_id[it["id"]] = summarize_item_detail(it)
            except Exception as e:
                print(f"[warn] Detail summarization failed for {it['id']}: {e!r}")

    # Build report markdown
    os.makedirs(report_dir, exist_ok=True)
    out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
    md = to_markdown(items, overview, details_by_id, report_title=report_title)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"[report] Wrote {out_path}")

    # Mark processed (only items we actually handled)
    now_utc_iso = datetime.now(tz=ZoneInfo("UTC")).isoformat()
    for it in items:
        k = it.get("_state_key")
        if k:
            mark_processed(state, k, now_utc_iso)

    save_state(state_path, state)

    # Email
    if not items and send_empty_email != "1":
        print("[email] No new items and SEND_EMPTY_REPORT_EMAIL=0 -> not sending email.")
    else:
        send_markdown(report_subject, md)


if __name__ == "__main__":
    main()
