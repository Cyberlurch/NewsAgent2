import argparse
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

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
...
    """
    import json

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    buckets = data.get("topic_buckets") or []
    if not isinstance(buckets, list):
        raise ValueError("channels.json: topic_buckets must be a list")

    channels: List[Dict[str, Any]] = []
    channel_topics: Dict[str, List[str]] = {}
    topic_weights: Dict[str, float] = {}

    for b in buckets:
        if not isinstance(b, dict):
            continue

        # FIX: allow both legacy 'topic' and new 'name'
        tname = (b.get("name") or b.get("topic") or "").strip()
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
    Round-Robin über Kanäle, aber Budgetverteilung über Topics via weights.

    - Items werden nach Topic gruppiert (Topic = erster Bucket des Channels)
    - Pro Topic wird ein Slotbudget vergeben, proportional zum Weight
    - Innerhalb eines Topics werden Kanäle round-robin ausgewählt
    - Pro Kanal gilt detail_items_per_channel_max
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
    # Erst: zu viele -> reduziere bei größten Budgets
    while allocated > detail_items_per_day:
        t = max(topic_budget.keys(), key=lambda x: topic_budget[x])
        if topic_budget[t] <= 0:
            break
        topic_budget[t] -= 1
        allocated -= 1

    # Dann: zu wenige -> fülle bei höchsten Weights nach
    while allocated < detail_items_per_day:
        t = max(weights.keys(), key=lambda x: weights[x])
        topic_budget[t] += 1
        allocated += 1

    selected: List[Dict[str, Any]] = []
    per_channel_count: Dict[str, int] = {}

    # Round robin innerhalb jedes Topics: Kanäle -> Items
    for topic, budget in topic_budget.items():
        if budget <= 0:
            continue

        topic_items = by_topic.get(topic, [])
        if not topic_items:
            continue

        # Items pro Kanal sammeln
        by_channel: Dict[str, List[Dict[str, Any]]] = {}
        for it in topic_items:
            by_channel.setdefault(it["channel"], []).append(it)

        channels_rr = list(by_channel.keys())
        idx = 0
        loops = 0

        while budget > 0 and channels_rr and loops < 10_000:
            loops += 1
            ch = channels_rr[idx % len(channels_rr)]
            idx += 1

            # Kanal-Limit
            if per_channel_count.get(ch, 0) >= detail_items_per_channel_max:
                # Kanal rausnehmen, wenn nichts mehr möglich
                channels_rr = [c for c in channels_rr if c != ch]
                continue

            lst = by_channel.get(ch) or []
            if not lst:
                channels_rr = [c for c in channels_rr if c != ch]
                continue

            # Nimm nächstes Item des Kanals
            it = lst.pop(0)
            selected.append(it)
            per_channel_count[ch] = per_channel_count.get(ch, 0) + 1
            budget -= 1

        if loops >= 10_000:
            print(f"[warn] Round-robin loop guard triggered for topic={topic!r}")

    # Falls wegen Limits nicht voll: global auffüllen (ohne Topic-Budget)
    if len(selected) < detail_items_per_day:
        remaining = [it for it in items if it not in selected]
        for it in remaining:
            if len(selected) >= detail_items_per_day:
                break
            ch = it["channel"]
            if per_channel_count.get(ch, 0) >= detail_items_per_channel_max:
                continue
            selected.append(it)
            per_channel_count[ch] = per_channel_count.get(ch, 0) + 1

    return selected[:detail_items_per_day]


def _clean_title(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


@dataclass
class RunConfig:
    report_key: str
    report_title: str
    report_subject: str
    channels_file: str
    hours: int
    report_dir: str
    send_email: bool
    email_to: str
    email_from: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    state_path: str
    retention_days: int
    max_items_per_channel: int
    detail_items_per_day: int
    detail_items_per_channel_max: int


def build_config(args: argparse.Namespace) -> RunConfig:
    report_key = os.getenv("REPORT_KEY", "cyberlurch").strip() or "cyberlurch"
    report_title = os.getenv("REPORT_TITLE", "The Cyberlurch Report").strip() or "The Cyberlurch Report"
    report_subject = os.getenv("REPORT_SUBJECT", report_title).strip() or report_title

    send_email = (os.getenv("SEND_EMAIL", "1").strip() != "0")
    email_to = os.getenv("EMAIL_TO", "").strip()
    email_from = os.getenv("EMAIL_FROM", "").strip()
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = _safe_int("SMTP_PORT", 587)
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_pass = os.getenv("SMTP_PASS", "").strip()

    state_path = os.getenv("STATE_PATH", "state/processed_items.json").strip() or "state/processed_items.json"
    retention_days = _safe_int("STATE_RETENTION_DAYS", 20)

    max_items_per_channel = _safe_int("MAX_ITEMS_PER_CHANNEL", 5)
    detail_items_per_day = _safe_int("DETAIL_ITEMS_PER_DAY", 8)
    detail_items_per_channel_max = _safe_int("DETAIL_ITEMS_PER_CHANNEL_MAX", 3)

    return RunConfig(
        report_key=report_key,
        report_title=report_title,
        report_subject=report_subject,
        channels_file=args.channels,
        hours=args.hours,
        report_dir=args.report_dir,
        send_email=send_email,
        email_to=email_to,
        email_from=email_from,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        state_path=state_path,
        retention_days=retention_days,
        max_items_per_channel=max_items_per_channel,
        detail_items_per_day=detail_items_per_day,
        detail_items_per_channel_max=detail_items_per_channel_max,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--channels", default="data/channels.json")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()

    cfg = build_config(args)

    print("=== NewsAgent2 run ===")
    print(f"[config] report_key={cfg.report_key!r}")
    print(f"[config] report_title={cfg.report_title!r}")
    print(f"[config] report_subject={cfg.report_subject!r}")
    print(f"[config] channels_file={cfg.channels_file!r} hours={cfg.hours}")
    print(f"[config] report_dir={cfg.report_dir!r}")
    print(
        "[config] limits: "
        f"MAX_ITEMS_PER_CHANNEL={cfg.max_items_per_channel}, "
        f"DETAIL_ITEMS_PER_DAY={cfg.detail_items_per_day}, "
        f"DETAIL_ITEMS_PER_CHANNEL_MAX={cfg.detail_items_per_channel_max}"
    )
    print(f"[state] path={cfg.state_path!r} retention_days={cfg.retention_days}")

    # Load/prune state
    state = load_state(cfg.state_path)
    state = prune_state(state, retention_days=cfg.retention_days)

    # Load channels
    try:
        channels, channel_topics, topic_weights = load_channels_config(cfg.channels_file)
    except Exception as e:
        print(f"[error] Failed to load channels config: {e!r}")
        raise

    print(f"[channels] Loaded channels: {len(channels)}")

    # Collect
    collected: List[Dict[str, Any]] = []
    for ch in channels:
        cname = ch["name"]
        url = ch["url"]

        try:
            vids = list_recent_videos(url=url, hours=cfg.hours, limit=cfg.max_items_per_channel)
        except Exception as e:
            print(f"[collect] ERROR channel={cname!r}: list_recent_videos failed: {e!r}")
            continue

        for v in vids:
            item_id = v.get("id") or ""
            title = _clean_title(v.get("title") or "")
            vurl = v.get("url") or ""

            item_state_key = make_item_key(
                report_key=cfg.report_key,
                source="youtube",
                item_id=item_id,
                url=vurl,
                title=title,
                channel=cname,
            )

            if is_processed(state, item_state_key):
                continue

            collected.append(
                {
                    "source": "youtube",
                    "channel": cname,
                    "title": title,
                    "url": vurl,
                    "id": item_id,
                    "_state_key": item_state_key,
                }
            )

    collected = dedupe_items(collected)
    print(f"[collect] Collected {len(collected)} item(s).")

    if not collected:
        # Write minimal report
        now_local = datetime.now(STO)
        header_ts = now_local.strftime("%Y-%m-%d %H:%M Uhr")
        report_md = f"{cfg.report_title}\n{header_ts}\n\nKeine neuen Inhalte in den letzten 24 Stunden.\n\nQuellen\n"
        os.makedirs(cfg.report_dir, exist_ok=True)
        out_path = os.path.join(cfg.report_dir, f"{cfg.report_key}_daily_summary_{now_local.strftime('%Y-%m-%d_%H-%M-%S')}.md")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(report_md)
        print(f"[report] Wrote {out_path}")

        # Save state (still prune timestamp)
        save_state(cfg.state_path, state)

        if cfg.send_email and cfg.email_to and cfg.email_from:
            try:
                send_markdown(
                    subject=cfg.report_subject,
                    md=report_md,
                    email_to=cfg.email_to,
                    email_from=cfg.email_from,
                    smtp_host=cfg.smtp_host,
                    smtp_port=cfg.smtp_port,
                    smtp_user=cfg.smtp_user,
                    smtp_pass=cfg.smtp_pass,
                )
                print("[email] Sent empty report email.")
            except Exception as e:
                print(f"[email] ERROR: failed to send email: {e!r}")
                raise
        return

    # Summarize overview + details selection
    # 1) Lightweight summary per item for overview building
    for it in collected:
        try:
            transcript = fetch_transcript(it["url"])
        except Exception as e:
            print(f"[collect] WARN: transcript fetch failed: {it['url']!r}: {e!r}")
            transcript = ""

        try:
            it["summary"] = summarize(transcript or it["title"])
        except Exception as e:
            print(f"[summarize] ERROR: summarize failed for {it['url']!r}: {e!r}")
            it["summary"] = ""

        it["transcript"] = transcript

    # 2) Detail items selection (topic-weighted, round-robin, channel caps)
    detail_items = choose_detail_items(
        items=collected,
        channel_topics=channel_topics,
        topic_weights=topic_weights,
        detail_items_per_day=cfg.detail_items_per_day,
        detail_items_per_channel_max=cfg.detail_items_per_channel_max,
    )

    # 3) Enrich detail items with a bigger summary
    for it in detail_items:
        try:
            it["detail"] = summarize_item_detail(it.get("transcript") or it.get("summary") or it.get("title") or "")
        except Exception as e:
            print(f"[summarize] ERROR: detail summarize failed for {it.get('url')!r}: {e!r}")
            it["detail"] = ""

    # Build report markdown
    now_local = datetime.now(STO)
    report_md = to_markdown(
        title=cfg.report_title,
        timestamp_local=now_local.strftime("%Y-%m-%d %H:%M Uhr"),
        items=collected,
        detail_items=detail_items,
    )

    # Write report
    os.makedirs(cfg.report_dir, exist_ok=True)
    out_path = os.path.join(cfg.report_dir, f"{cfg.report_key}_daily_summary_{now_local.strftime('%Y-%m-%d_%H-%M-%S')}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report_md)
    print(f"[report] Wrote {out_path}")

    # Update state for collected items (not just detail items)
    now_utc_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for it in collected:
        k = it.get("_state_key")
        if k:
            mark_processed(state, k, now_utc_iso)

    save_state(cfg.state_path, state)

    # Email
    if cfg.send_email and cfg.email_to and cfg.email_from:
        try:
            send_markdown(
                subject=cfg.report_subject,
                md=report_md,
                email_to=cfg.email_to,
                email_from=cfg.email_from,
                smtp_host=cfg.smtp_host,
                smtp_port=cfg.smtp_port,
                smtp_user=cfg.smtp_user,
                smtp_pass=cfg.smtp_pass,
            )
            print("[email] Sent report email.")
        except Exception as e:
            print(f"[email] ERROR: failed to send email: {e!r}")
            raise


if __name__ == "__main__":
    main()
