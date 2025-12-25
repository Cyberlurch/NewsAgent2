from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from .collector_foamed import collect_foamed_items
from .collectors_youtube import fetch_transcript, list_recent_videos
from .collectors_pubmed import search_recent_pubmed
from .emailer import send_markdown
from .rollups import (
    derive_monthly_summary,
    load_rollups_state,
    render_yearly_markdown,
    prune_rollups,
    rollups_for_year,
    save_rollups_state,
    upsert_monthly_rollup,
)
from .reporter import to_markdown
from .state_manager import (
    is_processed,
    mark_screened,
    mark_sent,
    load_state,
    mark_processed,
    prune_state,
    save_state,
    should_skip_pubmed_item,
)
from .summarizer import (
    summarize,
    summarize_item_detail,
    summarize_pubmed_bottom_line,
    summarize_foamed_bottom_line,
)

STO = ZoneInfo("Europe/Stockholm")

CYBERMED_WEEKLY_MAX_PUBMED = 10
CYBERMED_MONTHLY_MAX_PUBMED = 8
CYBERMED_WEEKLY_MAX_FOAMED = 8
CYBERMED_MONTHLY_MAX_FOAMED = 6
CYBERMED_WEEKLY_MAX_FOAMED_CANDIDATES = 15
CYBERMED_MONTHLY_MAX_FOAMED_CANDIDATES = 12
CYBERLURCH_WEEKLY_MAX_VIDEOS = 10
CYBERLURCH_MONTHLY_MAX_VIDEOS = 8
WEEKLY_MAX_DEEP_DIVES = 3
MONTHLY_MAX_DEEP_DIVES = 2


def _parse_iso_utc(value: str | None) -> datetime | None:
    text = (value or "").strip()
    if text == "":
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        try:
            dt = dt.astimezone(timezone.utc)
        except Exception:
            return None

    return dt


def _is_cybermed(report_key: str, report_profile: str) -> bool:
    """Cybermed detection must be stable and avoid touching Cyberlurch logic."""
    rk = (report_key or "").strip().lower()
    rp = (report_profile or "").strip().lower()
    return rk == "cybermed" or rp == "medical"


def _determine_year_in_review_year(*, now_sto: datetime, override_year: str | None, event_name: str) -> int:
    override = (override_year or "").strip()
    if override:
        try:
            return int(override)
        except Exception:
            print(f"[yearly] WARN: invalid YEAR_IN_REVIEW_YEAR={override!r} -> ignoring override")

    event = (event_name or "").strip().lower()
    is_jan1 = now_sto.month == 1 and now_sto.day == 1
    is_scheduled_jan1 = event == "schedule" and is_jan1

    if is_jan1 or is_scheduled_jan1:
        return now_sto.year - 1

    return now_sto.year


def _date_yyyymmdd_utc(dt: datetime) -> str:
    """PubMed accepts YYYY/MM/DD; current collector uses UTC date boundaries."""
    return dt.astimezone(timezone.utc).strftime("%Y/%m/%d")


def _safe_int(env_name: str, default: int) -> int:
    raw = (os.getenv(env_name, "") or "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        print(f"[warn] Invalid int in {env_name}={raw!r} -> using default {default}")
        return default


def _parse_hours_override(raw: str) -> int | None:
    text = (raw or "").strip()
    if text == "":
        return None
    try:
        val = int(float(text))
        if val <= 0:
            raise ValueError("must be positive")
        return val
    except Exception:
        print(f"[warn] Invalid LOOKBACK_HOURS_OVERRIDE={text!r} -> ignoring override")
        return None


def load_channels_config(path: str) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]], Dict[str, float]]:
    """
    Reads channels JSON and flattens the channels list.

    Tolerant format:

    {
      "topic_buckets": [
        {
          "name": "..."   # or "topic": "..."
          "weight": 1.0,  # optional
          "channels": [
            {"name": "...", "url": "..."}                       # default: youtube
            {"name": "...", "source": "pubmed", "query": "..."} # pubmed
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
    channel_topics: Dict[str, List[str]] = {}
    topic_weights: Dict[str, float] = {}

    for b in buckets:
        if not isinstance(b, dict):
            continue

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
            source = (c.get("source") or "").strip().lower() or None
            curl = (c.get("url") or "").strip()
            query = (c.get("query") or "").strip()

            if not cname:
                continue

            if not source:
                if "pubmed.ncbi.nlm.nih.gov" in curl or "eutils.ncbi.nlm.nih.gov" in curl:
                    source = "pubmed"
                else:
                    source = "youtube"

            if source == "youtube":
                if not curl:
                    continue
                channels.append({"name": cname, "source": "youtube", "url": curl})

            elif source == "pubmed":
                if not query and curl:
                    query = curl
                if not query:
                    continue
                channels.append({"name": cname, "source": "pubmed", "query": query, "url": curl})

            else:
                continue

            channel_topics.setdefault(cname, []).append(tname)

    channels.sort(key=lambda x: (x.get("source") or "", x.get("name") or "", x.get("url") or "", x.get("query") or ""))
    return channels, channel_topics, topic_weights


def load_foamed_sources_config(path: str) -> List[Dict[str, Any]]:
    """
    Load FOAMed/blog sources from JSON. Returns an empty list on errors for safety.
    """

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    except FileNotFoundError:
        print(f"[foamed] WARN: sources config not found at {path!r}")
    except Exception as e:
        print(f"[foamed] WARN: failed to load sources config {path!r}: {e!r}")
    return []


def _dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        src = (it.get("source") or "").strip().lower() or "youtube"
        iid = str(it.get("id") or "").strip()
        if not iid:
            continue
        key = f"{src}:{iid}"
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def _curate_top_items(
    items: List[Dict[str, Any]],
    max_n: int,
    *,
    score_key: str = "cybermed_score",
    top_pick_key: str = "top_pick",
) -> List[Dict[str, Any]]:
    if max_n <= 0:
        return []

    def _sort_key(it: Dict[str, Any]) -> tuple[Any, ...]:
        score_val = float(it.get(score_key) or 0.0)
        ts = it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc)
        return (
            1 if it.get(top_pick_key) else 0,
            score_val,
            ts,
        )

    return sorted(items, key=_sort_key, reverse=True)[:max_n]


def _mode_deep_dive_cap(report_mode: str, base_cap: int) -> int:
    if report_mode == "weekly":
        return min(base_cap, WEEKLY_MAX_DEEP_DIVES)
    if report_mode == "monthly":
        return min(base_cap, MONTHLY_MAX_DEEP_DIVES)
    if report_mode == "yearly":
        return 0
    return base_cap


def _foamed_candidate_cap(report_mode: str) -> int:
    if report_mode == "weekly":
        return CYBERMED_WEEKLY_MAX_FOAMED_CANDIDATES
    if report_mode == "monthly":
        return CYBERMED_MONTHLY_MAX_FOAMED_CANDIDATES
    return 40


def _trim_foamed_overview(items: List[Dict[str, Any]], report_mode: str) -> List[Dict[str, Any]]:
    if report_mode == "weekly":
        return _curate_top_items(items, CYBERMED_WEEKLY_MAX_FOAMED, score_key="foamed_score")
    if report_mode == "monthly":
        return _curate_top_items(items, CYBERMED_MONTHLY_MAX_FOAMED, score_key="foamed_score")
    return items


def _curate_cyberlurch_overview(
    items_sorted: List[Dict[str, Any]],
    report_mode: str,
    overview_items_max: int,
) -> List[Dict[str, Any]]:
    cap = overview_items_max
    if report_mode == "weekly":
        cap = min(cap, CYBERLURCH_WEEKLY_MAX_VIDEOS)
    elif report_mode == "monthly":
        cap = min(cap, CYBERLURCH_MONTHLY_MAX_VIDEOS)

    if report_mode not in {"weekly", "monthly"}:
        return items_sorted[: max(1, cap)]

    return _curate_top_items(items_sorted, max(1, cap), score_key="score")


def _allocate_detail_slots_by_topic(
    items: List[Dict[str, Any]],
    channel_topics: Dict[str, List[str]],
    topic_weights: Dict[str, float],
    total_slots: int,
) -> Dict[str, int]:
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
                    if any((x.get("source"), x.get("id")) == (it.get("source"), it.get("id")) for x in chosen):
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
            if any((x.get("source"), x.get("id")) == (it.get("source"), it.get("id")) for x in chosen):
                continue
            ch = (it.get("channel") or "").strip()
            if per_ch.get(ch, 0) >= detail_items_per_channel_max:
                continue
            chosen.append(it)
            per_ch[ch] = per_ch.get(ch, 0) + 1

    return chosen[:detail_items_per_day]


def _apply_prune_state_compat(state: Dict[str, Any], retention_days: int) -> Dict[str, Any]:
    try:
        res = prune_state(state, retention_days=retention_days)

        if isinstance(res, dict):
            return res

        if isinstance(res, tuple):
            if len(res) == 2 and all(isinstance(x, int) for x in res):
                removed_age, removed_cap = res
                if removed_age or removed_cap:
                    print(f"[state] pruned: removed_by_age={removed_age} removed_by_cap={removed_cap}")
                return state

            if len(res) == 3 and isinstance(res[0], dict):
                new_state = res[0]
                removed_age = res[1] if isinstance(res[1], int) else 0
                removed_cap = res[2] if isinstance(res[2], int) else 0
                if removed_age or removed_cap:
                    print(f"[state] pruned: removed_by_age={removed_age} removed_by_cap={removed_cap}")
                return new_state

            print(f"[state] WARN: prune_state returned unexpected tuple: len={len(res)} types={[type(x).__name__ for x in res]}")
            return state

        print(f"[state] WARN: prune_state returned unexpected type: {type(res)!r}")
        return state

    except Exception as e:
        print(f"[state] WARN: prune_state failed (continuing): {e!r}")
        return state


def _rollup_items_for_month(
    overview_items: List[Dict[str, Any]],
    detail_items: List[Dict[str, Any]],
    foamed_overview_items: List[Dict[str, Any]],
    *,
    max_items: int = 15,
) -> List[Dict[str, Any]]:
    candidates = []
    seen_keys: set[tuple[str, str]] = set()

    def _add(items: List[Dict[str, Any]]) -> None:
        for it in items:
            title = (it.get("title") or "").strip()
            url = (it.get("url") or "").strip()
            source = (it.get("source") or "").strip()
            channel = (it.get("channel") or "").strip()
            if not title and not url:
                continue
            key = (url or title, str(it.get("published_at") or ""))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            published = it.get("published_at")
            candidates.append(
                {
                    "title": title,
                    "url": url,
                    "channel": channel,
                    "source": source,
                    "published_at": published,
                    "top_pick": bool(it.get("top_pick")),
                }
            )

    _add(overview_items)
    _add(detail_items)
    _add(foamed_overview_items)

    def _sort_key(it: Dict[str, Any]) -> tuple[int, datetime]:
        ts_raw = it.get("published_at")
        ts = ts_raw if isinstance(ts_raw, datetime) else datetime.min.replace(tzinfo=timezone.utc)
        return (1 if it.get("top_pick") else 0, ts)

    sorted_items = sorted(candidates, key=_sort_key, reverse=True)
    out: List[Dict[str, Any]] = []
    for it in sorted_items:
        if len(out) >= max_items:
            break
        out.append(
            {
                "title": it.get("title") or "",
                "url": it.get("url") or "",
                "channel": it.get("channel") or "",
                "source": it.get("source") or "",
                "top_pick": bool(it.get("top_pick")),
                "date": (
                    it["published_at"].astimezone(timezone.utc).strftime("%Y-%m-%d")
                    if isinstance(it.get("published_at"), datetime)
                    else ""
                ),
            }
        )
    return out


def _run_yearly_report(
    *,
    rollups_state_path: str,
    report_key: str,
    base_report_title: str,
    base_report_subject: str,
    report_language: str,
    report_dir: str,
) -> None:
    os.makedirs(report_dir, exist_ok=True)
    rollups_state = load_rollups_state(rollups_state_path)
    now_sto = datetime.now(tz=STO)
    event_name = (os.getenv("GITHUB_EVENT_NAME", "") or "").strip().lower()
    target_year = _determine_year_in_review_year(
        now_sto=now_sto,
        override_year=os.getenv("YEAR_IN_REVIEW_YEAR"),
        event_name=event_name,
    )

    report_title = f"The Cyberlurch Year in Review — {target_year}"
    report_subject = report_title
    if _is_cybermed(report_key, os.getenv("REPORT_PROFILE", "")):
        report_title = f"The Cybermed Year in Review — {target_year}"
        report_subject = report_title
    elif "cybermed" in base_report_title.lower():
        report_title = f"The Cybermed Year in Review — {target_year}"
        report_subject = report_title
    elif "cyberlurch" in base_report_title.lower():
        report_title = f"The Cyberlurch Year in Review — {target_year}"
        report_subject = report_title
    else:
        report_title = f"{base_report_title} — Year in Review {target_year}"
        report_subject = report_title

    entries = rollups_for_year(rollups_state, report_key, target_year)
    if not entries and event_name == "schedule":
        print(f"[email] Scheduled yearly run found no rollups for {target_year} -> skipping email.")
        return

    md = render_yearly_markdown(
        report_title=report_title,
        report_language=report_language,
        year=target_year,
        rollups=entries,
    )

    out_path = os.path.join(report_dir, f"{report_key}_yearly_review_{target_year}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"[report] Wrote yearly report to {out_path}")

    try:
        send_markdown(report_subject, md)
    except Exception as e:
        print(f"[email] WARN: failed to send yearly email (report was generated): {e!r}")


def _update_state_after_run(
    *,
    state_path: str,
    state: Dict[str, Any],
    items_all_new: List[Dict[str, Any]],
    overview_items: List[Dict[str, Any]],
    detail_items: List[Dict[str, Any]],
    foamed_overview_items: List[Dict[str, Any]],
    report_key: str,
    report_mode: str,
    now_utc_iso: str,
    read_only: bool,
) -> None:
    if read_only:
        print("[state] Read-only mode -> skipping state write and processed markers.")
        return

    overview_pubmed_ids = {
        str(it.get("id") or "").strip()
        for it in overview_items
        if (it.get("source") or "").strip().lower() == "pubmed"
    }
    deep_dive_pubmed_ids = {
        str(it.get("id") or "").strip()
        for it in detail_items
        if (it.get("source") or "").strip().lower() == "pubmed"
    }
    foamed_overview_ids = {str(it.get("id") or "").strip() for it in foamed_overview_items}

    for it in items_all_new:
        src = (it.get("source") or "").strip().lower() or "youtube"
        iid = str(it.get("id") or "").strip()
        if not iid:
            continue
        try:
            meta = {
                "title": it.get("title"),
                "channel": it.get("channel"),
                "url": it.get("url"),
                "processed_at_utc": now_utc_iso,
            }
            if src == "pubmed":
                meta["screened_at_utc"] = meta.get("screened_at_utc") or now_utc_iso
                sent_overview = iid in overview_pubmed_ids
                sent_deep = iid in deep_dive_pubmed_ids
                if sent_overview or sent_deep:
                    mark_sent(
                        state,
                        report_key,
                        src,
                        iid,
                        sent_overview=sent_overview,
                        sent_deep_dive=sent_deep,
                        meta=meta,
                        when_utc=now_utc_iso,
                    )
                else:
                    mark_screened(state, report_key, src, iid, meta=meta)
            elif src == "foamed":
                if iid in foamed_overview_ids:
                    mark_processed(state, report_key, src, iid, meta=meta)
            else:
                mark_processed(state, report_key, src, iid, meta=meta)
        except Exception as e:
            print(f"[state] WARN: mark_processed failed for {src}:{iid!r}: {e!r}")

    if report_mode == "daily":
        state["last_successful_daily_run_utc"] = now_utc_iso

    try:
        save_state(state_path, state)
    except Exception as e:
        print(f"[state] WARN: failed to save state: {e!r}")


def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser(description="NewsAgent2 daily report")
    ap.add_argument("--channels", default="data/channels.json", help="Path to channels config JSON")
    ap.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    args = ap.parse_args()

    report_mode_raw = (os.getenv("REPORT_MODE", "daily") or "daily").strip().lower()
    report_mode = report_mode_raw if report_mode_raw in {"daily", "weekly", "monthly", "yearly"} else "daily"
    lookback_override = _parse_hours_override(os.getenv("LOOKBACK_HOURS_OVERRIDE", ""))
    read_only_mode = report_mode in {"weekly", "monthly", "yearly"}

    report_key = (os.getenv("REPORT_KEY", "cyberlurch") or "cyberlurch").strip()
    base_report_title = (os.getenv("REPORT_TITLE", "The Cyberlurch Report") or "The Cyberlurch Report").strip()
    base_report_subject = (os.getenv("REPORT_SUBJECT", base_report_title) or base_report_title).strip()
    report_title = base_report_title
    report_subject = base_report_subject
    report_dir = (os.getenv("REPORT_DIR", "reports") or "reports").strip()

    report_language = (os.getenv("REPORT_LANGUAGE", "de") or "de").strip()
    report_profile = (os.getenv("REPORT_PROFILE", "general") or "general").strip()
    os.makedirs(report_dir, exist_ok=True)

    send_empty_email = (os.getenv("SEND_EMPTY_REPORT_EMAIL", "1") or "1").strip()

    max_items_per_channel = _safe_int("MAX_ITEMS_PER_CHANNEL", 5)
    detail_items_per_day = _safe_int("DETAIL_ITEMS_PER_DAY", 8)
    detail_items_per_channel_max = _safe_int("DETAIL_ITEMS_PER_CHANNEL_MAX", 3)

    overview_items_max = _safe_int("OVERVIEW_ITEMS_MAX", 25)
    sent_cooldown_hours = _safe_int("PUBMED_SENT_COOLDOWN_HOURS", 48)
    reconsider_unsent_hours = _safe_int("RECONSIDER_UNSENT_HOURS", 36)
    max_text_chars_per_item = _safe_int("MAX_TEXT_CHARS_PER_ITEM", 12000)

    state_path = (os.getenv("STATE_PATH", "state/processed_items.json") or "state/processed_items.json").strip()
    rollups_state_path = (os.getenv("ROLLUPS_STATE_PATH", "state/rollups.json") or "state/rollups.json").strip()
    retention_days = _safe_int("STATE_RETENTION_DAYS", 20)
    rollups_max_months = _safe_int("ROLLUPS_MAX_MONTHS", 24)
    foamed_sources_path = (os.getenv("CYBERMED_FOAMED_SOURCES", "data/cybermed_foamed_sources.json") or "data/cybermed_foamed_sources.json").strip()

    if report_mode == "weekly":
        report_title = f"{base_report_title} — Weekly"
        report_subject = f"{base_report_subject} — Weekly"
    elif report_mode == "monthly":
        report_title = f"{base_report_title} — Monthly"
        report_subject = f"{base_report_subject} — Monthly"
    elif report_mode == "yearly":
        report_title = f"{base_report_title} — Year in Review"
        report_subject = f"{base_report_subject} — Year in Review"
    else:
        report_title = base_report_title
        report_subject = base_report_subject

    state = load_state(state_path)
    state = _apply_prune_state_compat(state, retention_days=retention_days)

    if report_mode == "yearly":
        _run_yearly_report(
            rollups_state_path=rollups_state_path,
            report_key=report_key,
            base_report_title=base_report_title,
            base_report_subject=base_report_subject,
            report_language=report_language,
            report_dir=report_dir,
        )
        return

    last_successful_daily_iso = str(state.get("last_successful_daily_run_utc") or state.get("last_successful_run_utc") or "")
    last_successful_daily = _parse_iso_utc(last_successful_daily_iso)

    effective_hours = args.hours
    if lookback_override is not None:
        effective_hours = lookback_override
    elif report_mode == "weekly":
        effective_hours = 168
    elif report_mode == "monthly":
        effective_hours = 720
    else:
        now_sto = datetime.now(tz=STO)
        if now_sto.weekday() == 0:
            if last_successful_daily:
                hours_since = max(
                    0,
                    int((datetime.now(timezone.utc) - last_successful_daily).total_seconds() // 3600) + 1,
                )
                effective_hours = max(args.hours, min(hours_since or args.hours, 72))
            else:
                effective_hours = max(args.hours, 72)

    args.hours = effective_hours

    print("=== NewsAgent2 run ===")
    print(f"[config] report_mode={report_mode} read_only={read_only_mode}")
    print(f"[config] report_key={report_key!r}")
    print(f"[config] report_title={report_title!r}")
    print(f"[config] report_subject={report_subject!r}")
    print(f"[config] channels_file={args.channels!r} hours={args.hours} (override={lookback_override is not None})")
    print(f"[config] report_dir={report_dir!r}")
    print(f"[config] report_language={report_language!r} report_profile={report_profile!r}")
    print(f"[config] limits: MAX_ITEMS_PER_CHANNEL={max_items_per_channel}, DETAIL_ITEMS_PER_DAY={detail_items_per_day}, DETAIL_ITEMS_PER_CHANNEL_MAX={detail_items_per_channel_max}")
    print(f"[config] overview_items_max={overview_items_max}, max_text_chars_per_item={max_text_chars_per_item}")
    print(f"[config] pubmed_sent_cooldown_hours={sent_cooldown_hours}, reconsider_unsent_hours={reconsider_unsent_hours}")
    print(f"[state] path={state_path!r} retention_days={retention_days}")
    print(f"[rollups] path={rollups_state_path!r} max_months={rollups_max_months}")
    if _is_cybermed(report_key, report_profile):
        print(f"[foamed] sources_config={foamed_sources_path!r}")

    selection_cfg: Dict[str, Any] = {}
    if _is_cybermed(report_key, report_profile):
        try:
            from .selector_medical import load_cybermed_selection_config

            selection_cfg = load_cybermed_selection_config()
            sel_section = selection_cfg.get("selection", {}) if isinstance(selection_cfg.get("selection"), dict) else {}
            reconsider_unsent_hours = int(sel_section.get("reconsider_unsent_hours", reconsider_unsent_hours) or reconsider_unsent_hours)
            overview_items_max = int(sel_section.get("overview_max_per_run", overview_items_max) or overview_items_max)
            detail_items_per_day = int(sel_section.get("deep_dive_max_per_run", detail_items_per_day) or detail_items_per_day)
        except Exception as e:
            print(f"[config] WARN: failed to load Cybermed selection config for run-time tuning: {e!r}")

    if report_mode == "weekly":
        overview_items_max = min(overview_items_max, 14)
        detail_items_per_day = min(detail_items_per_day, 4)
        detail_items_per_channel_max = min(detail_items_per_channel_max, 2)
    elif report_mode == "monthly":
        overview_items_max = min(overview_items_max, 10)
        detail_items_per_day = min(detail_items_per_day, 3)
        detail_items_per_channel_max = min(detail_items_per_channel_max, 2)

    deep_dive_limit = _mode_deep_dive_cap(report_mode, detail_items_per_day)

    foamed_sources: List[Dict[str, Any]] = []
    foamed_candidates: List[Dict[str, Any]] = []
    foamed_screened_total = 0
    foamed_after_state = 0
    foamed_skipped_by_state = 0
    foamed_selection_stats: Dict[str, Any] = {}
    foamed_overview_items: List[Dict[str, Any]] = []
    foamed_top_picks: List[Dict[str, Any]] = []
    foamed_meta_stats: Dict[str, Any] = {}
    foamed_collection_stats: Dict[str, Any] = {}

    try:
        channels, channel_topics, topic_weights = load_channels_config(args.channels)
    except Exception as e:
        if report_language.lower().startswith("en"):
            overview = f"## Executive Summary\n\n**Error:** Failed to load channels configuration: `{e!r}`\n"
        else:
            overview = f"## Kurzüberblick\n\n**Fehler:** Konnte Channels-Konfiguration nicht laden: `{e!r}`\n"
        out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
        md = to_markdown(
            [], overview, {}, report_title=report_title, report_language=report_language, report_mode=report_mode
        )
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

        if not read_only_mode:
            try:
                save_state(state_path, state)
            except Exception as ee:
                print(f"[state] WARN: failed to save state after config error: {ee!r}")
        return

    print(f"[channels] Loaded channels: {len(channels)}")

    items: List[Dict[str, Any]] = []
    skipped_by_state = 0

    # Cybermed observability (used to build an in-report transparency header).
    is_cybermed_run = _is_cybermed(report_key, report_profile)
    pubmed_candidates_total = 0
    pubmed_skipped_by_state = 0
    pubmed_query_failures = 0
    pubmed_candidates_by_channel: Dict[str, int] = {}
    pubmed_queries_used: List[Tuple[str, str]] = []  # (channel_name, term)
    pubmed_state_skip_reasons: Dict[str, int] = {}

    for ch in channels:
        cname = ch["name"]
        source = (ch.get("source") or "youtube").strip().lower()
        curl = (ch.get("url") or "").strip()
        query = (ch.get("query") or "").strip()

        if source == "youtube":
            try:
                vids = list_recent_videos(curl, hours=args.hours, max_items=max_items_per_channel)
            except Exception as e:
                print(f"[collect] ERROR source=youtube channel={cname!r}: list_recent_videos failed: {e!r}")
                continue

            for v in vids:
                vid = str(v.get("id") or "").strip()
                if not vid:
                    continue

                if not read_only_mode and is_processed(state, report_key, "youtube", vid):
                    skipped_by_state += 1
                    continue

                desc = (v.get("description") or "").strip()
                transcript = None
                try:
                    transcript = fetch_transcript(vid)
                except Exception as e:
                    print(f"[collect] WARN source=youtube channel={cname!r} video={vid!r}: fetch_transcript failed: {e!r}")
                    transcript = None

                text = (transcript or desc).strip()
                if not text:
                    print(f"[collect] WARN source=youtube channel={cname!r} video={vid!r}: no transcript/description -> skipping")
                    continue

                if len(text) > max_text_chars_per_item:
                    text = text[:max_text_chars_per_item].rstrip()

                items.append(
                    {
                        "source": "youtube",
                        "id": vid,
                        "channel": cname,
                        "title": (v.get("title") or "").strip(),
                        "url": (v.get("url") or "").strip(),
                        "published_at": v.get("published_at"),
                        "description": desc,
                        "text": text,
                    }
                )

        elif source == "pubmed":
            if is_cybermed_run:
                pubmed_queries_used.append((cname, query))
            try:
                arts = search_recent_pubmed(term=query, hours=args.hours, max_items=max_items_per_channel)
            except Exception as e:
                print(f"[collect] ERROR source=pubmed channel={cname!r}: search_recent_pubmed failed: {e!r}")
                if is_cybermed_run:
                    pubmed_query_failures += 1
                continue

            if is_cybermed_run:
                pubmed_candidates_total += len(arts)
                pubmed_candidates_by_channel[cname] = len(arts)

            for a in arts:
                pmid = str(a.get("id") or "").strip()
                if not pmid:
                    continue

                skip_by_state = False
                if not read_only_mode:
                    if is_cybermed_run:
                        skip_by_state, skip_reason = should_skip_pubmed_item(
                            state,
                            report_key,
                            pmid,
                            overview_cooldown_hours=sent_cooldown_hours,
                            reconsider_unsent_hours=reconsider_unsent_hours,
                        )
                        if skip_by_state:
                            pubmed_state_skip_reasons[skip_reason] = pubmed_state_skip_reasons.get(skip_reason, 0) + 1
                    else:
                        skip_by_state = is_processed(state, report_key, "pubmed", pmid)

                if skip_by_state:
                    skipped_by_state += 1
                    if is_cybermed_run:
                        pubmed_skipped_by_state += 1
                    continue

                text = (a.get("text") or "").strip()
                if not text:
                    continue

                if len(text) > max_text_chars_per_item:
                    text = text[:max_text_chars_per_item].rstrip()

                items.append(
                    {
                        "source": "pubmed",
                        "id": pmid,
                        "channel": cname,
                        "title": (a.get("title") or "").strip(),
                        "url": (a.get("url") or "").strip() or curl,
                        "published_at": a.get("published_at"),
                        "year": (a.get("published_at").year if a.get("published_at") else ""),
                        "journal": (a.get("journal") or "").strip(),
                        "journal_iso_abbrev": (a.get("journal_iso_abbrev") or "").strip(),
                        "journal_medline_ta": (a.get("journal_medline_ta") or "").strip(),
                        "doi": (a.get("doi") or "").strip(),
                        "description": (a.get("journal") or "").strip(),
                        "pmid": pmid,
                        "text": text,
                    }
                )

        else:
            print(f"[collect] WARN: unknown source={source!r} for channel={cname!r} -> skipping")
            continue

    if is_cybermed_run:
        foamed_sources = load_foamed_sources_config(foamed_sources_path)
        if foamed_sources:
            now_utc = datetime.now(timezone.utc)
            foamed_collected, foamed_collection_stats = collect_foamed_items(foamed_sources, now_utc, lookback_hours=args.hours)
            foamed_screened_total = len(foamed_collected)

            for it in foamed_collected:
                iid = str(it.get("id") or it.get("url") or "").strip()
                if not iid:
                    continue
                if not read_only_mode and is_processed(state, report_key, "foamed", iid):
                    foamed_skipped_by_state += 1
                    continue

                text_val = (it.get("text") or "").strip()
                if len(text_val) > max_text_chars_per_item:
                    text_val = text_val[:max_text_chars_per_item].rstrip()
                    it["text"] = text_val

                foamed_candidates.append(it)

            foamed_after_state = len(foamed_candidates)
            items.extend(foamed_candidates)
        else:
            print("[foamed] WARN: no FOAMed sources configured; skipping FOAMed collection.")

    items = _dedupe_items(items)
    items_all_new = list(items)
    print(f"[collect] Collected {len(items_all_new)} new unique item(s). (skipped_by_state={skipped_by_state})")

    # Cybermed selection policy (PubMed only): select a subset for inclusion in the report,
    # while still marking all newly screened items as processed for memory.
    selection_stats: Dict[str, Any] = {}
    pubmed_new_items: List[Dict[str, Any]] = []
    pubmed_overview_items: List[Dict[str, Any]] = []
    pubmed_deep_dive_items: List[Dict[str, Any]] = []

    selection_result = None

    if is_cybermed_run:
        non_pubmed_items: List[Dict[str, Any]] = [
            it for it in items_all_new if (it.get("source") or "").strip().lower() != "pubmed"
        ]
        foamed_new_items = [it for it in items_all_new if (it.get("source") or "").strip().lower() == "foamed"]
        non_pubmed_nonfoamed = [it for it in non_pubmed_items if (it.get("source") or "").strip().lower() != "foamed"]
        pubmed_new_items = [it for it in items_all_new if (it.get("source") or "").strip().lower() == "pubmed"]
        pubmed_overview_items = list(pubmed_new_items)
        pubmed_deep_dive_items = list(pubmed_new_items)

        try:
            from .selector_medical import select_cybermed_pubmed_items  # optional module

            sel_res = select_cybermed_pubmed_items(pubmed_new_items)
            pubmed_overview_items = list(getattr(sel_res, "overview_items", getattr(sel_res, "selected", pubmed_new_items)))
            pubmed_deep_dive_items = list(
                getattr(sel_res, "deep_dive_items", getattr(sel_res, "deep_dives", pubmed_new_items))
            )
            selection_result = sel_res
            selection_stats = dict(getattr(sel_res, "stats", {}) or {})
        except Exception as e:
            print(f"[select] WARN: Cybermed selector unavailable/failed; using pass-through selection. err={e!r}")
            selection_stats = {"enabled": False, "error": "selector_failed"}

        if pubmed_state_skip_reasons:
            selection_stats["state_skip_reasons"] = dict(pubmed_state_skip_reasons)
            selection_stats["state_skip_total"] = skipped_by_state

        try:
            from .selector_medical import select_cybermed_foamed_items

            if foamed_new_items:
                foamed_sel = select_cybermed_foamed_items(
                    foamed_new_items, max_overview=_foamed_candidate_cap(report_mode)
                )
                foamed_overview_items = list(getattr(foamed_sel, "overview_items", foamed_new_items))
                foamed_top_picks = list(getattr(foamed_sel, "top_picks", []))
                foamed_selection_stats = dict(getattr(foamed_sel, "stats", {}) or {})
            else:
                foamed_selection_stats = {"screened_candidates": 0, "included_overview": 0, "top_picks": 0}
        except Exception as e:
            print(f"[select] WARN: FOAMed selector failed; using pass-through selection. err={e!r}")
            foamed_overview_items = list(foamed_new_items)
            foamed_selection_stats = {"enabled": False, "error": "foamed_selector_failed", "screened_candidates": len(foamed_new_items), "included_overview": len(foamed_new_items), "top_picks": len([it for it in foamed_new_items if it.get("top_pick")])}

        if foamed_selection_stats is not None:
            foamed_selection_stats.setdefault("screened_candidates", len(foamed_new_items))
            foamed_selection_stats.setdefault("included_overview", len(foamed_overview_items))
            foamed_selection_stats.setdefault("top_picks", len([it for it in foamed_overview_items if it.get("top_pick")]))

        foamed_overview_items = _trim_foamed_overview(foamed_overview_items, report_mode)

        foamed_top_picks = []
        if foamed_overview_items and not any(it.get("top_pick") for it in foamed_overview_items):
            for it in foamed_overview_items[:2]:
                it["top_pick"] = True
                foamed_top_picks.append(it)
        else:
            foamed_top_picks = [it for it in foamed_overview_items if it.get("top_pick")]

        if isinstance(foamed_selection_stats, dict):
            foamed_selection_stats["top_picks"] = len(foamed_top_picks)

        items = list(non_pubmed_nonfoamed) + foamed_overview_items + pubmed_overview_items
        print(f"[select] Cybermed selected {len(pubmed_overview_items)} of {len(pubmed_new_items)} new PubMed item(s) for the report.")
        print(f"[select] Cybermed included {len(foamed_overview_items)} FOAMed item(s) ({len(foamed_top_picks)} top picks).")

        foamed_meta_stats = {
            "screened": foamed_screened_total,
            "after_state": foamed_after_state,
            "included_overview": len(foamed_overview_items),
            "top_picks": len(foamed_top_picks),
            "selection": foamed_selection_stats,
            **foamed_collection_stats,
        }
        for k in (
            "sources_total",
            "sources_ok",
            "sources_failed",
            "items_raw",
            "items_with_date",
            "items_date_unknown",
            "kept_last24h",
        ):
            foamed_meta_stats.setdefault(k, 0)
        foamed_meta_stats.setdefault("per_source", {})

        if report_mode in {"weekly", "monthly"}:
            pubmed_cap = (
                CYBERMED_WEEKLY_MAX_PUBMED if report_mode == "weekly" else CYBERMED_MONTHLY_MAX_PUBMED
            )
            pubmed_overview_items = _curate_top_items(
                pubmed_overview_items, min(pubmed_cap, max(1, overview_items_max))
            )

            deep_candidates: List[Dict[str, Any]] = []
            seen_pubmed_ids: Set[str] = set()
            for it in pubmed_overview_items:
                iid = str(it.get("id") or "").strip()
                if not iid or iid in seen_pubmed_ids:
                    continue
                deep_candidates.append(it)
                seen_pubmed_ids.add(iid)

            for it in pubmed_deep_dive_items:
                iid = str(it.get("id") or "").strip()
                if not iid or iid in seen_pubmed_ids:
                    continue
                deep_candidates.append(it)
                seen_pubmed_ids.add(iid)

            pubmed_deep_dive_items = _curate_top_items(deep_candidates, deep_dive_limit)
    else:
        items = items_all_new

    # Cybermed in-report transparency header (MUST be based on the real pipeline).
    cybermed_meta_block = ""
    cybermed_run_stats: Dict[str, Any] = {}
    if is_cybermed_run:
        pubmed_selected = len(pubmed_overview_items)
        pubmed_new_unique = len(pubmed_new_items)

        # PubMed date filtering is applied using UTC date boundaries (YYYY/MM/DD), not hour-resolution.
        now_utc = datetime.now(timezone.utc)
        since_utc = now_utc - timedelta(hours=args.hours)
        pubmed_datetype = (os.getenv("PUBMED_DATE_TYPE", "pdat") or "pdat").strip().lower()
        mindate = _date_yyyymmdd_utc(since_utc)
        maxdate = _date_yyyymmdd_utc(now_utc)

        # Derive journals list from the actual configured PubMed queries (best-effort).
        journals: List[str] = []
        seen_journals: set[str] = set()
        for _, term in pubmed_queries_used:
            for m in re.finditer(r'"([^\"]+)"\s*\[jour\]', term or ""):
                j = (m.group(1) or "").strip()
                if not j or j in seen_journals:
                    continue
                seen_journals.add(j)
                journals.append(j)

        if not journals:
            # Fallback: use channel names (strip the common prefix).
            seen_lbls: set[str] = set()
            for cname, _ in pubmed_queries_used:
                lbl = (cname or "").strip()
                if lbl.lower().startswith("pubmed:"):
                    lbl = lbl.split(":", 1)[1].strip()
                if not lbl or lbl in seen_lbls:
                    continue
                seen_lbls.add(lbl)
                journals.append(lbl)

        journal_list = ", ".join(journals) if journals else "(none)"
        q_count = len(pubmed_queries_used)

        # Selection policy summary (non-sensitive diagnostics only).
        sel_enabled = False
        sel_mode = ""
        sel_min_score = None
        sel_max_selected = None
        sel_below_threshold = None
        sel_excluded_by_allowlist = None
        sel_excluded_offtopic = None
        sel_excluded_deep_low = None
        sel_deep_hard = None
        if isinstance(selection_stats, dict) and selection_stats:
            sel_enabled = bool(selection_stats.get("enabled", False))
            sel_mode = str(selection_stats.get("journal_allowlist_mode", "") or "").strip()
            sel_min_score = selection_stats.get("min_score", None)
            sel_max_selected = selection_stats.get("max_selected_per_run", None)
            sel_below_threshold = selection_stats.get("below_threshold_overview", selection_stats.get("below_threshold", None))
            sel_excluded_by_allowlist = selection_stats.get("excluded_by_allowlist", None)
            sel_excluded_offtopic = selection_stats.get("excluded_overview_offtopic", None)
            sel_excluded_deep_low = selection_stats.get("excluded_deep_dive_low_score", None)
            sel_deep_hard = selection_stats.get("deep_dive_hard_excluded", None)

        lines: List[str] = []
        lines.append("**Cybermed report metadata**")
        lines.append(f"- {pubmed_candidates_total} papers screened from the following journals during the last {args.hours}h: {journal_list}")
        lines.append(f"- New (not previously processed): {pubmed_new_unique} (skipped_by_state: {pubmed_skipped_by_state})")
        lines.append(f"- Search criteria were (PubMed E-Utilities): datetype={pubmed_datetype.upper()} mindate={mindate} maxdate={maxdate} (UTC date boundaries), sort=date, retmax={max_items_per_channel}/query")
        lines.append(f"- PubMed queries executed: {q_count} (failed: {pubmed_query_failures})")
        if sel_enabled:
            lines.append(
                f"- Number of selected papers: {pubmed_selected} (after state + selection policy; "
                f"min_score={sel_min_score}, max_selected={sel_max_selected}, allowlist_mode={sel_mode or 'n/a'}, "
                f"below_threshold={sel_below_threshold}, excluded_by_allowlist={sel_excluded_by_allowlist}, "
                f"excluded_offtopic={sel_excluded_offtopic}, excluded_deep_dive_low_score={sel_excluded_deep_low}, "
                f"deep_dive_hard_excluded={sel_deep_hard})"
            )
        else:
            lines.append(f"- Number of selected papers: {pubmed_selected} (after state; selection policy disabled/unavailable)")

        skip_reasons = []
        if isinstance(selection_stats, dict):
            reasons_map = selection_stats.get("state_skip_reasons") or {}
            if isinstance(reasons_map, dict):
                skip_reasons = [f"{k}:{v}" for k, v in reasons_map.items()]
        if skip_reasons:
            lines.append(f"- PubMed state skip reasons: {', '.join(skip_reasons)}")

        lines.append(
            f"- FOAMed/blog posts screened in the last {args.hours}h: {foamed_screened_total} "
            f"(skipped_by_state: {foamed_skipped_by_state}, after_state: {foamed_after_state}, "
            f"included_overview: {len(foamed_overview_items)}, top_picks: {len(foamed_top_picks)}, "
            f"sources_total: {foamed_meta_stats.get('sources_total', 0)}, sources_failed: {foamed_meta_stats.get('sources_failed', 0)}, "
            f"items_raw: {foamed_meta_stats.get('items_raw', 0)}, items_with_date: {foamed_meta_stats.get('items_with_date', 0)}, "
            f"items_date_unknown: {foamed_meta_stats.get('items_date_unknown', 0)}, kept_last24h: {foamed_meta_stats.get('kept_last24h', 0)})"
        )
        lines.append("")
        lines.append("**PubMed queries (exact terms)**")
        for cname, term in pubmed_queries_used:
            if not (cname or "").strip() or not (term or "").strip():
                continue
            lines.append(f"- {cname}: `{term}`")

        cybermed_meta_block = "\n".join(lines).strip() + "\n\n"
        cybermed_run_stats = {
            "pubmed": {
                "candidates_total": pubmed_candidates_total,
                "new_unique": pubmed_new_unique,
                "selected_overview": pubmed_selected,
                "selected_deep_dives": len(pubmed_deep_dive_items),
                "skipped_by_state": pubmed_skipped_by_state,
                "query_failures": pubmed_query_failures,
                "queries_executed": q_count,
                "mindate": mindate,
                "maxdate": maxdate,
                "datetype": pubmed_datetype,
                "selection": selection_stats,
            },
            "foamed": foamed_meta_stats or {},
        }

    if is_cybermed_run:
        report_items = _dedupe_items(pubmed_overview_items + pubmed_deep_dive_items + foamed_overview_items)
    else:
        report_items = list(items)

    if not report_items:
        if report_language.lower().startswith("en"):
            overview = cybermed_meta_block + "## Executive Summary\n\nNo new content in the last 24 hours.\n"
        else:
            overview = cybermed_meta_block + "## Kurzüberblick\n\nKeine neuen Inhalte in den letzten 24 Stunden.\n"
        out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
        md = to_markdown(
            [], overview, {}, report_title=report_title, report_language=report_language, report_mode=report_mode
        )

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"[report] Wrote {out_path}")

        now_utc_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        _update_state_after_run(
            state_path=state_path,
            state=state,
            items_all_new=items_all_new,
            overview_items=[],
            detail_items=[],
            foamed_overview_items=foamed_overview_items,
            report_key=report_key,
            report_mode=report_mode,
            now_utc_iso=now_utc_iso,
            read_only=read_only_mode,
        )

        if send_empty_email == "1":
            try:
                send_markdown(report_subject, md)
            except Exception as e:
                print(f"[email] WARN: failed to send empty report email: {e!r}")
        else:
            print("[email] No new items and SEND_EMPTY_REPORT_EMAIL=0 -> not sending email.")
        return

    detail_items: List[Dict[str, Any]] = []
    deep_dive_ids: Set[str] = set()
    if is_cybermed_run:
        if report_mode in {"weekly", "monthly"}:
            overview_items = list(pubmed_overview_items)
        else:
            overview_items = sorted(
                pubmed_overview_items,
                key=lambda it: it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )[: max(1, overview_items_max)]

        detail_items = list(pubmed_deep_dive_items)[: max(0, deep_dive_limit)]
        report_items = _dedupe_items(overview_items + detail_items + foamed_overview_items)
        deep_dive_ids = {
            str(it.get("id") or "").strip()
            for it in detail_items
            if str(it.get("id") or "").strip()
        }
    else:
        items_sorted = sorted(
            report_items,
            key=lambda it: it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        curated_overview = _curate_cyberlurch_overview(items_sorted, report_mode, overview_items_max)
        overview_items = curated_overview
        detail_items = _choose_detail_items(
            items=items_sorted,
            channel_topics=channel_topics,
            topic_weights=topic_weights,
            detail_items_per_day=detail_items_per_day,
            detail_items_per_channel_max=detail_items_per_channel_max,
        )
        detail_items = detail_items[: max(0, deep_dive_limit)]
        if report_mode in {"weekly", "monthly"} and report_key.strip().lower() == "cyberlurch":
            report_items = _dedupe_items(overview_items + detail_items)
        else:
            report_items = items_sorted

    if not overview_items and detail_items:
        overview_items = list(detail_items)

    if report_mode in {"weekly", "monthly"}:
        for it in overview_items[: max(1, min(3, len(overview_items)))]:
            if not it.get("top_pick"):
                it["top_pick"] = True

    for it in overview_items:
        src = (it.get("source") or "").strip().lower()
        iid = str(it.get("id") or "").strip()
        if src != "pubmed" or not iid:
            continue
        try:
            bl = summarize_pubmed_bottom_line(it, language=report_language)
            it["bottom_line"] = bl
        except Exception as e:
            print(f"[summarize] WARN: summarize_pubmed_bottom_line failed for pubmed:{iid!r}: {e!r}")

    if foamed_overview_items:
        for it in foamed_overview_items:
            url_lbl = (it.get("url") or it.get("id") or "")
            try:
                bl = summarize_foamed_bottom_line(it, language=report_language)
                it["bottom_line"] = bl
            except Exception as e:
                print(f"[summarize] WARN: summarize_foamed_bottom_line failed for foamed item {url_lbl!r}: {e!r}")

    overview_body = ""
    if is_cybermed_run and not overview_items and foamed_overview_items:
        if report_language.lower().startswith("en"):
            overview_body = "## Executive Summary\n\nNo new PubMed papers selected in this run. Recent FOAMed posts are listed below.\n"
        else:
            overview_body = "## Kurzüberblick\n\nKeine neuen PubMed-Papers in diesem Lauf; aktuelle FOAMed-Beiträge stehen unten.\n"
    else:
        try:
            overview_body = summarize(overview_items, language=report_language, profile=report_profile).strip()
        except Exception as e:
            print(f"[summarize] ERROR: summarize() failed: {e!r}")
            if report_language.lower().startswith("en"):
                overview_body = "## Executive Summary\n\n**Error:** Failed to generate overview.\n"
            else:
                overview_body = "## Kurzüberblick\n\n**Fehler:** Konnte Kurzüberblick nicht erzeugen.\n"

    if cybermed_meta_block:
        overview_body = cybermed_meta_block + overview_body

    details_by_id: Dict[str, str] = {}
    for it in detail_items:
        src = (it.get("source") or "").strip().lower() or "youtube"
        iid = str(it.get("id") or "").strip()
        if not iid:
            continue
        key = f"{src}:{iid}"
        try:
            details_by_id[key] = summarize_item_detail(it, language=report_language, profile=report_profile).strip()
        except Exception as e:
            print(f"[summarize] WARN: summarize_item_detail failed for {key!r}: {e!r}")
            if report_language.lower().startswith("en"):
                details_by_id[key] = "Key takeaways:\n- (Failed to generate deep dive.)\n"
            else:
                details_by_id[key] = "Kernaussagen:\n- (Fehler beim Erzeugen der Detail-Zusammenfassung)\n"

    if is_cybermed_run and not deep_dive_ids and details_by_id:
        for key in details_by_id.keys():
            iid = key.split(":", 1)[1] if ":" in key else key
            iid = (iid or "").strip()
            if iid:
                deep_dive_ids.add(iid)

    out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")

    details_for_report: Dict[str, str] = {}
    for it in detail_items:
        src = (it.get("source") or "").strip().lower() or "youtube"
        iid = str(it.get("id") or "").strip()
        key = f"{src}:{iid}"
        if iid and key in details_by_id:
            details_for_report[iid] = details_by_id[key]

    if not is_cybermed_run:
        for it in overview_items:
            iid = str(it.get("id") or "").strip()
            if not iid or iid in details_for_report:
                continue
            bl = (it.get("bottom_line") or "").strip()
            if bl:
                details_for_report[iid] = f"**BOTTOM LINE:** {bl}"

    if is_cybermed_run and deep_dive_ids:
        for it in report_items + detail_items:
            iid = str(it.get("id") or "").strip()
            if iid and iid in deep_dive_ids:
                it["top_pick"] = True

    md = to_markdown(
        report_items,
        overview_body,
        details_for_report,
        report_title=report_title,
        report_language=report_language,
        foamed_stats=foamed_meta_stats,
        cybermed_stats=cybermed_run_stats if is_cybermed_run else None,
        report_mode=report_mode,
    )

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"[report] Wrote {out_path}")

    now_utc_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    if report_mode == "monthly":
        try:
            rollups_state = load_rollups_state(rollups_state_path)
            month_key = datetime.now(tz=STO).strftime("%Y-%m")
            rollup_items = _rollup_items_for_month(overview_items, detail_items, foamed_overview_items)
            executive_summary = derive_monthly_summary(
                overview_body,
                top_items=rollup_items,
                max_bullets=8,
            )
            upsert_monthly_rollup(
                rollups_state,
                report_key=report_key,
                month=month_key,
                generated_at=now_utc_iso,
                executive_summary=executive_summary,
                top_items=rollup_items,
            )
            prune_rollups(
                rollups_state,
                report_key=report_key,
                max_months=rollups_max_months,
                keep_month=month_key,
            )
            save_rollups_state(rollups_state_path, rollups_state)
        except Exception as e:
            print(f"[rollups] WARN: failed to persist monthly rollup: {e!r}")

    _update_state_after_run(
        state_path=state_path,
        state=state,
        items_all_new=items_all_new,
        overview_items=overview_items,
        detail_items=detail_items,
        foamed_overview_items=foamed_overview_items,
        report_key=report_key,
        report_mode=report_mode,
        now_utc_iso=now_utc_iso,
        read_only=read_only_mode,
    )

    try:
        send_markdown(report_subject, md)
    except Exception as e:
        print(f"[email] WARN: failed to send email (report was generated and state saved): {e!r}")


if __name__ == "__main__":
    main()
