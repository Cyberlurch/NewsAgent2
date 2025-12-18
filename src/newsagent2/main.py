from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from .collectors_youtube import fetch_transcript, list_recent_videos
from .collectors_pubmed import search_recent_pubmed
from .emailer import send_markdown
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
from .summarizer import summarize, summarize_item_detail, summarize_pubmed_bottom_line

STO = ZoneInfo("Europe/Stockholm")


def _is_cybermed(report_key: str, report_profile: str) -> bool:
    """Cybermed detection must be stable and avoid touching Cyberlurch logic."""
    rk = (report_key or "").strip().lower()
    rp = (report_profile or "").strip().lower()
    return rk == "cybermed" or rp == "medical"


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
    retention_days = _safe_int("STATE_RETENTION_DAYS", 20)

    print("=== NewsAgent2 run ===")
    print(f"[config] report_key={report_key!r}")
    print(f"[config] report_title={report_title!r}")
    print(f"[config] report_subject={report_subject!r}")
    print(f"[config] channels_file={args.channels!r} hours={args.hours}")
    print(f"[config] report_dir={report_dir!r}")
    print(f"[config] report_language={report_language!r} report_profile={report_profile!r}")
    print(f"[config] limits: MAX_ITEMS_PER_CHANNEL={max_items_per_channel}, DETAIL_ITEMS_PER_DAY={detail_items_per_day}, DETAIL_ITEMS_PER_CHANNEL_MAX={detail_items_per_channel_max}")
    print(f"[config] overview_items_max={overview_items_max}, max_text_chars_per_item={max_text_chars_per_item}")
    print(f"[config] pubmed_sent_cooldown_hours={sent_cooldown_hours}, reconsider_unsent_hours={reconsider_unsent_hours}")
    print(f"[state] path={state_path!r} retention_days={retention_days}")

    state = load_state(state_path)
    state = _apply_prune_state_compat(state, retention_days=retention_days)

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

    try:
        channels, channel_topics, topic_weights = load_channels_config(args.channels)
    except Exception as e:
        if report_language.lower().startswith("en"):
            overview = f"## Executive Summary\n\n**Error:** Failed to load channels configuration: `{e!r}`\n"
        else:
            overview = f"## Kurzüberblick\n\n**Fehler:** Konnte Channels-Konfiguration nicht laden: `{e!r}`\n"
        out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
        md = to_markdown([], overview, {}, report_title=report_title, report_language=report_language)
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

                if is_processed(state, report_key, "youtube", vid):
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

        items = list(non_pubmed_items) + pubmed_overview_items
        print(f"[select] Cybermed selected {len(pubmed_overview_items)} of {len(pubmed_new_items)} new PubMed item(s) for the report.")
    else:
        items = items_all_new

    # Cybermed in-report transparency header (MUST be based on the real pipeline).
    cybermed_meta_block = ""
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
        if isinstance(selection_stats, dict) and selection_stats:
            sel_enabled = bool(selection_stats.get("enabled", False))
            sel_mode = str(selection_stats.get("journal_allowlist_mode", "") or "").strip()
            sel_min_score = selection_stats.get("min_score", None)
            sel_max_selected = selection_stats.get("max_selected_per_run", None)
            sel_below_threshold = selection_stats.get("below_threshold_overview", selection_stats.get("below_threshold", None))
            sel_excluded_by_allowlist = selection_stats.get("excluded_by_allowlist", None)

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
                f"below_threshold={sel_below_threshold}, excluded_by_allowlist={sel_excluded_by_allowlist})"
            )
        else:
            lines.append(f"- Number of selected papers: {pubmed_selected} (after state; selection policy disabled/unavailable)")
        lines.append("")
        lines.append("**PubMed queries (exact terms)**")
        for cname, term in pubmed_queries_used:
            if not (cname or "").strip() or not (term or "").strip():
                continue
            lines.append(f"- {cname}: `{term}`")

        cybermed_meta_block = "\n".join(lines).strip() + "\n\n"

    if is_cybermed_run:
        report_items = _dedupe_items(pubmed_overview_items + pubmed_deep_dive_items)
    else:
        report_items = list(items)

    if not report_items:
        if report_language.lower().startswith("en"):
            overview = cybermed_meta_block + "## Executive Summary\n\nNo new content in the last 24 hours.\n"
        else:
            overview = cybermed_meta_block + "## Kurzüberblick\n\nKeine neuen Inhalte in den letzten 24 Stunden.\n"
        out_path = datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_daily_summary_%Y-%m-%d_%H-%M-%S.md")
        md = to_markdown([], overview, {}, report_title=report_title, report_language=report_language)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"[report] Wrote {out_path}")

        # Mark all newly screened items as processed (memory), even if none were selected for the report.
        now_utc_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
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
                    mark_screened(state, report_key, src, iid, meta=meta)
                else:
                    mark_processed(state, report_key, src, iid, meta=meta)
            except Exception as e:
                print(f"[state] WARN: mark_processed failed for {src}:{iid!r}: {e!r}")

        try:
            save_state(state_path, state)
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

    detail_items: List[Dict[str, Any]] = []
    if is_cybermed_run:
        overview_items = sorted(
            pubmed_overview_items,
            key=lambda it: it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )[: max(1, overview_items_max)]
        detail_items = list(pubmed_deep_dive_items)[: max(0, detail_items_per_day)]
        report_items = _dedupe_items(overview_items + detail_items)
    else:
        items_sorted = sorted(
            report_items,
            key=lambda it: it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        report_items = items_sorted
        overview_items = items_sorted[: max(1, overview_items_max)]
        detail_items = _choose_detail_items(
            items=items_sorted,
            channel_topics=channel_topics,
            topic_weights=topic_weights,
            detail_items_per_day=detail_items_per_day,
            detail_items_per_channel_max=detail_items_per_channel_max,
        )

    if not overview_items and detail_items:
        overview_items = list(detail_items)

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

    md = to_markdown(report_items, overview_body, details_for_report, report_title=report_title, report_language=report_language)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"[report] Wrote {out_path}")

    now_utc_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
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
            else:
                mark_processed(state, report_key, src, iid, meta=meta)
        except Exception as e:
            print(f"[state] WARN: mark_processed failed for {src}:{iid!r}: {e!r}")

    try:
        save_state(state_path, state)
    except Exception as e:
        print(f"[state] WARN: failed to save state: {e!r}")

    try:
        send_markdown(report_subject, md)
    except Exception as e:
        print(f"[email] WARN: failed to send email (report was generated and state saved): {e!r}")


if __name__ == "__main__":
    main()
