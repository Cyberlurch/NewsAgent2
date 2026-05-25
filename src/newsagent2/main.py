from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Dict, List, Set, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from .collector_foamed import collect_foamed_items
from .collectors_youtube import fetch_transcript, fetch_captions_text, list_recent_videos, get_yt_dlp_version
from .collectors_youtube_rss import list_recent_videos_rss
from .collectors_pubmed import fetch_pubmed_abstracts, search_recent_pubmed
from .collectors_youtube_api import fetch_video_snippets
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
from .cybermed_digest_store import (
    load_cybermed_daily_digest_store,
    select_cybermed_daily_digests_for_week,
    summarize_cybermed_weekly_digest_inputs,
    dedupe_weekly_digest_items,
)
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
from .cyberlurch_editorial import (
    PRIORITY_DAILY_CHANNELS,
    build_trend_clusters,
    is_deep_dive_eligible,
    normalize_channel_name,
    score_cyberlurch_deep_dive_candidate,
)
from .cyberlurch_cadence import annotate_cyberlurch_temporality, classify_cyberlurch_item_temporality, cyberlurch_cadence_profile

from .summarizer import (
    OPENAI_MODEL_CYBERLURCH_CHUNKS,
    OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST,
    OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST_FALLBACK,
    OPENAI_MODEL_CYBERLURCH_DEEPDIVE,
    OPENAI_MODEL_CYBERLURCH_OVERVIEW,
    summarize,
    summarize_item_detail,
    summarize_pubmed_bottom_line,
    summarize_foamed_bottom_line,
    summarize_cyberlurch_bottom_line,
    extract_pubmed_abstract,
    summarize_youtube_transcript_chunks,
    summarize_youtube_transcript_direct,
    _parse_structured_pubmed_abstract_sections,
)
from .pmc_fulltext import fetch_and_extract_fulltext, get_oa_links, get_pmcids_for_pmids
from .unpaywall import fetch_best_oa_fulltext, lookup_unpaywall, pick_best_oa_url
from .youtube_content_providers import fetch_video_content
from .utils.diagnostics import YouTubeDiagnosticsCounters
from .utils.text_quality import classify_low_signal_youtube_text

STO = ZoneInfo("Europe/Stockholm")

CYBERMED_WEEKLY_MAX_PUBMED = 20
CYBERMED_MONTHLY_MAX_PUBMED = 8
CYBERMED_WEEKLY_MAX_FOAMED = 15
CYBERMED_MONTHLY_MAX_FOAMED = 6
CYBERMED_WEEKLY_MAX_FOAMED_CANDIDATES = 15
CYBERMED_MONTHLY_MAX_FOAMED_CANDIDATES = 12
CYBERLURCH_WEEKLY_MAX_VIDEOS = 10
CYBERLURCH_MONTHLY_MAX_VIDEOS = 8
CYBERLURCH_WEEKLY_TOP_LINKS_MAX = 20
CYBERLURCH_MONTHLY_REPRESENTATIVE_LINKS_PER_TOPIC = 3
CYBERLURCH_YEARLY_REPRESENTATIVE_LINKS_PER_THEME = 3
WEEKLY_MAX_DEEP_DIVES = 5
MONTHLY_MAX_DEEP_DIVES = 2

def classify_direct_digest_error(exc: Exception) -> str:
    msg = str(exc).lower()
    if "empty_output" in msg:
        return "empty_output"
    if "timeout" in msg:
        return "timeout"
    if "response_format" in msg or "json_object" in msg:
        return "response_format_unsupported"
    if "openai" in msg or "api" in msg:
        return "openai_error"
    if "json" in msg:
        return "json_parse_error"
    return "unknown"


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


def _is_poplar_channel(channel: Dict[str, Any]) -> bool:
    name = re.sub(r"\s+", "", str(channel.get("name") or "").strip().lower())
    url = str(channel.get("url") or "").strip().lower()
    return "thepoplarreport" in name or "thepoplarreport" in url


def _is_blackscout_channel(channel: Dict[str, Any]) -> bool:
    name = re.sub(r"\s+", "", str(channel.get("name") or "").strip().lower())
    url = str(channel.get("url") or "").strip().lower()
    return "blackscoutsurvival" in name or "blackscoutsurvival" in url


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




def _calendar_env_metadata() -> Dict[str, Any]:
    return {
        "calendar_policy_enabled": _env_bool("CYBERMED_CALENDAR_POLICY_ENABLED", False),
        "calendar_local_date": (os.getenv("CYBERMED_CALENDAR_LOCAL_DATE_STOCKHOLM", "") or "").strip(),
        "calendar_no_send_today": _env_bool("CYBERMED_CALENDAR_NO_SEND_TODAY", False),
        "calendar_holiday_name": (os.getenv("CYBERMED_CALENDAR_HOLIDAY_NAME", "") or "").strip(),
        "calendar_mode_shifted": _env_bool("CYBERMED_CALENDAR_MODE_SHIFTED", False),
        "calendar_shifted_from": (os.getenv("CYBERMED_CALENDAR_SHIFTED_FROM", "") or "").strip(),
        "calendar_shifted_to": (os.getenv("CYBERMED_CALENDAR_SHIFTED_TO", "") or "").strip(),
        "calendar_shift_reason": (os.getenv("CYBERMED_CALENDAR_SHIFT_REASON", "") or "").strip(),
        "seasonal_greeting": (os.getenv("CYBERMED_SEASONAL_GREETING_TEXT", "") or "").strip(),
    }

def _safe_int(env_name: str, default: int) -> int:
    raw = (os.getenv(env_name, "") or "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        print(f"[warn] Invalid int in {env_name}={raw!r} -> using default {default}")
        return default


def _env_bool(env_name: str, default: bool) -> bool:
    raw = (os.getenv(env_name, "1" if default else "0") or "").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    if raw in {"1", "true", "yes", "on"}:
        return True
    return default





CYBERLURCH_DIGEST_SAFE_FIELDS = [
    "video_id","url","title","channel","published_at","processed_at_utc","topic_primary","topics",
    "text_source","content_status","transcript_processing","transcript_full_summary","transcript_key_points",
    "transcript_notable_claims","transcript_uncertainties","important_details","editorial_relevance","bottom_line",
    "cyberlurch_deep_dive_score","cyberlurch_deep_dive_reasons","top_pick","temporality",
]


CYBERLURCH_SYNTHETIC_VIDEO_IDS = {"fallback123", "v1", "id1", "id2", "ok1", "m1", "new1", "c2", "c3", "c9"}
CYBERLURCH_SYNTHETIC_TITLES = {"Metadata Only Title", "t", "A", "B", "T", "Meta", "Newer title", "Short", "Long", "FailDirect"}


def _is_youtube_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("https://www.youtube.com/") or u.startswith("http://www.youtube.com/") or u.startswith("https://youtu.be/") or u.startswith("http://youtu.be/")


def _is_valid_cyberlurch_digest_record(record: Dict[str, Any], known_channels: Optional[Set[str]] = None) -> bool:
    if not isinstance(record, dict):
        return False
    video_id = str(record.get("video_id") or "").strip()
    url = str(record.get("url") or "").strip()
    channel = str(record.get("channel") or "").strip()
    topic_primary = str(record.get("topic_primary") or "").strip().lower()
    title = str(record.get("title") or "").strip()
    topics = record.get("topics") or []
    topics_norm = {str(t).strip().lower() for t in topics if str(t).strip()}

    if not video_id or video_id in CYBERLURCH_SYNTHETIC_VIDEO_IDS:
        return False
    if not url or not _is_youtube_url(url):
        return False
    if not channel or channel in {"C", "Test Channel"}:
        return False
    if topic_primary in {"t", "test"}:
        return False
    if title in CYBERLURCH_SYNTHETIC_TITLES:
        return False
    if "test" in topics_norm or "t" in topics_norm:
        return False
    if known_channels:
        # Prefer known channels while keeping backward compatibility.
        _ = channel in known_channels
    return True


def sanitize_cyberlurch_digest_state(state: Dict[str, Any], known_channels: Optional[Set[str]] = None) -> tuple[Dict[str, Any], int]:
    digests = state.get("digests") if isinstance(state, dict) else None
    if not isinstance(digests, list):
        return {"version": 1, "updated_at_utc": "", "digests": []}, 0
    kept = []
    removed = 0
    for d in digests:
        if _is_valid_cyberlurch_digest_record(d, known_channels=known_channels):
            kept.append(_sanitize_cyberlurch_digest_record(d))
        else:
            removed += 1
    state["digests"] = kept
    return state, removed


def _item_from_digest_record(d: Dict[str, Any]) -> Dict[str, Any]:
    return {"source":"youtube","id":d.get("video_id"),"title":d.get("title"),"url":d.get("url"),"channel":d.get("channel"),"published_at":_parse_iso_utc(str(d.get("published_at") or "")),"text_source":d.get("text_source"),"content_status":d.get("content_status"),"transcript_processing":d.get("transcript_processing"),"transcript_full_summary":d.get("transcript_full_summary"),"transcript_key_points":d.get("transcript_key_points"),"transcript_notable_claims":d.get("transcript_notable_claims"),"transcript_uncertainties":d.get("transcript_uncertainties"),"important_details":d.get("important_details"),"editorial_relevance":d.get("editorial_relevance"),"bottom_line":d.get("bottom_line"),"topic_primary":d.get("topic_primary"),"topics":d.get("topics") or [],"temporality":str(d.get("temporality") or "").strip(),"cyberlurch_deep_dive_score":d.get("cyberlurch_deep_dive_score") or 0,"cyberlurch_deep_dive_reasons":d.get("cyberlurch_deep_dive_reasons") or [],"top_pick":bool(d.get("top_pick")),"text":str(d.get("transcript_full_summary") or d.get("bottom_line") or "")}


def _load_cyberlurch_digest_state(path: str) -> Dict[str, Any]:
    default = {"version": 1, "updated_at_utc": "", "digests": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("digests"), list):
            data, _ = sanitize_cyberlurch_digest_state(data)
            return data
    except Exception:
        pass
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    return default


def _save_cyberlurch_digest_state(path: str, state: Dict[str, Any], *, read_only_mode: bool) -> None:
    if read_only_mode:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    state, _ = sanitize_cyberlurch_digest_state(state)
    state["updated_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def _sanitize_cyberlurch_digest_record(item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in CYBERLURCH_DIGEST_SAFE_FIELDS:
        v = item.get(k)
        if k == "published_at" and isinstance(v, datetime):
            v = v.astimezone(timezone.utc).replace(microsecond=0).isoformat()
        out[k] = v
    return out


def _upsert_cyberlurch_digests(state: Dict[str, Any], items: List[Dict[str, Any]], retention_days: int) -> tuple[int, int]:
    existing = {str(d.get("video_id") or ""): d for d in state.get("digests", []) if isinstance(d, dict) and str(d.get("video_id") or "")}
    upserted = 0
    for it in items:
        vid = str(it.get("id") or it.get("video_id") or "").strip()
        if not vid:
            continue
        payload = dict(it)
        payload["video_id"] = vid
        payload["processed_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        sanitized = _sanitize_cyberlurch_digest_record(payload)
        if not _is_valid_cyberlurch_digest_record(sanitized):
            continue
        existing[vid] = sanitized
        upserted += 1
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, retention_days))
    pruned = 0
    kept = []
    for d in existing.values():
        pub = _parse_iso_utc(str(d.get("published_at") or ""))
        if pub and pub < cutoff:
            pruned += 1
            continue
        kept.append(d)
    state["digests"] = kept
    return upserted, pruned


def _load_cybermed_daily_digest_state(path: str) -> Dict[str, Any]:
    default = {"schema_version": 1, "digests": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("digests"), list):
            data["schema_version"] = 1
            return data
    except Exception:
        pass
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    return default


def _sanitize_cybermed_pubmed_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "item_id": str(item.get("id") or item.get("item_id") or "").strip(),
        "source_type": "pubmed",
        "pmid": str(item.get("pmid") or "").strip(),
        "doi": str(item.get("doi") or "").strip(),
        "title": str(item.get("title") or "").strip(),
        "journal": str(item.get("journal") or "").strip(),
        "published_at": str(item.get("published_at") or item.get("year") or "").strip(),
        "url": str(item.get("url") or "").strip(),
        "domain": str(item.get("domain") or item.get("category") or "").strip(),
        "evidence_strength_label": str(item.get("evidence_strength_label") or "").strip(),
        "evidence_strength_label_basis": str(item.get("evidence_strength_label_basis") or "").strip(),
        "clinical_relevance_1_5": item.get("clinical_relevance_1_5"),
        "practice_change_potential_1_5": item.get("practice_change_potential_1_5"),
        "text_confidence_label": str(item.get("text_confidence_label") or "").strip(),
        "top_pick": bool(item.get("top_pick")),
        "deep_dive_candidate": bool(item.get("deep_dive_candidate")),
        "bottom_line": str(item.get("bottom_line") or "").strip(),
        "reason_labels": [str(v) for v in (item.get("reason_labels") or []) if str(v).strip()],
        "content_source": str(item.get("content_source") or "").strip(),
        "content_length_bucket": str(item.get("content_length_bucket") or "").strip(),
        "publication_types": [str(v) for v in (item.get("publication_types") or []) if str(v).strip()],
        "evidence_tags": [str(v) for v in (item.get("evidence_tags") or []) if str(v).strip()],
    }


def _sanitize_cybermed_foamed_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "item_id": str(item.get("id") or item.get("item_id") or "").strip(),
        "source_type": "foamed",
        "source_name": str(item.get("source_name") or item.get("source") or "").strip(),
        "title": str(item.get("title") or "").strip(),
        "url": str(item.get("url") or "").strip(),
        "published_at": str(item.get("published_at") or "").strip(),
        "domain_group": str(item.get("domain_group") or "").strip(),
        "priority_tier": str(item.get("priority_tier") or "").strip(),
        "source_quality_label": str(item.get("source_quality_label") or "").strip(),
        "clinical_usefulness_1_5": item.get("clinical_usefulness_1_5"),
        "practice_relevance_1_5": item.get("practice_relevance_1_5"),
        "text_confidence_label": str(item.get("text_confidence_label") or "").strip(),
        "final_content_source": str(item.get("final_content_source") or "").strip(),
        "extraction_method": str(item.get("extraction_method") or "").strip(),
        "top_pick": bool(item.get("top_pick")),
        "bottom_line": str(item.get("bottom_line") or "").strip(),
        "reason_labels": [str(v) for v in (item.get("reason_labels") or []) if str(v).strip()],
    }


def _normalize_cybermed_weekly_digest_item(item: Dict[str, Any], *, deep_dive_ids: Set[str]) -> Dict[str, Any]:
    row = dict(item or {})
    item_id = str(row.get("item_id") or row.get("id") or row.get("pmid") or "").strip()
    source_type = str(row.get("source_type") or row.get("source") or "").strip().lower()
    source = source_type if source_type in {"pubmed", "foamed"} else str(row.get("source") or "").strip().lower()
    row["id"] = item_id
    row["item_id"] = item_id
    row["source_type"] = source_type
    row["source"] = source
    row["title"] = str(row.get("title") or "").strip()
    row["url"] = str(row.get("url") or "").strip()
    row["published_at"] = str(row.get("published_at") or "").strip()
    row["bottom_line"] = str(row.get("bottom_line") or "").strip()
    row["top_pick"] = bool(row.get("top_pick"))
    if source_type == "pubmed":
        row["journal"] = str(row.get("journal") or "").strip()
        row["evidence_strength_label"] = str(row.get("evidence_strength_label") or "").strip()
        row["clinical_relevance_1_5"] = row.get("clinical_relevance_1_5")
        row["practice_change_potential_1_5"] = row.get("practice_change_potential_1_5")
        row["text_confidence_label"] = str(row.get("text_confidence_label") or "").strip()
        row["deep_dive_candidate"] = bool(row.get("deep_dive_candidate"))
        row["cybermed_deep_dive"] = item_id in deep_dive_ids
    elif source_type == "foamed":
        row["source_name"] = str(row.get("source_name") or row.get("source") or "").strip()
        row["source_quality_label"] = str(row.get("source_quality_label") or "").strip()
        row["clinical_usefulness_1_5"] = row.get("clinical_usefulness_1_5")
        row["practice_relevance_1_5"] = row.get("practice_relevance_1_5")
        row["text_confidence_label"] = str(row.get("text_confidence_label") or "").strip()
        row["final_content_source"] = str(row.get("final_content_source") or "").strip()
        row["cybermed_deep_dive"] = False
    return row

def determine_monthly_rollup_month(now_sto: datetime, event_name: str, override_month: str | None) -> str:
    override = (override_month or "").strip()
    if override and re.match(r"^\d{4}-\d{2}$", override):
        return override
    event = (event_name or "").strip().lower()
    if event == "schedule" and now_sto.day == 1:
        prev = (now_sto.replace(day=1) - timedelta(days=1))
        return prev.strftime("%Y-%m")
    return now_sto.strftime("%Y-%m")

def _dedupe_videos_by_id(videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for v in videos:
        vid = str(v.get("id") or "").strip()
        if not vid or vid in seen:
            continue
        seen.add(vid)
        out.append(v)
    return out




def _channel_cache_key(channel: dict[str, Any]) -> str:
    name = str(channel.get("name") or "").strip()
    if name:
        return name
    return str(channel.get("url") or "").strip()


def _load_youtube_channel_id_cache(path: str = "state/youtube_channel_ids.json") -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("channels"), dict):
            return data
    except Exception:
        pass
    return {"channels": {}}


def _save_youtube_channel_id_cache(cache: dict[str, Any], *, read_only_mode: bool, path: str = "state/youtube_channel_ids.json") -> None:
    if read_only_mode:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def _write_channel_id_suggestions(suggestions: dict[str, str], report_dir: str) -> None:
    if (os.getenv("GITHUB_EVENT_NAME") or "").strip() != "workflow_dispatch" or not suggestions:
        return
    out_path = os.path.join(report_dir, "cyberlurch_channel_id_suggestions.json")
    os.makedirs(report_dir, exist_ok=True)
    payload = [{"channel": k, "channel_id": v} for k, v in sorted(suggestions.items())]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _annotate_cyberlurch_item_topics(items: List[Dict[str, Any]], channel_topics: Dict[str, List[str]]) -> None:
    for it in items:
        if not isinstance(it, dict):
            continue
        channel_name = str(it.get("channel") or "").strip()
        topics_raw = channel_topics.get(channel_name, [])
        topics = [str(t).strip() for t in topics_raw if str(t).strip()]
        if not topics:
            topics = ["Other"]
        topic_primary = topics[0]
        it["topics"] = topics
        it["topic_primary"] = topic_primary
        it["topic"] = topic_primary
        existing_temporality = str(it.get("temporality") or "").strip()
        it["temporality"] = existing_temporality or classify_cyberlurch_item_temporality(it)


def _metadata_only_text(*, title: str, channel: str, published_at: Any) -> str:
    if isinstance(published_at, datetime):
        published = published_at.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    else:
        published = str(published_at or "").strip()
    return (
        "METADATA_ONLY: Only title/channel/date metadata is available. Do not infer details beyond the title. "
        f"Title: {title}. Channel: {channel}. Published: {published}."
    )


def _pubmed_content_backfill_and_diagnostics(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    min_fulltxt = _safe_int("PUBMED_DEEPDIVE_MIN_FULLTEXT_CHARS", 1200)
    content_source_counts: Counter[str] = Counter()
    retrieval_counts: Counter[str] = Counter()
    oa_counts: Counter[str] = Counter()
    pubtype_counts: Counter[str] = Counter()
    attempted = success = failed = improved = 0
    with_content = metadata_only = 0
    backfill_needed = 0
    for it in items:
        abstract = str(it.get("abstract") or "").strip()
        fulltxt = str(it.get("full_text_excerpt") or "").strip()
        source = "metadata_only"
        method = "none"
        oa_status = "unknown"
        if fulltxt:
            source = "pmc_oa_fulltext" if str(it.get("fulltext_source") or "").startswith("pmc") else "unpaywall_oa_fulltext"
            method = "pmc_oa" if source == "pmc_oa_fulltext" else "unpaywall"
            oa_status = "oa_fulltext_available"
        elif abstract:
            source = "pubmed_structured_abstract" if bool(it.get("abstract_sections")) else "pubmed_abstract"
            method = "efetch_xml"
            oa_status = "oa_metadata_only"
        elif (it.get("doi") or "").strip():
            oa_status = "closed_or_unavailable"
        it["content_source"] = source if source in {"pubmed_abstract", "pubmed_structured_abstract", "pmc_oa_fulltext", "unpaywall_oa_fulltext"} else "metadata_only"
        it["content_retrieval_method"] = method
        it["content_length"] = len((abstract + "\n" + fulltxt).strip())
        it["abstract_length"] = len(abstract)
        it["fulltext_length"] = len(fulltxt)
        it["oa_status"] = oa_status
        has_content = _is_pubmed_item_content_usable(it, min_fulltxt)
        needs_backfill = (not abstract) or (len(abstract) < _safe_int("PUBMED_DEEPDIVE_MIN_ABSTRACT_CHARS", 200))
        if needs_backfill and ((it.get("pmcid") or "").strip() or (it.get("doi") or "").strip()):
            backfill_needed += 1
        if not has_content:
            metadata_only += 1
            for pt in (it.get("publication_types") or []):
                pubtype_counts[str(pt)] += 1
        else:
            with_content += 1
        content_source_counts[it["content_source"]] += 1
        retrieval_counts[method] += 1
        oa_counts[oa_status] += 1
    attempted = backfill_needed
    success = sum(1 for it in items if str(it.get("content_source") or "") in {"pmc_oa_fulltext", "unpaywall_oa_fulltext"})
    improved = success
    failed = max(0, attempted - success)
    coverage = round((with_content / max(1,len(items))) * 100, 1) if items else 0.0
    out={"pubmed_content_backfill_enabled": True,"pubmed_content_backfill_attempted_total": attempted,"pubmed_content_backfill_success_total": success,"pubmed_content_backfill_failed_total": failed,"pubmed_content_backfill_improved_total": improved,"pubmed_content_backfill_pmc_attempted_total": sum(1 for it in items if (it.get("pmcid") or "").strip()),"pubmed_content_backfill_pmc_success_total": sum(1 for it in items if str(it.get("content_source") or "") == "pmc_oa_fulltext"),"pubmed_content_backfill_unpaywall_attempted_total": sum(1 for it in items if (it.get("doi") or "").strip()),"pubmed_content_backfill_unpaywall_success_total": sum(1 for it in items if str(it.get("content_source") or "") == "unpaywall_oa_fulltext"),"pubmed_items_with_abstract_or_oa_fulltext_total": with_content,"pubmed_items_metadata_only_total": metadata_only,"pubmed_post_state_content_coverage_pct": coverage,"pubmed_content_source_counts": dict(content_source_counts),"pubmed_content_retrieval_method_counts": dict(retrieval_counts),"pubmed_oa_status_counts": dict(oa_counts),"pubmed_metadata_only_publication_type_counts": dict(pubtype_counts)}
    if attempted == 0 and items:
        out["pubmed_content_backfill_not_attempted_reason"] = "all_post_state_items_already_have_usable_content"
    return out



def _is_pubmed_item_content_usable(item: Dict[str, Any], min_fulltext_chars: int) -> bool:
    abstract = str(item.get("abstract") or "").strip()
    text = str(item.get("text") or "").strip()
    src = str(item.get("content_source") or "").strip()
    fulltext_length = int(item.get("fulltext_length") or 0)
    if abstract:
        return True
    if src in {"pubmed_abstract", "pubmed_structured_abstract", "pmc_oa_fulltext", "unpaywall_oa_fulltext"}:
        return True
    if fulltext_length >= max(1, int(min_fulltext_chars)):
        return True
    if text and not text.startswith("METADATA_ONLY:") and len(text) >= 120:
        return True
    return False


def _foamed_72h_text_diagnostics(items: List[Dict[str, Any]], min_chars: int) -> Dict[str, Any]:
    total = len(items)
    fulltext = [it for it in items if str(it.get("final_content_source") or it.get("content_source") or "") == "article_full_text"]
    excerpt = [it for it in items if str(it.get("final_content_source") or it.get("content_source") or "") == "article_excerpt"]
    usable_sources = {"article_full_text", "article_excerpt", "rss_full_content", "html_content"}
    usable = [it for it in items if str(it.get("final_content_source") or it.get("content_source") or "") in usable_sources and len(str(it.get("text") or "").strip()) >= min_chars]
    lens = sorted([len(str((it.get("text") or "")).strip()) for it in items if str((it.get("text") or "")).strip()])
    exc_lens = sorted([len(str((it.get("text") or "")).strip()) for it in excerpt if str((it.get("text") or "")).strip()])
    med = lens[len(lens)//2] if lens else 0
    exc_med = exc_lens[len(exc_lens)//2] if exc_lens else 0
    return {
        "foamed_72h_items_total": total,
        "foamed_72h_article_fulltext_total": len(fulltext),
        "foamed_72h_article_excerpt_total": len(excerpt),
        "foamed_72h_usable_text_total": len(usable),
        "foamed_72h_article_fulltext_pct": round((len(fulltext)/max(1,total))*100,1),
        "foamed_72h_usable_text_pct": round((len(usable)/max(1,total))*100,1),
        "foamed_72h_text_length_min": (lens[0] if lens else 0),
        "foamed_72h_text_length_median": med,
        "foamed_72h_text_length_max": (lens[-1] if lens else 0),
        "foamed_72h_article_excerpt_text_length_median": exc_med,
    }

def _write_cyberlurch_youtube_diagnostics(
    report_dir: str,
    diag: YouTubeDiagnosticsCounters,
    *,
    report_mode: str = "daily",
    extra_counts: dict[str, Any] | None = None,
) -> None:
    if (os.getenv("GITHUB_EVENT_NAME") or "").strip() != "workflow_dispatch":
        return
    try:
        os.makedirs(report_dir, exist_ok=True)
        safe_mode = (report_mode or "daily").strip().lower() or "daily"
        out_path = os.path.join(report_dir, f"cyberlurch_{safe_mode}_youtube_diagnostics.json")
        payload = diag.to_count_only_dict()
        if extra_counts:
            payload.update(extra_counts)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
        if safe_mode == "daily":
            legacy_path = os.path.join(report_dir, "cyberlurch_youtube_diagnostics.json")
            with open(legacy_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
                f.write("\n")
        print(f"[diagnostics] Wrote count-only YouTube diagnostics: {out_path}")
    except Exception as e:
        print(f"[diagnostics] WARN: failed to write YouTube diagnostics err_type={type(e).__name__}")


def _write_run_metadata_artifact(report_dir: str, report_key: str, report_mode: str, run_metadata: str) -> None:
    if (os.getenv("GITHUB_EVENT_NAME") or "").strip() != "workflow_dispatch":
        return
    meta = (run_metadata or "").strip()
    if not meta:
        return
    try:
        os.makedirs(report_dir, exist_ok=True)
        safe_mode = (report_mode or "daily").strip().lower() or "daily"
        out_path = os.path.join(report_dir, f"{report_key}_{safe_mode}_run_metadata.md")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(meta.rstrip() + "\n")
        print(f"[diagnostics] Wrote run metadata artifact: {out_path}")
    except Exception as e:
        print(f"[diagnostics] WARN: failed to write run metadata artifact err_type={type(e).__name__}")


def _write_cybermed_diagnostics(
    report_dir: str,
    report_mode: str,
    diagnostics_payload: Dict[str, Any],
) -> None:
    if (os.getenv("GITHUB_EVENT_NAME") or "").strip() != "workflow_dispatch":
        return
    try:
        os.makedirs(report_dir, exist_ok=True)
        safe_mode = (report_mode or "daily").strip().lower() or "daily"
        out_path = os.path.join(report_dir, f"cybermed_{safe_mode}_diagnostics.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(diagnostics_payload, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
        print(f"[diagnostics] Wrote count-only Cybermed diagnostics: {out_path}")
    except Exception as e:
        print(f"[diagnostics] WARN: failed to write Cybermed diagnostics err_type={type(e).__name__}")


def _report_output_path(report_dir: str, report_key: str, report_mode: str) -> str:
    mode = (report_mode or "daily").strip().lower() or "daily"
    if mode == "yearly":
        year = datetime.now(tz=STO).strftime("%Y")
        return os.path.join(report_dir, f"{report_key}_yearly_review_{year}.md")
    suffix = "daily"
    if mode == "weekly":
        suffix = "weekly"
    elif mode == "monthly":
        suffix = "monthly"
    return datetime.now(tz=STO).strftime(f"{report_dir}/{report_key}_{suffix}_summary_%Y-%m-%d_%H-%M-%S.md")

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


def _pubmed_text_has_sufficient_content(
    item: Dict[str, Any],
    *,
    min_abstract_chars: int,
    min_fulltext_chars: int,
) -> bool:
    abstract = (item.get("abstract") or "").strip()
    if len(abstract) >= min_abstract_chars:
        return True
    fulltext_excerpt = (item.get("full_text_excerpt") or "").strip()
    if len(fulltext_excerpt) >= min_fulltext_chars:
        return True
    text = (item.get("text") or "").strip()
    for marker in ("[PMC Open Access full text]", "[Unpaywall OA full text"):
        if marker in text:
            idx = text.find(marker)
            excerpt = text[idx:]
            if len(excerpt.strip()) >= min_fulltext_chars:
                return True
    return False


def _store_fulltext(item: Dict[str, Any], *, marker: str, text: str, source: str, max_chars: int) -> None:
    trimmed = text[:max_chars].rstrip()
    item["full_text_excerpt"] = f"{marker}\n{trimmed}"
    base_text = (item.get("text") or "").strip()
    combined_parts = [part for part in (base_text, f"{marker}\n{trimmed}") if part]
    if combined_parts:
        item["text"] = "\n\n".join(combined_parts).strip()
    item["fulltext_source"] = source


def _apply_pmc_fulltext(
    item: Dict[str, Any],
    pmcid: str,
    *,
    timeout_s: float,
    max_bytes: int,
    max_chars: int,
) -> Tuple[bool, bool, bool]:
    links = get_oa_links(pmcid, timeout=timeout_s) if pmcid else []
    if not links:
        return False, False, False

    item["_deep_dive_pmc_oa_found"] = True
    text, skipped = fetch_and_extract_fulltext(
        links,
        timeout_s=float(timeout_s),
        max_bytes=max(1024, max_bytes),
        max_chars=max(1000, max_chars),
    )
    if text:
        _store_fulltext(
            item,
            marker="[PMC Open Access full text]",
            text=text,
            source="pmc",
            max_chars=max_chars,
        )
        item["_deep_dive_pmc_downloaded"] = True
        item["_deep_dive_pmc_oa_found"] = True
        item["_deep_dive_fulltext_enriched"] = True
    return True, bool(text), skipped


def _apply_unpaywall_fulltext(
    item: Dict[str, Any],
    *,
    email: str,
    timeout_s: float,
    max_bytes: int,
    max_chars: int,
    min_chars: int,
) -> Tuple[bool, bool, bool]:
    doi = (item.get("doi") or "").strip()
    if not doi or not email:
        return False, False, False

    data = lookup_unpaywall(doi, email, timeout=int(timeout_s))
    choice = pick_best_oa_url(data)
    if not choice:
        return False, False, False

    item["_deep_dive_unpaywall_found"] = True
    text, source_type, size_exceeded = fetch_best_oa_fulltext(
        choice,
        timeout=int(timeout_s),
        max_bytes=max(1024, max_bytes),
        max_chars=max(1000, max_chars),
    )
    if text and len(text) >= max(200, min_chars):
        marker = f"[Unpaywall OA full text ({source_type})]"
        source_label = f"unpaywall_{source_type or 'html'}"
        _store_fulltext(item, marker=marker, text=text, source=source_label, max_chars=max_chars)
        item["_deep_dive_unpaywall_downloaded"] = True
        item["_deep_dive_unpaywall_found"] = True
        item["_deep_dive_fulltext_enriched"] = True
        item["fulltext_license"] = choice.get("license", "")
        item["fulltext_host_type"] = choice.get("host_type", "")
        return True, True, size_exceeded

    return True, False, size_exceeded


def _select_pubmed_deep_dives_with_content(
    candidates: List[Dict[str, Any]],
    *,
    deep_dive_limit: int,
    use_pmc_oa_fulltext: bool,
    use_unpaywall_fulltext: bool,
    unpaywall_email: str,
    min_abstract_chars: int,
    min_fulltext_chars: int,
    fulltext_timeout_s: int,
    fulltext_max_bytes: int,
    fulltext_max_chars: int,
    unpaywall_min_chars: int,
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    if deep_dive_limit <= 0 or not candidates:
        return selected

    pmcid_map: Dict[str, str] = {}
    if use_pmc_oa_fulltext:
        pmids = [
            (it.get("pmid") or it.get("id") or "").strip()
            for it in candidates
            if not _pubmed_text_has_sufficient_content(
                it, min_abstract_chars=min_abstract_chars, min_fulltext_chars=min_fulltext_chars
            )
        ]
        pmcid_map = get_pmcids_for_pmids(pmids, timeout=float(fulltext_timeout_s))

    enriched = 0
    oa_found = 0
    downloaded = 0
    unpaywall_oa_found = 0
    unpaywall_downloaded = 0
    unpaywall_enabled = use_unpaywall_fulltext and bool(unpaywall_email)
    for cand in candidates:
        if len(selected) >= deep_dive_limit:
            break

        cand.setdefault("fulltext_source", "none")

        if _pubmed_text_has_sufficient_content(
            cand, min_abstract_chars=min_abstract_chars, min_fulltext_chars=min_fulltext_chars
        ):
            selected.append(cand)
            continue

        pmid = (cand.get("pmid") or cand.get("id") or "").strip()
        pmcid = pmcid_map.get(pmid, "")
        used_fulltext = False

        if use_pmc_oa_fulltext and pmcid:
            pmc_found, pmc_downloaded, _ = _apply_pmc_fulltext(
                cand,
                pmcid,
                timeout_s=float(fulltext_timeout_s),
                max_bytes=fulltext_max_bytes,
                max_chars=fulltext_max_chars,
            )
            if pmc_found:
                oa_found += 1
            if pmc_downloaded:
                downloaded += 1
                enriched += 1
                used_fulltext = True

        if not used_fulltext and unpaywall_enabled:
            found, dl_ok, size_exceeded = _apply_unpaywall_fulltext(
                cand,
                email=unpaywall_email,
                timeout_s=float(fulltext_timeout_s),
                max_bytes=fulltext_max_bytes,
                max_chars=fulltext_max_chars,
                min_chars=unpaywall_min_chars,
            )
            if found:
                unpaywall_oa_found += 1
            if dl_ok:
                unpaywall_downloaded += 1
                enriched += 1
                used_fulltext = True
            if size_exceeded:
                cand["_deep_dive_unpaywall_size_exceeded"] = True

        if _pubmed_text_has_sufficient_content(
            cand, min_abstract_chars=min_abstract_chars, min_fulltext_chars=min_fulltext_chars
        ):
            selected.append(cand)

    if selected:
        print(
            f"[deepdive] Selected {len(selected)} PubMed deep-dive candidate(s) "
            f"(enriched={enriched}, oa_found={oa_found}, downloaded={downloaded}, unpaywall_oa_found={unpaywall_oa_found}, "
            f"unpaywall_downloaded={unpaywall_downloaded}, limit={deep_dive_limit})"
        )
    else:
        print(
            f"[deepdive] No PubMed deep-dive candidates met content requirements "
            f"(limit={deep_dive_limit}, enriched={enriched}, oa_found={oa_found}, downloaded={downloaded}, "
            f"unpaywall_oa_found={unpaywall_oa_found}, unpaywall_downloaded={unpaywall_downloaded})"
        )

    return selected


_NEGATIVE_CUES = ("no significant", "did not", "no benefit", "not significant", "no difference")
_POSITIVE_CUES = ("reduced", "improved", "beneficial", "benefit", "safe", "practice-changing")
_HARM_CUES = ("increased harm", "worse", "higher adverse", "more adverse")


def _build_pubmed_shared_synopsis(item: Dict[str, Any]) -> Dict[str, Any]:
    abstract = str(item.get("abstract") or "").strip()
    sections = _parse_structured_pubmed_abstract_sections(abstract)
    design = str(sections.get("design") or "").strip()
    setting = str(sections.get("setting") or "").strip()
    subjects = str(sections.get("subjects") or "").strip()
    interventions = str(sections.get("interventions") or "").strip()
    measurements = str(sections.get("measurements") or "").strip()
    results = str(sections.get("results") or "").strip()
    conclusions = str(sections.get("conclusions") or "").strip()
    objective = str(sections.get("objectives") or "").strip()
    primary_source = measurements or objective or results
    low_primary = primary_source.lower()
    low_results = results.lower()
    direction = "unclear"
    significance = "not_reported"
    if any(c in low_primary for c in _NEGATIVE_CUES):
        direction = "negative_or_null"
        significance = "not_significant"
    elif "p<" in low_primary or "significant" in low_primary:
        direction = "positive" if any(c in low_primary for c in ("reduced", "improved", "lower")) else "mixed_or_unclear"
        significance = "significant_or_reported"
    elif "borderline" in low_primary or "trend" in low_primary:
        direction = "mixed_or_unclear"
        significance = "borderline"
    if "composite" in low_results and ("mixed" in low_results or "individual" in low_results):
        direction = "mixed_or_unclear"
    if not design and str(item.get("publication_types") or ""):
        design = ", ".join(str(x) for x in (item.get("publication_types") or [])[:2])
    bottom_line = str(item.get("bottom_line") or "").strip()
    if not bottom_line:
        if direction == "negative_or_null":
            bottom_line = "Primary endpoint was not significantly improved; any secondary signals should be interpreted cautiously."
        elif direction == "mixed_or_unclear":
            bottom_line = "Results were mixed; interpret potential benefits cautiously and prioritize the primary endpoint."
        else:
            bottom_line = "Findings suggest potential benefit, but interpretation should stay aligned with reported endpoints and study limits."
    return {
        "study_type": design,
        "population_setting": "; ".join([x for x in (setting, subjects) if x]),
        "intervention_or_exposure": interventions,
        "comparator": "",
        "primary_endpoint": objective or measurements,
        "primary_result_direction": direction,
        "primary_result_significance": significance,
        "key_secondary_results": results,
        "limitations": "Not explicitly stated in abstract." if not conclusions else "",
        "clinical_interpretation": conclusions or bottom_line,
        "bottom_line": bottom_line,
    }


def _detect_pubmed_bottom_line_conflicts(overview: str, deep: str) -> List[str]:
    ov = (overview or "").lower()
    dd = (deep or "").lower()
    conflicts: List[str] = []
    if any(c in ov for c in _NEGATIVE_CUES) and any(c in dd for c in ("reduced", "improved", "beneficial")):
        conflicts.append("negative_overview_vs_positive_deep_dive")
    if any(c in ov for c in _HARM_CUES) and any(c in dd for c in ("safe", "no harm")):
        conflicts.append("harm_overview_vs_safe_deep_dive")
    if ("primary endpoint" in ov and any(c in ov for c in _NEGATIVE_CUES)) and "practice-changing" in dd:
        conflicts.append("negative_primary_vs_unqualified_practice_changing")
    return conflicts

def _foamed_health_bucket(state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return {}
    bucket = state.setdefault("foamed_source_health", {})
    if not isinstance(bucket, dict):
        bucket = {}
        state["foamed_source_health"] = bucket
    return bucket


def _foamed_source_disabled(entry: Dict[str, Any], now_utc: datetime) -> bool:
    if not isinstance(entry, dict):
        return False
    disabled_until = _parse_iso_utc(str(entry.get("disabled_until_utc") or ""))
    return bool(disabled_until and disabled_until > now_utc)


def _filter_disabled_foamed_sources(
    sources: List[Dict[str, Any]],
    state: Dict[str, Any],
    now_utc: datetime,
    *,
    auto_disable_enabled: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    health = _foamed_health_bucket(state)
    foamed_audit_enabled = _env_bool("FOAMED_AUDIT", False)
    filtered: List[Dict[str, Any]] = []
    skipped = 0
    active_disabled = 0
    strategy_overrides = 0

    for src in sources:
        name = (src.get("name") or "").strip()
        if not name:
            continue
        entry = health.get(name) or {}
        if _foamed_source_disabled(entry, now_utc):
            active_disabled += 1
            strategy = str(src.get("extraction_strategy") or "").strip().lower()
            explicit_viable_strategy = strategy == "html_only"
            override_allowed = bool(src.get("ignore_auto_disable_if_strategy_viable")) and explicit_viable_strategy and foamed_audit_enabled
            if override_allowed:
                strategy_overrides += 1
                filtered.append(dict(src, strategy_override_disabled=True, disabled_state_present=True))
                continue
            if auto_disable_enabled:
                skipped += 1
                continue
        filtered.append(dict(src, strategy_override_disabled=False, disabled_state_present=bool(_foamed_source_disabled(entry, now_utc))))

    return filtered, {"skipped_disabled_count": skipped, "disabled_active_count": active_disabled, "strategy_override_disabled_count": strategy_overrides}


def _update_foamed_health_state(
    state: Dict[str, Any],
    per_source_stats: Dict[str, Any] | None,
    now_utc: datetime,
    *,
    auto_disable_enabled: bool,
    disable_after_403: int,
    disable_days_403: int,
    disable_after_404: int,
    disable_days_404: int,
    source_names: Set[str] | None = None,
) -> Dict[str, Any]:
    bucket = _foamed_health_bucket(state)
    now_iso = now_utc.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    newly_disabled: List[str] = []
    per_source_stats = per_source_stats or {}
    scope_names: Set[str] = set(source_names or [])

    for name, stats in per_source_stats.items():
        if not isinstance(stats, dict):
            continue
        src_name = str(name or "").strip()
        if not src_name:
            continue
        scope_names.add(src_name)

        health_entry = bucket.get(src_name) or {}
        prev_disabled = _foamed_source_disabled(health_entry, now_utc)
        failures = int(health_entry.get("consecutive_failures") or 0)
        health = str(stats.get("health") or "").strip().lower()

        if health in {"ok_rss", "ok_html"}:
            failures = 0
            health_entry["disabled_until_utc"] = ""
            health_entry["last_ok_utc"] = now_iso
        elif health:
            failures += 1
            if auto_disable_enabled and health == "blocked_403" and disable_after_403 > 0 and failures >= disable_after_403:
                health_entry["disabled_until_utc"] = (
                    now_utc + timedelta(days=max(0, disable_days_403))
                ).replace(microsecond=0).astimezone(timezone.utc).isoformat()
            elif auto_disable_enabled and health == "not_found_404" and disable_after_404 > 0 and failures >= disable_after_404:
                health_entry["disabled_until_utc"] = (
                    now_utc + timedelta(days=max(0, disable_days_404))
                ).replace(microsecond=0).astimezone(timezone.utc).isoformat()

        health_entry["consecutive_failures"] = failures
        if health:
            health_entry["last_health"] = health
        health_entry["last_seen_utc"] = now_iso

        if not prev_disabled and _foamed_source_disabled(health_entry, now_utc):
            newly_disabled.append(src_name)

        bucket[src_name] = health_entry

    active_disabled = 0
    for name in scope_names:
        entry = bucket.get(name) or {}
        if _foamed_source_disabled(entry, now_utc):
            active_disabled += 1

    return {
        "newly_disabled_count": len(newly_disabled),
        "newly_disabled_examples": newly_disabled[:5],
        "disabled_active_count": active_disabled,
    }


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
                channels.append({"name": cname, "source": "youtube", "url": curl, "channel_id": (c.get("channel_id") or "").strip()})

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
        return min(base_cap, _safe_int("CYBERLURCH_WEEKLY_DETAIL_ITEMS_PER_PERIOD", 8))
    if report_mode == "monthly":
        return min(base_cap, _safe_int("CYBERLURCH_MONTHLY_DETAIL_ITEMS_PER_PERIOD", 6))
    if report_mode == "yearly":
        return _safe_int("CYBERLURCH_YEARLY_EVERGREEN_DEEPDIVES", 5)
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


def _ensure_bottom_lines_for_rollup(items: List[Dict[str, Any]], *, language: str) -> None:
    attempted = 0
    filled = 0
    skipped = 0
    for it in items:
        if attempted >= 25:
            break
        bottom_line = (it.get("bottom_line") or "").strip()
        if bottom_line:
            skipped += 1
            continue
        source = (it.get("source") or "").strip().lower()
        summary = ""
        if source == "pubmed":
            attempted += 1
            try:
                summary = summarize_pubmed_bottom_line(it, language=language) or ""
            except Exception:
                summary = ""
        elif source == "foamed":
            attempted += 1
            try:
                summary = summarize_foamed_bottom_line(it, language=language) or ""
            except Exception:
                summary = ""
        else:
            skipped += 1
            continue
        summary = summary.strip()
        if summary:
            it["bottom_line"] = summary
            filled += 1
        else:
            skipped += 1
    print(f"[rollups] bottom_line fill: attempted={attempted} filled={filled} skipped={skipped}")


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
                    "bottom_line": (it.get("bottom_line") or "").strip(),
                    "topic_primary": (it.get("topic_primary") or "").strip(),
                    "topics": it.get("topics") or [],
                    "text_source": (it.get("text_source") or "").strip(),
                    "content_status": (it.get("content_status") or "").strip(),
                    "transcript_processing": (it.get("transcript_processing") or "").strip(),
                    "editorial_relevance": (it.get("editorial_relevance") or "").strip(),
                    "temporality": (it.get("temporality") or "").strip(),
                    "transcript_full_summary_short": str(it.get("transcript_full_summary") or "").strip()[:600],
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
                "bottom_line": (it.get("bottom_line") or "").strip(),
                "topic_primary": (it.get("topic_primary") or "").strip(),
                "topics": it.get("topics") or [],
                "text_source": (it.get("text_source") or "").strip(),
                "content_status": (it.get("content_status") or "").strip(),
                "transcript_processing": (it.get("transcript_processing") or "").strip(),
                "editorial_relevance": (it.get("editorial_relevance") or "").strip(),
                "temporality": (it.get("temporality") or "").strip(),
                "transcript_full_summary_short": (it.get("transcript_full_summary_short") or "").strip()[:600],
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
    yearly_meta = "\n".join(
        [
            f"target_year: {target_year}",
            f"monthly_rollups_found: {len(entries)}",
            f"enriched_rollups_used_total: {len([e for e in entries if (e.get('top_items') or [])])}",
            f"thin_rollups_used_total: {len([e for e in entries if not (e.get('top_items') or [])])}",
        ]
    )
    _write_run_metadata_artifact(report_dir, report_key, "yearly", yearly_meta)

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
    cybermed_max_items_per_channel = _safe_int("CYBERMED_MAX_ITEMS_PER_CHANNEL", 25)
    if report_key.strip().lower() == "cyberlurch":
        detail_items_per_day = _safe_int(
            "CYBERLURCH_DETAIL_ITEMS_PER_DAY",
            _safe_int("DETAIL_ITEMS_PER_DAY", 10),
        )
        detail_items_per_channel_max = _safe_int(
            "CYBERLURCH_DETAIL_ITEMS_PER_CHANNEL_MAX",
            _safe_int("DETAIL_ITEMS_PER_CHANNEL_MAX", 1),
        )
    else:
        detail_items_per_day = _safe_int("DETAIL_ITEMS_PER_DAY", 8)
        detail_items_per_channel_max = _safe_int("DETAIL_ITEMS_PER_CHANNEL_MAX", 3)

    overview_items_max = _safe_int("OVERVIEW_ITEMS_MAX", 25)
    sent_cooldown_hours = _safe_int("PUBMED_SENT_COOLDOWN_HOURS", 48)
    reconsider_unsent_hours = _safe_int("RECONSIDER_UNSENT_HOURS", 36)
    max_text_chars_per_item = _safe_int("MAX_TEXT_CHARS_PER_ITEM", 12000)
    pubmed_use_pmc_oa_fulltext = _env_bool("PUBMED_DEEPDIVE_USE_PMC_OA_FULLTEXT", True)
    pubmed_use_unpaywall_fulltext = _env_bool("PUBMED_DEEPDIVE_USE_UNPAYWALL_FULLTEXT", False)
    pubmed_fulltext_max_bytes = _safe_int("PUBMED_DEEPDIVE_FULLTEXT_MAX_BYTES", 25000000)
    pubmed_fulltext_max_chars = _safe_int("PUBMED_DEEPDIVE_FULLTEXT_MAX_CHARS", 30000)
    pubmed_fulltext_timeout_s = _safe_int("PUBMED_DEEPDIVE_FULLTEXT_TIMEOUT_S", 20)
    pubmed_unpaywall_min_chars = _safe_int("PUBMED_DEEPDIVE_UNPAYWALL_MIN_CHARS", 1500)
    pubmed_min_abstract_chars = _safe_int("PUBMED_DEEPDIVE_MIN_ABSTRACT_CHARS", 600)
    pubmed_min_fulltext_chars = _safe_int("PUBMED_DEEPDIVE_MIN_FULLTEXT_CHARS", 2000)
    unpaywall_email = (os.getenv("UNPAYWALL_EMAIL") or os.getenv("NCBI_EMAIL") or "").strip()
    unpaywall_enabled = pubmed_use_unpaywall_fulltext and bool(unpaywall_email)
    if pubmed_use_unpaywall_fulltext and not unpaywall_email:
        print("[unpaywall] WARN: UNPAYWALL_EMAIL not set; disabling Unpaywall OA enrichment.")

    state_path = (os.getenv("STATE_PATH", "state/processed_items.json") or "state/processed_items.json").strip()
    rollups_state_path = (os.getenv("ROLLUPS_STATE_PATH", "state/rollups.json") or "state/rollups.json").strip()
    retention_days = _safe_int("STATE_RETENTION_DAYS", 20)
    rollups_max_months = _safe_int("ROLLUPS_MAX_MONTHS", 24)
    cyberlurch_digest_state_path = (os.getenv("CYBERLURCH_DIGEST_STATE_PATH", "state/cyberlurch_digests.json") or "state/cyberlurch_digests.json").strip()
    cybermed_digest_state_path = (os.getenv("CYBERMED_DAILY_DIGEST_STATE_PATH", "state/cybermed_daily_digests.json") or "state/cybermed_daily_digests.json").strip()
    cybermed_digest_state_path = cybermed_digest_state_path or "state/cybermed_daily_digests.json"
    cybermed_digest_state_abs_path = str(Path(cybermed_digest_state_path).resolve())
    cyberlurch_digest_retention_days = _safe_int("CYBERLURCH_DIGEST_RETENTION_DAYS", 400)
    foamed_sources_path = (os.getenv("CYBERMED_FOAMED_SOURCES", "data/cybermed_foamed_sources.json") or "data/cybermed_foamed_sources.json").strip()
    foamed_auto_disable_enabled = _env_bool("FOAMED_AUTO_DISABLE", True)
    foamed_disable_after_403 = _safe_int("FOAMED_DISABLE_AFTER_403", 3)
    foamed_disable_days_403 = _safe_int("FOAMED_DISABLE_DAYS_403", 7)
    foamed_disable_after_404 = _safe_int("FOAMED_DISABLE_AFTER_404", 2)
    foamed_disable_days_404 = _safe_int("FOAMED_DISABLE_DAYS_404", 30)

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
    now_utc = datetime.now(timezone.utc)
    report_since_utc = now_utc - timedelta(hours=args.hours)

    print("=== NewsAgent2 run ===")
    print(f"[config] report_mode={report_mode} read_only={read_only_mode}")
    print(f"[config] report_key={report_key!r}")
    print(f"[config] report_title={report_title!r}")
    print(f"[config] report_subject={report_subject!r}")
    print(f"[config] channels_file={args.channels!r} hours={args.hours} (override={lookback_override is not None})")
    print(f"[config] report_dir={report_dir!r}")
    print(f"[config] report_language={report_language!r} report_profile={report_profile!r}")
    if report_key.strip().lower() == "cyberlurch":
        print(
            f"[models] cyberlurch_chunks={OPENAI_MODEL_CYBERLURCH_CHUNKS} "
            f"cyberlurch_overview={OPENAI_MODEL_CYBERLURCH_OVERVIEW} "
            f"cyberlurch_deepdive={OPENAI_MODEL_CYBERLURCH_DEEPDIVE}"
        )
        print(
            f"[models] cyberlurch_direct_digest={OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST} "
            f"cyberlurch_direct_digest_fallback={OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST_FALLBACK}"
        )
    provider = (os.getenv("YOUTUBE_TRANSCRIPT_PROVIDER", "none") or "none").strip().lower()
    if report_key.strip().lower()=="cyberlurch" and report_mode in {"weekly","monthly","yearly"} and not _env_bool("CYBERLURCH_MANAGED_TRANSCRIPTS_FOR_ROLLUPS", False):
        os.environ["YOUTUBE_TRANSCRIPT_PROVIDER"] = "none"
        provider = "none"
    api_key_present = bool((os.getenv("YOUTUBE_TRANSCRIPT_API_KEY") or "").strip())
    base_url_present = bool((os.getenv("YOUTUBE_TRANSCRIPT_API_BASE_URL") or "").strip())
    max_videos = max(0, _safe_int("MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN", 25))
    min_chars = max(1, _safe_int("MANAGED_TRANSCRIPT_MIN_CHARS", 300))
    if not _is_cybermed(report_key, report_profile):
        print(
            f"[transcript-provider] provider={provider} api_key_present={api_key_present} base_url_present={base_url_present} "
            f"max_videos={max_videos} min_chars={min_chars}"
        )
    print(f"[config] limits: MAX_ITEMS_PER_CHANNEL={max_items_per_channel}, DETAIL_ITEMS_PER_DAY={detail_items_per_day}, DETAIL_ITEMS_PER_CHANNEL_MAX={detail_items_per_channel_max}")
    yt_api_key_present = bool((os.getenv("YOUTUBE_API_KEY") or "").strip())
    yt_api_enabled = _env_bool("YOUTUBE_API_METADATA", True)
    print(f"[youtube-api] metadata_enabled={yt_api_enabled} api_key_present={yt_api_key_present}")
    print(f"[config] overview_items_max={overview_items_max}, max_text_chars_per_item={max_text_chars_per_item}")
    print(f"[config] pubmed_sent_cooldown_hours={sent_cooldown_hours}, reconsider_unsent_hours={reconsider_unsent_hours}")
    print(f"[state] path={state_path!r} retention_days={retention_days}")
    print(f"[rollups] path={rollups_state_path!r} max_months={rollups_max_months}")
    if _is_cybermed(report_key, report_profile):
        foamed_audit_enabled = _env_bool("FOAMED_AUDIT", False)
        foamed_audit_check_disabled = _env_bool("FOAMED_AUDIT_CHECK_DISABLED", False)
        foamed_article_fetch_enabled_cfg = _env_bool("FOAMED_ARTICLE_FETCH", False)
        foamed_article_fetch_max_per_run = _safe_int("FOAMED_ARTICLE_FETCH_MAX_PER_RUN", 25)
        foamed_render_fallback = _env_bool("FOAMED_RENDER_FALLBACK", False)
        print(f"[config] cybermed_limits: CYBERMED_MAX_ITEMS_PER_CHANNEL={cybermed_max_items_per_channel}")
        print(
            "[config] cybermed_foamed: "
            f"FOAMED_AUDIT={foamed_audit_enabled} "
            f"FOAMED_AUDIT_CHECK_DISABLED={foamed_audit_check_disabled} "
            f"FOAMED_ARTICLE_FETCH={foamed_article_fetch_enabled_cfg} "
            f"FOAMED_ARTICLE_FETCH_MAX_PER_RUN={foamed_article_fetch_max_per_run} "
            f"FOAMED_RENDER_FALLBACK={foamed_render_fallback}"
        )
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
    cadence = cyberlurch_cadence_profile(report_mode)

    foamed_sources: List[Dict[str, Any]] = []
    foamed_candidates: List[Dict[str, Any]] = []
    foamed_collected: List[Dict[str, Any]] = []
    foamed_screened_total = 0
    foamed_after_state = 0
    foamed_skipped_by_state = 0
    foamed_selection_stats: Dict[str, Any] = {}
    foamed_overview_items: List[Dict[str, Any]] = []
    foamed_top_picks: List[Dict[str, Any]] = []
    foamed_meta_stats: Dict[str, Any] = {}
    foamed_collection_stats: Dict[str, Any] = {}
    foamed_disabled_audit_stats: Dict[str, Any] = {}

    run_metadata = ""

    try:
        channels, channel_topics, topic_weights = load_channels_config(args.channels)
    except Exception as e:
        if report_language.lower().startswith("en"):
            overview = f"## Executive Summary\n\n**Error:** Failed to load channels configuration: `{e!r}`\n"
        else:
            overview = f"## Kurzüberblick\n\n**Fehler:** Konnte Channels-Konfiguration nicht laden: `{e!r}`\n"
        out_path = _report_output_path(report_dir, report_key, report_mode)
        md = to_markdown(
            [],
            overview,
            {},
            report_title=report_title,
            report_language=report_language,
            report_mode=report_mode,
            run_metadata=run_metadata,
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
    if _env_bool("FORCE_REPROCESS", False) and (os.getenv("YOUTUBE_TRANSCRIPT_PROVIDER") or "none").strip().lower() != "none":
        print("[cost] FORCE_REPROCESS=true may re-call managed transcript provider for already seen videos.")

    items: List[Dict[str, Any]] = []
    skipped_by_state = 0
    youtube_diag = YouTubeDiagnosticsCounters()
    youtube_diag.cyberlurch_digest_upserted_total = 0
    youtube_diag.cyberlurch_digest_pruned_total = 0
    youtube_diag.cyberlurch_digest_store_total = 0
    youtube_diag.cyberlurch_digest_invalid_records_removed_total = 0
    weekly_digest_items_total = 0
    weekly_digest_used_total = 0
    weekly_digest_supplemental_collection_items_total = 0
    weekly_digest_full_text_ratio = 0.0
    monthly_digest_items_total = 0
    monthly_digest_used_total = 0
    monthly_digest_supplemental_collection_items_total = 0
    monthly_digest_period_start = ""
    monthly_digest_period_end = ""
    weekly_digest_fallback_collection_used = False
    monthly_digest_fallback_collection_used = False
    digest_store_loaded_total = 0
    digest_store_selected_total = 0
    digest_store_used_as_primary = False
    digest_store_collection_skipped_due_to_primary = False
    digest_store_collection_fallback_used = False
    digest_store_collection_supplement_used = False
    cyberlurch_digest_invalid_records_removed_total = 0
    cyberlurch_digest_invalid_records_skipped_total = 0
    use_digest_store_primary = False
    collect_with_digest_supplement = False
    youtube_diag.yt_dlp_version = get_yt_dlp_version()
    youtube_diag.channels_file_used = args.channels

    # Cybermed observability (used to build an in-report transparency header).
    runtime_start = time.monotonic()
    runtime_pubmed_collect_seconds = 0.0
    runtime_pubmed_backfill_seconds = 0.0
    runtime_foamed_collect_seconds = 0.0
    runtime_foamed_article_fetch_seconds = 0.0
    runtime_foamed_rolling_audit_seconds = 0.0
    runtime_selection_seconds = 0.0
    runtime_summarization_seconds = 0.0
    runtime_report_render_seconds = 0.0
    runtime_email_seconds = 0.0
    is_cybermed_run = _is_cybermed(report_key, report_profile)
    qa_replay_requested = _env_bool("CYBERMED_QA_REPLAY_MODE", False)
    qa_replay_enabled = False
    qa_replay_safety_passed = False
    qa_replay_skipped_reason = ""
    qa_replay_state_bypass_pubmed_total = 0
    qa_replay_state_bypass_foamed_total = 0
    qa_replay_state_mutation_disabled = False
    qa_replay_email_disabled_confirmed = False
    if qa_replay_requested:
        event_name = (os.getenv("GITHUB_EVENT_NAME", "") or "").strip().lower()
        email_mode = (os.getenv("EMAIL_MODE", "") or "").strip().lower()
        send_email = (os.getenv("SEND_EMAIL", "") or "").strip()
        failed_checks: List[str] = []
        if not is_cybermed_run:
            failed_checks.append("report_key_not_cybermed")
        if email_mode != "none":
            failed_checks.append("email_mode_not_none")
        if send_email != "0":
            failed_checks.append("send_email_not_zero")
        if event_name not in {"workflow_dispatch", "manual"}:
            failed_checks.append("manual_context_unavailable")
        if report_mode != "daily" and event_name not in {"workflow_dispatch", "manual"}:
            failed_checks.append("report_mode_not_manual_or_daily")
        if event_name == "schedule":
            failed_checks.append("scheduled_delivery")
        qa_replay_safety_passed = not failed_checks
        if qa_replay_safety_passed:
            qa_replay_enabled = True
            qa_replay_state_mutation_disabled = True
            qa_replay_email_disabled_confirmed = True
            print("[cybermed-qa-replay] requested=1 safety=passed state_bypass=active state_mutation=disabled email_disabled=confirmed")
        else:
            qa_replay_skipped_reason = "safety_failed:" + ",".join(failed_checks)
            print(f"[cybermed-qa-replay] requested=1 safety=failed reason={qa_replay_skipped_reason} state_bypass=inactive")
    else:
        print("[cybermed-qa-replay] requested=0 state_bypass=inactive")
    pubmed_candidates_total = 0
    pubmed_skipped_by_state = 0
    pubmed_query_failures = 0
    pubmed_failed_channels: Set[str] = set()
    pubmed_candidates_by_channel: Dict[str, int] = {}
    pubmed_queries_used: List[Tuple[str, str]] = []  # (channel_name, term)
    all_pubmed_raw_items: List[Dict[str, Any]] = []
    pubmed_channel_completeness: Dict[str, Dict[str, Any]] = {}
    pubmed_state_skip_reasons: Dict[str, int] = {}
    channel_id_cache = _load_youtube_channel_id_cache()
    discovered_channel_ids: dict[str, str] = {}

    cybermed_weekly_diag: Dict[str, Any] = {}
    cybermed_weekly_digest_only = False
    cybermed_weekly_qa_fixture_mode = False
    cybermed_weekly_qa_fixture_requested = False
    cybermed_weekly_qa_fixture_safety_passed = False
    cybermed_weekly_qa_fixture_skipped_reason = ""
    cybermed_weekly_qa_fixture_path = ""
    if is_cybermed_run and report_mode == "weekly":
        cybermed_weekly_digest_only = True
        digest_store_path = (os.getenv("CYBERMED_DAILY_DIGEST_STATE_PATH", "state/cybermed_daily_digests.json") or "state/cybermed_daily_digests.json").strip()
        fixture_mode_requested = _env_bool("CYBERMED_WEEKLY_QA_FIXTURE_MODE", False)
        fixture_path = (os.getenv("CYBERMED_WEEKLY_QA_FIXTURE_PATH", "tests/fixtures/cybermed_weekly_digest_store_nonempty.json") or "").strip()
        cybermed_weekly_qa_fixture_requested = fixture_mode_requested
        cybermed_weekly_qa_fixture_path = fixture_path
        event_name = (os.getenv("GITHUB_EVENT_NAME", "") or "").strip().lower()
        fixture_safety = (
            fixture_mode_requested
            and (report_key.strip().lower() == "cybermed")
            and (report_mode == "weekly")
            and ((os.getenv("EMAIL_MODE", "") or "").strip().lower() == "none")
            and ((os.getenv("SEND_EMAIL", "") or "").strip() == "0")
            and (event_name in {"workflow_dispatch", "manual"} or event_name == "")
            and fixture_path != ""
        )
        cybermed_weekly_qa_fixture_safety_passed = fixture_safety
        if fixture_mode_requested and not fixture_safety:
            failed_checks: List[str] = []
            if (report_key.strip().lower() != "cybermed"):
                failed_checks.append("report_key_not_cybermed")
            if report_mode != "weekly":
                failed_checks.append("report_mode_not_weekly")
            if ((os.getenv("EMAIL_MODE", "") or "").strip().lower() != "none"):
                failed_checks.append("email_mode_not_none")
            if ((os.getenv("SEND_EMAIL", "") or "").strip() != "0"):
                failed_checks.append("send_email_not_zero")
            if not (event_name in {"workflow_dispatch", "manual"} or event_name == ""):
                failed_checks.append("event_not_manual")
            if fixture_path == "":
                failed_checks.append("fixture_path_empty")
            cybermed_weekly_qa_fixture_skipped_reason = "safety_failed:" + ",".join(failed_checks)
        if fixture_safety:
            cybermed_weekly_qa_fixture_mode = True
            digest_store_path = fixture_path
        store = load_cybermed_daily_digest_store(digest_store_path)
        daily = select_cybermed_daily_digests_for_week(store, datetime.now(tz=STO).date())
        summary = summarize_cybermed_weekly_digest_inputs(daily)
        digest_store_used_as_primary = True
        digest_store_collection_skipped_due_to_primary = True
        use_digest_store_primary = True
        digest_store_loaded_total = len(store.get("digests", []))
        digest_store_selected_total = len(daily)
        week_pubmed = []
        week_foamed = []
        week_deep = []
        week_top = []
        for d in daily:
            items_d = d.get("items") or {}
            week_pubmed.extend(items_d.get("pubmed") or [])
            week_foamed.extend(items_d.get("foamed") or [])
            week_deep.extend(d.get("deep_dives") or [])
            week_top.extend(d.get("top_picks") or [])
        merged = week_pubmed + week_foamed
        deduped, suppressed, suppressed_reasons = dedupe_weekly_digest_items(merged)
        pubmed_sorted = sorted([it for it in deduped if str(it.get("source_type") or it.get("source") or "").strip().lower() == "pubmed"], key=lambda x: (
            1 if bool(x.get("top_pick")) else 0,
            1 if bool(x.get("deep_dive_candidate")) else 0,
            {"a": 5, "b": 4, "c": 3, "d": 2, "e": 1}.get(str(x.get("evidence_strength_label") or "").strip().lower(), 0),
            int(x.get("practice_change_potential_1_5") or 0),
            int(x.get("clinical_relevance_1_5") or 0),
            {"high": 3, "moderate": 2, "low": 1}.get(str(x.get("text_confidence_label") or "").strip().lower(), 0),
            str(x.get("published_at") or ""),
        ), reverse=True)[:CYBERMED_WEEKLY_MAX_PUBMED]
        foamed_sorted = sorted([it for it in deduped if str(it.get("source_type") or it.get("source") or "").strip().lower() == "foamed"], key=lambda x: (
            1 if bool(x.get("top_pick")) else 0,
            {"core": 3, "important": 2, "optional": 1}.get(str(x.get("source_quality_label") or "").strip().lower(), 0),
            int(x.get("clinical_usefulness_1_5") or 0),
            int(x.get("practice_relevance_1_5") or 0),
            {"high": 3, "moderate": 2, "low": 1}.get(str(x.get("text_confidence_label") or "").strip().lower(), 0),
            str(x.get("published_at") or ""),
        ), reverse=True)[:CYBERMED_WEEKLY_MAX_FOAMED]
        selected_top_picks = sorted([it for it in deduped if bool(it.get("top_pick"))], key=lambda x: str(x.get("published_at") or ""), reverse=True)[:5]
        selected_deep_dives = sorted([it for it in deduped if bool(it.get("deep_dive_candidate"))], key=lambda x: str(x.get("published_at") or ""), reverse=True)[:WEEKLY_MAX_DEEP_DIVES]
        selected_deep_dive_ids = {
            str(d.get("item_id") or d.get("id") or d.get("pmid") or "").strip()
            for d in selected_deep_dives
            if str(d.get("item_id") or d.get("id") or d.get("pmid") or "").strip()
        }
        items = [
            _normalize_cybermed_weekly_digest_item(it, deep_dive_ids=selected_deep_dive_ids)
            for it in (pubmed_sorted + foamed_sorted)
        ]
        preview = []
        for it in (pubmed_sorted + foamed_sorted)[:10]:
            preview.append({
                "source_type": str(it.get("source_type") or it.get("source") or "").strip().lower(),
                "evidence_strength_label": str(it.get("evidence_strength_label") or "").strip(),
                "source_quality_label": str(it.get("source_quality_label") or "").strip(),
                "clinical_relevance_1_5": it.get("clinical_relevance_1_5"),
                "clinical_usefulness_1_5": it.get("clinical_usefulness_1_5"),
                "practice_change_potential_1_5": it.get("practice_change_potential_1_5"),
                "practice_relevance_1_5": it.get("practice_relevance_1_5"),
                "text_confidence_label": str(it.get("text_confidence_label") or "").strip(),
                "top_pick": bool(it.get("top_pick")),
                "deep_dive_candidate": bool(it.get("deep_dive_candidate")),
                "rank_bucket": "top_pick" if bool(it.get("top_pick")) else ("deep_dive" if bool(it.get("deep_dive_candidate")) else "overview"),
            })
        cybermed_weekly_diag = {
            "cybermed_weekly_from_daily_digests_enabled": True,
            "cybermed_weekly_digest_only_mode": True,
            "cybermed_weekly_collection_skipped": True,
            "cybermed_weekly_collection_skipped_reason": "weekly_from_daily_digests",
            "cybermed_weekly_digest_store_path": digest_store_path,
            "cybermed_weekly_ranking_enabled": True,
            "cybermed_weekly_qa_fixture_requested": cybermed_weekly_qa_fixture_requested,
            "cybermed_weekly_qa_fixture_mode": cybermed_weekly_qa_fixture_mode,
            "cybermed_weekly_qa_fixture_path": cybermed_weekly_qa_fixture_path,
            "cybermed_weekly_qa_fixture_safety_passed": cybermed_weekly_qa_fixture_safety_passed,
            "cybermed_weekly_qa_fixture_skipped_reason": cybermed_weekly_qa_fixture_skipped_reason,
            "cybermed_weekly_qa_fixture_state_mutation_disabled": cybermed_weekly_qa_fixture_mode,
            "cybermed_weekly_period_start": str(min([d.get("run_date") for d in daily], default="")),
            "cybermed_weekly_period_end": str(max([d.get("run_date") for d in daily], default="")),
            "cybermed_weekly_daily_digests_found_total": summary["daily_digests_found_total"],
            "cybermed_weekly_daily_digests_with_items_total": summary["daily_digests_with_items_total"],
            "cybermed_weekly_pubmed_items_loaded_total": summary["pubmed_items_loaded_total"],
            "cybermed_weekly_foamed_items_loaded_total": summary["foamed_items_loaded_total"],
            "cybermed_weekly_deep_dives_loaded_total": summary["deep_dives_loaded_total"],
            "cybermed_weekly_top_picks_loaded_total": summary["top_picks_loaded_total"],
            "cybermed_weekly_duplicates_suppressed_total": suppressed,
            "cybermed_weekly_duplicates_suppressed_reason_counts": suppressed_reasons,
            "cybermed_weekly_pubmed_items_selected_total": len(pubmed_sorted),
            "cybermed_weekly_foamed_items_selected_total": len(foamed_sorted),
            "cybermed_weekly_deep_dives_selected_total": len(selected_deep_dives),
            "cybermed_weekly_top_picks_selected_total": len(selected_top_picks),
            "cybermed_weekly_ranking_reason_counts": {"top_pick": len([x for x in deduped if x.get("top_pick")]), "deep_dive_candidate": len([x for x in deduped if x.get("deep_dive_candidate")])},
            "cybermed_weekly_items_preview_sanitized": preview,
            "cybermed_weekly_empty_reason": "" if items else ("No Cybermed daily digests were available for this weekly period." if not daily else "Daily digests were processed, but no items passed selection this week."),
        }

    if not is_cybermed_run and report_key.strip().lower() == "cyberlurch" and report_mode in {"weekly", "monthly"}:
        use_digest = _env_bool("CYBERLURCH_WEEKLY_USE_DIGEST_STORE" if report_mode=="weekly" else "CYBERLURCH_MONTHLY_USE_DIGEST_STORE", True)
        collect_if_digest = _env_bool("CYBERLURCH_WEEKLY_COLLECT_IF_DIGEST_AVAILABLE" if report_mode=="weekly" else "CYBERLURCH_MONTHLY_COLLECT_IF_DIGEST_AVAILABLE", False)
        supplement = _env_bool("CYBERLURCH_WEEKLY_SUPPLEMENT_WITH_COLLECTION" if report_mode=="weekly" else "CYBERLURCH_MONTHLY_SUPPLEMENT_WITH_COLLECTION", False)
        if use_digest:
            dstate = _load_cyberlurch_digest_state(cyberlurch_digest_state_path)
            dstate, removed_invalid = sanitize_cyberlurch_digest_state(dstate)
            cyberlurch_digest_invalid_records_removed_total += removed_invalid
            digest_store_loaded_total = len(dstate.get("digests", []))
            now = datetime.now(timezone.utc)
            selected = []
            for d in dstate.get("digests", []):
                if not _is_valid_cyberlurch_digest_record(d):
                    continue
                pub = _parse_iso_utc(str(d.get("published_at") or ""))
                if not pub:
                    continue
                if report_mode == "weekly" and pub >= now - timedelta(days=7):
                    selected.append(d)
                elif report_mode == "monthly":
                    mk = determine_monthly_rollup_month(datetime.now(tz=STO), os.getenv("GITHUB_EVENT_NAME", ""), os.getenv("ROLLUP_MONTH_OVERRIDE"))
                    monthly_digest_period_start = f"{mk}-01"
                    monthly_digest_period_end = f"{mk}-31"
                    if pub.astimezone(STO).strftime("%Y-%m") == mk:
                        selected.append(d)
            selected = sorted(selected, key=lambda x: _parse_iso_utc(str(x.get("published_at") or "")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            digest_store_selected_total = len(selected)
            weekly_digest_items_total = len(selected) if report_mode == "weekly" else 0
            monthly_digest_items_total = len(selected) if report_mode == "monthly" else 0
            full_text = sum(1 for d in selected if str(d.get("text_source") or "").strip() not in {"", "metadata_only"})
            weekly_digest_full_text_ratio = (full_text / len(selected)) if (report_mode == "weekly" and selected) else 0.0
            if selected:
                items = [_item_from_digest_record(d) for d in selected]
                use_digest_store_primary = True
                digest_store_used_as_primary = True
                weekly_digest_used_total = len(items) if report_mode == "weekly" else 0
                monthly_digest_used_total = len(items) if report_mode == "monthly" else 0
                if supplement or collect_if_digest:
                    collect_with_digest_supplement = True
                    digest_store_collection_supplement_used = True
                    print(f"[digest-store] {report_mode} supplement collection enabled; collecting supplemental items.")
                else:
                    digest_store_collection_skipped_due_to_primary = True
                    print(f"[digest-store] {report_mode} using {len(selected)} digest records as primary source; skipping fresh collection.")
            else:
                weekly_digest_fallback_collection_used = report_mode == "weekly"
                monthly_digest_fallback_collection_used = report_mode == "monthly"
                digest_store_collection_fallback_used = True
                print(f"[digest-store] {report_mode} no digest records available; using collection fallback.")

    if (not use_digest_store_primary) or collect_with_digest_supplement:
        for ch in channels:
            cname = ch["name"]
            source = (ch.get("source") or "youtube").strip().lower()
            curl = (ch.get("url") or "").strip()
            query = (ch.get("query") or "").strip()
            is_poplar = _is_poplar_channel(ch)
            is_blackscout = _is_blackscout_channel(ch)

            if source == "youtube":
                youtube_diag.channels_attempted_total += 1
                cache_key = _channel_cache_key(ch)
                cached_channel_id = str(channel_id_cache.get("channels", {}).get(cache_key, {}).get("channel_id") or "").strip()
                if cached_channel_id and not str(ch.get("channel_id") or "").strip():
                    ch = dict(ch)
                    ch["channel_id"] = cached_channel_id
                vids: List[Dict[str, Any]] = []
                ytdlp_failed = False
                used_rss_primary = False
                if str(ch.get("channel_id") or "").strip():
                    youtube_diag.rss_primary_attempted_total += 1
                    try:
                        rss_primary = list_recent_videos_rss(
                            ch, hours=args.hours, max_items=max_items_per_channel, diagnostics=youtube_diag.__dict__
                        )
                        if rss_primary:
                            youtube_diag.rss_primary_success_total += 1
                            youtube_diag.channels_success_total += 1
                            vids = rss_primary
                            used_rss_primary = True
                        else:
                            youtube_diag.rss_primary_empty_total += 1
                    except Exception:
                        youtube_diag.rss_primary_error_total += 1

                if not used_rss_primary:
                    try:
                        vids = list_recent_videos(
                            curl,
                            hours=args.hours,
                            max_items=max_items_per_channel,
                            diagnostics=youtube_diag.__dict__,
                            force_full_metadata=force_ytdlp_full_metadata if "force_ytdlp_full_metadata" in locals() else False,
                        )
                        youtube_diag.channels_success_total += 1
                    except Exception as e:
                        ytdlp_failed = True
                        youtube_diag.channels_error_total += 1
                        print(f"[collect] ERROR source=youtube: list_recent_videos failed err_type={type(e).__name__}")

                if _env_bool("YOUTUBE_METADATA_FALLBACK", True) and (ytdlp_failed or not vids):
                    rss_items = list_recent_videos_rss(
                        ch,
                        hours=args.hours,
                        max_items=max_items_per_channel,
                        diagnostics=youtube_diag.__dict__,
                    )
                    if rss_items and ytdlp_failed:
                        youtube_diag.channels_success_total += 1
                    vids = _dedupe_videos_by_id(list(vids) + list(rss_items))

                if not vids:
                    continue

                api_key = (os.getenv("YOUTUBE_API_KEY") or "").strip()
                api_enabled = _env_bool("YOUTUBE_API_METADATA", True) and bool(api_key)
                force_ytdlp_full_metadata = _env_bool("YTDLP_FULL_METADATA_ENRICHMENT", False)
                api_max_videos = max(1, _safe_int("YOUTUBE_API_MAX_VIDEOS_PER_RUN", 150))
                snippets: dict[str, dict[str, Any]] = {}
                if api_enabled:
                    id_batch = [str(v.get("id") or "").strip() for v in vids if str(v.get("id") or "").strip()][:api_max_videos]
                    for i in range(0, len(id_batch), 50):
                        snippets.update(fetch_video_snippets(id_batch[i : i + 50], api_key, youtube_diag.__dict__))

                for v in vids:
                    snippet = snippets.get(str(v.get("id") or "").strip()) or {}
                    if snippet:
                        if snippet.get("title"):
                            v["title"] = snippet["title"]
                        if snippet.get("description"):
                            v["description"] = snippet["description"]
                        if snippet.get("channel"):
                            v["channel"] = snippet["channel"]
                        if snippet.get("published_at"):
                            v["published_at"] = _parse_iso_utc(snippet.get("published_at")) or v.get("published_at")
                        pub_after = v.get("published_at")
                        if isinstance(pub_after, datetime):
                            if pub_after.tzinfo is None:
                                pub_after = pub_after.replace(tzinfo=timezone.utc)
                            if pub_after < report_since_utc:
                                youtube_diag.youtube_api_post_enrichment_date_skipped_total = int(
                                    getattr(youtube_diag, "youtube_api_post_enrichment_date_skipped_total", 0)
                                ) + 1
                                v["_skip_after_enrichment"] = True
                        channel_id = str(snippet.get("channel_id") or "").strip()
                        if channel_id.startswith("UC"):
                            discovered_channel_ids[cache_key] = channel_id
                            youtube_diag.youtube_api_channel_ids_discovered_total += 1
                            channel_id_cache.setdefault("channels", {})[cache_key] = {
                                "channel_id": channel_id,
                                "source": "youtube_api",
                                "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                            }

                for v in vids:
                    if v.get("_skip_after_enrichment"):
                        continue
                    vid = str(v.get("id") or "").strip()
                    if not vid:
                        continue

                    force_reprocess = _env_bool("FORCE_REPROCESS", False)
                    already_processed = is_processed(state, report_key, "youtube", vid)
                    if not read_only_mode and already_processed and not force_reprocess:
                        skipped_by_state += 1
                        continue

                    youtube_diag.videos_total += 1
                    if is_poplar:
                        youtube_diag.poplar_total += 1
                    if is_blackscout:
                        youtube_diag.blackscout_total += 1
                    desc = (v.get("description") or "").strip()
                    allow_managed_reprocess = _env_bool("FORCE_REPROCESS_ALLOW_MANAGED_TRANSCRIPTS", False)
                    providers_override = None
                    if force_reprocess and not allow_managed_reprocess and already_processed:
                        providers_override = "youtube_transcript_api,description,timedtext,yt_dlp_captions,metadata_only"
                        youtube_diag.managed_transcript_skipped_force_reprocess_cost_guard_total = int(
                            getattr(youtube_diag, "managed_transcript_skipped_force_reprocess_cost_guard_total", 0)
                        ) + 1
                    provider_result = fetch_video_content(
                        video_id=vid,
                        video_url=(v.get("url") or "").strip() or f"https://www.youtube.com/watch?v={vid}",
                        description=desc,
                        diagnostics=youtube_diag.__dict__,
                        providers_override=providers_override,
                    )
                    text = (provider_result.text or "").strip()
                    text_source = provider_result.source
                    content_status = "full_text" if text_source in {"managed_transcript", "youtube_transcript_api", "description", "timedtext", "yt_dlp_captions"} and bool(text) else "metadata_only"
                    if not text:
                        youtube_diag.metadata_only_total += 1
                        text_source = "metadata_only"
                        text = _metadata_only_text(
                            title=(v.get("title") or "").strip(),
                            channel=cname,
                            published_at=v.get("published_at"),
                        )

                    full_text_for_processing = ""
                    if text_source == "managed_transcript" and text:
                        full_text_for_processing = text
                    if len(text) > max_text_chars_per_item:
                        text = text[:max_text_chars_per_item].rstrip()

                    item_payload = {
                            "source": "youtube",
                            "id": vid,
                            "channel": cname,
                            "title": (v.get("title") or "").strip(),
                            "url": (v.get("url") or "").strip(),
                            "published_at": v.get("published_at"),
                            "description": desc,
                            "text": text,
                            "content_status": content_status,
                            "text_source": text_source,
                        }
                    if full_text_for_processing:
                        item_payload["_full_text_for_processing"] = full_text_for_processing
                    items.append(item_payload)
                    if content_status == "full_text":
                        youtube_diag.full_text_items_total += 1
                    else:
                        youtube_diag.metadata_only_items_total += 1

            elif source == "pubmed":
                if is_cybermed_run:
                    pubmed_queries_used.append((cname, query))
                pubmed_max_items = cybermed_max_items_per_channel if is_cybermed_run else max_items_per_channel
                try:
                    pubmed_collect_start = time.monotonic()
                    if is_cybermed_run:
                        arts, pubmed_meta = search_recent_pubmed(term=query, hours=args.hours, max_items=pubmed_max_items, return_metadata=True)
                    else:
                        arts = search_recent_pubmed(term=query, hours=args.hours, max_items=pubmed_max_items)
                        pubmed_meta = {}
                    if is_cybermed_run:
                        runtime_pubmed_collect_seconds += max(0.0, time.monotonic() - pubmed_collect_start)
                except Exception as e:
                    print(f"[collect] ERROR source=pubmed channel={cname!r}: search_recent_pubmed failed: {e!r}")
                    if is_cybermed_run:
                        pubmed_query_failures += 1
                        pubmed_failed_channels.add(cname)
                    continue

                if is_cybermed_run:
                    pubmed_candidates_total += len(arts)
                    pubmed_candidates_by_channel[cname] = len(arts)
                    all_pubmed_raw_items.extend(arts)
                    pubmed_channel_completeness[cname] = {
                        "raw_count": int(len(arts)),
                        "esearch_count_total": int(pubmed_meta.get("esearch_count_total", 0) or 0),
                        "retmax": int(pubmed_meta.get("retmax", pubmed_max_items) or pubmed_max_items),
                        "idlist_count": int(pubmed_meta.get("idlist_count", 0) or 0),
                        "parsed_article_count": int(pubmed_meta.get("parsed_article_count", len(arts)) or len(arts)),
                        "possibly_truncated": bool(pubmed_meta.get("possibly_truncated", False)),
                        "publication_types_count": len([it for it in arts if it.get("publication_types")]),
                        "mesh_headings_count": len([it for it in arts if it.get("mesh_headings")]),
                        "keywords_count": len([it for it in arts if it.get("keywords")]),
                        "abstract_sections_count": len([it for it in arts if it.get("abstract_sections")]),
                        "abstract_count": len([it for it in arts if (it.get("abstract") or "").strip()]),
                        "doi_count": len([it for it in arts if (it.get("doi") or "").strip()]),
                    }

                for a in arts:
                    pmid = str(a.get("id") or "").strip()
                    if not pmid:
                        continue

                    skip_by_state = False
                    if not read_only_mode and not qa_replay_enabled:
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
                    if qa_replay_enabled and is_cybermed_run:
                        qa_replay_state_bypass_pubmed_total += 1

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
                            "pmcid": (a.get("pmcid") or "").strip(),
                            "pii": (a.get("pii") or "").strip(),
                            "text": text,
                            "abstract": (a.get("abstract") or "").strip(),
                            "publication_types": list(a.get("publication_types") or []),
                            "mesh_headings": list(a.get("mesh_headings") or []),
                            "keywords": list(a.get("keywords") or []),
                            "abstract_sections": list(a.get("abstract_sections") or []),
                            "evidence_tags": list(a.get("evidence_tags") or []),
                        }
                    )

            else:
                print(f"[collect] WARN: unknown source={source!r} for channel={cname!r} -> skipping")
                continue

    _save_youtube_channel_id_cache(channel_id_cache, read_only_mode=read_only_mode)
    _write_channel_id_suggestions(discovered_channel_ids, report_dir)
    youtube_diag.managed_transcript_billable_success_estimate = youtube_diag.managed_transcript_success_total
    if report_key.strip().lower() == "cyberlurch" or youtube_diag.channels_attempted_total > 0:
        print(f"[collect] youtube_diagnostics: {youtube_diag.to_log_line()}")

    run_metadata = ""
    if report_key.strip().lower() == "cyberlurch":
        run_metadata = youtube_diag.to_metadata_section()

    if is_cybermed_run and not cybermed_weekly_digest_only:
        foamed_sources = load_foamed_sources_config(foamed_sources_path)
        if foamed_sources:
            now_utc = datetime.now(timezone.utc)
            foamed_sources_filtered, filter_stats = _filter_disabled_foamed_sources(
                foamed_sources,
                state,
                now_utc,
                auto_disable_enabled=foamed_auto_disable_enabled,
            )
            foamed_collect_start = time.monotonic()
            foamed_collected, foamed_collection_stats = collect_foamed_items(foamed_sources_filtered, now_utc, lookback_hours=args.hours)
            runtime_foamed_collect_seconds += max(0.0, time.monotonic() - foamed_collect_start)
            runtime_foamed_article_fetch_seconds += float((foamed_collection_stats or {}).get("article_fetch_duration_seconds", 0.0) or 0.0)
            foamed_screened_total = len(foamed_collected)
            auto_disable_meta = _update_foamed_health_state(
                state,
                (foamed_collection_stats or {}).get("per_source"),
                now_utc,
                auto_disable_enabled=foamed_auto_disable_enabled,
                disable_after_403=foamed_disable_after_403,
                disable_days_403=foamed_disable_days_403,
                disable_after_404=foamed_disable_after_404,
                disable_days_404=foamed_disable_days_404,
                source_names={(s.get("name") or "").strip() for s in foamed_sources},
            )
            foamed_collection_stats = dict(foamed_collection_stats or {})
            foamed_collection_stats["auto_disable"] = {
                "enabled": bool(foamed_auto_disable_enabled),
                **filter_stats,
                **auto_disable_meta,
            }

            for it in foamed_collected:
                iid = str(it.get("id") or it.get("url") or "").strip()
                if not iid:
                    continue
                if not read_only_mode and not qa_replay_enabled and is_processed(state, report_key, "foamed", iid):
                    foamed_skipped_by_state += 1
                    continue
                if qa_replay_enabled:
                    qa_replay_state_bypass_foamed_total += 1

                text_val = (it.get("text") or "").strip()
                if len(text_val) > max_text_chars_per_item:
                    text_val = text_val[:max_text_chars_per_item].rstrip()
                    it["text"] = text_val

                foamed_candidates.append(it)

            foamed_after_state = len(foamed_candidates)
            items.extend(foamed_candidates)

            rolling_days = _safe_int("FOAMED_ROLLING_AUDIT_DAYS", 0)
            heavy_audit_mode = _env_bool("CYBERMED_HEAVY_AUDIT_MODE", False)
            foamed_rolling_diag: Dict[str, Any] = {
                "foamed_rolling_audit_requested_days": rolling_days,
                "foamed_rolling_audit_enabled": False,
                "foamed_rolling_audit_days": rolling_days,
                "foamed_rolling_audit_skipped_reason": "",
            }
            if rolling_days > 0 and not heavy_audit_mode:
                foamed_rolling_diag["foamed_rolling_audit_skipped_reason"] = "heavy_audit_mode_disabled"
            if rolling_days > 0 and heavy_audit_mode:
                prev_max = os.getenv("FOAMED_ARTICLE_FETCH_MAX_PER_RUN")
                os.environ["FOAMED_ARTICLE_FETCH_MAX_PER_RUN"] = str(_safe_int("FOAMED_ROLLING_AUDIT_FETCH_MAX_PER_RUN", 60))
                try:
                    rolling_start = time.monotonic()
                    rolling_items, rolling_stats = collect_foamed_items(foamed_sources, now_utc, lookback_hours=rolling_days * 24)
                    runtime_foamed_rolling_audit_seconds += max(0.0, time.monotonic() - rolling_start)
                finally:
                    if prev_max is None:
                        os.environ.pop("FOAMED_ARTICLE_FETCH_MAX_PER_RUN", None)
                    else:
                        os.environ["FOAMED_ARTICLE_FETCH_MAX_PER_RUN"] = prev_max
                min_chars = _safe_int("FOAMED_MIN_USABLE_TEXT_CHARS", 400)
                bysrc=[]
                productive=0
                for sm in list((rolling_stats.get("foamed_source_strategy_summary") or [])):
                    n=str((sm or {}).get("name") or "")
                    src_items=[it for it in rolling_items if str(it.get("foamed_source") or "")==n]
                    seen=len(src_items)
                    full=sum(1 for it in src_items if str(it.get("final_content_source") or it.get("content_source") or "")=="article_full_text")
                    exc=sum(1 for it in src_items if str(it.get("final_content_source") or it.get("content_source") or "")=="article_excerpt")
                    usable=sum(1 for it in src_items if str(it.get("final_content_source") or it.get("content_source") or "") in {"article_full_text","article_excerpt","rss_full_content","html_content"} and len(str(it.get("text") or "").strip())>=min_chars)
                    st=str((sm or {}).get("source_status") or "")
                    prod=usable>=1 and st not in {"blocked","stale_or_broken_url","audit_only"}
                    if prod: productive+=1
                    bysrc.append({"name":n,"domain_group":str((sm or {}).get("domain_group") or "mixed"),"priority_tier":str((sm or {}).get("priority_tier") or "2 important"),"extraction_strategy":str((sm or {}).get("extraction_strategy") or ""),"source_status":st,"items_seen_30d":seen,"items_fulltext_30d":full,"items_excerpt_30d":exc,"items_usable_text_30d":usable,"article_fetch_success_30d":int((sm or {}).get("article_fetch_success",0) or 0),"last_seen_date":None,"blocked_or_failed_reason":(st if st in {"blocked","stale_or_broken_url"} else ""),"productive_source":prod})
                seen_total=sum(x['items_seen_30d'] for x in bysrc)
                full_total=sum(x['items_fulltext_30d'] for x in bysrc)
                excerpt_total=sum(x['items_excerpt_30d'] for x in bysrc)
                usable_total=sum(x['items_usable_text_30d'] for x in bysrc)
                rolling_total=len(bysrc)
                foamed_rolling_diag={"foamed_rolling_audit_enabled":True,"foamed_rolling_audit_days":rolling_days,"foamed_rolling_sources_total":rolling_total,"foamed_rolling_sources_with_recent_items_total":sum(1 for x in bysrc if x['items_seen_30d']>0),"foamed_rolling_sources_with_fulltext_items_total":sum(1 for x in bysrc if x['items_fulltext_30d']>0),"foamed_rolling_sources_blocked_total":sum(1 for x in bysrc if x['source_status']=='blocked'),"foamed_rolling_sources_broken_total":sum(1 for x in bysrc if x['source_status']=='stale_or_broken_url'),"foamed_rolling_sources_no_recent_content_total":sum(1 for x in bysrc if x['items_seen_30d']==0),"foamed_rolling_productive_sources_total":productive,"foamed_rolling_productive_sources_pct":round((productive/max(1,rolling_total))*100,1),"foamed_rolling_items_seen_total":seen_total,"foamed_rolling_items_fulltext_total":full_total,"foamed_rolling_items_excerpt_total":excerpt_total,"foamed_rolling_items_usable_text_total":usable_total,"foamed_rolling_items_fulltext_pct":round((full_total/max(1,seen_total))*100,1),"foamed_rolling_items_usable_text_pct":round((usable_total/max(1,seen_total))*100,1),"foamed_rolling_by_source_summary":bysrc}
                foamed_rolling_diag["foamed_rolling_audit_requested_days"] = rolling_days
                foamed_rolling_diag["foamed_rolling_audit_skipped_reason"] = ""
            if _env_bool("FOAMED_AUDIT", False) and _env_bool("FOAMED_AUDIT_CHECK_DISABLED", False):
                enabled_names = {(s.get("name") or "").strip() for s in foamed_sources_filtered if isinstance(s, dict)}
                disabled_cfg = [s for s in foamed_sources if isinstance(s, dict) and (s.get("name") or "").strip() not in enabled_names]
                disabled_items, disabled_stats = collect_foamed_items(disabled_cfg, now_utc, lookback_hours=args.hours)
                _ = disabled_items
                per_source_disabled = disabled_stats.get("per_source") or {}
                summary = []
                reachable = 0
                blocked = 0
                for src_cfg in disabled_cfg:
                    name = str((src_cfg or {}).get("name") or "").strip()
                    if not name:
                        continue
                    st = per_source_disabled.get(name) if isinstance(per_source_disabled, dict) else {}
                    hs = ((state.get("foamed_source_health") or {}) if isinstance(state, dict) else {}).get(name, {})
                    if isinstance(st, dict) and str(st.get("health") or "") in {"ok_rss", "ok_html"}:
                        reachable += 1
                    else:
                        blocked += 1
                    summary.append({
                        "name": name,
                        "last_health": str((hs or {}).get("last_health") or ""),
                        "disabled_until_utc": str((hs or {}).get("disabled_until_utc") or (hs or {}).get("disabled_until") or ""),
                        "audit_health": str((st or {}).get("health") or ""),
                        "source_status": str((st or {}).get("source_status") or ""),
                        "alternative_path": str((st or {}).get("alternative_path") or "none"),
                        "discovery_content_mode": str((st or {}).get("discovery_content_mode") or (st or {}).get("content_mode") or "unknown"),
                        "final_content_source": str((st or {}).get("final_content_source") or (st or {}).get("content_mode") or "unknown"),
                        "feed_status_code": int((st or {}).get("feed_status_code", 0) or 0),
                        "homepage_status_code": int((st or {}).get("homepage_status_code", 0) or 0),
                        "content_mode": str((st or {}).get("content_mode") or "unknown"),
                        "candidates_found": int((st or {}).get("candidates_found", 0) or 0),
                        "kept_in_window_count": int((st or {}).get("kept_last24h", 0) or 0),
                        "wp_rest_available": bool((st or {}).get("wp_rest_available", False)),
                        "wp_rest_items_seen": int((st or {}).get("wp_rest_items_seen", 0) or 0),
                        "wp_rest_items_in_window": int((st or {}).get("wp_rest_items_in_window", 0) or 0),
                        "sitemap_available": bool((st or {}).get("sitemap_available", False)),
                        "sitemap_items_seen": int((st or {}).get("sitemap_items_seen", 0) or 0),
                        "sitemap_items_in_window": int((st or {}).get("sitemap_items_in_window", 0) or 0),
                        "completeness_warning": list((((st or {}).get("audit") or {}).get("completeness_warning") or (["source_unavailable"] if str((st or {}).get("health") or "") not in {"ok_rss", "ok_html"} else []))),
                    })
                foamed_disabled_audit_stats = {
                    "foamed_disabled_audit_enabled": True,
                    "foamed_disabled_sources_checked_total": len(summary),
                    "foamed_disabled_sources_reachable_total": reachable,
                    "foamed_disabled_sources_still_blocked_total": blocked,
                    "foamed_disabled_audit_summary": summary[:10],
                }
            elif _env_bool("FOAMED_AUDIT_CHECK_DISABLED", False):
                foamed_disabled_audit_stats = {
                    "foamed_disabled_audit_enabled": True,
                    "foamed_disabled_audit_not_implemented_reason": "FOAMED_AUDIT must be enabled to run disabled-source audit checks.",
                }
        else:
            print("[foamed] WARN: no FOAMed sources configured; skipping FOAMed collection.")
            if _env_bool("FOAMED_AUDIT_CHECK_DISABLED", False):
                foamed_disabled_audit_stats = {
                    "foamed_disabled_audit_enabled": True,
                    "foamed_disabled_audit_not_implemented_reason": "No FOAMed sources configured for disabled-source audit checks.",
                }

    items = _dedupe_items(items)
    items_all_new = list(items)
    if use_digest_store_primary and report_mode in {"weekly", "monthly"} and not collect_with_digest_supplement:
        print(f"[digest-store] {report_mode} loaded {len(items_all_new)} digest item(s) for report.")
    else:
        print(f"[collect] Collected {len(items_all_new)} new unique item(s). (skipped_by_state={skipped_by_state})")

    # Cybermed selection policy (PubMed only): select a subset for inclusion in the report,
    # while still marking all newly screened items as processed for memory.
    selection_stats: Dict[str, Any] = {}
    pubmed_new_items: List[Dict[str, Any]] = []
    pubmed_overview_items: List[Dict[str, Any]] = []
    pubmed_deep_dive_items: List[Dict[str, Any]] = []

    selection_result = None

    if is_cybermed_run and cybermed_weekly_digest_only:
        pubmed_overview_items = [it for it in items_all_new if (it.get("source") or "").strip().lower() == "pubmed"]
        foamed_overview_items = [it for it in items_all_new if (it.get("source") or "").strip().lower() == "foamed"]
        pubmed_deep_dive_items = [it for it in pubmed_overview_items if bool(it.get("cybermed_deep_dive"))]
        selection_stats = {
            "enabled": True,
            "mode": "weekly_digest_only",
            "selector_bypassed": True,
            "included_overview": len(pubmed_overview_items),
            "included_deep_dives": len(pubmed_deep_dive_items),
            "foamed_included_overview": len(foamed_overview_items),
        }
    elif is_cybermed_run:
        selection_start = time.monotonic()
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
        runtime_selection_seconds += max(0.0, time.monotonic() - selection_start)
    else:
        items = items_all_new

    if is_cybermed_run and pubmed_deep_dive_items:
        pubmed_deep_dive_items = _select_pubmed_deep_dives_with_content(
            pubmed_deep_dive_items,
            deep_dive_limit=deep_dive_limit,
            use_pmc_oa_fulltext=pubmed_use_pmc_oa_fulltext,
            use_unpaywall_fulltext=unpaywall_enabled,
            unpaywall_email=unpaywall_email,
            min_abstract_chars=pubmed_min_abstract_chars,
            min_fulltext_chars=pubmed_min_fulltext_chars,
            fulltext_timeout_s=pubmed_fulltext_timeout_s,
            fulltext_max_bytes=pubmed_fulltext_max_bytes,
            fulltext_max_chars=pubmed_fulltext_max_chars,
            unpaywall_min_chars=pubmed_unpaywall_min_chars,
        )

    # Cybermed in-report transparency header (MUST be based on the real pipeline).
    cybermed_meta_block = ""
    cybermed_run_stats: Dict[str, Any] = {}
    cybermed_diagnostics_payload: Dict[str, Any] = {}
    if is_cybermed_run:
        pubmed_selected = len(pubmed_overview_items)
        pubmed_new_unique = len(pubmed_new_items)

        # PubMed date filtering is applied using UTC date boundaries (YYYY/MM/DD), not hour-resolution.
        pubmed_now_utc = datetime.now(timezone.utc)
        pubmed_since_utc = pubmed_now_utc - timedelta(hours=args.hours)
        pubmed_datetype = (os.getenv("PUBMED_DATE_TYPE", "pdat") or "pdat").strip().lower()
        mindate = _date_yyyymmdd_utc(pubmed_since_utc)
        maxdate = _date_yyyymmdd_utc(pubmed_now_utc)

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
        selection_count_only = {
            "cybermed_overview_candidates_total": len(pubmed_new_items),
            "cybermed_overview_selected_total": len(pubmed_overview_items),
            "cybermed_deep_dive_candidates_total": len(pubmed_deep_dive_items),
            "cybermed_deep_dive_selected_total": min(len(pubmed_deep_dive_items), max(0, deep_dive_limit)),
            "cybermed_top_pick_total": len([it for it in pubmed_overview_items if it.get("top_pick")]),
            "cybermed_foamed_candidates_total": len(foamed_candidates),
            "cybermed_foamed_selected_total": len(foamed_overview_items),
            "cybermed_foamed_top_pick_total": len(foamed_top_picks),
        }
        pubmed_per_channel = []
        for cname, _term in pubmed_queries_used:
            topic = ""
            for cfg in channels:
                if (cfg.get("name") or "").strip() == cname:
                    topic = (cfg.get("topic") or "").strip()
                    break
            pubmed_per_channel.append(
                {
                    "name": cname,
                    "topic": topic,
                    "raw_count": int(pubmed_candidates_by_channel.get(cname, 0)),
                    "esearch_count_total": int((pubmed_channel_completeness.get(cname, {}) or {}).get("esearch_count_total", 0) or 0),
                    "retmax": int((pubmed_channel_completeness.get(cname, {}) or {}).get("retmax", 0) or 0),
                    "idlist_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("idlist_count", 0) or 0),
                    "parsed_article_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("parsed_article_count", 0) or 0),
                    "possibly_truncated": bool((pubmed_channel_completeness.get(cname, {}) or {}).get("possibly_truncated", False)),
                    "publication_types_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("publication_types_count", 0) or 0),
                    "mesh_headings_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("mesh_headings_count", 0) or 0),
                    "keywords_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("keywords_count", 0) or 0),
                    "abstract_sections_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("abstract_sections_count", 0) or 0),
                    "abstract_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("abstract_count", 0) or 0),
                    "doi_count": int((pubmed_channel_completeness.get(cname, {}) or {}).get("doi_count", 0) or 0),
                    "accepted_count": int(pubmed_candidates_by_channel.get(cname, 0)),
                    "selected_count": int(
                        len([it for it in pubmed_overview_items if (it.get("channel") or "").strip() == cname])
                    ),
                    **({"failure_class": "pubmed_query_failed"} if cname in pubmed_failed_channels else {}),
                }
            )
        foamed_per_source = []
        for source_name, source_stats in (foamed_meta_stats.get("per_source") or {}).items():
            if not isinstance(source_stats, dict):
                continue
            error_val = source_stats.get("error")
            error_class = ""
            if isinstance(error_val, BaseException):
                error_class = type(error_val).__name__
            elif isinstance(error_val, str) and error_val.strip():
                error_class = error_val.split(":", 1)[0].strip()[:80]
            foamed_per_source.append(
                {
                    "name": source_name,
                    "health": (source_stats.get("health") or "").strip(),
                    "method": (source_stats.get("method") or "").strip(),
                    "why": (source_stats.get("why") or "").strip(),
                    "feed_ok": int(source_stats.get("feed_ok", 0) or 0),
                    "feed_failed": int(source_stats.get("feed_failed", 0) or 0),
                    "html_fallback_used": int(source_stats.get("html_fallback_used", 0) or 0),
                    "entries_total": int(source_stats.get("entries_total", 0) or 0),
                    "entries_with_date": int(source_stats.get("entries_with_date", 0) or 0),
                    "items_raw": int(source_stats.get("items_raw", 0) or 0),
                    "items_with_date": int(source_stats.get("items_with_date", 0) or 0),
                    "items_date_unknown": int(source_stats.get("items_date_unknown", 0) or 0),
                    "kept_in_window_count": int(source_stats.get("kept_last24h", 0) or 0),
                    "feed_status_code": int(source_stats.get("feed_status_code", 0) or 0),
                    "homepage_status_code": int(source_stats.get("homepage_status_code", 0) or 0),
                    "candidates_found": int(source_stats.get("candidates_found", 0) or 0),
                    "pages_fetched": int(source_stats.get("pages_fetched", 0) or 0),
                    "content_mode": str(source_stats.get("content_mode") or "unknown"),
                    "article_fetch_attempted": int(source_stats.get("article_fetch_attempted", 0) or 0),
                    "article_fetch_success": int(source_stats.get("article_fetch_success", 0) or 0),
                    "article_fetch_failed": int(source_stats.get("article_fetch_failed", 0) or 0),
                    "article_fetch_improved_text": int(source_stats.get("article_fetch_improved_text", 0) or 0),
                    "article_fetch_blocked": int(source_stats.get("article_fetch_blocked", 0) or 0),
                    "article_fetch_timeout": int(source_stats.get("article_fetch_timeout", 0) or 0),
                    "article_fetch_ssl_error": int(source_stats.get("article_fetch_ssl_error", 0) or 0),
                    "extraction_method_counts": dict(source_stats.get("extraction_method_counts") or {}),
                    "content_source_counts": dict(source_stats.get("content_source_counts") or {}),
                    "median_article_text_length": int(source_stats.get("median_article_text_length", 0) or 0),
                    "median_final_text_length": int(source_stats.get("median_final_text_length", 0) or 0),
                    "text_len_min": int(source_stats.get("text_len_min", 0) or 0),
                    "text_len_median": int(source_stats.get("text_len_median", 0) or 0),
                    "text_len_max": int(source_stats.get("text_len_max", 0) or 0),
                    "items_with_text_total": int(source_stats.get("items_with_text_total", 0) or 0),
                    "items_title_only_total": int(source_stats.get("items_title_only_total", 0) or 0),
                    "possible_excerpt_total": int(source_stats.get("possible_excerpt_total", 0) or 0),
                    "possible_full_content_total": int(source_stats.get("possible_full_content_total", 0) or 0),
                    **({"error_class": error_class} if error_class else {}),
                }
            )
        foamed_health_state = state.get("foamed_source_health") if isinstance(state, dict) else {}
        if not isinstance(foamed_health_state, dict):
            foamed_health_state = {}
        disabled_sources = []
        for source_name, source_health in foamed_health_state.items():
            if not isinstance(source_health, dict):
                continue
            disabled_until = source_health.get("disabled_until_utc") or source_health.get("disabled_until")
            if not disabled_until:
                continue
            disabled_sources.append(
                {
                    "name": str(source_name),
                    "last_health": str(source_health.get("last_health") or ""),
                    "consecutive_failures": int(source_health.get("consecutive_failures", 0) or 0),
                    "disabled_until_utc": str(disabled_until),
                }
            )
        pubmed_state_skip_reasons = {}
        if isinstance(selection_stats, dict):
            state_reasons = selection_stats.get("state_skip_reasons") or {}
            if isinstance(state_reasons, dict):
                pubmed_state_skip_reasons = {str(k): int(v or 0) for k, v in state_reasons.items()}
        pubmed_publication_type_counts = Counter()
        pubmed_evidence_tag_counts = Counter()
        pubmed_mesh_heading_counts = Counter()
        pubmed_keyword_counts = Counter()
        pubmed_raw_publication_type_counts = Counter()
        pubmed_raw_evidence_tag_counts = Counter()
        pubmed_raw_mesh_heading_counts = Counter()
        pubmed_raw_keyword_counts = Counter()
        for it in all_pubmed_raw_items:
            pubmed_raw_publication_type_counts.update(str(v) for v in (it.get("publication_types") or []) if str(v).strip())
            pubmed_raw_evidence_tag_counts.update(str(v) for v in (it.get("evidence_tags") or []) if str(v).strip())
            pubmed_raw_mesh_heading_counts.update(str(v) for v in (it.get("mesh_headings") or []) if str(v).strip())
            pubmed_raw_keyword_counts.update(str(v) for v in (it.get("keywords") or []) if str(v).strip())
        presentation_missing_label_counts = {
            "pubmed_evidence_strength_label_missing_total": len([it for it in pubmed_overview_items if not str(it.get("evidence_strength_label") or "").strip()]),
            "pubmed_clinical_relevance_1_5_missing_total": len([it for it in pubmed_overview_items if it.get("clinical_relevance_1_5") in {None, ""}]),
            "pubmed_practice_change_potential_1_5_missing_total": len([it for it in pubmed_overview_items if it.get("practice_change_potential_1_5") in {None, ""}]),
            "pubmed_text_confidence_label_missing_total": len([it for it in pubmed_overview_items if not str(it.get("text_confidence_label") or "").strip()]),
            "foamed_source_quality_label_missing_total": len([it for it in foamed_overview_items if not str(it.get("source_quality_label") or "").strip()]),
            "foamed_clinical_usefulness_1_5_missing_total": len([it for it in foamed_overview_items if it.get("clinical_usefulness_1_5") in {None, ""}]),
            "foamed_practice_relevance_1_5_missing_total": len([it for it in foamed_overview_items if it.get("practice_relevance_1_5") in {None, ""}]),
            "foamed_text_confidence_label_missing_total": len([it for it in foamed_overview_items if not str(it.get("text_confidence_label") or "").strip()]),
        }
        pubmed_items_with_presentation_labels_total = len([
            it for it in pubmed_overview_items if any([
                str(it.get("evidence_strength_label") or "").strip(),
                it.get("clinical_relevance_1_5") not in {None, ""},
                it.get("practice_change_potential_1_5") not in {None, ""},
                str(it.get("text_confidence_label") or "").strip(),
            ])
        ])
        foamed_items_with_presentation_labels_total = len([
            it for it in foamed_overview_items if any([
                str(it.get("source_quality_label") or "").strip(),
                it.get("clinical_usefulness_1_5") not in {None, ""},
                it.get("practice_relevance_1_5") not in {None, ""},
                str(it.get("text_confidence_label") or "").strip(),
            ])
        ])
        top_pick_items_rendered_total = len([it for it in (pubmed_overview_items + foamed_overview_items) if it.get("top_pick")])
        pubmed_evidence_strength_label_counts = Counter(str(it.get("evidence_strength_label") or "") for it in pubmed_overview_items if str(it.get("evidence_strength_label") or "").strip())
        pubmed_evidence_strength_label_basis_counts = Counter(str(it.get("evidence_strength_label_basis") or "") for it in pubmed_overview_items if str(it.get("evidence_strength_label_basis") or "").strip())
        pubmed_clinical_relevance_label_distribution = Counter(str(it.get("clinical_relevance_1_5")) for it in pubmed_overview_items if it.get("clinical_relevance_1_5") not in {None, ""})
        pubmed_practice_impact_label_distribution = Counter(str(it.get("practice_change_potential_1_5")) for it in pubmed_overview_items if it.get("practice_change_potential_1_5") not in {None, ""})
        foamed_source_quality_label_counts = Counter(str(it.get("source_quality_label") or "") for it in foamed_overview_items if str(it.get("source_quality_label") or "").strip())
        foamed_text_confidence_label_counts = Counter(str(it.get("text_confidence_label") or "") for it in foamed_overview_items if str(it.get("text_confidence_label") or "").strip())
        foamed_clinical_usefulness_distribution = Counter(str(it.get("clinical_usefulness_1_5")) for it in foamed_overview_items if it.get("clinical_usefulness_1_5") not in {None, ""})
        foamed_practice_relevance_distribution = Counter(str(it.get("practice_relevance_1_5")) for it in foamed_overview_items if it.get("practice_relevance_1_5") not in {None, ""})
        pubmed_label_calibration_preview = [
            {
                "evidence_strength_label": str(it.get("evidence_strength_label") or ""),
                "evidence_strength_label_basis": str(it.get("evidence_strength_label_basis") or ""),
                "publication_types": [str(v) for v in (it.get("publication_types") or [])][:5],
                "evidence_tags": [str(v) for v in (it.get("evidence_tags") or [])][:5],
                "content_source": str(it.get("content_source") or ""),
                "content_length_bucket": str(it.get("content_length_bucket") or "none"),
                "clinical_relevance_1_5": int(it.get("clinical_relevance_1_5", 0) or 0),
                "practice_change_potential_1_5": int(it.get("practice_change_potential_1_5", 0) or 0),
                "text_confidence_label": str(it.get("text_confidence_label") or ""),
            }
            for it in pubmed_overview_items[:10]
        ]
        for it in pubmed_new_items:
            pubmed_publication_type_counts.update(str(v) for v in (it.get("publication_types") or []) if str(v).strip())
            pubmed_evidence_tag_counts.update(str(v) for v in (it.get("evidence_tags") or []) if str(v).strip())
            pubmed_mesh_heading_counts.update(str(v) for v in (it.get("mesh_headings") or []) if str(v).strip())
            pubmed_keyword_counts.update(str(v) for v in (it.get("keywords") or []) if str(v).strip())

        cybermed_diagnostics_payload = {
            "report_mode": report_mode,
            "effective_hours": int(args.hours),
            "effective_runtime_config": {
                "FOAMED_AUDIT": _env_bool("FOAMED_AUDIT", False),
                "FOAMED_AUDIT_CHECK_DISABLED": _env_bool("FOAMED_AUDIT_CHECK_DISABLED", False),
                "FOAMED_ARTICLE_FETCH": _env_bool("FOAMED_ARTICLE_FETCH", False),
                "FOAMED_ARTICLE_FETCH_MAX_PER_RUN": _safe_int("FOAMED_ARTICLE_FETCH_MAX_PER_RUN", 0),
                "FOAMED_ROLLING_AUDIT_DAYS": _safe_int("FOAMED_ROLLING_AUDIT_DAYS", 0),
                "FOAMED_ROLLING_AUDIT_FETCH_MAX_PER_RUN": _safe_int("FOAMED_ROLLING_AUDIT_FETCH_MAX_PER_RUN", 60),
                "CYBERMED_RUNTIME_DIAGNOSTICS": _env_bool("CYBERMED_RUNTIME_DIAGNOSTICS", True),
                "CYBERMED_HEAVY_AUDIT_MODE": _env_bool("CYBERMED_HEAVY_AUDIT_MODE", False),
            },
            "pubmed_date_type": pubmed_datetype,
            "pubmed_window": {"mindate_utc": mindate, "maxdate_utc": maxdate},
            "pubmed_channels_total": len(pubmed_queries_used),
            "pubmed_queries_attempted_total": len(pubmed_queries_used),
            "pubmed_query_failures_total": pubmed_query_failures,
            "pubmed_journals": journals,
            "pubmed_items_raw_total": pubmed_candidates_total,
            "pubmed_items_after_state_filter_total": len(pubmed_new_items),
            "pubmed_items_skipped_by_state_total": int(pubmed_skipped_by_state),
            "pubmed_items_selected_overview_total": len(pubmed_overview_items),
            "pubmed_items_selected_deep_dive_total": len(pubmed_deep_dive_items),
            "pubmed_items_missing_abstract_total": len([it for it in pubmed_new_items if not (it.get("abstract") or "").strip()]),
            "pubmed_items_with_abstract_total": len([it for it in pubmed_new_items if (it.get("abstract") or "").strip()]),
            "pubmed_items_with_doi_total": len([it for it in pubmed_new_items if (it.get("doi") or "").strip()]),
            "pubmed_items_with_publication_types_total": len([it for it in pubmed_new_items if it.get("publication_types")]),
            "pubmed_items_with_mesh_headings_total": len([it for it in pubmed_new_items if it.get("mesh_headings")]),
            "pubmed_items_with_keywords_total": len([it for it in pubmed_new_items if it.get("keywords")]),
            "pubmed_items_with_abstract_sections_total": len([it for it in pubmed_new_items if it.get("abstract_sections")]),
            "pubmed_raw_items_with_publication_types_total": len([it for it in all_pubmed_raw_items if it.get("publication_types")]),
            "pubmed_raw_items_with_mesh_headings_total": len([it for it in all_pubmed_raw_items if it.get("mesh_headings")]),
            "pubmed_raw_items_with_keywords_total": len([it for it in all_pubmed_raw_items if it.get("keywords")]),
            "pubmed_raw_items_with_abstract_sections_total": len([it for it in all_pubmed_raw_items if it.get("abstract_sections")]),
            "pubmed_raw_items_with_abstract_total": len([it for it in all_pubmed_raw_items if (it.get("abstract") or "").strip()]),
            "pubmed_raw_items_with_doi_total": len([it for it in all_pubmed_raw_items if (it.get("doi") or "").strip()]),
            "pubmed_publication_type_counts": dict(pubmed_publication_type_counts.most_common(20)),
            "pubmed_evidence_tag_counts": dict(pubmed_evidence_tag_counts.most_common(30)),
            "pubmed_mesh_heading_top_counts": dict(pubmed_mesh_heading_counts.most_common(30)),
            "pubmed_keyword_top_counts": dict(pubmed_keyword_counts.most_common(30)),
            "pubmed_raw_publication_type_counts": dict(pubmed_raw_publication_type_counts.most_common(20)),
            "pubmed_raw_evidence_tag_counts": dict(pubmed_raw_evidence_tag_counts.most_common(30)),
            "pubmed_raw_mesh_heading_top_counts": dict(pubmed_raw_mesh_heading_counts.most_common(30)),
            "pubmed_raw_keyword_top_counts": dict(pubmed_raw_keyword_counts.most_common(30)),
            "pubmed_state_skip_reasons": pubmed_state_skip_reasons,
            "pubmed_per_channel": pubmed_per_channel,
            "foamed_sources_total": int(foamed_meta_stats.get("sources_total", 0) or 0),
            "foamed_sources_config_total": len(foamed_sources),
            "foamed_sources_processed_total": int(foamed_meta_stats.get("sources_total", 0) or 0),
            "foamed_sources_skipped_disabled_total": max(0, len(foamed_sources) - int(foamed_meta_stats.get("sources_total", 0) or 0)),
            "foamed_auto_disable_enabled": bool(foamed_auto_disable_enabled),
            "foamed_auto_disable_disabled_active_count": len(disabled_sources),
            "foamed_auto_disable_newly_disabled_count": int(foamed_meta_stats.get("newly_disabled_count", 0) or 0),
            "foamed_disabled_sources": disabled_sources[:10],
            "foamed_sources_ok_total": int(foamed_meta_stats.get("sources_ok", 0) or 0),
            "foamed_sources_failed_total": int(foamed_meta_stats.get("sources_failed", 0) or 0),
            "foamed_items_raw_total": int(foamed_meta_stats.get("items_raw", 0) or 0),
            "foamed_items_after_state_filter_total": int(foamed_after_state),
            "foamed_items_skipped_by_state_total": int(foamed_skipped_by_state),
            "foamed_items_with_date_total": int(foamed_meta_stats.get("items_with_date", 0) or 0),
            "foamed_items_date_unknown_total": int(foamed_meta_stats.get("items_date_unknown", 0) or 0),
            "foamed_items_kept_in_window_total": int(foamed_meta_stats.get("kept_last24h", 0) or 0),
            "foamed_items_selected_overview_total": len(foamed_overview_items),
            "foamed_items_selected_top_pick_total": len(foamed_top_picks),
            "foamed_article_fetch_enabled": bool(foamed_meta_stats.get("foamed_article_fetch_enabled", False)),
            "foamed_article_fetch_attempted_total": int(foamed_meta_stats.get("foamed_article_fetch_attempted_total", 0) or 0),
            "foamed_article_fetch_success_total": int(foamed_meta_stats.get("foamed_article_fetch_success_total", 0) or 0),
            "foamed_article_fetch_failed_total": int(foamed_meta_stats.get("foamed_article_fetch_failed_total", 0) or 0),
            "foamed_article_fetch_improved_text_total": int(foamed_meta_stats.get("foamed_article_fetch_improved_text_total", 0) or 0),
            "foamed_article_fetch_blocked_total": int(foamed_meta_stats.get("foamed_article_fetch_blocked_total", 0) or 0),
            "foamed_article_fetch_timeout_total": int(foamed_meta_stats.get("foamed_article_fetch_timeout_total", 0) or 0),
            "foamed_article_fetch_ssl_error_total": int(foamed_meta_stats.get("foamed_article_fetch_ssl_error_total", 0) or 0),
            "foamed_article_extraction_method_counts": dict(foamed_meta_stats.get("foamed_article_extraction_method_counts") or {}),
            "foamed_content_source_counts": dict(foamed_meta_stats.get("foamed_content_source_counts") or {}),
            "foamed_discovery_content_mode_counts": dict(foamed_meta_stats.get("foamed_discovery_content_mode_counts") or {}),
            "foamed_final_content_source_counts": dict(foamed_meta_stats.get("foamed_final_content_source_counts") or {}),
            "foamed_source_strategy_summary": list(foamed_meta_stats.get("foamed_source_strategy_summary") or []),
            "foamed_usable_fulltext_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") == "usable_fulltext"]),
            "foamed_usable_html_only_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") == "usable_html_only"]),
            "foamed_blocked_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") == "blocked"]),
            "foamed_broken_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") in {"stale_or_broken_url"}]),
            "foamed_excerpt_only_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") in {"usable_excerpt_only","usable_discovery_only"}]),
            "foamed_audit_only_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") == "audit_only"]),
            "foamed_tls_or_timeout_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") == "tls_or_timeout_problem"]),
            "foamed_no_recent_content_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if (s or {}).get("source_status") == "no_recent_content"]),
            "foamed_top30_core_sources_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("priority_tier") or "") == "1 core"]),
            "foamed_top30_core_sources_usable_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("priority_tier") or "") == "1 core" and str((s or {}).get("source_status") or "") in {"usable_fulltext","usable_html_only","usable_excerpt_only","usable_discovery_only"}]),
            "foamed_top30_sources_fulltext_capable_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("source_status") or "") in {"usable_fulltext","usable_html_only"}]),
            "foamed_top30_sources_excerpt_only_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("source_status") or "") in {"usable_excerpt_only","usable_discovery_only"}]),
            "foamed_top30_sources_blocked_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("source_status") or "") == "blocked"]),
            "foamed_top30_sources_broken_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("source_status") or "") == "stale_or_broken_url"]),
            "foamed_top30_sources_audit_only_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("source_status") or "") == "audit_only"]),
            "foamed_top30_sources_no_recent_content_total": len([s for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or []) if str((s or {}).get("source_status") or "") == "no_recent_content"]),
            "foamed_top30_domain_group_counts": dict(Counter([str((s or {}).get("domain_group") or "mixed") for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or [])]).most_common()),
            "foamed_top30_priority_tier_counts": dict(Counter([str((s or {}).get("priority_tier") or "2 important") for s in (foamed_meta_stats.get("foamed_source_strategy_summary") or [])]).most_common()),
            "foamed_per_source": foamed_per_source,
            "foamed_audit_enabled": bool(((foamed_meta_stats.get("audit") or {}).get("enabled", False))),
            "foamed_sources_with_rss_total": len([1 for _n,st in (foamed_meta_stats.get("per_source") or {}).items() if isinstance(st, dict) and str(st.get("method") or "") in {"rss", "discovered_feed"}]),
            "foamed_sources_with_html_fallback_total": len([1 for _n,st in (foamed_meta_stats.get("per_source") or {}).items() if isinstance(st, dict) and st.get("html_fallback_used")]),
            "foamed_sources_html_failed_total": len([1 for _n,st in (foamed_meta_stats.get("per_source") or {}).items() if isinstance(st, dict) and str(st.get("health") or "") in {"blocked_403", "not_found_404", "parse_failed", "other"} and st.get("html_fallback_used")]),
            "foamed_sources_rss_excerpt_only_total": len([1 for _n,st in ((foamed_meta_stats.get("audit") or {}).get("sources") or {}).items() if isinstance(st, dict) and str(st.get("content_mode") or "") == "rss_excerpt"]),
            "foamed_sources_possible_partial_content_total": len([1 for _n,st in ((foamed_meta_stats.get("audit") or {}).get("sources") or {}).items() if isinstance(st, dict) and bool(st.get("completeness_warning"))]),
            "foamed_audit_summary": [
                {"name": str(n), "health": str((st or {}).get("health") or ""), "method": str((st or {}).get("method") or ""), "rss_items_seen": int((st or {}).get("rss_items_seen",0) or 0), "rss_items_in_window": int((st or {}).get("rss_items_in_window",0) or 0), "html_candidates_seen": int((st or {}).get("html_candidates_seen",0) or 0), "html_items_in_window": int((st or {}).get("html_items_in_window",0) or 0), "html_not_in_rss_count": int((st or {}).get("html_not_in_rss_count",0) or 0), "rss_not_in_html_count": int((st or {}).get("rss_not_in_html_count",0) or 0), "audit_pages_fetched": int((st or {}).get("audit_pages_fetched",0) or 0), "content_mode": str((st or {}).get("content_mode", "unknown") or "unknown"), "text_len_median": int((st or {}).get("text_len_median",0) or 0), "completeness_warning": list((st or {}).get("completeness_warning") or [])}
                for n, st in (((foamed_meta_stats.get("audit") or {}).get("sources") or {}).items()) if isinstance(st, dict)
            ],
            "pubmed_channels_possibly_truncated_total": len([1 for _n,st in pubmed_channel_completeness.items() if bool(st.get("possibly_truncated"))]),
            "pubmed_channels_with_zero_results_total": len([1 for _n,st in pubmed_channel_completeness.items() if int(st.get("raw_count",0) or 0) == 0]),
            "pubmed_raw_items_missing_abstract_total": len([it for it in all_pubmed_raw_items if not (it.get("abstract") or "").strip()]),
            "pubmed_raw_items_missing_publication_types_total": len([it for it in all_pubmed_raw_items if not it.get("publication_types")]),
            "pubmed_raw_completeness_warnings": [],
            "selection_counts": selection_count_only,
            "selection_diagnostics": (selection_stats.get("selection_diagnostics") if isinstance(selection_stats, dict) else {}),
            "cybermed_presentation_v1_enabled": True,
            "pubmed_items_with_presentation_labels_total": int(pubmed_items_with_presentation_labels_total),
            "foamed_items_with_presentation_labels_total": int(foamed_items_with_presentation_labels_total),
            "top_pick_items_rendered_total": int(top_pick_items_rendered_total),
            "presentation_missing_label_counts": presentation_missing_label_counts,
            "pubmed_evidence_strength_label_counts": dict(pubmed_evidence_strength_label_counts),
            "pubmed_evidence_strength_label_basis_counts": dict(pubmed_evidence_strength_label_basis_counts),
            "pubmed_clinical_relevance_label_distribution": dict(pubmed_clinical_relevance_label_distribution),
            "pubmed_practice_impact_label_distribution": dict(pubmed_practice_impact_label_distribution),
            "foamed_source_quality_label_counts": dict(foamed_source_quality_label_counts),
            "foamed_text_confidence_label_counts": dict(foamed_text_confidence_label_counts),
            "foamed_clinical_usefulness_distribution": dict(foamed_clinical_usefulness_distribution),
            "foamed_practice_relevance_distribution": dict(foamed_practice_relevance_distribution),
            "pubmed_label_calibration_preview": pubmed_label_calibration_preview,
            "pubmed_summary_consistency_enabled": True,
            "pubmed_summary_consistency_checked_total": 0,
            "pubmed_summary_consistency_conflict_total": 0,
            "pubmed_summary_consistency_conflict_type_counts": {},
            "pubmed_summary_consistency_resolved_total": 0,
            "pubmed_shared_synopsis_generated_total": 0,
            "pubmed_shared_synopsis_failed_total": 0,
            "pubmed_summary_consistency_preview": [],
            "cybermed_qa_replay_requested": bool(qa_replay_requested),
            "cybermed_qa_replay_enabled": bool(qa_replay_enabled),
            "cybermed_qa_replay_safety_passed": bool(qa_replay_safety_passed),
            "cybermed_qa_replay_skipped_reason": str(qa_replay_skipped_reason or ""),
            "cybermed_qa_replay_state_bypass_pubmed_total": int(qa_replay_state_bypass_pubmed_total),
            "cybermed_qa_replay_state_bypass_foamed_total": int(qa_replay_state_bypass_foamed_total),
            "cybermed_qa_replay_state_mutation_disabled": bool(qa_replay_state_mutation_disabled),
            "cybermed_qa_replay_email_disabled_confirmed": bool(qa_replay_email_disabled_confirmed),
            "runtime_total_seconds": round(max(0.0, time.monotonic() - runtime_start), 6),
            "runtime_pubmed_collect_seconds": round(runtime_pubmed_collect_seconds, 6),
            "runtime_pubmed_backfill_seconds": round(runtime_pubmed_backfill_seconds, 6),
            "runtime_foamed_collect_seconds": round(runtime_foamed_collect_seconds, 6),
            "runtime_foamed_article_fetch_seconds": round(runtime_foamed_article_fetch_seconds, 6),
            "runtime_foamed_rolling_audit_seconds": round(runtime_foamed_rolling_audit_seconds, 6),
            "runtime_selection_seconds": round(runtime_selection_seconds, 6),
            "runtime_summarization_seconds": round(runtime_summarization_seconds, 6),
            "runtime_report_render_seconds": round(runtime_report_render_seconds, 6),
            "runtime_email_seconds": round(runtime_email_seconds, 6),
            **foamed_disabled_audit_stats,
            **(foamed_rolling_diag if "foamed_rolling_diag" in locals() else {}),
        }
        if isinstance(foamed_selection_stats, dict):
            for key in [
                "foamed_final_selected_overview_total",
                "foamed_final_selected_top_pick_total",
                "foamed_final_selected_source_quality_counts",
                "foamed_final_selected_text_confidence_counts",
                "foamed_final_selected_clinical_usefulness_distribution",
                "foamed_final_selected_practice_relevance_distribution",
                "foamed_top_pick_floor_rejection_counts",
                "foamed_selected_reason_counts",
                "foamed_exclusion_reason_counts",
                "foamed_duplicates_suppressed_total",
                "foamed_duplicates_suppressed_reason_counts",
                "foamed_final_selected_preview",
                "foamed_final_selected_optional_top_pick_violations_total",
                "foamed_final_selected_low_label_top_pick_violations_total",
            ]:
                if key in foamed_selection_stats:
                    cybermed_diagnostics_payload[key] = foamed_selection_stats.get(key)
        cybermed_diagnostics_payload.update(_calendar_env_metadata())


        warnings = []
        if cybermed_diagnostics_payload.get("pubmed_channels_possibly_truncated_total", 0): warnings.append("channel_hit_retmax_cap")
        if cybermed_diagnostics_payload.get("pubmed_channels_with_zero_results_total", 0): warnings.append("zero_result_channel")
        if cybermed_diagnostics_payload.get("pubmed_raw_items_missing_publication_types_total", 0): warnings.append("metadata_missing_publication_types")
        if cybermed_diagnostics_payload.get("pubmed_raw_items_missing_abstract_total", 0) > max(3, len(all_pubmed_raw_items)//2): warnings.append("many_items_missing_abstract")
        cybermed_diagnostics_payload["pubmed_raw_completeness_warnings"] = warnings
        post_state_pubmed = list(pubmed_new_items)
        pubmed_backfill_start = time.monotonic()
        pubmed_backfill_diag = _pubmed_content_backfill_and_diagnostics(post_state_pubmed)
        runtime_pubmed_backfill_seconds += max(0.0, time.monotonic() - pubmed_backfill_start)
        cybermed_diagnostics_payload.update(pubmed_backfill_diag)
        foamed_min_chars = _safe_int("FOAMED_MIN_USABLE_TEXT_CHARS", 400)
        cybermed_diagnostics_payload.update(_foamed_72h_text_diagnostics(list(foamed_collected), foamed_min_chars))
        raw_cov = round(
            (len([it for it in all_pubmed_raw_items if (it.get("abstract") or "").strip()]) / max(1, len(all_pubmed_raw_items))) * 100,
            1,
        ) if all_pubmed_raw_items else 0.0
        cybermed_diagnostics_payload["pubmed_raw_content_coverage_pct"] = raw_cov
        foamed_72h_article_fulltext_pct = float(cybermed_diagnostics_payload.get("foamed_72h_article_fulltext_pct", 0.0) or 0.0)
        ready_pubmed = float(cybermed_diagnostics_payload.get("pubmed_post_state_content_coverage_pct", 0.0)) >= 75.0
        ready_foamed_summary = float(cybermed_diagnostics_payload.get("foamed_72h_usable_text_pct", 0.0) or 0.0) >= 80.0
        rolling_prod = int(cybermed_diagnostics_payload.get("foamed_rolling_productive_sources_total", 0) or 0)
        heavy_audit_mode = _env_bool("CYBERMED_HEAVY_AUDIT_MODE", False)
        ready_foamed_cov = (rolling_prod >= 8) if heavy_audit_mode else "not_evaluated"
        blocking = []
        if not ready_pubmed: blocking.append("pubmed_content_coverage_below_75")
        if not ready_foamed_summary: blocking.append("foamed_usable_text_below_80")
        if heavy_audit_mode and not (rolling_prod >= 8): blocking.append("foamed_rolling_productive_sources_below_8")
        if not heavy_audit_mode:
            cybermed_diagnostics_payload["foamed_ready_for_coverage_not_evaluated_reason"] = "heavy_audit_mode_disabled"
        cybermed_diagnostics_payload["cybermed_readiness"] = {
            "pubmed_post_state_content_coverage_pct": cybermed_diagnostics_payload.get("pubmed_post_state_content_coverage_pct", 0.0),
            "pubmed_ready_for_ranking": ready_pubmed,
            "foamed_72h_article_fulltext_pct": foamed_72h_article_fulltext_pct,
            "foamed_72h_usable_text_pct": float(cybermed_diagnostics_payload.get("foamed_72h_usable_text_pct", 0.0) or 0.0),
            "foamed_ready_for_summaries": ready_foamed_summary,
            "foamed_rolling_productive_sources_total": rolling_prod,
            "foamed_ready_for_coverage": ready_foamed_cov,
            "overall_ready_for_relevance_logic": ready_pubmed and ready_foamed_summary and ((rolling_prod >= 8) if heavy_audit_mode else True),
            "blocking_reasons": blocking,
        }

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


    if is_cybermed_run and cybermed_weekly_digest_only:
        pubmed_overview_items = [it for it in items if (it.get("source") or "").strip().lower() == "pubmed"]
        foamed_overview_items = [it for it in items if (it.get("source") or "").strip().lower() == "foamed"]
        pubmed_deep_dive_items = [it for it in pubmed_overview_items if bool(it.get("cybermed_deep_dive"))]
        report_items = _dedupe_items(pubmed_overview_items + pubmed_deep_dive_items + foamed_overview_items)
    elif is_cybermed_run:
        report_items = _dedupe_items(pubmed_overview_items + pubmed_deep_dive_items + foamed_overview_items)
    else:
        report_items = list(items)

    if not report_items:
        if report_language.lower().startswith("en"):
            overview = cybermed_meta_block + "## Executive Summary\n\nNo new content in the last 24 hours.\n"
        else:
            overview = cybermed_meta_block + "## Kurzüberblick\n\nKeine neuen Inhalte in den letzten 24 Stunden.\n"
        out_path = _report_output_path(report_dir, report_key, report_mode)
        md = to_markdown(
            [],
            overview,
            {},
            report_title=report_title,
            report_language=report_language,
            report_mode=report_mode,
            run_metadata=run_metadata if report_key.strip().lower() == "cyberlurch" else None,
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
            read_only=(read_only_mode or qa_replay_enabled or cybermed_weekly_digest_only),
        )

        if send_empty_email == "1":
            try:
                send_markdown(report_subject, md)
            except Exception as e:
                print(f"[email] WARN: failed to send empty report email: {e!r}")
        else:
            print("[email] No new items and SEND_EMPTY_REPORT_EMAIL=0 -> not sending email.")
        if report_key.strip().lower() == "cyberlurch":
            _write_cyberlurch_youtube_diagnostics(report_dir, youtube_diag, report_mode=report_mode)
            _write_run_metadata_artifact(report_dir, report_key, report_mode, run_metadata)
        if report_key.strip().lower() == "cybermed":
            if report_mode == "daily":
                digest_path = (os.getenv("CYBERMED_DAILY_DIGEST_STATE_PATH", "state/cybermed_daily_digests.json") or "state/cybermed_daily_digests.json").strip()
                digest_path = digest_path or "state/cybermed_daily_digests.json"
                digest_abs_path = str(Path(digest_path).resolve())
                digest_id = f"cybermed_daily_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
                email_mode = (os.getenv("EMAIL_MODE", "") or "").strip().lower()
                send_email = (os.getenv("SEND_EMAIL", "1") or "1").strip()
                event_name = (os.getenv("GITHUB_EVENT_NAME", "") or "").strip().lower()
                overwrite_requested = _env_bool("CYBERMED_DIGEST_STORE_OVERWRITE", False)
                overwrite_allowed = overwrite_requested and email_mode == "none" and send_email == "0" and event_name in {"workflow_dispatch", "manual"}
                skip_reason = ""
                write_error_class = ""
                if qa_replay_enabled and not _env_bool("CYBERMED_DIGEST_STORE_ALLOW_QA_REPLAY", False):
                    skip_reason = "qa_replay_mode"
                if not skip_reason:
                    dstate = _load_cybermed_daily_digest_state(digest_path)
                    digests = list(dstate.get("digests") or [])
                    existing_idx = next((i for i, d in enumerate(digests) if str((d or {}).get("digest_id") or "") == digest_id), -1)
                    if existing_idx >= 0 and not overwrite_allowed:
                        skip_reason = "digest_already_exists"
                    else:
                        payload = {"digest_id": digest_id, "report_key": "cybermed", "cadence": "daily", "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "generated_at_utc": now_utc_iso, "lookback_hours": int(args.hours), "qa_replay": bool(qa_replay_enabled), "email_mode": email_mode, "items": {"pubmed": [], "foamed": []}, "deep_dives": [], "top_picks": [], "diagnostic_summary": {"pubmed_selected_total": 0, "pubmed_deep_dive_total": 0, "pubmed_evidence_label_counts": {}, "foamed_selected_total": 0, "foamed_top_pick_total": 0, "foamed_source_quality_counts": {}, "foamed_text_confidence_counts": {}, "summary_consistency_conflict_total": 0, "summary_consistency_resolved_total": 0, "qa_replay": bool(qa_replay_enabled), "state_mutation_disabled": bool(qa_replay_state_mutation_disabled)}}
                        if existing_idx >= 0:
                            digests[existing_idx] = payload
                        else:
                            digests.append(payload)
                        dstate["schema_version"] = 1
                        dstate["digests"] = digests
                        try:
                            with open(digest_path, "w", encoding="utf-8") as f:
                                json.dump(dstate, f, ensure_ascii=False, separators=(",", ":"))
                                f.write("\n")
                        except Exception as e:
                            skip_reason = "write_verification_failed"
                            write_error_class = e.__class__.__name__
                            print(f"[cybermed-digest-store] WARN: write failed/verification error: {e!r}")
                write_verified = False
                file_exists_after_write = os.path.exists(digest_path)
                digest_count_after_write = 0
                expected_digest_present = False
                if skip_reason == "":
                    try:
                        verified_state = _load_cybermed_daily_digest_state(digest_path)
                        verified_digests = list(verified_state.get("digests") or [])
                        digest_count_after_write = len(verified_digests)
                        expected_digest_present = any(str((d or {}).get("digest_id") or "") == digest_id for d in verified_digests)
                        write_verified = file_exists_after_write and expected_digest_present
                        if not write_verified:
                            skip_reason = "write_verification_failed"
                            print("[cybermed-digest-store] WARN: write verification failed")
                    except Exception as e:
                        skip_reason = "write_verification_failed"
                        write_error_class = e.__class__.__name__
                        print(f"[cybermed-digest-store] WARN: write failed/verification error: {e!r}")
                elif skip_reason == "digest_already_exists":
                    verified_state = _load_cybermed_daily_digest_state(digest_path)
                    verified_digests = list(verified_state.get("digests") or [])
                    digest_count_after_write = len(verified_digests)
                    expected_digest_present = any(str((d or {}).get("digest_id") or "") == digest_id for d in verified_digests)
                cybermed_diagnostics_payload.update({
                    "cybermed_digest_store_enabled": True,
                    "cybermed_digest_store_path": digest_path,
                    "cybermed_digest_store_abs_path": digest_abs_path,
                    "cybermed_digest_store_written": skip_reason == "" and write_verified,
                    "cybermed_digest_store_skipped_reason": skip_reason,
                    "cybermed_digest_store_digest_id": digest_id,
                    "cybermed_digest_store_items_pubmed_total": 0,
                    "cybermed_digest_store_items_foamed_total": 0,
                    "cybermed_digest_store_deep_dives_total": 0,
                    "cybermed_digest_store_top_picks_total": 0,
                    "cybermed_digest_store_overwrite": overwrite_allowed,
                    "cybermed_digest_store_schema_version": 1,
                    "cybermed_digest_store_write_verified": write_verified,
                    "cybermed_digest_store_file_exists_after_write": file_exists_after_write,
                    "cybermed_digest_store_digest_count_after_write": digest_count_after_write,
                    "cybermed_digest_store_expected_digest_present": expected_digest_present,
                    "cybermed_digest_store_write_error_class": write_error_class,
                })
            if cybermed_weekly_digest_only:
                rendered_pubmed_total = len([it for it in report_items if (it.get("source") or "").strip().lower() == "pubmed"])
                rendered_foamed_total = len([it for it in report_items if (it.get("source") or "").strip().lower() == "foamed"])
                rendered_deep_dives_total = 0
                rendered_top_picks_total = int(cybermed_weekly_diag.get("cybermed_weekly_top_picks_selected_total", 0) or 0)
                cybermed_weekly_diag.update({
                    "cybermed_weekly_rendered_pubmed_items_total": rendered_pubmed_total,
                    "cybermed_weekly_rendered_foamed_items_total": rendered_foamed_total,
                    "cybermed_weekly_rendered_deep_dives_total": rendered_deep_dives_total,
                    "cybermed_weekly_rendered_top_picks_total": rendered_top_picks_total,
                    "cybermed_weekly_report_matches_digest_inputs": rendered_pubmed_total == int(cybermed_weekly_diag.get("cybermed_weekly_pubmed_items_selected_total", 0) or 0) and rendered_foamed_total == int(cybermed_weekly_diag.get("cybermed_weekly_foamed_items_selected_total", 0) or 0),
                })
            cybermed_diagnostics_payload.update(cybermed_weekly_diag)
        _write_cybermed_diagnostics(report_dir, report_mode, cybermed_diagnostics_payload)
        return

    detail_items: List[Dict[str, Any]] = []
    deep_dive_ids: Set[str] = set()
    deep_dive_skip_note = ""
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
        annotate_cyberlurch_temporality(items_sorted)
        curated_overview = _curate_cyberlurch_overview(items_sorted, report_mode, overview_items_max)
        overview_items = curated_overview
        full_text_items = [
            it for it in items_sorted
            if it.get("content_status") != "metadata_only" and it.get("text_source") != "metadata_only"
        ]
        metadata_only_items = [it for it in items_sorted if it.get("content_status") == "metadata_only" or it.get("text_source") == "metadata_only"]
        youtube_diag.full_text_items_total = len(full_text_items)
        youtube_diag.metadata_only_items_total = len(metadata_only_items)
        total_items = len(full_text_items) + len(metadata_only_items)
        youtube_diag.full_text_ratio = (len(full_text_items) / total_items) if total_items else 0.0
        priority_set = {x.strip() for x in (os.getenv("CYBERLURCH_PRIORITY_DAILY_CHANNELS", "CanadianPrepper,preppernewsflash").split(",")) if x.strip()}
        priority_norm = {normalize_channel_name(x) for x in priority_set}
        priority_daily_items = [it for it in items_sorted if normalize_channel_name(it.get("channel") or "") in priority_norm]
        trend_diag = build_trend_clusters(full_text_items)
        eligible=[]
        for it in full_text_items:
            if is_deep_dive_eligible(it, channel_topics):
                score_cyberlurch_deep_dive_candidate(it, full_text_items, channel_topics, state)
                temporality = str(it.get("temporality") or "current_affairs")
                if report_mode == "monthly" and temporality in {"breaking_news", "current_affairs"}:
                    it["cyberlurch_deep_dive_score"] = float(it.get("cyberlurch_deep_dive_score") or 0.0) - 2.5
                if report_mode == "yearly" and temporality not in {"evergreen", "mixed", "trend_analysis"}:
                    continue
                eligible.append(it)
        eligible_sorted = sorted(eligible, key=lambda it:(float(it.get("cyberlurch_deep_dive_score") or 0.0), it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        detail_items=[]; used_ch=set(); dup_suppressed=0
        for it in eligible_sorted:
            chn=normalize_channel_name(it.get("channel") or "")
            if chn in used_ch:
                dup_suppressed += 1
                continue
            detail_items.append(it); used_ch.add(chn)
            if len(detail_items)>=max(0, deep_dive_limit): break
        for it in items_sorted:
            if normalize_channel_name(it.get("channel") or "") in priority_norm and it not in overview_items:
                overview_items.append(it)
        overview_items = sorted(overview_items, key=lambda it: it.get("published_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        if report_mode in {"weekly", "monthly"} and report_key.strip().lower() == "cyberlurch":
            report_items = _dedupe_items(overview_items + detail_items)
        else:
            report_items = items_sorted
        if not detail_items:
            deep_dive_skip_note = "Deep dives skipped because no transcript, caption, or description content was available."

    if not overview_items and detail_items:
        overview_items = list(detail_items)

    if report_mode in {"weekly", "monthly"}:
        for it in overview_items[: max(1, min(3, len(overview_items)))]:
            if not it.get("top_pick"):
                it["top_pick"] = True

    for it in detail_items:
        if (it.get("source") or "").strip().lower() == "pubmed":
            it.setdefault("fulltext_source", "none")

    cyberlurch_diag = {
        "deep_dive_candidates_total": len(full_text_items) if not is_cybermed_run else 0,
        "deep_dive_eligible_total": len(eligible) if not is_cybermed_run else 0,
        "deep_dive_ineligible_mainstream_news_total": len([it for it in full_text_items if normalize_channel_name(it.get("channel") or "") in {"tagesschau","zdfheute","vanessawingardh"}]) if not is_cybermed_run else 0,
        "deep_dive_ineligible_short_text_total": len([it for it in full_text_items if len((it.get("text") or "").strip()) < max(200,int((os.getenv("CYBERLURCH_DEEPDIVE_MIN_TEXT_CHARS") or "2500")))]) if not is_cybermed_run else 0,
        "deep_dive_selected_total": len(detail_items),
        "deep_dive_selected_channels_total": len({normalize_channel_name(it.get("channel") or "") for it in detail_items}),
        "deep_dive_channel_duplicates_suppressed_total": dup_suppressed if not is_cybermed_run else 0,
        "priority_daily_items_found_total": len(priority_daily_items) if not is_cybermed_run else 0,
        "priority_daily_items_included_total": len([it for it in overview_items if normalize_channel_name(it.get("channel") or "") in priority_norm]) if not is_cybermed_run else 0,
        "priority_daily_deep_dives_selected_total": len([it for it in detail_items if normalize_channel_name(it.get("channel") or "") in priority_norm]) if not is_cybermed_run else 0,
        "trend_clusters_total": trend_diag.get("trend_clusters_total",0) if not is_cybermed_run else 0,
        "trend_boosted_items_total": trend_diag.get("trend_boosted_items_total",0) if not is_cybermed_run else 0,
        "items_by_temporality": {
            "breaking_news": len([it for it in report_items if str(it.get("temporality") or "") == "breaking_news"]),
            "current_affairs": len([it for it in report_items if str(it.get("temporality") or "") == "current_affairs"]),
            "trend_analysis": len([it for it in report_items if str(it.get("temporality") or "") == "trend_analysis"]),
            "evergreen": len([it for it in report_items if str(it.get("temporality") or "") == "evergreen"]),
            "mixed": len([it for it in report_items if str(it.get("temporality") or "") == "mixed"]),
        },
        "cadence_profile": cadence,
    }

    deep_dive_diag = {
        "candidates": len([it for it in detail_items if (it.get("source") or "").strip().lower() == "pubmed"]),
        "total_items": len(detail_items),
        "enriched_fulltext_count": 0,
        "unpaywall_oa_found_count": 0,
        "download_success_count": 0,
        "parse_fallback_used_count": 0,
        "not_reported_all_fields_count": 0,
        "json_failures_count": 0,
        "markdown_fallback_used_count": 0,
        "sparse_after_json_count": 0,
        "placeholder_rerun_count": 0,
        "placeholder_value_high_count": 0,
        "structured_rescue_used_count": 0,
    }
    pubmed_shared_synopsis: Dict[str, Dict[str, Any]] = {}
    consistency_preview: List[Dict[str, Any]] = []
    consistency_conflict_type_counts: Counter = Counter()
    consistency_checked_total = 0
    consistency_conflict_total = 0
    consistency_resolved_total = 0

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
        synopsis = _build_pubmed_shared_synopsis(it)
        pubmed_shared_synopsis[iid] = synopsis
        it["pubmed_shared_synopsis"] = synopsis

    if foamed_overview_items:
        for it in foamed_overview_items:
            url_lbl = (it.get("url") or it.get("id") or "")
            try:
                bl = summarize_foamed_bottom_line(it, language=report_language)
                it["bottom_line"] = bl
            except Exception as e:
                print(f"[summarize] WARN: summarize_foamed_bottom_line failed for foamed item {url_lbl!r}: {e!r}")
    for it in detail_items:
        if (it.get("source") or "").strip().lower() != "pubmed":
            continue
        iid = str(it.get("id") or "").strip()
        if iid and iid not in pubmed_shared_synopsis:
            synopsis = _build_pubmed_shared_synopsis(it)
            pubmed_shared_synopsis[iid] = synopsis
            it["pubmed_shared_synopsis"] = synopsis

    overview_body = ""
    if is_cybermed_run and not overview_items and foamed_overview_items:
        if report_language.lower().startswith("en"):
            overview_body = "## Executive Summary\n\nNo new PubMed papers selected in this run. Recent FOAMed posts are listed below.\n"
        else:
            overview_body = "## Kurzüberblick\n\nKeine neuen PubMed-Papers in diesem Lauf; aktuelle FOAMed-Beiträge stehen unten.\n"
    else:
        all_overview_metadata_only = bool(overview_items) and all(
            (it.get("content_status") == "metadata_only") or (it.get("text_source") == "metadata_only")
            for it in overview_items
        )
        if (not is_cybermed_run) and all_overview_metadata_only:
            if report_language.lower().startswith("en"):
                overview_body = (
                    "## Executive Summary\n\n"
                    "Content extraction was unavailable for the selected Cyberlurch videos in this run. "
                    "This summary is metadata-only (title/channel/date) and does not infer themes beyond listed titles.\n"
                )
            else:
                overview_body = (
                    "## Kurzüberblick\n\n"
                    "Die Inhalts-Extraktion war für die ausgewählten Cyberlurch-Videos in diesem Lauf nicht verfügbar. "
                    "Diese Zusammenfassung basiert nur auf Metadaten (Titel/Kanal/Datum) und leitet keine weitergehenden Themen ab.\n"
                )
        else:
            try:
                is_cyberlurch_run = (report_key or "").strip().lower() == "cyberlurch"
                if is_cyberlurch_run:
                    chunk_enabled = _env_bool("CYBERLURCH_CHUNK_TRANSCRIPTS", True)
                    direct_max_chars = _safe_int("CYBERLURCH_DIRECT_TRANSCRIPT_MAX_CHARS", 80000)
                    min_chars = _safe_int("CYBERLURCH_TRANSCRIPT_CHUNKING_MIN_CHARS", 80000)
                    budget = _safe_int("CYBERLURCH_MAX_CHUNKED_TRANSCRIPTS_PER_RUN", 5)
                    chunked = 0
                    for it in sorted(overview_items, key=lambda x: x.get("published_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True):
                        full_text = str(it.get("_full_text_for_processing") or it.get("text") or "")
                        it["transcript_full_chars_available"] = len(full_text)
                        it["transcript_chars_used_for_summary"] = len(str(it.get("text") or ""))
                        it["transcript_was_truncated"] = bool(it["transcript_chars_used_for_summary"] < it["transcript_full_chars_available"])
                        if it.get("text_source") == "managed_transcript" and it.get("content_status") == "full_text":
                            youtube_diag.transcript_full_chars_available_max = max(
                                int(getattr(youtube_diag, "transcript_full_chars_available_max", 0)),
                                len(full_text),
                            )
                            if len(full_text) <= direct_max_chars:
                                youtube_diag.transcript_direct_attempted_total += 1
                                try:
                                    it["_full_text_for_processing"] = full_text
                                    res = summarize_youtube_transcript_direct(it, language=report_language, profile=report_profile)
                                    it.update({k: v for k, v in res.items() if k.startswith("transcript_") or k in {"important_details", "editorial_relevance"}})
                                    it["transcript_processing"] = "direct_full_transcript_fallback" if bool(res.get("fallback_text_used")) else "direct_full_transcript"
                                    it["transcript_direct_success"] = True
                                    it["transcript_chars_used_for_summary"] = len(full_text)
                                    it["transcript_was_truncated"] = False
                                    youtube_diag.transcript_direct_success_total += 1
                                    youtube_diag.transcript_direct_chars_processed_total += int(res.get("chars_processed_total") or 0)
                                    if bool(res.get("json_parse_error")):
                                        youtube_diag.transcript_direct_json_parse_error_total += 1
                                    if bool(res.get("json_recovered")):
                                        youtube_diag.transcript_direct_json_recovered_total += 1
                                    if bool(res.get("fallback_text_used")):
                                        youtube_diag.transcript_direct_fallback_text_total += 1
                                    if bool(res.get("response_format_used")):
                                        youtube_diag.transcript_direct_response_format_used_total += 1
                                    if bool(res.get("response_format_rejected")):
                                        youtube_diag.transcript_direct_response_format_rejected_total += 1
                                except Exception as e:
                                    it["transcript_direct_success"] = False
                                    youtube_diag.transcript_direct_error_total += 1
                                    kind = classify_direct_digest_error(e)
                                    youtube_diag.transcript_direct_error_by_kind[kind] = int(youtube_diag.transcript_direct_error_by_kind.get(kind, 0)) + 1
                                    print(f"[transcript-direct] error_kind={kind} model={OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST} chars={len(full_text)}")
                                    it["transcript_processing"] = "excerpt_fallback"
                            elif len(full_text) >= min_chars:
                                youtube_diag.transcript_chunking_attempted_total += 1
                                if (not chunk_enabled) or chunked >= budget:
                                    youtube_diag.transcript_chunking_skipped_budget_total += 1
                                    it["transcript_processing"] = "excerpt"
                                    continue
                                try:
                                    it["_full_text_for_processing"] = full_text
                                    res = summarize_youtube_transcript_chunks(it, language=report_language, profile=report_profile)
                                    it.update({k:v for k,v in res.items() if k.startswith("transcript_")})
                                    it["transcript_processing"] = "chunked_full_transcript"
                                    it["transcript_chunking_success"] = True
                                    it["transcript_chars_used_for_summary"] = len(full_text)
                                    it["transcript_was_truncated"] = False
                                    youtube_diag.transcript_chunking_success_total += 1
                                    youtube_diag.transcript_chunks_total += int(res.get("chunks_total") or 0)
                                    youtube_diag.transcript_chars_processed_total += int(res.get("chars_processed_total") or 0)
                                    chunked += 1
                                except Exception:
                                    it["transcript_chunking_success"] = False
                                    youtube_diag.transcript_chunking_error_total += 1
                                    it["transcript_processing"] = "excerpt"
                            else:
                                youtube_diag.transcript_chunking_not_needed_total += 1
                    full_lengths = [
                        len(str(it.get("_full_text_for_processing") or it.get("text") or ""))
                        for it in overview_items
                        if (it.get("text_source") == "managed_transcript" and (it.get("_full_text_for_processing") or it.get("text")))
                    ]
                    if full_lengths:
                        youtube_diag.transcript_full_chars_available_max = max(full_lengths)
                        youtube_diag.transcript_full_chars_available_median = int(median(full_lengths))
                    for it in overview_items:
                        if it.get("text_source") != "managed_transcript":
                            continue
                        if str(it.get("transcript_processing") or "").strip() == "chunked_full_transcript" and bool(it.get("transcript_chunking_success")):
                            youtube_diag.managed_transcript_chunked_total += 1
                            youtube_diag.transcript_processing_chunked_total += 1
                        elif str(it.get("transcript_processing") or "").strip() in {"direct_full_transcript", "direct_full_transcript_fallback"} and bool(it.get("transcript_direct_success")):
                            youtube_diag.transcript_processing_direct_total += 1
                        elif str(it.get("transcript_processing") or "").strip() == "excerpt_fallback":
                            youtube_diag.managed_transcript_excerpt_total += 1
                            youtube_diag.transcript_processing_excerpt_total += 1
                        elif bool(it.get("transcript_was_truncated")):
                            youtube_diag.managed_transcript_excerpt_total += 1
                            youtube_diag.transcript_processing_excerpt_total += 1
                        else:
                            youtube_diag.managed_transcript_full_within_limit_total += 1
                            youtube_diag.transcript_processing_not_needed_total += 1
                summarize_start = time.monotonic()
                overview_body = summarize(overview_items, language=report_language, profile=report_profile).strip()
                runtime_summarization_seconds += max(0.0, time.monotonic() - summarize_start)
            except Exception as e:
                print(f"[summarize] ERROR: summarize() failed: {e!r}")
                if report_language.lower().startswith("en"):
                    overview_body = "## Executive Summary\n\n**Error:** Failed to generate overview.\n"
                else:
                    overview_body = "## Kurzüberblick\n\n**Fehler:** Konnte Kurzüberblick nicht erzeugen.\n"

    if cybermed_meta_block:
        overview_body = cybermed_meta_block + overview_body
    if deep_dive_skip_note and deep_dive_skip_note not in overview_body:
        overview_body = overview_body.rstrip() + "\n\n" + deep_dive_skip_note + "\n"

    if is_cybermed_run and (pubmed_use_pmc_oa_fulltext or unpaywall_enabled):
        pubmed_detail_items = [
            it for it in detail_items if (it.get("source") or "").strip().lower() == "pubmed"
        ]
        attempted = len(pubmed_detail_items)
        enriched = 0
        oa_found = 0
        downloaded = 0
        skipped_size = 0
        unpaywall_found = 0
        unpaywall_downloaded = 0
        if attempted:
            try:
                pmcid_map: Dict[str, str] = {}
                if pubmed_use_pmc_oa_fulltext:
                    pmids = [(it.get("pmid") or it.get("id") or "").strip() for it in pubmed_detail_items]
                    pmcid_map = get_pmcids_for_pmids(pmids, timeout=pubmed_fulltext_timeout_s)
                for it in pubmed_detail_items:
                    it.setdefault("fulltext_source", "none")
                    pmid = (it.get("pmid") or it.get("id") or "").strip()
                    pmcid = pmcid_map.get(pmid, "")
                    if pubmed_use_pmc_oa_fulltext and pmid and pmcid:
                        pmc_found, pmc_downloaded, pmc_skipped = _apply_pmc_fulltext(
                            it,
                            pmcid,
                            timeout_s=float(pubmed_fulltext_timeout_s),
                            max_bytes=pubmed_fulltext_max_bytes,
                            max_chars=pubmed_fulltext_max_chars,
                        )
                        if pmc_found:
                            oa_found += 1
                        if pmc_downloaded:
                            downloaded += 1
                            enriched += 1
                        if pmc_skipped:
                            skipped_size += 1
                    if (
                        unpaywall_enabled
                        and it.get("fulltext_source", "none") in {"", "none"}
                        and (it.get("doi") or "").strip()
                    ):
                        found, dl_ok, size_exceeded = _apply_unpaywall_fulltext(
                            it,
                            email=unpaywall_email,
                            timeout_s=float(pubmed_fulltext_timeout_s),
                            max_bytes=pubmed_fulltext_max_bytes,
                            max_chars=pubmed_fulltext_max_chars,
                            min_chars=pubmed_unpaywall_min_chars,
                        )
                        if found:
                            unpaywall_found += 1
                        if dl_ok:
                            unpaywall_downloaded += 1
                            enriched += 1
                        if size_exceeded:
                            skipped_size += 1
            except Exception as e:
                print(f"[pmc] WARN: deepdive_fulltext enrichment failed: {e!r}")
        print(
            f"[pmc] deepdive_fulltext: attempted={attempted} enriched={enriched} "
            f"(oa_found={oa_found}, downloaded={downloaded}, unpaywall_oa_found={unpaywall_found}, "
            f"unpaywall_downloaded={unpaywall_downloaded}, skipped_size={skipped_size})"
        )

    if is_cybermed_run:
        pubmed_detail_items = [
            it for it in detail_items if (it.get("source") or "").strip().lower() == "pubmed"
        ]
        missing_before = 0
        missing_pmids: List[str] = []
        for it in pubmed_detail_items:
            abstract_raw = (it.get("abstract") or "").strip()
            if (
                not abstract_raw
                or "no abstract" in abstract_raw.lower()
                or len(abstract_raw) < 200
            ):
                missing_before += 1
                pmid = (it.get("pmid") or it.get("id") or "").strip()
                if pmid:
                    missing_pmids.append(pmid)

        fetched: Dict[str, str] = {}
        if missing_pmids:
            try:
                fetched = fetch_pubmed_abstracts(missing_pmids)
            except Exception as e:
                print(f"[deepdive] WARN: fetch_pubmed_abstracts failed: {e!r}")

        refetched = 0
        for it in pubmed_detail_items:
            pmid = (it.get("pmid") or it.get("id") or "").strip()
            existing_abs = (it.get("abstract") or "").strip()
            fetched_abs = fetched.get(pmid, "").strip()
            if fetched_abs and len(fetched_abs) > len(existing_abs):
                it["abstract"] = fetched_abs
                refetched += 1

            title = (it.get("title") or "").strip()
            journal = (it.get("journal") or "").strip()
            pub_year = ""
            pub_dt = it.get("published_at")
            if isinstance(pub_dt, datetime):
                pub_year = str(pub_dt.year)
            id_parts: List[str] = []
            if pmid:
                id_parts.append(f"PMID {pmid}")
            doi_val = (it.get("doi") or "").strip()
            if doi_val:
                id_parts.append(f"DOI {doi_val}")
            abstract_block = (it.get("abstract") or "").strip()
            fulltext_block = (it.get("full_text_excerpt") or "").strip()

            lines: List[str] = []
            if title:
                lines.append(title)
            if journal and pub_year:
                lines.append(f"{journal} ({pub_year})")
            elif journal:
                lines.append(journal)
            elif pub_year:
                lines.append(pub_year)
            if id_parts:
                lines.append(" / ".join(id_parts))
            lines.append("")
            if abstract_block:
                lines.append(abstract_block)
            if fulltext_block:
                if abstract_block:
                    lines.append("")
                lines.append(fulltext_block)
            it["text"] = "\n".join(lines).strip()

        abstract_lengths = sorted([len((it.get("abstract") or "").strip()) for it in pubmed_detail_items])
        fulltext_lengths = sorted([len((it.get("full_text_excerpt") or "").strip()) for it in pubmed_detail_items])

        def _stats(lengths: List[int]) -> str:
            if not lengths:
                return "0/0/0"
            return f"{lengths[0]}/{int(median(lengths))}/{lengths[-1]}"

        print(
            "[deepdive] pubmed_inputs: "
            f"items={len(pubmed_detail_items)} "
            f"missing_before={missing_before} "
            f"refetched={refetched} "
            f"abstract_len(min/med/max)={_stats(abstract_lengths)} "
            f"fulltext_len(min/med/max)={_stats(fulltext_lengths)}"
        )

    deep_dive_requested = len(detail_items)
    deep_dive_retried = 0
    deep_dive_empty_outputs = 0
    missing_abstract_count = 0
    placeholder_counts: List[int] = []
    pubmed_deep_dive_items = [
        it for it in detail_items if (it.get("source") or "").strip().lower() == "pubmed"
    ]
    pubmed_missing_abs_pmids: List[str] = []
    for it in pubmed_deep_dive_items:
        abstract_raw = (it.get("abstract") or "").strip()
        if not abstract_raw:
            pmid = (it.get("pmid") or it.get("id") or "").strip()
            if pmid:
                pubmed_missing_abs_pmids.append(pmid)

    fetched_pubmed_abstracts: Dict[str, str] = {}
    if pubmed_missing_abs_pmids:
        try:
            fetched_pubmed_abstracts = fetch_pubmed_abstracts(pubmed_missing_abs_pmids)
        except Exception as e:
            print(f"[deepdive] WARN: fetch_pubmed_abstracts (pre-deepdive) failed: {e!r}")

    refetched_abstracts = 0
    evidence_lengths: List[int] = []
    for it in pubmed_deep_dive_items:
        pmid = (it.get("pmid") or it.get("id") or "").strip()
        abstract_val = (it.get("abstract") or "").strip()
        if pmid and not abstract_val:
            fetched_abs = (fetched_pubmed_abstracts.get(pmid) or "").strip()
            if fetched_abs:
                it["abstract"] = fetched_abs
                abstract_val = fetched_abs
                refetched_abstracts += 1

        title = (it.get("title") or "").strip()
        fulltext_excerpt = (it.get("full_text_excerpt") or "").strip()
        evidence_parts: List[str] = []
        if title:
            evidence_parts.append(title)
        if abstract_val:
            evidence_parts.append(f"ABSTRACT:\n{abstract_val}")
        if fulltext_excerpt:
            evidence_parts.append(f"OA FULLTEXT EXCERPT:\n{fulltext_excerpt}")
        evidence_text = "\n\n".join(evidence_parts).strip()
        if evidence_text:
            it["text"] = evidence_text
        evidence_len = len(evidence_text)
        evidence_lengths.append(evidence_len)
        print(
            f"[deepdive] pubmed_evidence pmid={pmid} abstract_chars={len(abstract_val)} fulltext_chars={len(fulltext_excerpt)} evidence_chars={evidence_len}"
        )

    def _stats(values: List[int]) -> str:
        if not values:
            return "0/0/0"
        ordered = sorted(values)
        return f"{ordered[0]}/{int(median(ordered))}/{ordered[-1]}"

    if pubmed_deep_dive_items:
        print(
            "[deepdive] pubmed_evidence_stats: "
            f"items={len(pubmed_deep_dive_items)} "
            f"missing_abstracts={len(pubmed_missing_abs_pmids)} "
            f"refetched={refetched_abstracts} "
            f"evidence_chars(min/med/max)={_stats(evidence_lengths)}"
        )
    for it in detail_items:
        src = (it.get("source") or "").strip().lower()
        if src == "pubmed":
            if not (it.get("abstract") or "").strip():
                missing_abstract_count += 1

    details_by_id: Dict[str, str] = {}
    details_for_report: Dict[str, str] = {}
    for it in detail_items:
        if (not is_cybermed_run) and ((it.get("content_status") == "metadata_only") or (it.get("text_source") == "metadata_only")):
            continue
        src = (it.get("source") or "").strip().lower() or "youtube"
        iid_raw = str(it.get("id") or "").strip()
        iid = str(it.get("id") or it.get("url") or it.get("title") or "").strip()
        if not iid and not iid_raw:
            continue
        key = f"{src}:{iid_raw}" if iid_raw else ""
        try:
            detail_block = summarize_item_detail(it, language=report_language, profile=report_profile).strip()
        except Exception as e:
            print(f"[summarize] WARN: summarize_item_detail failed for {key!r}: {e!r}")
            if report_language.lower().startswith("en"):
                detail_block = "Key takeaways:\n- (Failed to generate deep dive.)\n"
            else:
                detail_block = "Kernaussagen:\n- (Fehler beim Erzeugen der Detail-Zusammenfassung)\n"

        if key:
            details_by_id[key] = detail_block
        if iid_raw and detail_block:
            details_by_id.setdefault(iid_raw, detail_block)
        if iid and detail_block:
            details_for_report[iid] = detail_block
        if key and detail_block:
            details_for_report.setdefault(key, detail_block)

        if it.get("_deep_dive_retried"):
            deep_dive_retried += 1
        if it.get("_deep_dive_empty_output"):
            deep_dive_empty_outputs += 1
        if src == "pubmed":
            if it.get("_deep_dive_parse_fallback"):
                deep_dive_diag["parse_fallback_used_count"] += 1
            if it.get("_deep_dive_all_fields_placeholder"):
                deep_dive_diag["not_reported_all_fields_count"] += 1
            if it.get("_deep_dive_json_failed"):
                deep_dive_diag["json_failures_count"] += 1
            if it.get("_deep_dive_used_markdown_fallback"):
                deep_dive_diag["markdown_fallback_used_count"] += 1
            if it.get("_deep_dive_sparse_after_json"):
                deep_dive_diag["sparse_after_json_count"] += 1
            placeholder_count = int(it.get("_deep_dive_placeholder_value_count") or 0)
            placeholder_counts.append(placeholder_count)
            if it.get("_deep_dive_placeholder_rerun"):
                deep_dive_diag["placeholder_rerun_count"] += 1
            if placeholder_count >= 5:
                deep_dive_diag["placeholder_value_high_count"] += 1
            if it.get("_deep_dive_structured_rescue_used"):
                deep_dive_diag["structured_rescue_used_count"] += 1

    if is_cybermed_run:
        overview_map = {str(it.get("id") or "").strip(): it for it in overview_items if (it.get("source") or "").strip().lower() == "pubmed"}
        for it in detail_items:
            if (it.get("source") or "").strip().lower() != "pubmed":
                continue
            iid = str(it.get("id") or "").strip()
            if not iid or iid not in overview_map:
                continue
            consistency_checked_total += 1
            ov_item = overview_map[iid]
            synopsis = pubmed_shared_synopsis.get(iid) or _build_pubmed_shared_synopsis(ov_item)
            shared_bottom_line = str(synopsis.get("bottom_line") or "").strip()
            overview_bl = str(ov_item.get("bottom_line") or "").strip()
            deep_text = str(details_for_report.get(iid) or details_by_id.get(f"pubmed:{iid}") or "").strip()
            conflicts = _detect_pubmed_bottom_line_conflicts(overview_bl, deep_text)
            status = "consistent"
            action = ""
            if conflicts:
                consistency_conflict_total += 1
                consistency_conflict_type_counts.update(conflicts)
                ov_item["bottom_line"] = shared_bottom_line or overview_bl
                if shared_bottom_line:
                    details_for_report[iid] = f"**BOTTOM LINE:** {shared_bottom_line}\n\n{deep_text}"
                consistency_resolved_total += 1
                status = "resolved_conflict"
                action = "replace_overview_bottom_line_with_shared_synopsis"
            consistency_preview.append({
                "evidence_strength_label": str(ov_item.get("evidence_strength_label") or ""),
                "evidence_strength_label_basis": str(ov_item.get("evidence_strength_label_basis") or ""),
                "study_type_present": bool(str(synopsis.get("study_type") or "").strip()),
                "primary_endpoint_present": bool(str(synopsis.get("primary_endpoint") or "").strip()),
                "primary_result_direction": str(synopsis.get("primary_result_direction") or ""),
                "primary_result_significance": str(synopsis.get("primary_result_significance") or ""),
                "consistency_status": status,
                "conflict_types": conflicts,
                "resolution_action": action,
            })

    deep_dive_diag["enriched_fulltext_count"] = len(
        [
            it
            for it in detail_items
            if (it.get("source") or "").strip().lower() == "pubmed" and it.get("_deep_dive_fulltext_enriched")
        ]
    )
    deep_dive_diag["unpaywall_oa_found_count"] = len(
        [
            it
            for it in detail_items
            if (it.get("source") or "").strip().lower() == "pubmed" and it.get("_deep_dive_unpaywall_found")
        ]
    )
    deep_dive_diag["download_success_count"] = len(
        [
            it
            for it in detail_items
            if (it.get("source") or "").strip().lower() == "pubmed"
            and (it.get("_deep_dive_unpaywall_downloaded") or it.get("_deep_dive_pmc_downloaded"))
        ]
    )
    if placeholder_counts:
        placeholder_counts_sorted = sorted(placeholder_counts)
        deep_dive_diag["placeholder_value_min"] = placeholder_counts_sorted[0]
        deep_dive_diag["placeholder_value_median"] = int(median(placeholder_counts_sorted))
        deep_dive_diag["placeholder_value_max"] = placeholder_counts_sorted[-1]
    else:
        deep_dive_diag["placeholder_value_min"] = 0
        deep_dive_diag["placeholder_value_median"] = 0
        deep_dive_diag["placeholder_value_max"] = 0

    placeholder_stats = (
        f"{deep_dive_diag['placeholder_value_min']}/"
        f"{deep_dive_diag['placeholder_value_median']}/"
        f"{deep_dive_diag['placeholder_value_max']}"
    )
    print(
        "[deepdive] diagnostics: "
        f"total={deep_dive_diag.get('total_items', 0)} "
        f"placeholder_reruns={deep_dive_diag.get('placeholder_rerun_count', 0)} "
        f"placeholder_count(min/med/max)={placeholder_stats}"
    )

    if is_cybermed_run and not deep_dive_ids and details_for_report:
        for iid in details_for_report.keys():
            iid = (iid or "").strip()
            if iid:
                deep_dive_ids.add(iid)

    if not is_cybermed_run:
        default_cap = 12
        if report_mode in {"weekly", "monthly"}:
            default_cap = 20
        cap = max(0, _safe_int("CYBERLURCH_BOTTOM_LINE_MAX_ITEMS", default_cap))
        seen_keys: Set[str] = set()
        candidates: List[Dict[str, Any]] = []
        skipped_thin = 0

        def _mark_keys(item: Dict[str, Any]) -> None:
            keys = {
                str(item.get("id") or "").strip(),
                str(item.get("url") or "").strip(),
                str(item.get("title") or "").strip(),
            }
            for key in keys:
                if key:
                    seen_keys.add(key)

        for it in overview_items + detail_items:
            if not isinstance(it, dict):
                continue
            keys = [
                str(it.get("id") or "").strip(),
                str(it.get("url") or "").strip(),
                str(it.get("title") or "").strip(),
            ]
            if any(key and key in seen_keys for key in keys):
                continue
            _mark_keys(it)
            bottom_line = (it.get("bottom_line") or "").strip()
            if bottom_line:
                continue
            text_len = len((it.get("text") or "").strip())
            if text_len < 200:
                skipped_thin += 1
                continue
            candidates.append(it)

        generated = 0
        lengths: List[int] = []
        for it in candidates[:cap]:
            try:
                summary = summarize_cyberlurch_bottom_line(it, language=report_language) or ""
            except Exception:
                summary = ""
            summary = summary.strip()
            if summary:
                it["bottom_line"] = summary
                generated += 1
                lengths.append(len(summary))

        def _stats(values: List[int]) -> str:
            if not values:
                return "0/0/0"
            ordered = sorted(values)
            return f"{ordered[0]}/{int(median(ordered))}/{ordered[-1]}"

        print(
            "[bottomline] cyberlurch: "
            f"candidates={len(candidates)} generated={generated} skipped_thin={skipped_thin} "
            f"bl_chars(min/med/max)={_stats(lengths)}"
        )

    out_path = _report_output_path(report_dir, report_key, report_mode)

    if not is_cybermed_run:
        for it in overview_items:
            iid = str(it.get("id") or it.get("url") or it.get("title") or "").strip()
            if not iid or iid in details_for_report:
                continue
            bl = (it.get("bottom_line") or "").strip()
            if bl:
                details_for_report[iid] = f"**BOTTOM LINE:** {bl}"

    if is_cybermed_run and deep_dive_ids:
        for it in report_items + detail_items:
            iid = str(it.get("id") or it.get("url") or it.get("title") or "").strip()
            if iid and iid in deep_dive_ids:
                it["top_pick"] = True

    if is_cybermed_run:
        cybermed_run_stats.setdefault("pubmed", {})
        cybermed_run_stats["pubmed"]["generated_deep_dives"] = len(details_for_report)
        cybermed_run_stats["deep_dives"] = {
            "candidates": deep_dive_diag.get("candidates", 0),
            "total_deep_dive_items": deep_dive_diag.get("total_items", 0),
            "requested_deep_dives": deep_dive_requested,
            "generated_deep_dives": len(details_for_report),
            "retried_deep_dives": deep_dive_retried,
            "empty_deep_dive_outputs": deep_dive_empty_outputs,
            "missing_abstract_count": missing_abstract_count,
            "enriched_fulltext_count": deep_dive_diag.get("enriched_fulltext_count", 0),
            "unpaywall_oa_found_count": deep_dive_diag.get("unpaywall_oa_found_count", 0),
            "download_success_count": deep_dive_diag.get("download_success_count", 0),
            "parse_fallback_used_count": deep_dive_diag.get("parse_fallback_used_count", 0),
            "not_reported_all_fields_count": deep_dive_diag.get("not_reported_all_fields_count", 0),
            "deep_dive_json_failures": deep_dive_diag.get("json_failures_count", 0),
            "deep_dive_markdown_fallbacks": deep_dive_diag.get("markdown_fallback_used_count", 0),
            "deep_dive_sparse_after_json": deep_dive_diag.get("sparse_after_json_count", 0),
            "placeholder_reruns": deep_dive_diag.get("placeholder_rerun_count", 0),
            "placeholder_value_high_count": deep_dive_diag.get("placeholder_value_high_count", 0),
            "placeholder_value_min": deep_dive_diag.get("placeholder_value_min", 0),
            "placeholder_value_median": deep_dive_diag.get("placeholder_value_median", 0),
            "placeholder_value_max": deep_dive_diag.get("placeholder_value_max", 0),
            "structured_rescue_used_count": deep_dive_diag.get("structured_rescue_used_count", 0),
        }

    for _it in report_items:
        _it.pop("_full_text_for_processing", None)

    if report_key.strip().lower() == "cyberlurch":
        _annotate_cyberlurch_item_topics(report_items, channel_topics)
        _annotate_cyberlurch_item_topics(detail_items, channel_topics)

    report_render_start = time.monotonic()
    md = to_markdown(
        report_items,
        overview_body,
        details_for_report,
        report_title=report_title,
        report_language=report_language,
        foamed_stats=foamed_meta_stats,
        cybermed_stats=cybermed_run_stats if is_cybermed_run else None,
        report_mode=report_mode,
        run_metadata=run_metadata,
    )

    runtime_report_render_seconds += max(0.0, time.monotonic() - report_render_start)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"[report] Wrote {out_path}")
    if report_key.strip().lower() == "cyberlurch":
        items_by_topic: dict[str, int] = {}
        deep_dives_by_topic: dict[str, int] = {}
        for it in report_items:
            for topic in channel_topics.get(str(it.get("channel") or "").strip(), []):
                items_by_topic[topic] = items_by_topic.get(topic, 0) + 1
        for it in detail_items:
            for topic in channel_topics.get(str(it.get("channel") or "").strip(), []):
                deep_dives_by_topic[topic] = deep_dives_by_topic.get(topic, 0) + 1
        items_by_temporality: Dict[str, int] = {}
        for it in report_items:
            temp = str(it.get("temporality") or "").strip() or "unknown"
            items_by_temporality[temp] = items_by_temporality.get(temp, 0) + 1
        extra_counts = {
            "channels_config_total": len(channels),
            "topic_buckets_total": len(topic_weights),
            "active_topic_buckets_total": len([k for k, v in items_by_topic.items() if v > 0]),
            "items_by_topic": items_by_topic,
            "deep_dives_selected_total": len(detail_items),
            "cadence_profile": cyberlurch_cadence_profile(report_mode) if report_key.strip().lower() == "cyberlurch" else {},
            "items_by_temporality": items_by_temporality,
            **(cyberlurch_diag if report_key.strip().lower()=="cyberlurch" else {}),
            "deep_dives_by_topic": deep_dives_by_topic,
            "weekly_digest_items_total": weekly_digest_items_total,
            "weekly_digest_used_total": weekly_digest_used_total,
            "weekly_digest_fallback_collection_used": weekly_digest_fallback_collection_used,
            "weekly_digest_supplemental_collection_items_total": weekly_digest_supplemental_collection_items_total,
            "weekly_digest_full_text_ratio": weekly_digest_full_text_ratio,
            "monthly_digest_items_total": monthly_digest_items_total,
            "monthly_digest_used_total": monthly_digest_used_total,
            "monthly_digest_fallback_collection_used": monthly_digest_fallback_collection_used,
            "monthly_digest_supplemental_collection_items_total": monthly_digest_supplemental_collection_items_total,
            "monthly_digest_period_start": monthly_digest_period_start,
            "monthly_digest_period_end": monthly_digest_period_end,
            "cyberlurch_digest_invalid_records_removed_total": cyberlurch_digest_invalid_records_removed_total,
            "cyberlurch_digest_invalid_records_skipped_total": cyberlurch_digest_invalid_records_skipped_total,
            "digest_store_loaded_total": digest_store_loaded_total,
            "digest_store_selected_total": digest_store_selected_total,
            "digest_store_used_as_primary": digest_store_used_as_primary,
            "digest_store_collection_skipped_due_to_primary": digest_store_collection_skipped_due_to_primary,
            "digest_store_collection_fallback_used": digest_store_collection_fallback_used,
            "digest_store_collection_supplement_used": digest_store_collection_supplement_used,
        }
        _write_run_metadata_artifact(report_dir, report_key, report_mode, run_metadata)

    now_utc_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    if report_mode == "monthly":
        try:
            rollups_state = load_rollups_state(rollups_state_path)
            override = (os.getenv("ROLLUP_MONTH_OVERRIDE") or "").strip()
            month_key = determine_monthly_rollup_month(datetime.now(tz=STO), os.getenv("GITHUB_EVENT_NAME", ""), override)
            if override and month_key != override:
                print(f"[rollups] WARN: invalid ROLLUP_MONTH_OVERRIDE={override!r}; expected YYYY-MM")
            candidates = overview_items + detail_items + foamed_overview_items
            _ensure_bottom_lines_for_rollup(candidates, language=report_language)
            rollup_items = _rollup_items_for_month(overview_items, detail_items, foamed_overview_items)
            if report_key.strip().lower() == "cyberlurch":
                rollup_items = _rollup_items_for_month(overview_items, detail_items, foamed_overview_items, max_items=30)
                annotate_cyberlurch_temporality(rollup_items)
            executive_summary = derive_monthly_summary(
                overview_body,
                top_items=rollup_items,
                max_bullets=8,
            )
            extra_fields = None
            if report_key.strip().lower() == "cyberlurch":
                tcounts = Counter(str(it.get("topic_primary") or "Other") for it in rollup_items)
                ccounts = Counter(str(it.get("channel") or "Unknown") for it in rollup_items)
                tmpcounts: Counter[str] = Counter()
                for it in rollup_items:
                    temporality_value = str(it.get("temporality") or "").strip() or "current_affairs"
                    tmpcounts[temporality_value] += 1
                extra_fields = {
                    "topic_summaries": [f"{k}: {v} item(s)" for k, v in tcounts.most_common(8)],
                    "topic_trajectories": [f"{k}: sustained stream" for k, v in tcounts.most_common(6) if v >= 2],
                    "top_channels": [{"channel": k, "count": v} for k, v in ccounts.most_common(10)],
                    "top_themes": [{"theme": k, "count": v} for k, v in tcounts.most_common(10)],
                    "evergreen_highlights": [it.get("title") for it in rollup_items if str(it.get("temporality") or "") == "evergreen"][:8],
                    "representative_items": rollup_items[:20],
                    "full_text_count": sum(1 for it in rollup_items if str(it.get("content_status") or "") != "metadata_only"),
                    "metadata_only_count": sum(1 for it in rollup_items if str(it.get("content_status") or "") == "metadata_only"),
                    "items_by_temporality": dict(tmpcounts),
                }
            upsert_monthly_rollup(
                rollups_state,
                report_key=report_key,
                month=month_key,
                generated_at=now_utc_iso,
                executive_summary=executive_summary,
                top_items=rollup_items,
                extra_fields=extra_fields,
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

    if report_key.strip().lower() == "cyberlurch" and report_mode == "daily":
        dstate = _load_cyberlurch_digest_state(cyberlurch_digest_state_path)
        dstate, removed_invalid = sanitize_cyberlurch_digest_state(dstate)
        cyberlurch_digest_invalid_records_removed_total += removed_invalid
        upsert_source = _dedupe_items(items_all_new + report_items + overview_items + detail_items)
        upserted, pruned = _upsert_cyberlurch_digests(dstate, upsert_source, cyberlurch_digest_retention_days)
        _save_cyberlurch_digest_state(cyberlurch_digest_state_path, dstate, read_only_mode=read_only_mode)
        youtube_diag.cyberlurch_digest_upserted_total = upserted
        youtube_diag.cyberlurch_digest_pruned_total = pruned
        youtube_diag.cyberlurch_digest_store_total = len(dstate.get("digests", []))
        youtube_diag.cyberlurch_digest_invalid_records_removed_total = cyberlurch_digest_invalid_records_removed_total
        youtube_diag.cyberlurch_digest_invalid_records_skipped_total = cyberlurch_digest_invalid_records_skipped_total
        print(f"[digest-store] upserted={upserted} pruned={pruned} total={youtube_diag.cyberlurch_digest_store_total} path={cyberlurch_digest_state_path}")
    cybermed_digest_diag = {
        "cybermed_digest_store_enabled": False,
        "cybermed_digest_store_path": cybermed_digest_state_path,
        "cybermed_digest_store_abs_path": cybermed_digest_state_abs_path,
        "cybermed_digest_store_written": False,
        "cybermed_digest_store_skipped_reason": "",
        "cybermed_digest_store_digest_id": "",
        "cybermed_digest_store_items_pubmed_total": 0,
        "cybermed_digest_store_items_foamed_total": 0,
        "cybermed_digest_store_deep_dives_total": 0,
        "cybermed_digest_store_top_picks_total": 0,
        "cybermed_digest_store_overwrite": False,
        "cybermed_digest_store_schema_version": 1,
        "cybermed_digest_store_write_verified": False,
        "cybermed_digest_store_file_exists_after_write": False,
        "cybermed_digest_store_digest_count_after_write": 0,
        "cybermed_digest_store_expected_digest_present": False,
        "cybermed_digest_store_write_error_class": "",
    }
    if report_key.strip().lower() == "cybermed" and report_mode == "daily":
        email_mode = (os.getenv("EMAIL_MODE", "") or "").strip().lower()
        send_email = (os.getenv("SEND_EMAIL", "1") or "1").strip()
        event_name = (os.getenv("GITHUB_EVENT_NAME", "") or "").strip().lower()
        qa_replay_allow = _env_bool("CYBERMED_DIGEST_STORE_ALLOW_QA_REPLAY", False)
        overwrite_requested = _env_bool("CYBERMED_DIGEST_STORE_OVERWRITE", False)
        digest_id = f"cybermed_daily_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        overwrite_allowed = overwrite_requested and email_mode == "none" and send_email == "0" and event_name in {"workflow_dispatch", "manual"}
        cybermed_digest_diag.update({
            "cybermed_digest_store_enabled": True,
            "cybermed_digest_store_digest_id": digest_id,
            "cybermed_digest_store_overwrite": overwrite_allowed,
        })
        print(f"[cybermed-digest-store] enabled=True digest_id={digest_id} overwrite_allowed={overwrite_allowed} path={cybermed_digest_state_path} abs_path={cybermed_digest_state_abs_path}")
        skip_reason = ""
        if qa_replay_enabled and not qa_replay_allow:
            skip_reason = "qa_replay_mode"
        if qa_replay_enabled and qa_replay_allow and not overwrite_allowed:
            skip_reason = "qa_replay_requires_safe_overwrite"
        if not skip_reason:
            dstate = _load_cybermed_daily_digest_state(cybermed_digest_state_path)
            digests = list(dstate.get("digests") or [])
            existing_idx = next((i for i, d in enumerate(digests) if str((d or {}).get("digest_id") or "") == digest_id), -1)
            if existing_idx >= 0 and not overwrite_allowed:
                skip_reason = "digest_already_exists"
            else:
                pubmed_items = [_sanitize_cybermed_pubmed_item(it) for it in pubmed_overview_items]
                foamed_items = [_sanitize_cybermed_foamed_item(it) for it in foamed_overview_items]
                deep_dives = []
                for it in pubmed_deep_dive_items:
                    deep_dives.append({
                        "item_id": str(it.get("id") or "").strip(),
                        "pmid": str(it.get("pmid") or "").strip(),
                        "doi": str(it.get("doi") or "").strip(),
                        "title": str(it.get("title") or "").strip(),
                        "journal": str(it.get("journal") or "").strip(),
                        "url": str(it.get("url") or "").strip(),
                        "evidence_strength_label": str(it.get("evidence_strength_label") or "").strip(),
                        "clinical_relevance_1_5": it.get("clinical_relevance_1_5"),
                        "practice_change_potential_1_5": it.get("practice_change_potential_1_5"),
                        "bottom_line": str(it.get("bottom_line") or "").strip(),
                        "deep_dive_reasons": [str(v) for v in (it.get("deep_dive_reasons") or []) if str(v).strip()],
                        "study_type": str(it.get("study_type") or "").strip(),
                        "population_setting": str(it.get("population_setting") or "").strip(),
                        "intervention_or_exposure": str(it.get("intervention_or_exposure") or "").strip(),
                        "comparator": str(it.get("comparator") or "").strip(),
                        "primary_endpoint": str(it.get("primary_endpoint") or "").strip(),
                        "primary_result_direction": str(it.get("primary_result_direction") or "").strip(),
                        "primary_result_significance": str(it.get("primary_result_significance") or "").strip(),
                        "clinical_interpretation": str(it.get("clinical_interpretation") or "").strip(),
                    })
                top_picks = [str((it or {}).get("id") or "").strip() for it in (pubmed_overview_items + foamed_overview_items) if (it or {}).get("top_pick")]
                digest_payload = {
                    "digest_id": digest_id,
                    "report_key": "cybermed",
                    "cadence": "daily",
                    "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "generated_at_utc": now_utc_iso,
                    "lookback_hours": int(args.hours),
                    "qa_replay": bool(qa_replay_enabled),
                    "email_mode": email_mode,
                    "items": {"pubmed": pubmed_items, "foamed": foamed_items},
                    "deep_dives": deep_dives,
                    "top_picks": top_picks,
                    "diagnostic_summary": {
                        "pubmed_selected_total": len(pubmed_items),
                        "pubmed_deep_dive_total": len(deep_dives),
                        "pubmed_evidence_label_counts": dict(Counter(str(it.get("evidence_strength_label") or "") for it in pubmed_items if str(it.get("evidence_strength_label") or "").strip())),
                        "foamed_selected_total": len(foamed_items),
                        "foamed_top_pick_total": len([1 for it in foamed_items if it.get("top_pick")]),
                        "foamed_source_quality_counts": dict(Counter(str(it.get("source_quality_label") or "") for it in foamed_items if str(it.get("source_quality_label") or "").strip())),
                        "foamed_text_confidence_counts": dict(Counter(str(it.get("text_confidence_label") or "") for it in foamed_items if str(it.get("text_confidence_label") or "").strip())),
                        "summary_consistency_conflict_total": int(consistency_conflict_total),
                        "summary_consistency_resolved_total": int(consistency_resolved_total),
                        "qa_replay": bool(qa_replay_enabled),
                        "state_mutation_disabled": bool(qa_replay_state_mutation_disabled),
                    },
                }
                if existing_idx >= 0:
                    digests[existing_idx] = digest_payload
                else:
                    digests.append(digest_payload)
                dstate["schema_version"] = 1
                dstate["digests"] = digests
                cybermed_digest_diag.update({
                    "cybermed_digest_store_items_pubmed_total": len(pubmed_items),
                    "cybermed_digest_store_items_foamed_total": len(foamed_items),
                    "cybermed_digest_store_deep_dives_total": len(deep_dives),
                    "cybermed_digest_store_top_picks_total": len(top_picks),
                })
                if not read_only_mode:
                    try:
                        os.makedirs(os.path.dirname(cybermed_digest_state_path) or ".", exist_ok=True)
                        with open(cybermed_digest_state_path, "w", encoding="utf-8") as f:
                            json.dump(dstate, f, ensure_ascii=False, separators=(",", ":"))
                            f.write("\n")
                        cybermed_digest_diag["cybermed_digest_store_file_exists_after_write"] = os.path.exists(cybermed_digest_state_path)
                        verified_state = _load_cybermed_daily_digest_state(cybermed_digest_state_path)
                        verified_digests = list(verified_state.get("digests") or [])
                        cybermed_digest_diag["cybermed_digest_store_digest_count_after_write"] = len(verified_digests)
                        verified_digest = next((d for d in verified_digests if str((d or {}).get("digest_id") or "") == digest_id), None)
                        expected_present = verified_digest is not None
                        cybermed_digest_diag["cybermed_digest_store_expected_digest_present"] = expected_present
                        counts_match = True
                        if verified_digest is not None:
                            stored_items = (verified_digest.get("items") or {})
                            if len(pubmed_items) > 0:
                                counts_match = counts_match and len(list(stored_items.get("pubmed") or [])) == len(pubmed_items)
                            if len(foamed_items) > 0:
                                counts_match = counts_match and len(list(stored_items.get("foamed") or [])) == len(foamed_items)
                            if len(deep_dives) > 0:
                                counts_match = counts_match and len(list(verified_digest.get("deep_dives") or [])) == len(deep_dives)
                            if len(top_picks) > 0:
                                counts_match = counts_match and len(list(verified_digest.get("top_picks") or [])) == len(top_picks)
                        write_verified = cybermed_digest_diag["cybermed_digest_store_file_exists_after_write"] and expected_present and counts_match
                        cybermed_digest_diag["cybermed_digest_store_write_verified"] = bool(write_verified)
                        if write_verified:
                            cybermed_digest_diag["cybermed_digest_store_written"] = True
                        else:
                            cybermed_digest_diag["cybermed_digest_store_skipped_reason"] = "write_verification_failed"
                            print("[cybermed-digest-store] WARN: write verification failed")
                    except Exception as e:
                        cybermed_digest_diag["cybermed_digest_store_write_error_class"] = e.__class__.__name__
                        cybermed_digest_diag["cybermed_digest_store_skipped_reason"] = "write_verification_failed"
                        print(f"[cybermed-digest-store] WARN: write failed/verification error: {e!r}")
        if skip_reason:
            cybermed_digest_diag["cybermed_digest_store_skipped_reason"] = skip_reason
            if skip_reason == "digest_already_exists":
                verified_state = _load_cybermed_daily_digest_state(cybermed_digest_state_path)
                verified_digests = list(verified_state.get("digests") or [])
                cybermed_digest_diag["cybermed_digest_store_digest_count_after_write"] = len(verified_digests)
                cybermed_digest_diag["cybermed_digest_store_expected_digest_present"] = any(
                    str((d or {}).get("digest_id") or "") == digest_id for d in verified_digests
                )
                cybermed_digest_diag["cybermed_digest_store_write_verified"] = False
                cybermed_digest_diag["cybermed_digest_store_written"] = False
        print(
            f"[cybermed-digest-store] written={cybermed_digest_diag['cybermed_digest_store_written']} "
            f"skipped_reason={cybermed_digest_diag['cybermed_digest_store_skipped_reason'] or 'none'} "
            f"pubmed={cybermed_digest_diag['cybermed_digest_store_items_pubmed_total']} "
            f"foamed={cybermed_digest_diag['cybermed_digest_store_items_foamed_total']}"
        )
    if report_key.strip().lower() == "cyberlurch":
        _write_cyberlurch_youtube_diagnostics(report_dir, youtube_diag, report_mode=report_mode, extra_counts=extra_counts)
    if report_key.strip().lower() == "cybermed":
        runtime_total_seconds = max(0.0, time.monotonic() - runtime_start)
        cybermed_diagnostics_payload.update({
            "runtime_total_seconds": round(runtime_total_seconds, 6),
            "runtime_pubmed_collect_seconds": round(runtime_pubmed_collect_seconds, 6),
            "runtime_pubmed_backfill_seconds": round(runtime_pubmed_backfill_seconds, 6),
            "runtime_foamed_collect_seconds": round(runtime_foamed_collect_seconds, 6),
            "runtime_foamed_article_fetch_seconds": round(runtime_foamed_article_fetch_seconds, 6),
            "runtime_foamed_rolling_audit_seconds": round(runtime_foamed_rolling_audit_seconds, 6),
            "runtime_selection_seconds": round(runtime_selection_seconds, 6),
            "runtime_summarization_seconds": round(runtime_summarization_seconds, 6),
            "runtime_report_render_seconds": round(runtime_report_render_seconds, 6),
            "runtime_email_seconds": round(runtime_email_seconds, 6),
            "pubmed_summary_consistency_enabled": True,
            "pubmed_summary_consistency_checked_total": consistency_checked_total,
            "pubmed_summary_consistency_conflict_total": consistency_conflict_total,
            "pubmed_summary_consistency_conflict_type_counts": dict(consistency_conflict_type_counts),
            "pubmed_summary_consistency_resolved_total": consistency_resolved_total,
            "pubmed_shared_synopsis_generated_total": len(pubmed_shared_synopsis),
            "pubmed_shared_synopsis_failed_total": 0,
            "pubmed_summary_consistency_preview": consistency_preview[:10],
        })
        cybermed_diagnostics_payload.update(cybermed_digest_diag)
        if cybermed_weekly_digest_only:
            rendered_pubmed_total = len([it for it in report_items if (it.get("source") or "").strip().lower() == "pubmed"])
            rendered_foamed_total = len([it for it in report_items if (it.get("source") or "").strip().lower() == "foamed"])
            rendered_deep_dives_total = len(detail_items)
            rendered_top_picks_total = int(cybermed_weekly_diag.get("cybermed_weekly_top_picks_selected_total", 0) or 0)
            cybermed_weekly_diag.update({
                "cybermed_weekly_rendered_pubmed_items_total": rendered_pubmed_total,
                "cybermed_weekly_rendered_foamed_items_total": rendered_foamed_total,
                "cybermed_weekly_rendered_deep_dives_total": rendered_deep_dives_total,
                "cybermed_weekly_rendered_top_picks_total": rendered_top_picks_total,
                "cybermed_weekly_report_matches_digest_inputs": rendered_pubmed_total == int(cybermed_weekly_diag.get("cybermed_weekly_pubmed_items_selected_total", 0) or 0) and rendered_foamed_total == int(cybermed_weekly_diag.get("cybermed_weekly_foamed_items_selected_total", 0) or 0),
            })
        cybermed_diagnostics_payload.update(cybermed_weekly_diag)
        _write_cybermed_diagnostics(report_dir, report_mode, cybermed_diagnostics_payload)

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
        read_only=(read_only_mode or qa_replay_enabled or cybermed_weekly_digest_only),
    )

    try:
        email_start = time.monotonic()
        send_markdown(report_subject, md)
        runtime_email_seconds += max(0.0, time.monotonic() - email_start)
    except Exception as e:
        print(f"[email] WARN: failed to send email (report was generated and state saved): {e!r}")


if __name__ == "__main__":
    main()
