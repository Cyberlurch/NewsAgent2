from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo


def load_cybermed_daily_digest_store(path: str) -> dict:
    default = {"schema_version": 1, "digests": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default
        digests = data.get("digests")
        if not isinstance(digests, list):
            return default
        valid = [d for d in digests if isinstance(d, dict) and str(d.get("digest_id") or "").strip()]
        return {"schema_version": int(data.get("schema_version") or 1), "digests": valid}
    except Exception:
        return default


def select_cybermed_daily_digests_for_week(store: dict, today: date, timezone: str = "Europe/Stockholm") -> list[dict]:
    tz = ZoneInfo(timezone)
    now = datetime.combine(today, datetime.min.time(), tz)
    # last completed Mon-Sun week
    weekday = now.weekday()
    this_week_start = (now - timedelta(days=weekday)).date()
    week_end = this_week_start - timedelta(days=1)
    week_start = week_end - timedelta(days=6)

    event = (os.getenv("GITHUB_EVENT_NAME") or "").strip().lower()
    if event in {"workflow_dispatch", "manual"}:
        week_start = today - timedelta(days=7)
        week_end = today

    selected = []
    for d in store.get("digests", []):
        run_date = str(d.get("run_date") or "").strip()
        try:
            dd = datetime.strptime(run_date, "%Y-%m-%d").date()
        except Exception:
            continue
        if week_start <= dd <= week_end:
            selected.append(d)
    return sorted(selected, key=lambda x: str(x.get("run_date") or ""))


def summarize_cybermed_weekly_digest_inputs(digests: list[dict]) -> dict:
    pubmed = []
    foamed = []
    deep_dives = []
    top_picks = []
    for d in digests:
        items = d.get("items") or {}
        pubmed.extend(items.get("pubmed") or [])
        foamed.extend(items.get("foamed") or [])
        deep_dives.extend(d.get("deep_dives") or [])
        top_picks.extend(d.get("top_picks") or [])
    return {
        "daily_digests_found_total": len(digests),
        "daily_digests_with_items_total": len([d for d in digests if (d.get("items") or {}).get("pubmed") or (d.get("items") or {}).get("foamed")]),
        "pubmed_items_loaded_total": len(pubmed),
        "foamed_items_loaded_total": len(foamed),
        "deep_dives_loaded_total": len(deep_dives),
        "top_picks_loaded_total": len(top_picks),
    }


def normalized_title(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", "", (text or "").lower())).strip()


def dedupe_key(item: Dict[str, Any]) -> Tuple[str, str]:
    for key in ("pmid", "doi", "url", "item_id", "id"):
        v = str(item.get(key) or "").strip().lower()
        if v:
            return key, v
    return "title", normalized_title(str(item.get("title") or ""))


_EVIDENCE_ORDER = {"a": 5, "b": 4, "c": 3, "d": 2, "e": 1}
_CONF_ORDER = {"high": 3, "moderate": 2, "low": 1}
_FOAMED_QUALITY_ORDER = {"core": 3, "important": 2, "optional": 1}


def _as_int(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def _score_pubmed(it: Dict[str, Any]) -> tuple:
    return (
        1 if bool(it.get("top_pick")) else 0,
        1 if bool(it.get("deep_dive_candidate")) else 0,
        _EVIDENCE_ORDER.get(str(it.get("evidence_strength_label") or "").strip().lower(), 0),
        _as_int(it.get("practice_change_potential_1_5")),
        _as_int(it.get("clinical_relevance_1_5")),
        _CONF_ORDER.get(str(it.get("text_confidence_label") or "").strip().lower(), 0),
        str(it.get("published_at") or ""),
    )


def _score_foamed(it: Dict[str, Any]) -> tuple:
    return (
        1 if bool(it.get("top_pick")) else 0,
        _FOAMED_QUALITY_ORDER.get(str(it.get("source_quality_label") or "").strip().lower(), 0),
        _as_int(it.get("clinical_usefulness_1_5")),
        _as_int(it.get("practice_relevance_1_5")),
        _CONF_ORDER.get(str(it.get("text_confidence_label") or "").strip().lower(), 0),
        str(it.get("published_at") or ""),
    )


def _winner_score(it: Dict[str, Any]) -> tuple:
    source = str(it.get("source_type") or it.get("source") or "").strip().lower()
    if source == "pubmed":
        return _score_pubmed(it) + (1 if str(it.get("bottom_line") or "").strip() else 0,)
    if source == "foamed":
        return _score_foamed(it) + (1 if str(it.get("bottom_line") or "").strip() else 0,)
    return (0, 0, 0, 0, 0, 0, 0)


def dedupe_weekly_digest_items(items: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int, Dict[str, int]]:
    winners: Dict[Tuple[str, str], Dict[str, Any]] = {}
    suppressed = 0
    reasons: Dict[str, int] = {}
    for it in items:
        key = dedupe_key(it)
        prev = winners.get(key)
        if prev is None:
            winners[key] = it
            continue
        if _winner_score(it) > _winner_score(prev):
            winners[key] = it
            reasons["replaced_with_stronger_item"] = reasons.get("replaced_with_stronger_item", 0) + 1
        else:
            reasons["kept_existing_stronger_item"] = reasons.get("kept_existing_stronger_item", 0) + 1
        suppressed += 1
    return list(winners.values()), suppressed, reasons
