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
