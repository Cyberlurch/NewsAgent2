from __future__ import annotations

import json
import copy
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

from .cybermed_quality import (
    assert_no_generation_error_text,
    sanitize_cybermed_payload,
)


CYBERMED_WEEKLY_SCHEMA_VERSION = 1


class CybermedDigestStoreError(RuntimeError):
    """Raised when an existing aggregate store cannot be trusted."""


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
        valid = []
        for digest in digests:
            if not isinstance(digest, dict) or not str(digest.get("digest_id") or "").strip():
                continue
            safe_digest, _ = sanitize_cybermed_payload(digest)
            valid.append(safe_digest)
        return {"schema_version": int(data.get("schema_version") or 1), "digests": valid}
    except Exception:
        return default


def latest_cybermed_daily_digest_generated_at(store: dict) -> str:
    """Return the newest valid Cybermed Daily generation timestamp.

    The digest store is report-specific and is therefore the safe migration
    fallback for Daily lookback calculation.  The legacy global marker can be
    overwritten by another newsletter that ran in the same workflow.
    """

    latest: tuple[datetime, str] | None = None
    for digest in store.get("digests", []):
        if not isinstance(digest, dict):
            continue
        if str(digest.get("report_key") or "cybermed").strip().lower() != "cybermed":
            continue
        if str(digest.get("cadence") or "daily").strip().lower() != "daily":
            continue
        raw = str(digest.get("generated_at_utc") or "").strip()
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
            normalized = parsed.astimezone(ZoneInfo("UTC"))
        except Exception:
            continue
        if latest is None or normalized > latest[0]:
            latest = (normalized, raw)
    return latest[1] if latest else ""


def cybermed_weekly_reporting_period(today: date, timezone: str = "Europe/Stockholm") -> tuple[date, date]:
    tz = ZoneInfo(timezone)
    now = datetime.combine(today, datetime.min.time(), tz)
    week_start = (now - timedelta(days=now.weekday())).date()
    week_end = now.date()

    override_start = (os.getenv("CYBERMED_WEEKLY_PERIOD_START") or "").strip()
    override_end = (os.getenv("CYBERMED_WEEKLY_PERIOD_END") or "").strip()
    if override_start or override_end:
        try:
            if override_start:
                week_start = datetime.strptime(override_start, "%Y-%m-%d").date()
            if override_end:
                week_end = datetime.strptime(override_end, "%Y-%m-%d").date()
        except Exception:
            pass

    return week_start, week_end


def select_cybermed_daily_digests_for_week(store: dict, today: date, timezone: str = "Europe/Stockholm") -> list[dict]:
    week_start, week_end = cybermed_weekly_reporting_period(today, timezone)

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


def select_cybermed_daily_digests_for_month(
    store: dict,
    month_key: str,
) -> list[dict]:
    selected: list[dict] = []
    for d in store.get("digests", []):
        run_date = str(d.get("run_date") or "").strip()
        if len(run_date) < 7:
            continue
        if run_date[:7] == month_key:
            selected.append(d)
    return sorted(selected, key=lambda x: str(x.get("run_date") or ""))


def load_cybermed_weekly_digest_store(path: str) -> dict:
    """Load the persisted Weekly store without mutating a corrupt file.

    Missing stores are a normal migration state. Existing malformed stores are
    fatal because treating them as empty could silently replace recoverable
    history during the next scheduled run.
    """

    default = {"schema_version": CYBERMED_WEEKLY_SCHEMA_VERSION, "digests": []}
    store_path = Path(path)
    if not store_path.exists():
        return default
    try:
        data = json.loads(store_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise CybermedDigestStoreError(
            f"Cybermed Weekly store is unreadable: {store_path}"
        ) from exc
    if not isinstance(data, dict) or not isinstance(data.get("digests"), list):
        raise CybermedDigestStoreError(
            f"Cybermed Weekly store has an invalid schema: {store_path}"
        )

    valid: list[dict] = []
    for digest in data["digests"]:
        if not isinstance(digest, dict):
            raise CybermedDigestStoreError(
                f"Cybermed Weekly store contains a non-object digest: {store_path}"
            )
        digest_id = str(digest.get("digest_id") or "").strip()
        period_start = str(digest.get("period_start") or "").strip()
        period_end = str(digest.get("period_end") or "").strip()
        if not digest_id or not _valid_date(period_start) or not _valid_date(period_end):
            raise CybermedDigestStoreError(
                f"Cybermed Weekly store contains an invalid digest record: {store_path}"
            )
        safe_digest, _ = sanitize_cybermed_payload(digest)
        valid.append(safe_digest)
    return {
        "schema_version": int(data.get("schema_version") or CYBERMED_WEEKLY_SCHEMA_VERSION),
        "digests": valid,
    }


def save_cybermed_weekly_digest_store(path: str, store: dict) -> None:
    """Atomically save and verify the dedicated Cybermed Weekly store."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": CYBERMED_WEEKLY_SCHEMA_VERSION,
        "digests": list(store.get("digests") or []),
    }
    payload, _ = sanitize_cybermed_payload(payload)
    assert_no_generation_error_text(payload, boundary="weekly_digest_store")
    tmp = target.with_name(f"{target.name}.tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, target)
    verified = load_cybermed_weekly_digest_store(str(target))
    if len(verified["digests"]) != len(payload["digests"]):
        raise CybermedDigestStoreError(
            f"Cybermed Weekly store verification failed: {target}"
        )


def select_cybermed_weekly_digests_for_month(store: dict, month_key: str) -> list[dict]:
    """Select Weekly snapshots whose source dates overlap the requested month."""

    try:
        month_start = datetime.strptime(f"{month_key}-01", "%Y-%m-%d").date()
    except Exception:
        return []
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    month_end = next_month - timedelta(days=1)

    selected: list[dict] = []
    for digest in store.get("digests", []):
        source_dates = [
            value
            for value in (digest.get("source_run_dates") or [])
            if _valid_date(str(value or ""))
        ]
        if source_dates:
            if any(month_start <= datetime.strptime(value, "%Y-%m-%d").date() <= month_end for value in source_dates):
                selected.append(digest)
            continue
        try:
            start = datetime.strptime(str(digest.get("period_start") or ""), "%Y-%m-%d").date()
            end = datetime.strptime(str(digest.get("period_end") or ""), "%Y-%m-%d").date()
        except Exception:
            continue
        if start <= month_end and end >= month_start:
            selected.append(digest)
    return sorted(selected, key=lambda row: str(row.get("period_start") or ""))


def prepare_cybermed_monthly_source_digests(store: dict, month_key: str) -> tuple[list[dict], dict]:
    """Return a calendar-month-safe view of persisted Weekly digests.

    Interior legacy weeks are safe as a unit.  Boundary weeks are copied and
    trimmed using explicit Daily provenance; publication dates are never used.
    """
    try:
        month_start = datetime.strptime(f"{month_key}-01", "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("month_key must use YYYY-MM") from exc
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    month_end = next_month - timedelta(days=1)
    first_monday = month_start - timedelta(days=month_start.weekday())
    if first_monday + timedelta(days=4) < month_start:
        first_monday += timedelta(days=7)
    expected = []
    cursor = first_monday
    while cursor <= month_end:
        expected.append((cursor, cursor + timedelta(days=4)))
        cursor += timedelta(days=7)
    expected_keys = [f"{start}..{end}" for start, end in expected]
    by_period = {
        (str(row.get("period_start") or ""), str(row.get("period_end") or "")): row
        for row in store.get("digests", []) if isinstance(row, dict)
    }
    found: list[str] = []
    missing: list[str] = []
    boundary: list[str] = []
    unsafe: list[dict] = []
    selected: list[dict] = []
    for start, end in expected:
        label = f"{start}..{end}"
        row = by_period.get((str(start), str(end)))
        if row is None:
            missing.append(label)
            continue
        found.append(label)
        crosses = start < month_start or end > month_end
        if not crosses:
            selected.append(copy.deepcopy(row))
            continue
        boundary.append(label)
        cloned = copy.deepcopy(row)
        kept: list[dict] = []
        removed_keys: set[tuple[str, str]] = set()
        for kind in ("pubmed", "foamed"):
            output = []
            for item in ((cloned.get("items") or {}).get(kind) or []):
                dates = [str(value) for value in (item.get("source_daily_run_dates") or []) if _valid_date(str(value))]
                if not dates:
                    unsafe.append({"weekly_period": label, "item": dedupe_key(item)})
                    removed_keys.update(_match_keys(item))
                elif any(month_start <= datetime.strptime(value, "%Y-%m-%d").date() <= month_end for value in dates):
                    output.append(item)
                    kept.append(item)
                else:
                    removed_keys.update(_match_keys(item))
            cloned.setdefault("items", {})[kind] = output
        kept_keys = {key for item in kept for key in _match_keys(item)}
        cloned["deep_dives"] = [d for d in (cloned.get("deep_dives") or []) if any(key in kept_keys for key in _match_keys(d))]
        kept_ids = {str(i.get(k) or "").strip().lower() for i in kept for k in ("item_id", "id", "pmid", "url") if str(i.get(k) or "").strip()}
        cloned["top_picks"] = [p for p in (cloned.get("top_picks") or []) if (str(p).strip().lower() in kept_ids if not isinstance(p, dict) else any(key in kept_keys for key in _match_keys(p)))]
        selected.append(cloned)
    diagnostics = {
        "target_month": month_key,
        "expected_weekly_periods": expected_keys,
        "found_weekly_periods": found,
        "missing_weekly_periods": missing,
        "boundary_weekly_periods": boundary,
        "unsafe_boundary_items": unsafe,
        "coverage_complete": not missing and not unsafe,
    }
    return selected, diagnostics


def make_cybermed_weekly_digest(
    *,
    period_start: str,
    period_end: str,
    generated_at_utc: str,
    source_digests: List[Dict[str, Any]],
    pubmed_items: List[Dict[str, Any]],
    foamed_items: List[Dict[str, Any]],
    deep_dives: List[Dict[str, Any]],
    top_picks: List[Any],
    diagnostic_summary: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    if not _valid_date(period_start) or not _valid_date(period_end):
        raise ValueError("Cybermed Weekly period must use YYYY-MM-DD")
    if period_end < period_start:
        raise ValueError("Cybermed Weekly period_end precedes period_start")
    return {
        "digest_id": f"cybermed_weekly_{period_start}_{period_end}",
        "report_key": "cybermed",
        "cadence": "weekly",
        "period_start": period_start,
        "period_end": period_end,
        "generated_at_utc": generated_at_utc,
        "source_daily_digest_ids": [
            str(row.get("digest_id") or "").strip()
            for row in source_digests
            if str(row.get("digest_id") or "").strip()
        ],
        "source_run_dates": sorted({
            str(row.get("run_date") or "").strip()
            for row in source_digests
            if _valid_date(str(row.get("run_date") or "").strip())
        }),
        "items": {
            "pubmed": [_json_safe_record(row) for row in pubmed_items if isinstance(row, dict)],
            "foamed": [_json_safe_record(row) for row in foamed_items if isinstance(row, dict)],
        },
        "deep_dives": [_json_safe_record(row) for row in deep_dives if isinstance(row, dict)],
        "top_picks": [_json_safe(value) for value in top_picks],
        "diagnostic_summary": _json_safe_record(diagnostic_summary or {}),
    }


def upsert_cybermed_weekly_digest(
    store: dict,
    digest: Dict[str, Any],
    *,
    overwrite: bool = False,
) -> tuple[dict, str]:
    digest_id = str(digest.get("digest_id") or "").strip()
    if not digest_id:
        raise ValueError("Cybermed Weekly digest_id is required")
    rows = list(store.get("digests") or [])
    existing_index = next(
        (index for index, row in enumerate(rows) if str((row or {}).get("digest_id") or "") == digest_id),
        -1,
    )
    if existing_index >= 0 and not overwrite:
        return {
            "schema_version": CYBERMED_WEEKLY_SCHEMA_VERSION,
            "digests": rows,
        }, "digest_already_exists"
    if existing_index >= 0:
        rows[existing_index] = digest
        status = "replaced"
    else:
        rows.append(digest)
        status = "inserted"
    rows.sort(key=lambda row: (str(row.get("period_start") or ""), str(row.get("digest_id") or "")))
    return {
        "schema_version": CYBERMED_WEEKLY_SCHEMA_VERSION,
        "digests": rows,
    }, status


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


def _weekly_pubmed_merit(it: Dict[str, Any]) -> tuple:
    """PubMed merit independent of the lower-cadence top-pick decision."""

    return _score_pubmed({**it, "top_pick": False})


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
        provenance = sorted({str(v) for row in (prev, it) for v in (row.get("source_daily_run_dates") or []) if _valid_date(str(v))})
        if _winner_score(it) > _winner_score(prev):
            winners[key] = it
            reasons["replaced_with_stronger_item"] = reasons.get("replaced_with_stronger_item", 0) + 1
        else:
            reasons["kept_existing_stronger_item"] = reasons.get("kept_existing_stronger_item", 0) + 1
        suppressed += 1
        if provenance:
            winners[key]["source_daily_run_dates"] = provenance
    return list(winners.values()), suppressed, reasons


def aggregate_cybermed_digest_inputs(
    digests: List[Dict[str, Any]],
    *,
    pubmed_cap: int,
    foamed_cap: int,
    deep_dive_cap: int,
    top_pick_cap: int = 5,
) -> Dict[str, Any]:
    """Apply the production Weekly selection policy to persisted digest inputs.

    This function is deliberately pure: aggregate runs must not collect content
    or manufacture summaries.  In particular, ``top_pick`` on an input is a
    Daily decision and is not copied to the Weekly result.
    """

    pubmed: list[dict] = []
    foamed: list[dict] = []
    deep_dives: list[dict] = []
    for digest in digests:
        run_date = str(digest.get("run_date") or "")
        items = digest.get("items") or {}
        for target, rows in ((pubmed, items.get("pubmed") or []), (foamed, items.get("foamed") or [])):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                annotated = dict(row)
                if _valid_date(run_date):
                    annotated["source_daily_run_dates"] = sorted(set(annotated.get("source_daily_run_dates") or []) | {run_date})
                target.append(annotated)
        deep_dives.extend(row for row in (digest.get("deep_dives") or []) if isinstance(row, dict))

    deduped, suppressed, reasons = dedupe_weekly_digest_items(pubmed + foamed)
    ranked_pubmed = sorted(
        [row for row in deduped if _source_type(row) == "pubmed"],
        key=lambda row: (_score_pubmed(row), dedupe_key(row)),
        reverse=True,
    )[: max(0, int(pubmed_cap))]
    ranked_foamed = sorted(
        [row for row in deduped if _source_type(row) == "foamed"],
        key=lambda row: (_score_foamed(row), dedupe_key(row)),
        reverse=True,
    )[: max(0, int(foamed_cap))]
    selected = ranked_pubmed + ranked_foamed
    selected_keys = {key for row in selected for key in _match_keys(row)}

    deep_lookup: dict[tuple[str, str], dict] = {}
    for record in deep_dives:
        for key in _match_keys(record):
            deep_lookup.setdefault(key, record)
    ranked_deep_candidates = [row for _, row in sorted(
        [
            (index, row)
            for index, row in enumerate(deduped)
            if _source_type(row) == "pubmed"
            if bool(row.get("deep_dive_candidate"))
            or any(key in deep_lookup for key in _match_keys(row))
        ],
        # This is the production display ordering.  Python's stable sort and
        # reverse=True intentionally make later Daily records win date ties.
        key=lambda pair: (
            str(pair[1].get("published_at") or ""),
            _weekly_pubmed_merit(pair[1]),
            pair[0],
        ),
        reverse=True,
    )[: max(0, int(deep_dive_cap))]]
    selected_deep: list[dict] = []
    for item in ranked_deep_candidates:
        record = next(
            (deep_lookup[key] for key in _match_keys(item) if key in deep_lookup),
            None,
        )
        if record is not None:
            selected_deep.append(record)

    selected_deep_keys = {
        key for row in ranked_deep_candidates for key in _match_keys(row)
    }
    top_picks = [
        row
        for row in ranked_pubmed
        if any(key in selected_deep_keys for key in _match_keys(row))
        or not any(key in deep_lookup for key in _match_keys(row))
    ][: max(0, int(top_pick_cap))]
    top_pick_keys = {key for row in top_picks for key in _match_keys(row)}
    ranked_pubmed = [
        {**row, "top_pick": any(key in top_pick_keys for key in _match_keys(row))}
        for row in ranked_pubmed
    ]
    ranked_foamed = [{**row, "top_pick": False} for row in ranked_foamed]
    return {
        "pubmed": ranked_pubmed,
        "foamed": ranked_foamed,
        "deep_dives": selected_deep,
        "top_picks": [
            str(row.get("item_id") or row.get("id") or row.get("pmid") or row.get("url") or "").strip()
            for row in top_picks
        ],
        "duplicates_suppressed_total": suppressed,
        "duplicates_suppressed_reason_counts": reasons,
        "selected_keys": selected_keys,
        "deep_dive_items": ranked_deep_candidates,
    }


def build_weekly_backfill_candidates(
    daily_digests: List[Dict[str, Any]],
    *,
    generated_at_utc: str,
    pubmed_cap: int = 20,
    foamed_cap: int = 15,
    deep_dive_cap: int = 5,
) -> List[Dict[str, Any]]:
    """Build Monday-Friday Weekly snapshots from persisted Daily digests."""

    grouped: dict[date, list[dict]] = {}
    for digest in daily_digests:
        run_date = str((digest or {}).get("run_date") or "").strip()
        if not _valid_date(run_date):
            continue
        day = datetime.strptime(run_date, "%Y-%m-%d").date()
        if day.weekday() > 4:
            continue
        week_start = day - timedelta(days=day.weekday())
        grouped.setdefault(week_start, []).append(digest)

    candidates: list[dict] = []
    for week_start, rows in sorted(grouped.items()):
        rows.sort(key=lambda row: str(row.get("run_date") or ""))
        period_end = week_start + timedelta(days=4)
        source_dates = {str(row.get("run_date") or "").strip() for row in rows}
        if period_end.isoformat() not in source_dates:
            # Never persist a partial current week: it would share the same
            # deterministic ID as Friday's completed Weekly snapshot.
            continue
        aggregate = aggregate_cybermed_digest_inputs(
            rows,
            pubmed_cap=pubmed_cap,
            foamed_cap=foamed_cap,
            deep_dive_cap=deep_dive_cap,
        )
        unique_urls = {
            str(row.get("url") or "").strip()
            for row in aggregate["pubmed"] + aggregate["foamed"]
            if str(row.get("url") or "").strip()
        }
        candidates.append(
            make_cybermed_weekly_digest(
                period_start=week_start.isoformat(),
                period_end=period_end.isoformat(),
                generated_at_utc=generated_at_utc,
                source_digests=rows,
                pubmed_items=aggregate["pubmed"],
                foamed_items=aggregate["foamed"],
                deep_dives=aggregate["deep_dives"],
                top_picks=aggregate["top_picks"],
                diagnostic_summary={
                    "backfilled_from_daily": True,
                    "source_daily_digests_total": len(rows),
                    "pubmed_selected_total": len(aggregate["pubmed"]),
                    "foamed_selected_total": len(aggregate["foamed"]),
                    "unique_article_urls_total": len(unique_urls),
                    "deep_dives_selected_total": len(aggregate["deep_dives"]),
                    "top_picks_selected_total": len(aggregate["top_picks"]),
                    "duplicates_suppressed_total": aggregate["duplicates_suppressed_total"],
                    "duplicates_suppressed_reason_counts": aggregate["duplicates_suppressed_reason_counts"],
                },
            )
        )
    return candidates


def _source_type(item: Dict[str, Any]) -> str:
    return str(item.get("source_type") or item.get("source") or "").strip().lower()


def _match_keys(item: Dict[str, Any]) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    for field in ("item_id", "id", "pmid", "doi", "url"):
        value = str(item.get(field) or "").strip().lower()
        if value:
            keys.append((field, value))
    title = normalized_title(str(item.get("title") or ""))
    if title:
        keys.append(("title", title))
    return keys


def _valid_date(value: str) -> bool:
    try:
        return datetime.strptime(str(value or ""), "%Y-%m-%d").strftime("%Y-%m-%d") == value
    except Exception:
        return False


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZoneInfo("UTC"))
        return value.astimezone(ZoneInfo("UTC")).replace(microsecond=0).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return _json_safe_record(value)
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _json_safe_record(record: Dict[str, Any]) -> Dict[str, Any]:
    json_safe = {
        str(key): _json_safe(value)
        for key, value in record.items()
        if not str(key).startswith("_")
    }
    cleaned, _ = sanitize_cybermed_payload(json_safe)
    return cleaned
