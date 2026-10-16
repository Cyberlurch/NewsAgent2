"""Pure, conservative edition coverage, independent of delivery and content quality.

No inferred historical policy, file access, model calls or store migrations here.
"""
from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

POLICY = "stockholm-calendar-v2"
LEGACY_WEEKDAYS = "weekdays-v1"
STATUSES = {"complete", "incomplete", "unknown"}


def days(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first or (last-first).days > 3660:
        raise ValueError("history_range_invalid")
    return [(first+timedelta(days=i)).isoformat() for i in range((last-first).days+1)]


def expected_dates(start, end, policy, report="cybermed"):
    if policy not in {POLICY, LEGACY_WEEKDAYS}:
        return None
    from .swedish_calendar import is_swedish_no_send_day
    return [d for d in days(start, end) if not (
        is_swedish_no_send_day(date.fromisoformat(d))
        if policy == POLICY and report == "cybermed" else date.fromisoformat(d).weekday() >= 5)]


def result(reasons=(), *, expected=(), observed=(), children=()):
    """Known defects dominate unknown evidence. Reasons use fixed category codes."""
    reasons = list(reasons)
    for child in children:
        if child["status"] != "complete":
            reasons.append((child["status"], "upstream_"+child["status"]))
            # Preserve the cause: a missing edition can support a labelled
            # partial review; an orphan/foreign reference cannot. An opaque
            # upstream_incomplete must never hide a structural defect.
            reasons.extend((child["status"], reason) for reason in child["reasons"])
    status = "incomplete" if any(s == "incomplete" for s, _ in reasons) else (
        "unknown" if reasons else "complete")
    return {"schema_version": 1, "status": status,
            "reasons": sorted({r for _, r in reasons}),
            "expected": sorted(set(expected)), "observed": sorted(set(observed))}


def valid_coverage(value):
    return (isinstance(value, dict) and set(value) == {"schema_version", "status", "reasons", "expected", "observed"}
            and value["schema_version"] == 1 and value["status"] in STATUSES
            and all(isinstance(value[k], list) and all(isinstance(v, str) and len(v) <= 200
                    and all(c.isalnum() or c in "_-.:" for c in v) for v in value[k])
                    for k in ("reasons", "expected", "observed")))


def coverage_fields_valid(row):
    return ("history_coverage" not in row or valid_coverage(row["history_coverage"]))


def grouped(rows, key):
    groups = defaultdict(list)
    for row in rows:
        groups[row.get(key, "")].append(row)
    return groups


def _refs(row, key, reasons):
    refs = row.get(key)
    if refs is None:
        reasons.append(("unknown", "edition_references_unknown"))
        return []
    if not isinstance(refs, list) or any(not isinstance(r, str) or not r for r in refs):
        reasons.append(("incomplete", "edition_references_invalid"))
        return []
    if len(refs) != len(set(refs)):
        reasons.append(("incomplete", "duplicate_reference"))
    return list(refs)


def weekly_coverage(row, daily_rows, *, required_policy=None, bounds=None):
    reasons, children = [], []
    policy = row.get("calendar_policy") or required_policy
    start, logical_end = row["period_start"], row["period_end"]
    end = row.get("coverage_end") or logical_end
    if not start <= end <= logical_end:
        return result([("incomplete", "coverage_outside_period")])
    if bounds:
        start, end = max(start, bounds[0]), min(end, bounds[1])
    if start > end:
        return result([("incomplete", "reference_outside_period")])
    expected = expected_dates(start, end, policy)
    if expected is None:
        reasons.append(("unknown", "historical_policy_unknown"))
    ids = grouped(daily_rows, "digest_id")
    dates = grouped(daily_rows, "run_date")
    refs = _refs(row, "source_daily_digest_ids", reasons)
    observed = []
    for ref in refs:
        matches = ids.get(ref, [])
        if not matches:
            reasons.append(("incomplete", "orphan_daily_reference"))
            continue
        if len(matches) != 1:
            reasons.append(("incomplete", "duplicate_daily_id"))
            continue
        daily = matches[0]
        d = daily.get("run_date", "")
        if daily.get("report_key", "cybermed") != "cybermed" or daily.get("cadence", "daily") != "daily":
            reasons.append(("incomplete", "foreign_daily_reference"))
        elif not row["period_start"] <= d <= (row.get("coverage_end") or logical_end):
            reasons.append(("incomplete", "daily_outside_week"))
        elif start <= d <= end:
            observed.append(d)
            if len(dates[d]) > 1:
                reasons.append(("incomplete", "duplicate_daily_date"))
    claimed = row.get("source_run_dates")
    if claimed is not None and (not isinstance(claimed, list) or sorted(claimed) != sorted({daily.get("run_date") for ref in refs for daily in ids.get(ref, [])})):
        reasons.append(("incomplete", "daily_dates_reference_mismatch"))
    if expected is not None and not set(expected) <= set(observed):
        reasons.append(("incomplete", "required_daily_missing_or_unreferenced"))
    # A standalone v2 week has one canonical closing date, even when shifted.
    if not bounds and policy == POLICY:
        from .edition_plan import weekly_dates
        if (row.get("coverage_end") or logical_end) != weekly_dates(date.fromisoformat(row["period_start"]))[2].isoformat():
            reasons.append(("incomplete", "week_not_closed"))
    return result(reasons, expected=expected or [], observed=observed, children=children)


def month_bounds(month):
    first = date.fromisoformat(month+"-01")
    return first.isoformat(), first.replace(day=calendar.monthrange(first.year, first.month)[1]).isoformat()


def monthly_coverage(row, weekly_rows, daily_rows, *, required_policy=None):
    month = row["month"]
    start, end = month_bounds(month)
    policy = row.get("calendar_policy") or required_policy
    expected = expected_dates(start, end, policy)
    reasons, children, observed = [], [], []
    if expected is None:
        reasons.append(("unknown", "historical_policy_unknown"))
    refs = _refs(row, "source_weekly_digest_ids", reasons)
    ids = grouped(weekly_rows, "digest_id")
    periods = grouped(weekly_rows, "period_start")
    for ref in refs:
        matches = ids.get(ref, [])
        if len(matches) != 1:
            reasons.append(("incomplete", "duplicate_weekly_id" if matches else "orphan_weekly_reference"))
            continue
        week = matches[0]
        if week.get("report_key", "cybermed") != "cybermed" or week.get("cadence", "weekly") != "weekly":
            reasons.append(("incomplete", "foreign_weekly_reference"))
            continue
        if len(periods[week["period_start"]]) > 1:
            reasons.append(("incomplete", "duplicate_weekly_period"))
        if week["period_start"] > end or week["period_end"] < start:
            reasons.append(("incomplete", "weekly_outside_month"))
            continue
        child = weekly_coverage(week, daily_rows, required_policy=policy, bounds=(start, end))
        children.append(child)
        if set(observed) & set(child["observed"]):
            reasons.append(("incomplete", "overlapping_weekly_coverage"))
        observed.extend(child["observed"])
    if expected is not None and not set(expected) <= set(observed):
        reasons.append(("incomplete", "required_daily_missing_or_unreferenced"))
    return result(reasons, expected=expected or [], observed=observed, children=children)


def cyberlurch_monthly_coverage(row):
    """Check stored local references without treating them as an issue ledger."""
    reasons = [('unknown', 'edition_history_unknown')]
    registry = row.get('source_registry')
    if registry is None:
        return result(reasons)
    if not isinstance(registry, list) or any(not isinstance(r, dict) for r in registry):
        return result([('incomplete', 'source_registry_invalid')])
    ids = [r.get('ref_id') for r in registry]
    if any(not isinstance(ref, str) or not ref for ref in ids) or len(set(ids)) != len(ids):
        reasons.append(('incomplete', 'source_registry_duplicate_or_invalid_id'))
    known = {ref for ref in ids if isinstance(ref, str)}
    refs = _refs(row, 'source_refs_used', reasons)
    for field in ('themes', 'notable_developments'):
        sections = row.get(field, [])
        if not isinstance(sections, list) or any(not isinstance(r, dict) for r in sections):
            reasons.append(('incomplete', 'source_sections_invalid'))
            continue
        for section in sections:
            refs += _refs(section, 'source_refs', reasons)
    if any(ref not in known for ref in refs):
        reasons.append(('incomplete', 'orphan_source_reference'))
    for source in registry:
        if source.get('report_key', 'cyberlurch') != 'cyberlurch':
            reasons.append(('incomplete', 'foreign_source_reference'))
        raw = source.get('source_date', '')
        try:
            source_day = date.fromisoformat(raw[:10]).isoformat()
        except (ValueError, TypeError):
            reasons.append(('unknown', 'source_date_unknown'))
            continue
        if source_day[:7] != row['month']:
            reasons.append(('incomplete', 'source_outside_month'))
    return result(reasons)


def month_horizon(report, year, as_of):
    """Today's policy for due-horizon scenarios, not a historical send promise."""
    from .edition_plan import monthly_due
    instant = datetime.fromisoformat(as_of.replace("Z", "+00:00")) if isinstance(as_of, str) else as_of
    if instant.tzinfo is None:
        raise ValueError("history_as_of_requires_timezone")
    local = instant.astimezone(ZoneInfo("Europe/Stockholm"))
    due, closing, open_, future = [], [], [], []
    for i in range(1, 13):
        month = f"{year}-{i:02d}"
        first, last = month_bounds(month)
        if first > local.date().isoformat():
            future.append(month)
        elif last >= local.date().isoformat():
            open_.append(month)
        else:
            next_first = date.fromisoformat(last)+timedelta(days=1)
            day = monthly_due(next_first.year, next_first.month) if report == "cybermed" else next_first
            # Workflow slots are Stockholm time; an elapsed date alone is insufficient.
            slot = datetime.combine(day, datetime.min.time(), ZoneInfo("Europe/Stockholm")).replace(hour=5, minute=40 if report == "cybermed" else 30)
            (due if local >= slot else closing).append(month)
    return {"due_months": due, "awaiting_close": closing, "open_months": open_, "future_months": future,
            "horizon_policy": POLICY, "historical_schedule_verified": False}


def yearly_coverage(report, year, months, *, weekly_rows=(), daily_rows=(), as_of, required_policy=None):
    horizon = month_horizon(report, year, as_of)
    rows = [r for r in months if str(r.get("month", "")).startswith(str(year)+"-")]
    by_month = grouped(rows, "month")
    reasons, children = [], []
    for month in horizon["due_months"]:
        matches = by_month.get(month, [])
        if not matches:
            reasons.append(("incomplete" if required_policy in {POLICY, LEGACY_WEEKDAYS} else "unknown", "due_month_missing"))
            continue
        if len(matches) != 1:
            reasons.append(("incomplete", "duplicate_month"))
            continue
        if report == "cybermed":
            children.append(monthly_coverage(matches[0], weekly_rows, daily_rows, required_policy=required_policy))
        else:
            # Calendar presence and substantive evidence classification stay separate.
            children.append(cyberlurch_monthly_coverage(matches[0]))
    assessed = result(reasons, expected=horizon["due_months"], observed=by_month, children=children)
    final = not (horizon["open_months"] or horizon["future_months"] or horizon["awaiting_close"])
    return {**assessed, "horizon": horizon, "final_year_ready": final and assessed["status"] == "complete"}


def require_complete(coverage, category="history_coverage"):
    if coverage["status"] != "complete":
        raise RuntimeError(category+":"+coverage["status"])


def preview_note(coverage):
    if coverage.get('final_year_ready') is False and coverage['status'] == 'complete':
        return "> Coverage preview: complete for the assessed months; the final year is not ready.\n\n"
    return ("" if coverage["status"] == "complete" else
            f"> Coverage preview: {coverage['status']}. Historical edition coverage is not verified as complete.\n\n")


def load_inputs(report):
    """Strict existing readers, centrally injected paths; no writes or repair."""
    from .run_context import path_for
    from .rollups import load_rollups_state_readonly
    months = load_rollups_state_readonly(str(path_for("ROLLUPS_STATE_PATH"))).get("reports", {}).get(report, [])
    if report != "cybermed":
        return [], [], months
    from .cybermed_digest_store import load_cybermed_daily_digest_store, load_cybermed_weekly_digest_store
    daily = load_cybermed_daily_digest_store(str(path_for("CYBERMED_DAILY_DIGEST_STATE_PATH")))["digests"]
    weekly = load_cybermed_weekly_digest_store(str(path_for("CYBERMED_WEEKLY_DIGEST_STATE_PATH")))["digests"]
    return daily, weekly, months
