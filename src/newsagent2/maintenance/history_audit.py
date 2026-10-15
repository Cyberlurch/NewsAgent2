"""Offline history audit used by the existing backfill CLI; never an Apply path."""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo
import hashlib
import json
from pathlib import Path
import re

from .. import history_coverage as hc
from ..run_context import path_for, PATHS
from ..store_io import read_store, digest_store, StoreError

_SAFE = re.compile(r"[A-Za-z0-9_.:-]{1,160}\Z")


def safe_id(value):
    return value if isinstance(value, str) and _SAFE.fullmatch(value) else "invalid_identifier"


def _valid_date(value):
    try:
        return isinstance(value, str) and date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _valid_row(row, kind):
    if not isinstance(row, dict) or not hc.coverage_fields_valid(row):
        return False
    if kind == 'video':
        from ..youtube_source import digest_valid
        if not digest_valid({'digests': [row]}):
            return False
    if any(k in row and not isinstance(row[k], str) for k in ('digest_id', 'calendar_policy', 'edition_id', 'content_version')):
        return False
    date_keys = {"daily": ("run_date",), "weekly": ("period_start", "period_end"), "monthly": ()}
    if any(not _valid_date(row.get(k)) for k in date_keys.get(kind, ())):
        return False
    if kind == "monthly" and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(row.get("month", ""))):
        return False
    if kind == "weekly" and (row["period_start"] > row["period_end"] or
            ("coverage_end" in row and (not _valid_date(row["coverage_end"]) or not row["period_start"] <= row["coverage_end"] <= row["period_end"]))):
        return False
    for key in ("source_daily_digest_ids", "source_weekly_digest_ids", "source_run_dates"):
        if key in row and (not isinstance(row[key], list) or any(not isinstance(v, str) for v in row[key])):
            return False
    if "full_text_count" in row and (type(row["full_text_count"]) is not int or row["full_text_count"] < 0):
        return False
    for field in ("top_items", "representative_items", "source_registry", "deep_dives", "cybermed_items"):
        if field in row and (not isinstance(row[field], list) or any(not isinstance(v, dict) for v in row[field])):
            return False
    if "items" in row and (not isinstance(row["items"], dict) or any(not isinstance(v, list) or any(not isinstance(i, dict) for i in v) for v in row["items"].values())):
        return False
    return True


def audit(report, start, end, as_of, *, paths=None, source_commit="unknown"):
    hc.days(start, end)
    instant = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("history_as_of_requires_timezone")
    if report not in {"cybermed", "cyberlurch"}:
        raise ValueError("history_single_report_required")
    if source_commit != "unknown" and not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("history_source_commit_invalid")
    paths = paths or {name: path_for(name) for name in PATHS}
    inputs, findings, records, locations = {}, [], [], {}

    def finding(category, location, **fields):
        findings.append({"category": category, "location": location, **fields})

    def load(name, kind):
        p = Path(paths[name])
        try:
            raw = p.read_bytes()
        except FileNotFoundError:
            inputs[name] = {"status": "missing_new_store", "sha256": None}
            return [] if kind != "journal" else {}
        except OSError:
            inputs[name] = {"status": "unreadable_existing_store", "sha256": None}
            finding("unreadable_existing_store", name)
            return [] if kind != "journal" else {}
        inputs[name] = {"status": "existing", "sha256": hashlib.sha256(raw).hexdigest()}
        try:
            if kind == "monthly":
                # Other newsletter payloads are deliberately not validated/read as rows.
                validator = lambda x: (isinstance(x, dict) and isinstance(x.get("reports"), dict)
                    and isinstance(x["reports"].get(report, []), list))
            elif kind == "journal":
                validator = lambda x: isinstance(x, dict) and isinstance(x.get("editions"), dict)
            else:
                validator = digest_store
            data = read_store(p, {}, kind=kind, validate=validator)
            if p.read_bytes() != raw:
                raise StoreError(kind, "input_changed_during_audit")
            version = data.get('schema_version', data.get('version'))
            inputs[name]['schema'] = version if type(version) is int else 'unspecified_legacy'
            if version is not None and version != 1:
                raise StoreError(kind, 'unsupported_existing_schema')
            rows = data["reports"].get(report, []) if kind == "monthly" else data.get("digests", [])
            if kind == "journal":
                from ..delivery import journal_valid
                rows = {k: v for k, v in data["editions"].items() if isinstance(v, dict) and v.get("report_key") == report}
                # Validate only selected receipts; never export recovery/audience details.
                if not journal_valid({"editions": rows}):
                    raise StoreError(kind, "invalid_existing_store")
                return rows
            valid = []
            for i, row in enumerate(rows):
                if not _valid_row(row, kind):
                    finding("invalid_record", f"{name}#/"+("reports/"+report if kind == "monthly" else "digests")+f"/{i}")
                else:
                    locations[id(row)] = f"{name}#/"+("reports/"+report if kind == "monthly" else "digests")+f"/{i}"
                    valid.append(row)
            return valid
        except StoreError as exc:
            inputs[name]["status"] = exc.category
            finding(exc.category, name)
            return [] if kind != "journal" else {}

    daily = load("CYBERMED_DAILY_DIGEST_STATE_PATH", "daily") if report == "cybermed" else []
    weekly = load("CYBERMED_WEEKLY_DIGEST_STATE_PATH", "weekly") if report == "cybermed" else []
    video = load("CYBERLURCH_DIGEST_STATE_PATH", "video") if report == "cyberlurch" else []
    months = load("ROLLUPS_STATE_PATH", "monthly")
    journal = load("DELIVERY_JOURNAL_PATH", "journal")
    for kind, rows, key in (("daily", daily, "run_date"), ("weekly", weekly, "period_start"), ("monthly", months, "month")):
        for value, group in hc.grouped(rows, key).items():
            if len(group) > 1:
                finding("duplicate_"+kind+"_period", kind+"/"+safe_id(value), count=len(group))
        for value, group in hc.grouped(rows, "digest_id").items():
            if value and len(group) > 1:
                finding("duplicate_"+kind+"_id", kind+"/"+safe_id(value), count=len(group))

    def delivery(kind, row):
        # Dates alone never bind a receipt to this exact content version.
        edition_id, content_version = row.get("edition_id"), row.get("content_version")
        receipt = journal.get(edition_id, {}) if edition_id and content_version else {}
        if receipt.get("content_version") != content_version or not receipt:
            return {"status": "unknown", "state_persisted": None}
        return {"status": receipt["status"], "state_persisted": receipt["state_persisted"]}

    def record(kind, row, coverage):
        key = row.get("digest_id") or row.get("month") or row.get("run_date")
        entry = {"kind": kind, "id": safe_id(key), "location": locations[id(row)], "coverage": coverage,
                 "coverage_scope": "stored_daily_record" if kind == "daily" else "upstream_edition_calendar",
                 "policy_basis": row.get("calendar_policy") if row.get("calendar_policy") in {hc.POLICY, hc.LEGACY_WEEKDAYS} else "unknown",
                 "delivery": delivery(kind, row), "generation": "stored_record",
                 "ingestion": "unknown", "content_quality": "not_medically_validated" if report == "cybermed" else "separate_source_basis",
                 "source_references": [safe_id(v) for v in row.get("source_daily_digest_ids", row.get("source_weekly_digest_ids", []))]}
        for field in ("run_date", "period_start", "period_end", "coverage_end", "month"):
            if field in row:
                entry[field] = safe_id(row[field])
        timestamp = row.get("generated_at_utc", row.get("generated_at", ""))
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            entry["generated_at"] = parsed.isoformat() if parsed.tzinfo else "unknown_timezone"
        except (ValueError, AttributeError, TypeError):
            entry["generated_at"] = "unknown"
        if kind == "daily":
            entry["content_state"] = "nonempty" if any(row.get("items", {}).get(k) for k in ("pubmed", "foamed")) or row.get("deep_dives") else "valid_empty"
        if kind == "monthly" and report == "cyberlurch":
            from ..cyberlurch_yearly import classify_monthly_rollup
            entry["evidence_class"] = classify_monthly_rollup(row)
        records.append(entry)

    for row in daily:
        if start <= row["run_date"] <= end:
            record("daily", row, hc.result())
    for row in weekly:
        if row["period_start"] <= end and row["period_end"] >= start:
            cov = hc.weekly_coverage(row, daily)
            record("weekly", row, cov)
    for row in months:
        if start[:7] <= row["month"] <= end[:7]:
            cov = hc.monthly_coverage(row, weekly, daily) if report == "cybermed" else hc.cyberlurch_monthly_coverage(row)
            record("monthly", row, cov)
    actual = {d["run_date"] for d in daily}
    comparison_end = min(end, instant.astimezone(ZoneInfo("Europe/Stockholm")).date().isoformat())
    scenario = hc.expected_dates(start, comparison_end, hc.LEGACY_WEEKDAYS, report) or [] if start <= comparison_end else []
    years = {str(y): hc.yearly_coverage(report, y, months, weekly_rows=weekly, daily_rows=daily, as_of=as_of)
             for y in range(date.fromisoformat(start).year, date.fromisoformat(end).year+1)}
    errors = any(f["category"] in {"invalid_record", "invalid_existing_store", "corrupted_existing_store", "unreadable_existing_store", "input_changed_during_audit", "unsupported_existing_schema"} for f in findings)
    receipts = [{k: receipt[k] for k in ("edition_id", "cadence", "period_start", "period_end", "content_version", "status", "generation_validated", "state_persisted")}
                for receipt in journal.values() if receipt["period_start"] <= end and receipt["period_end"] >= start]
    return {"schema_version": 1, "report_key": report, "from_date": start, "to_date": end, "as_of": as_of,
            "source_commit": source_commit, "delivery_editions": receipts, "inputs": inputs, "findings": findings, "records": records, "years": years,
            "video_record_count": len(video) if report == "cyberlurch" else None,
            "historical_schedule": "unknown_unless_record_policy_is_explicit", "operating_start": "unknown",
            "comparison_scenario": {"policy": hc.LEGACY_WEEKDAYS, "assumption_only": True,
                "absent_daily_dates": sorted(set(scenario)-actual) if report == "cybermed" else None,
                "note": "Video publication dates do not establish edition coverage; missing dates are not proof of missed sends."},
            "recovery": {"apply_enabled": False, "source_status": "not_established_by_coverage_audit",
                "next_step": "Review targeted source availability, input/target hashes, backup, diff and idempotence in 07b; no SMTP replay."},
            "exit_code": 2 if errors else 1 if findings or any(r["coverage"]["status"] != "complete" for r in records) or any(y["status"] != "complete" for y in years.values()) else 0}


def markdown(payload):
    lines = [f"# {payload['report_key']} history audit", "", f"Source commit: `{payload['source_commit']}`; as-of: `{payload['as_of']}`.",
             "", "Coverage, substantive source quality, SMTP acceptance and persistence are separate dimensions.",
             "Historical operating start/policy may be unknown. Calendar scenarios are assumptions, not missed-send proof.", "",
             "| Kind | Stored ID | Coverage | SMTP status |", "|---|---|---|---|"]
    for row in payload["records"]:
        lines.append(f"| {row['kind']} | {row['id']} | {row['coverage']['status']} | {row['delivery']['status']} |")
    lines += ["", "## Findings", ""]+[f"- {f['category']}: `{f['location']}`" for f in payload["findings"]]
    lines += ["", "## Recovery", "", payload["recovery"]["next_step"], "", "## Machine-readable evidence", "", "```json", json.dumps({k: payload[k] for k in ("inputs", "comparison_scenario", "years")}, indent=2, sort_keys=True), "```", ""]
    return "\n".join(lines)


def write_report(payload, output_dir, *, paths=None):
    from ..store_io import atomic_text
    output = Path(output_dir).resolve()
    targets = paths or {name: path_for(name) for name in PATHS}
    # Resolve symlinks before checking. Never allow output onto any protected input.
    forbidden = {Path(p).resolve() for p in targets.values()}
    if any(output == p or output.is_relative_to(p) for p in forbidden) or any((output/name).resolve() in forbidden for name in ("history_audit.json", "history_audit.md")):
        raise ValueError("history_output_overlaps_state")
    roots = [Path(n).resolve() for n in ("state", "data", "health", ".cache")]
    roots += [p.parent for p in forbidden]
    if any(output.is_relative_to(p) for p in roots):
        raise ValueError("history_output_overlaps_state")
    atomic_text(output/"history_audit.json", json.dumps(payload, indent=2, sort_keys=True)+"\n")
    atomic_text(output/"history_audit.md", markdown(payload))
