from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from ..cybermed_digest_store import (
    build_weekly_backfill_candidates,
    load_cybermed_daily_digest_store,
    load_cybermed_weekly_digest_store,
    save_cybermed_weekly_digest_store,
    upsert_cybermed_weekly_digest,
)


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception as exc:
        raise SystemExit(f"Invalid date {value!r}; expected YYYY-MM-DD") from exc


def _stable_generation_marker(digests: list[dict[str, Any]]) -> str:
    markers = sorted(
        str(row.get("generated_at_utc") or "").strip()
        for row in digests
        if str(row.get("generated_at_utc") or "").strip()
    )
    if markers:
        return markers[-1]
    dates = sorted(
        str(row.get("run_date") or "").strip()
        for row in digests
        if str(row.get("run_date") or "").strip()
    )
    return f"{dates[-1]}T23:59:59+00:00" if dates else "1970-01-01T00:00:00+00:00"


def _candidate_hash(candidates: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        candidates,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def build_audit(
    *,
    daily_path: Path,
    weekly_path: Path,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    daily_store = load_cybermed_daily_digest_store(str(daily_path))
    source_digests = [
        row
        for row in daily_store.get("digests", [])
        if isinstance(row, dict)
        and from_date <= _parse_date(str(row.get("run_date") or "")) <= to_date
    ]
    candidates = build_weekly_backfill_candidates(
        source_digests,
        generated_at_utc=_stable_generation_marker(source_digests),
    )
    existing_store = load_cybermed_weekly_digest_store(str(weekly_path))
    existing_ids = {
        str(row.get("digest_id") or "")
        for row in existing_store.get("digests", [])
        if isinstance(row, dict)
    }
    return {
        "schema_version": 1,
        "mode": "audit",
        "apply_executed": False,
        "daily_store_path": str(daily_path),
        "weekly_store_path": str(weekly_path),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "source_daily_digests_total": len(source_digests),
        "candidate_weekly_digests_total": len(candidates),
        "candidate_sha256": _candidate_hash(candidates),
        "would_insert_total": sum(
            1 for row in candidates if str(row.get("digest_id") or "") not in existing_ids
        ),
        "would_skip_existing_total": sum(
            1 for row in candidates if str(row.get("digest_id") or "") in existing_ids
        ),
        "candidates": candidates,
    }


def apply_audit(
    audit: dict[str, Any],
    *,
    weekly_path: Path,
    expected_sha256: str,
) -> dict[str, Any]:
    actual_sha256 = str(audit.get("candidate_sha256") or "")
    if not expected_sha256 or expected_sha256 != actual_sha256:
        raise SystemExit(
            "Refusing Weekly backfill apply: expected SHA-256 does not match the current audit"
        )
    store = load_cybermed_weekly_digest_store(str(weekly_path))
    inserted = 0
    skipped = 0
    for candidate in audit.get("candidates") or []:
        store, status = upsert_cybermed_weekly_digest(
            store,
            candidate,
            overwrite=False,
        )
        if status == "inserted":
            inserted += 1
        else:
            skipped += 1
    if inserted:
        save_cybermed_weekly_digest_store(str(weekly_path), store)
    audit["mode"] = "apply"
    audit["apply_executed"] = True
    audit["inserted_total"] = inserted
    audit["skipped_existing_total"] = skipped
    audit["applied_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return audit


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit or apply deterministic Cybermed Daily-to-Weekly backfill."
    )
    parser.add_argument("--daily-path", default="state/cybermed_daily_digests.json")
    parser.add_argument("--weekly-path", default="state/cybermed_weekly_digests.json")
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--output", default="out/cybermed_weekly_backfill_audit.json")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-sha256", default="")
    args = parser.parse_args()

    from_date = _parse_date(args.from_date)
    to_date = _parse_date(args.to_date)
    if to_date < from_date:
        raise SystemExit("Invalid date range: to-date must be >= from-date")

    audit = build_audit(
        daily_path=Path(args.daily_path),
        weekly_path=Path(args.weekly_path),
        from_date=from_date,
        to_date=to_date,
    )
    if args.apply:
        audit = apply_audit(
            audit,
            weekly_path=Path(args.weekly_path),
            expected_sha256=str(args.expected_sha256 or "").strip(),
        )

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        "[cybermed-weekly-backfill] "
        f"mode={audit['mode']} candidates={audit['candidate_weekly_digests_total']} "
        f"sha256={audit['candidate_sha256']} output={output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
