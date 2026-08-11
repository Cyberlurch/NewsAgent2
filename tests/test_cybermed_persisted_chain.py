import json
from datetime import date

import pytest

from newsagent2.cybermed_digest_store import (
    CybermedDigestStoreError,
    load_cybermed_weekly_digest_store,
    make_cybermed_weekly_digest,
    save_cybermed_weekly_digest_store,
    select_cybermed_weekly_digests_for_month,
    upsert_cybermed_weekly_digest,
)
from newsagent2.maintenance.backfill_cybermed_weekly import apply_audit, build_audit


def _daily(run_date: str, item_id: str) -> dict:
    return {
        "digest_id": f"cybermed_daily_{run_date}",
        "report_key": "cybermed",
        "cadence": "daily",
        "run_date": run_date,
        "generated_at_utc": f"{run_date}T05:40:00+00:00",
        "items": {
            "pubmed": [{
                "item_id": item_id,
                "source_type": "pubmed",
                "pmid": item_id,
                "title": f"Paper {item_id}",
                "bottom_line": "Stored clinical result",
                "practice_change_potential_1_5": 4,
            }],
            "foamed": [],
        },
        "deep_dives": [],
        "top_picks": [],
    }


def test_weekly_store_is_atomic_and_refuses_corrupt_existing_file(tmp_path):
    path = tmp_path / "weekly.json"
    digest = make_cybermed_weekly_digest(
        period_start="2026-08-03",
        period_end="2026-08-07",
        generated_at_utc="2026-08-07T05:40:00+00:00",
        source_digests=[_daily("2026-08-03", "1")],
        pubmed_items=[],
        foamed_items=[],
        deep_dives=[],
        top_picks=[],
    )
    store, status = upsert_cybermed_weekly_digest(
        {"schema_version": 1, "digests": []}, digest
    )
    assert status == "inserted"
    save_cybermed_weekly_digest_store(str(path), store)
    assert load_cybermed_weekly_digest_store(str(path))["digests"][0]["digest_id"] == digest["digest_id"]
    assert not path.with_name("weekly.json.tmp").exists()

    path.write_text("{broken", encoding="utf-8")
    before = path.read_text(encoding="utf-8")
    with pytest.raises(CybermedDigestStoreError):
        load_cybermed_weekly_digest_store(str(path))
    assert path.read_text(encoding="utf-8") == before


def test_monthly_selection_uses_weekly_source_dates_for_boundary_weeks():
    store = {
        "digests": [
            {
                "digest_id": "w1",
                "period_start": "2026-04-27",
                "period_end": "2026-05-01",
                "source_run_dates": ["2026-04-27", "2026-05-01"],
            },
            {
                "digest_id": "w2",
                "period_start": "2026-06-01",
                "period_end": "2026-06-05",
                "source_run_dates": ["2026-06-01"],
            },
        ]
    }
    assert [
        row["digest_id"]
        for row in select_cybermed_weekly_digests_for_month(store, "2026-05")
    ] == ["w1"]


def test_weekly_backfill_requires_reviewed_hash_before_mutation(tmp_path):
    daily_path = tmp_path / "daily.json"
    weekly_path = tmp_path / "weekly.json"
    daily_path.write_text(
        json.dumps({
            "schema_version": 1,
            "digests": [
                _daily("2026-08-03", "1"),
                _daily("2026-08-04", "2"),
                _daily("2026-08-07", "3"),
                _daily("2026-08-14", "4"),
            ],
        }),
        encoding="utf-8",
    )

    audit = build_audit(
        daily_path=daily_path,
        weekly_path=weekly_path,
        from_date=date(2026, 8, 1),
        to_date=date(2026, 8, 14),
    )
    assert audit["candidate_weekly_digests_total"] == 2
    assert audit["apply_executed"] is False
    assert not weekly_path.exists()

    with pytest.raises(SystemExit, match="does not match"):
        apply_audit(audit, weekly_path=weekly_path, expected_sha256="0" * 64)
    assert not weekly_path.exists()

    applied = apply_audit(
        audit,
        weekly_path=weekly_path,
        expected_sha256=audit["candidate_sha256"],
    )
    assert applied["inserted_total"] == 2
    assert len(load_cybermed_weekly_digest_store(str(weekly_path))["digests"]) == 2
