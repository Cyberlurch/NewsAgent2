from newsagent2.cybermed_digest_store import (
    aggregate_cybermed_digest_inputs,
    prepare_cybermed_monthly_source_digests,
)
from datetime import datetime
from newsagent2.main import STO, determine_monthly_rollup_month


def weekly(start, end, items=None):
    return {
        "digest_id": f"cybermed_weekly_{start}_{end}",
        "period_start": start,
        "period_end": end,
        "items": {"pubmed": items or [], "foamed": []},
        "deep_dives": [], "top_picks": [],
    }


def complete_august(final_items=None):
    periods = [
        ("2026-08-03", "2026-08-07"), ("2026-08-10", "2026-08-14"),
        ("2026-08-17", "2026-08-21"), ("2026-08-24", "2026-08-28"),
        ("2026-08-31", "2026-09-04"),
    ]
    return {"digests": [weekly(a, b, final_items if i == 4 else []) for i, (a, b) in enumerate(periods)]}


def test_august_expected_periods_and_boundary_filtering():
    august = {"id": "aug", "source": "pubmed", "source_daily_run_dates": ["2026-08-31"]}
    september = {"id": "sep", "source": "pubmed", "source_daily_run_dates": ["2026-09-01"]}
    selected, diag = prepare_cybermed_monthly_source_digests(complete_august([august, september]), "2026-08")
    assert diag["coverage_complete"] is True
    assert diag["expected_weekly_periods"][-1] == "2026-08-31..2026-09-04"
    assert [x["id"] for x in selected[-1]["items"]["pubmed"]] == ["aug"]


def test_opening_boundary_is_required():
    _, diag = prepare_cybermed_monthly_source_digests({"digests": []}, "2026-09")
    assert diag["expected_weekly_periods"][0] == "2026-08-31..2026-09-04"


def test_unsafe_legacy_boundary_fails_but_internal_legacy_is_safe():
    store = complete_august([{"id": "legacy", "source": "pubmed"}])
    _, diag = prepare_cybermed_monthly_source_digests(store, "2026-08")
    assert diag["coverage_complete"] is False
    store["digests"][-1]["items"]["pubmed"] = []
    store["digests"][0]["items"]["pubmed"] = [{"id": "internal", "source": "pubmed"}]
    selected, diag = prepare_cybermed_monthly_source_digests(store, "2026-08")
    assert diag["coverage_complete"] is True
    assert selected[0]["items"]["pubmed"][0]["id"] == "internal"


def test_missing_week_is_incomplete():
    store = complete_august()
    store["digests"].pop(2)
    _, diag = prepare_cybermed_monthly_source_digests(store, "2026-08")
    assert diag["coverage_complete"] is False
    assert diag["missing_weekly_periods"] == ["2026-08-17..2026-08-21"]


def test_weekly_duplicate_provenance_is_sorted_union():
    daily = [
        {"run_date": "2026-08-04", "items": {"pubmed": [{"id": "1", "source": "pubmed"}], "foamed": []}},
        {"run_date": "2026-08-03", "items": {"pubmed": [{"id": "1", "source": "pubmed"}], "foamed": []}},
    ]
    result = aggregate_cybermed_digest_inputs(daily, pubmed_cap=10, foamed_cap=10, deep_dive_cap=3)
    assert result["pubmed"][0]["source_daily_run_dates"] == ["2026-08-03", "2026-08-04"]


def test_first_friday_scheduled_monthly_targets_previous_month():
    now = datetime(2026, 9, 4, 5, 40, tzinfo=STO)
    assert determine_monthly_rollup_month(now, "schedule", None) == "2026-08"
