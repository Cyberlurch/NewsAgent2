import json
from pathlib import Path

from newsagent2.cybermed_digest_store import build_weekly_backfill_candidates


DAILY_DATES = [f"2026-08-{day:02d}" for day in range(10, 15)]
TOP_PICKS = ["42584898", "42579389", "42573415", "42584186", "42593537"]
DEEP_DIVES = ["42584898", "42584186", "42585602", "42579389", "42573415"]


def _identity(row: dict) -> str:
    return str(
        row.get("item_id")
        or row.get("id")
        or row.get("pmid")
        or row.get("url")
        or ""
    )


def test_august_weekly_backfill_matches_restored_production_digest(monkeypatch):
    # Aggregation has no provider dependency.  Making accidental network use
    # fatal protects that property as the implementation evolves.
    monkeypatch.setattr("socket.socket.connect", lambda *_: (_ for _ in ()).throw(
        AssertionError("Weekly aggregation must not call a provider")
    ))
    daily_store = json.loads(Path("state/cybermed_daily_digests.json").read_text())
    source = [row for row in daily_store["digests"] if row.get("run_date") in DAILY_DATES]

    candidates = build_weekly_backfill_candidates(
        source, generated_at_utc="2026-08-14T23:59:59+00:00"
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["digest_id"] == "cybermed_weekly_2026-08-10_2026-08-14"
    assert candidate["source_run_dates"] == DAILY_DATES
    assert len(candidate["items"]["pubmed"]) == 20
    assert len(candidate["items"]["foamed"]) == 12
    assert len({row["url"] for rows in candidate["items"].values() for row in rows}) == 32
    assert candidate["diagnostic_summary"]["duplicates_suppressed_total"] == 18
    assert candidate["top_picks"] == TOP_PICKS

    flagged_pubmed = [
        _identity(row) for row in candidate["items"]["pubmed"] if row.get("top_pick") is True
    ]
    assert flagged_pubmed == TOP_PICKS
    assert not any(row.get("top_pick") is True for row in candidate["items"]["foamed"])
    assert [_identity(row) for row in candidate["deep_dives"]] == DEEP_DIVES


def test_weekly_backfill_still_rejects_incomplete_week():
    daily_store = json.loads(Path("state/cybermed_daily_digests.json").read_text())
    source = [
        row for row in daily_store["digests"]
        if "2026-08-10" <= row.get("run_date", "") <= "2026-08-13"
    ]
    assert build_weekly_backfill_candidates(source, generated_at_utc="fixed") == []
