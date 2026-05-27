from __future__ import annotations

import json
from pathlib import Path

import pytest
from datetime import date

from newsagent2.maintenance import backfill_digest_stores as backfill
from newsagent2.maintenance.backfill_digest_stores import BackfillConfig, build_backfill_plan, run


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_missing_dates_are_reported_and_nonempty_not_overwritten(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    _write_json(
        state_dir / "cybermed_daily_digests.json",
        {
            "schema_version": 1,
            "digests": [
                {
                    "run_date": "2026-01-01",
                    "items": {"pubmed": [{"title": "x"}], "foamed": []},
                    "top_picks": [{"title": "tp"}],
                    "deep_dives": [],
                }
            ],
        },
    )
    _write_json(state_dir / "cyberlurch_digests.json", {"version": 1, "digests": []})
    _write_json(state_dir / "rollups.json", {"version": 1, "reports": {}})

    cfg = BackfillConfig(
        report="cybermed",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 3),
        apply=False,
        output_dir=tmp_path / "out",
        enable_openai=False,
    )
    plan = build_backfill_plan(cfg, state_dir=state_dir)
    cm = plan["reports"]["cybermed"]
    assert cm["missing_dates"] == ["2026-01-02", "2026-01-03"]
    assert any(x["reason"] == "existing_nonempty_digest" for x in cm["would_skip"])


def test_dry_run_produces_artifact_and_writes_no_state(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    original = {"schema_version": 1, "digests": []}
    _write_json(state_dir / "cybermed_daily_digests.json", original)
    _write_json(state_dir / "cyberlurch_digests.json", {"version": 1, "digests": []})
    _write_json(state_dir / "rollups.json", {"version": 1, "reports": {}})
    _write_json(state_dir / "processed_items.json", {"k": [1]})

    cfg = BackfillConfig(
        report="both",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 2),
        apply=False,
        output_dir=tmp_path / "out" / "backfill_audit",
        enable_openai=False,
    )
    rc = run(cfg, state_dir=state_dir)
    assert rc == 0
    assert (cfg.output_dir / "backfill_plan.json").exists()
    assert json.loads((state_dir / "cybermed_daily_digests.json").read_text(encoding="utf-8")) == original
    assert json.loads((state_dir / "processed_items.json").read_text(encoding="utf-8")) == {"k": [1]}


def test_apply_false_never_writes_state(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    _write_json(state_dir / "cybermed_daily_digests.json", {"schema_version": 1, "digests": []})
    _write_json(state_dir / "cyberlurch_digests.json", {"version": 1, "digests": []})
    _write_json(state_dir / "rollups.json", {"version": 1, "reports": {}})

    before = (state_dir / "rollups.json").read_text(encoding="utf-8")
    cfg = BackfillConfig(
        report="cyberlurch",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 1),
        apply=False,
        output_dir=tmp_path / "out",
        enable_openai=False,
    )
    run(cfg, state_dir=state_dir)
    after = (state_dir / "rollups.json").read_text(encoding="utf-8")
    assert before == after


def test_invalid_date_ranges_fail_safely(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["prog", "--report", "both", "--from-date", "2026-01-03", "--to-date", "2026-01-01"])
    with pytest.raises(SystemExit):
        backfill.parse_args()
