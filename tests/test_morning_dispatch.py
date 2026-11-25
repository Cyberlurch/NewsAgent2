"""Independent calendar dispatch shares the exact native edition identities."""
from datetime import datetime
import json
import sys
from pathlib import Path
import importlib.util

import pytest
import yaml

from newsagent2 import edition_plan as ep, delivery
from newsagent2.run_context import path_for
from newsagent2.store_io import atomic_store
from test_recovery_scope import result as receipt

spec = importlib.util.spec_from_file_location("morning_dispatch", Path("tools/morning_dispatch.py"))
morning = importlib.util.module_from_spec(spec)
spec.loader.exec_module(morning)


@pytest.mark.parametrize("key,day", [
    ("cyberlurch", "2026-11-27"),
    ("cybermed", "2026-11-27"),
    ("cyberlurch", "2026-12-01"),
    ("cybermed", "2026-12-04"),
    ("cybermed", "2026-12-23"),
    ("cybermed", "2026-12-25"),
    ("cyberlurch", "2027-01-01"),
    ("cybermed", "2027-01-04"),
    ("cybermed", "2027-07-02"),
])
def test_calendar_dispatch_matches_native_dates_cadences_and_ids(key, day):
    args = dict(created_at=day+"T08:00:00+01:00",
                job_started_at=day+"T08:01:00+01:00", mode="real")
    native = ep.make_plan(key, trigger="schedule", **args)
    explicit = ep.make_plan(key, trigger="workflow_dispatch", logical_date=day, **args)
    assert explicit["trigger"] == "workflow_dispatch"
    assert explicit["calendar_dispatch"] is True
    assert explicit["entries"] == native["entries"]
    assert ep.validate_plan(explicit)
    assert [r["cadence"] for r in explicit["entries"]] == [
        c for c in ep.CADENCES if c in [e["cadence"] for e in native["entries"]]]


@pytest.mark.parametrize("overrides", [
    {"logical_date": ""},
    {"logical_date": "2026-11-24"},
    {"logical_date": "2026-11-26"},
    {"logical_date": "2026-11-25", "month_override": "2026-09"},
    {"logical_date": "2026-11-25", "year_override": "2025"},
])
def test_calendar_dispatch_does_not_authorize_historical_or_overridden_edition(overrides):
    with pytest.raises(ValueError):
        ep.make_plan("cybermed", trigger="workflow_dispatch", mode="real",
                     created_at="2026-11-25T06:00:00+01:00",
                     job_started_at="2026-11-25T06:01:00+01:00", **overrides)


def test_real_calendar_dispatch_prunes_accepted_editions(monkeypatch, tmp_path):
    day = "2026-11-25"
    row = receipt("cyberlurch", start=day, end=day, body="Saved report.")
    row.update(status="accepted_by_smtp", state_persisted=True,
               smtp_phase="completed", accepted=["a"*64])
    atomic_store(path_for("DELIVERY_JOURNAL_PATH"),
                 {"version":1, "editions": {row["edition_id"]:row}},
                 kind="journal", validate=delivery.journal_valid)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("REPORT_MODE", "scheduled")
    monkeypatch.setenv("EMAIL_MODE", "real")
    before = path_for("DELIVERY_JOURNAL_PATH").read_bytes()
    out = tmp_path / "plan.json"
    monkeypatch.setattr(sys, "argv", ["plan", "--report-key", "cyberlurch",
        "--logical-date", day, "--created-at", day+"T06:00:00Z",
        "--job-started-at", day+"T06:01:00Z", "--output", str(out)])
    ep.main()
    plan = json.loads(out.read_text())
    assert plan["entries"] == [] and plan["reason"] == "already_finalized_no_op"
    assert plan["already_finalized_editions"] == [row["edition_id"]]
    assert path_for("DELIVERY_JOURNAL_PATH").read_bytes() == before


@pytest.mark.parametrize("stamp", ["2026-11-25T05:50:00+00:00", "2027-07-02T04:50:00+00:00"])
def test_morning_dispatch_is_dst_correct_and_not_repeated(tmp_path, stamp):
    now = datetime.fromisoformat(stamp)
    calls = []
    path = tmp_path / "morning.json"
    def submit(workflow, data):
        calls.append((workflow, data))
        return len(calls)
    result = morning.dispatch_morning(now, path, submit=submit)
    assert list(result) == list(morning.WORKFLOWS)
    assert len(calls) == 2
    assert calls[0][1]["inputs"]["logical_date"] == stamp[:10]
    assert calls[0][1]["inputs"]["report_mode"] == "scheduled"
    assert morning.dispatch_morning(now, path, submit=submit) == result
    assert len(calls) == 2


def test_uncertain_dispatch_is_not_retried_and_does_not_block_other_newsletter(tmp_path):
    now = datetime.fromisoformat("2026-11-25T07:00:00+01:00")
    path = tmp_path / "morning.json"
    calls = []
    def submit(workflow, data):
        calls.append(workflow)
        if workflow == "newsagent.yml":
            raise RuntimeError("dispatch_outcome_unverified")
        return 12
    result = morning.dispatch_morning(now, path, submit=submit)
    assert result["newsagent.yml"]["status"] == "unverified"
    assert result["cybermed.yml"]["run_id"] == 12
    morning.dispatch_morning(now, path, submit=submit)
    assert calls == list(morning.WORKFLOWS)


def test_outside_window_is_noop(tmp_path):
    path = tmp_path / "morning.json"
    now = datetime.fromisoformat("2026-11-25T16:00:00+01:00")
    assert morning.dispatch_morning(now, path, submit=lambda *a: pytest.fail("unexpected dispatch")) == {}
    assert not path.exists()


def test_all_state_writers_keep_shared_queue():
    for path in ("newsagent.yml","cybermed.yml","newsletter-recover-state.yml",
                 "cybermed-daily-backfill.yml","cybermed-weekly-backfill.yml"):
        workflow = yaml.safe_load((Path(".github/workflows") / path).read_text())
        assert workflow["concurrency"] == {
            "group": "newsagent2-state-writers-${{ github.ref }}",
            "cancel-in-progress": False, "queue": "max"}
