import pathlib
from datetime import datetime
from zoneinfo import ZoneInfo
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import main


def test_year_in_review_target_year_selection():
    now = datetime(2025, 12, 15, 8, 0, tzinfo=ZoneInfo("Europe/Stockholm"))
    assert (
        main._determine_year_in_review_year(
            now_sto=now,
            override_year="2025",
            event_name="workflow_dispatch",
        )
        == 2025
    )

    assert (
        main._determine_year_in_review_year(
            now_sto=now,
            override_year=None,
            event_name="workflow_dispatch",
        )
        == 2025
    )

    jan_first = datetime(2025, 1, 1, 6, 0, tzinfo=ZoneInfo("Europe/Stockholm"))
    assert (
        main._determine_year_in_review_year(
            now_sto=jan_first,
            override_year=None,
            event_name="schedule",
        )
        == 2024
    )


def test_yearly_scheduled_empty_skips_email(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.delenv("YEAR_IN_REVIEW_YEAR", raising=False)
    monkeypatch.setattr(main, "send_markdown", lambda subject, md: calls.append(subject))

    rollups_path = tmp_path / "rollups.json"
    report_dir = tmp_path / "reports"

    main._run_yearly_report(
        rollups_state_path=str(rollups_path),
        report_key="cyberlurch",
        base_report_title="The Cyberlurch Report",
        base_report_subject="The Cyberlurch Report",
        report_language="en",
        report_dir=str(report_dir),
    )

    assert calls == []
    assert not list(report_dir.glob("*.md"))


def test_yearly_manual_empty_still_sends(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.delenv("YEAR_IN_REVIEW_YEAR", raising=False)
    monkeypatch.setattr(main, "send_markdown", lambda subject, md: calls.append(subject))

    rollups_path = tmp_path / "rollups.json"
    report_dir = tmp_path / "reports"

    main._run_yearly_report(
        rollups_state_path=str(rollups_path),
        report_key="cyberlurch",
        base_report_title="The Cyberlurch Report",
        base_report_subject="The Cyberlurch Report",
        report_language="en",
        report_dir=str(report_dir),
    )

    assert len(calls) == 1
    assert list(report_dir.glob("cyberlurch_yearly_review_*.md"))
