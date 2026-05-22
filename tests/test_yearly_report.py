import pathlib
from datetime import datetime
from zoneinfo import ZoneInfo
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import main
from newsagent2 import reporter


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


def test_yearly_thin_rollup_uses_single_limitation_note():
    md = reporter.render_cyberlurch_yearly_analysis(
        [{"month": "2026-01", "executive_summary": ["Only summary"], "top_items": [{"title": "x", "url": "https://example.com"}]}],
        target_year=2026,
        generated_at="2026-05-22",
    )
    expected = "Some earlier months contain thinner rollup detail; summaries below use the available monthly titles, channels and derived summaries."
    assert md.count(expected) == 1


def test_yearly_avoids_generic_filler_and_uses_enriched_data():
    md = reporter.render_cyberlurch_yearly_analysis(
        [{
            "month": "2026-01",
            "executive_summary": ["Summary one"],
            "top_themes": [{"theme": "AI policy", "count": 3}],
            "top_channels": [{"channel": "Channel A", "count": 4}],
            "topic_summaries": ["AI policy: 3 item(s)"],
            "topic_trajectories": ["AI policy: sustained stream"],
            "evergreen_highlights": ["Evergreen explainer"],
            "representative_items": [{"title": "x", "url": "https://example.com"}],
        }],
        target_year=2026,
        generated_at="2026-05-22",
    )
    assert "Themes are aggregated from enriched monthly top_themes and topic_summaries." not in md
    assert "Repeated crisis streams are summarized as trajectories." not in md
    assert "Narratives were tracked across months and channels." not in md
    assert "AI policy" in md
    assert "Channel A" in md
    assert "January 2026" in md
