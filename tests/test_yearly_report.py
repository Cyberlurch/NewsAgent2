import pathlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import main
from newsagent2 import reporter
from newsagent2 import rollups


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


def test_cybermed_yearly_partial_coverage_note():
    md = rollups.render_yearly_markdown(
        report_title="The Cybermed Year in Review — 2025",
        report_language="en",
        year=2025,
        rollups=[{"month": "2025-01", "top_items": [{"title": "A trial", "bottom_line": "Signal A"}], "executive_summary": []}],
        daily_digests=[{"date": "2025-01-15", "items": {"pubmed": [{"title": "B guideline", "practice_change_potential_1_5": 5}]}}],
        diagnostics={
            "cybermed_yearly_monthly_rollups_loaded_total": 1,
            "cybermed_yearly_daily_digests_loaded_total": 1,
            "cybermed_yearly_coverage_start": "2025-01-01",
            "cybermed_yearly_coverage_end": "2025-01-15",
            "cybermed_yearly_coverage_incomplete": True,
        },
    )
    assert "## Coverage note" in md
    assert "Coverage incomplete: YES" in md
    assert "## Top papers of the year" in md
    assert "## Potentially practice-changing items" in md
    assert "## Clinical themes of the year" in md


def test_cybermed_yearly_reads_monthly_rollups_only(monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("YEAR_IN_REVIEW_YEAR", "2026")
    monkeypatch.setenv("REPORT_PROFILE", "medical")
    daily_path = tmp_path / "daily.json"
    daily_path.write_text(
        json.dumps({
            "schema_version": 1,
            "digests": [{
                "digest_id": "poison",
                "date": "2026-03-01",
                "items": {"pubmed": [{"title": "MUST NOT ENTER YEARLY"}]},
            }],
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("CYBERMED_DAILY_DIGEST_STATE_PATH", str(daily_path))
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: None)

    def _capture(**kwargs):
        captured.update(kwargs)
        return "# Yearly\n"

    monkeypatch.setattr(main, "render_yearly_markdown", _capture)
    rollups_path = tmp_path / "rollups.json"
    rollups_path.write_text(
        json.dumps({
            "version": 1,
            "reports": {
                "cybermed": [{
                    "month": "2026-02",
                    "generated_at": "2026-03-01T05:40:00+00:00",
                    "executive_summary": ["Monthly only"],
                    "top_items": [],
                    "cybermed_items": [{"title": "FROM MONTHLY"}],
                }],
            },
        }),
        encoding="utf-8",
    )

    main._run_yearly_report(
        rollups_state_path=str(rollups_path),
        report_key="cybermed",
        base_report_title="Cybermed Report",
        base_report_subject="Cybermed Report",
        report_language="en",
        report_dir=str(tmp_path / "out"),
    )

    assert captured["daily_digests"] == []
    assert captured["diagnostics"]["cybermed_yearly_direct_daily_inputs_enabled"] is False
    assert captured["diagnostics"]["cybermed_yearly_upstream_cadence"] == "monthly"
