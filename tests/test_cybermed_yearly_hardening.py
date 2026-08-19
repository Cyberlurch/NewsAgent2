import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from newsagent2 import main, rollups


def _months(year=2026, count=12):
    return [
        {
            "month": f"{year}-{month:02d}",
            "top_items": [{"title": f"Legacy {month}", "pmid": str(month)}],
            "cybermed_items": (
                [{
                    "title": f"Rich {month}", "pmid": str(month),
                    "bottom_line": "BOTTOM LINE: Example result.",
                    "practice_change_potential_1_5": 4,
                    "domain": "Anaesthesia",
                }]
                if month % 2 == 0 else []
            ),
        }
        for month in range(1, count + 1)
    ]


def test_mixed_monthly_schema_scores_bottom_lines_themes_and_sanitation():
    entries = _months()
    entries[0]["top_items"][0]["bottom_line"] = "RateLimit quota exceeded provider error"
    diagnostics = {}
    md = rollups.render_yearly_markdown(
        report_title="Cybermed Year in Review", report_language="en", year=2026,
        rollups=entries, diagnostics=diagnostics,
    )
    assert "Legacy 1" in md and "Rich 2" in md and "Legacy 7" in md
    assert "## Legacy / not scored" in md
    assert "Legacy 1 — impact" not in md
    assert md.count("**BOTTOM LINE:** Example result.") == 6
    assert "RateLimit" not in md and "quota exceeded" not in md and "provider error" not in md
    assert "Anaesthesia (6)" in md
    assert "Sepsis/infection" not in md and "AI/methods" not in md
    assert diagnostics["cybermed_yearly_rich_months_total"] == 6
    assert diagnostics["cybermed_yearly_legacy_months_total"] == 6


def test_yearly_dedup_prefers_rich_record():
    entries = _months()
    entries[1]["cybermed_items"][0]["pmid"] = "1"
    entries[1]["cybermed_items"][0]["title"] = "Richer duplicate"
    diagnostics = {}
    md = rollups.render_yearly_markdown(
        report_title="Cybermed Year in Review", report_language="en", year=2026,
        rollups=entries, diagnostics=diagnostics,
    )
    assert diagnostics["cybermed_yearly_duplicates_suppressed_total"] == 1
    assert "Richer duplicate" in md
    assert "- Legacy 1\n" not in md


def test_target_year_first_friday_schedule_and_override():
    first_friday = datetime(2027, 1, 1, tzinfo=ZoneInfo("Europe/Stockholm"))
    assert main._determine_year_in_review_year(
        now_sto=first_friday, override_year=None, event_name="schedule"
    ) == 2026
    assert main._determine_year_in_review_year(
        now_sto=first_friday, override_year="2024", event_name="schedule"
    ) == 2024


@pytest.mark.parametrize("email_mode", ["none", "test"])
def test_manual_partial_audit_is_immutable(monkeypatch, tmp_path, email_mode):
    path = tmp_path / "rollups.json"
    path.write_text(json.dumps({"reports": {"cybermed": _months(count=11)}}), encoding="utf-8")
    before = path.read_bytes()
    monkeypatch.setenv("REPORT_PROFILE", "medical")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("YEAR_IN_REVIEW_YEAR", "2026")
    monkeypatch.setenv("EMAIL_MODE", email_mode)
    sent = []
    monkeypatch.setattr(main, "send_markdown", lambda *args: sent.append(args))
    main._run_yearly_report(
        rollups_state_path=str(path), report_key="cybermed",
        base_report_title="Cybermed", base_report_subject="Cybermed",
        report_language="en", report_dir=str(tmp_path / "reports"),
    )
    assert path.read_bytes() == before
    assert bool(sent) is (email_mode == "test")
    report = tmp_path / "reports" / "cybermed_yearly_review_2026.md"
    assert "Coverage incomplete: YES" in report.read_text()


def test_workflow_first_friday_orders_monthly_before_yearly():
    workflow = open(".github/workflows/cybermed.yml", encoding="utf-8").read()
    plan = workflow[workflow.index("planned=()"):workflow.index('modes="${planned[*]}"')]
    assert 'if [ "$dom" = "01" ]' not in plan
    assert 'if [ "$dom" -le 7 ]' in plan
    assert plan.index('planned+=("daily")') < plan.index('planned+=("weekly")')
    assert plan.index('planned+=("weekly")') < plan.index('planned+=("monthly")')
    assert plan.index('planned+=("monthly")') < plan.index('planned+=("yearly")')


def test_real_incomplete_fails_before_send(monkeypatch, tmp_path):
    path = tmp_path / "rollups.json"
    path.write_text(json.dumps({"reports": {"cybermed": _months(count=11)}}))
    monkeypatch.setenv("REPORT_PROFILE", "medical")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("YEAR_IN_REVIEW_YEAR", "2026")
    monkeypatch.setenv("EMAIL_MODE", "real")
    sent = []
    monkeypatch.setattr(main, "send_markdown", lambda *args: sent.append(args))
    with pytest.raises(RuntimeError, match="incomplete_coverage"):
        main._run_yearly_report(
            rollups_state_path=str(path), report_key="cybermed",
            base_report_title="Cybermed", base_report_subject="Cybermed",
            report_language="en", report_dir=str(tmp_path / "reports"),
        )
    assert not sent


def test_real_complete_coverage_sends_and_reports_exact_months(monkeypatch, tmp_path):
    path = tmp_path / "rollups.json"
    path.write_text(json.dumps({"reports": {"cybermed": _months()}}))
    monkeypatch.setenv("REPORT_PROFILE", "medical")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("YEAR_IN_REVIEW_YEAR", "2026")
    monkeypatch.setenv("EMAIL_MODE", "real")
    sent = []
    monkeypatch.setattr(main, "send_markdown", lambda *args: sent.append(args))
    main._run_yearly_report(
        rollups_state_path=str(path), report_key="cybermed",
        base_report_title="Cybermed", base_report_subject="Cybermed",
        report_language="en", report_dir=str(tmp_path / "reports"),
    )
    assert len(sent) == 1
    diagnostics = json.loads(
        (tmp_path / "reports" / "cybermed_yearly_diagnostics.json").read_text()
    )
    assert diagnostics["coverage_complete"] is True
    assert diagnostics["found_months"] == diagnostics["expected_months"]
    assert diagnostics["missing_months"] == []
