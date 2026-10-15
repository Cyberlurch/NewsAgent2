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
    from datetime import date
    from newsagent2.edition_plan import planned_modes
    workflow = open(".github/workflows/cybermed.yml", encoding="utf-8").read()
    assert 'edition_plan --report-key cybermed' in workflow
    assert 'for mode in $RUN_MODES; do' in workflow
    assert planned_modes('cybermed',date(2026,1,2))==['daily','weekly','monthly','yearly']
    assert planned_modes('cybermed',date(2027,1,1))==[]
    assert planned_modes('cybermed',date(2027,1,4))==['daily','monthly','yearly']


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


def test_real_complete_coverage_sends_and_reports_exact_months(monkeypatch, tmp_path, freeze_clock):
    from newsagent2 import history_coverage as hc
    from newsagent2.cybermed_digest_store import make_cybermed_weekly_digest
    from datetime import date, timedelta
    import os
    freeze_clock('2027-01-15T12:00:00+00:00')
    daily_by_id, weeks_by_id = {}, {}
    months = _months()
    for m in months:
        start,end=hc.month_bounds(m['month']);cursor=date.fromisoformat(start)
        cursor-=timedelta(days=cursor.weekday());refs=[]
        if (cursor+timedelta(days=4)).isoformat()<start:cursor+=timedelta(days=7)
        while cursor.isoformat()<=end:
            first=cursor.isoformat();last=(cursor+timedelta(days=4)).isoformat()
            ds=[{'digest_id':'cybermed_daily_'+d,'run_date':d} for d in hc.expected_dates(first,last,hc.POLICY)]
            w=make_cybermed_weekly_digest(period_start=first,period_end=last,generated_at_utc='2027-01-15T12:00:00Z',source_digests=ds,pubmed_items=[],foamed_items=[],deep_dives=[],top_picks=[])
            w['calendar_policy']=hc.POLICY
            weeks_by_id[w['digest_id']]=w;refs.append(w['digest_id'])
            daily_by_id.update({d['digest_id']:d for d in ds});cursor+=timedelta(days=7)
        m.update(calendar_policy=hc.POLICY,source_weekly_digest_ids=refs)
    for name,rows in [('CYBERMED_DAILY_DIGEST_STATE_PATH',list(daily_by_id.values())),('CYBERMED_WEEKLY_DIGEST_STATE_PATH',list(weeks_by_id.values()))]:
        __import__('pathlib').Path(os.environ[name]).write_text(json.dumps({'digests':rows}))
    path = tmp_path / "rollups.json"
    path.write_text(json.dumps({"reports": {"cybermed": months}}))
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
