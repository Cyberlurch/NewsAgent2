from datetime import date

from newsagent2.cybermed_digest_store import (
    load_cybermed_daily_digest_store,
    select_cybermed_daily_digests_for_month,
    select_cybermed_daily_digests_for_week,
    summarize_cybermed_weekly_digest_inputs,
    dedupe_weekly_digest_items,
)


def test_loader_missing_file(tmp_path):
    data = load_cybermed_daily_digest_store(str(tmp_path / 'missing.json'))
    assert data['schema_version'] == 1
    assert data['digests'] == []


def test_loader_skips_malformed_entries(tmp_path):
    p = tmp_path / 'd.json'
    p.write_text('{"schema_version":1,"digests":[{"digest_id":"ok","run_date":"2026-05-20"},"bad",{}]}', encoding='utf-8')
    data = load_cybermed_daily_digest_store(str(p))
    assert len(data['digests']) == 1


def test_week_select_and_summary(tmp_path):
    p = tmp_path / 'd.json'
    p.write_text('{"schema_version":1,"digests":[{"digest_id":"d1","run_date":"2026-05-18","items":{"pubmed":[{"item_id":"p1"}],"foamed":[]},"deep_dives":[],"top_picks":[]},{"digest_id":"d2","run_date":"2026-05-19","items":{"pubmed":[],"foamed":[{"item_id":"f1"}]},"deep_dives":[{"item_id":"p1"}],"top_picks":[{"item_id":"f1"}]}]}', encoding='utf-8')
    store = load_cybermed_daily_digest_store(str(p))
    selected = select_cybermed_daily_digests_for_week(store, date(2026,5,22))
    summary = summarize_cybermed_weekly_digest_inputs(selected)
    assert len(selected) == 2
    assert summary['pubmed_items_loaded_total'] == 1
    assert summary['foamed_items_loaded_total'] == 1


def test_dedupe_prefers_stronger_duplicate():
    items = [
        {"source_type": "pubmed", "pmid": "1", "evidence_strength_label": "C", "clinical_relevance_1_5": 2, "practice_change_potential_1_5": 2, "text_confidence_label": "low", "top_pick": False, "bottom_line": ""},
        {"source_type": "pubmed", "pmid": "1", "evidence_strength_label": "A", "clinical_relevance_1_5": 5, "practice_change_potential_1_5": 5, "text_confidence_label": "high", "top_pick": True, "bottom_line": "ok"},
    ]
    deduped, suppressed, reasons = dedupe_weekly_digest_items(items)
    assert len(deduped) == 1
    assert deduped[0]["evidence_strength_label"] == "A"
    assert suppressed == 1
    assert sum(reasons.values()) == 1


def test_month_select_filters_by_month_key(tmp_path):
    p = tmp_path / 'd.json'
    p.write_text('{"schema_version":1,"digests":[{"digest_id":"d0","run_date":"2026-04-30"},{"digest_id":"d1","run_date":"2026-05-01"},{"digest_id":"d2","run_date":"2026-05-31"},{"digest_id":"d3","run_date":"2026-06-01"}]}', encoding='utf-8')
    store = load_cybermed_daily_digest_store(str(p))
    selected = select_cybermed_daily_digests_for_month(store, "2026-05")
    assert [d["digest_id"] for d in selected] == ["d1", "d2"]

def test_scheduled_friday_selects_current_reporting_week(monkeypatch):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    store = {"digests": [
        {"digest_id": "prev_mon", "run_date": "2026-05-18"},
        {"digest_id": "prev_sun", "run_date": "2026-05-24"},
        {"digest_id": "cur_mon", "run_date": "2026-05-25"},
        {"digest_id": "cur_fri", "run_date": "2026-05-29"},
    ]}
    selected = select_cybermed_daily_digests_for_week(store, date(2026, 5, 29))
    assert [d["digest_id"] for d in selected] == ["cur_mon", "cur_fri"]


def test_scheduled_friday_prior_completed_week_requires_explicit_override(monkeypatch):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    store = {"digests": [
        {"digest_id": f"d{day}", "run_date": f"2026-05-{day:02d}"}
        for day in range(18, 25)
    ]}
    assert select_cybermed_daily_digests_for_week(store, date(2026, 5, 29)) == []

    monkeypatch.setenv("CYBERMED_WEEKLY_PERIOD_START", "2026-05-18")
    monkeypatch.setenv("CYBERMED_WEEKLY_PERIOD_END", "2026-05-24")
    selected = select_cybermed_daily_digests_for_week(store, date(2026, 5, 29))
    assert [d["run_date"] for d in selected] == [f"2026-05-{day:02d}" for day in range(18, 25)]


def test_manual_workflow_dispatch_uses_same_default_weekly_period(monkeypatch):
    store = {"digests": [
        {"digest_id": "prev_sun", "run_date": "2026-05-24"},
        {"digest_id": "cur_mon", "run_date": "2026-05-25"},
        {"digest_id": "cur_fri", "run_date": "2026-05-29"},
    ]}
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    scheduled = select_cybermed_daily_digests_for_week(store, date(2026, 5, 29))
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    manual = select_cybermed_daily_digests_for_week(store, date(2026, 5, 29))
    assert [d["digest_id"] for d in manual] == [d["digest_id"] for d in scheduled] == ["cur_mon", "cur_fri"]
