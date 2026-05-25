from datetime import date

from newsagent2.cybermed_digest_store import load_cybermed_daily_digest_store, select_cybermed_daily_digests_for_week, summarize_cybermed_weekly_digest_inputs


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
    selected = select_cybermed_daily_digests_for_week(store, date(2026,5,25))
    summary = summarize_cybermed_weekly_digest_inputs(selected)
    assert len(selected) == 2
    assert summary['pubmed_items_loaded_total'] == 1
    assert summary['foamed_items_loaded_total'] == 1
