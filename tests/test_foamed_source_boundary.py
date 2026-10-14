"""Small synthetic fixtures; all HTTP and newsletter providers are mocked."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pytest
import requests

from newsagent2 import collector_foamed as collector, foamed_probe, main, state_manager, rollups
from newsagent2.run_context import PATHS, execution_context, path_for
from newsagent2.foamed_sources import ccr_items, load_sources
from newsagent2.cybermed_digest_store import aggregate_cybermed_digest_inputs
from newsagent2.paper_delivery import report_papers
from newsagent2.store_io import StoreError

NOW = datetime(2026, 10, 14, 6, tzinfo=timezone.utc)


def feed(day=None):
    date = f"<pubDate>{day.strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>" if day else ""
    return f'<rss version="2.0"><channel><title>Example</title><item><title>Learning</title><link>https://example.org/post</link>{date}</item></channel></rss>'.encode()


def config(**values):
    return {"name": "Example", "feed_url": "https://example.org/feed", "homepage": "",
            "extraction_strategy": "rss_only", **values}


@pytest.mark.parametrize("content,result,ok", [
    (b'<rss version="2.0"><channel><title>Empty</title></channel></rss>', "empty_feed", True),
    (b'<feed xmlns="http://www.w3.org/2005/Atom"><title>Empty</title></feed>', "empty_feed", True),
    (b'<html><body>Not RSS</body></html>', "html_instead_of_feed", False),
    (b'<rss><channel>broken', "parse_failed", False),
    (feed(NOW), "entries", True),
])
def test_feed_result_is_not_http_status(monkeypatch, content, result, ok):
    monkeypatch.setattr(collector, "_fetch_url", lambda *a, **kw: collector._FetchResult(
        True, 200, content, "https://example.org/current", None, "text/html", 1))
    _, stats = collector.collect_foamed_items([config()], NOW)
    row = stats["per_source"]["Example"]
    assert row["feed_parse_result"] == result
    assert row["feed_ok"] == ok
    assert row["feed_content_type"] == "text/html"
    assert row["feed_redirects"] == 1
    assert row["attempted"] is True


@pytest.mark.parametrize("day,expected,count", [(NOW-timedelta(days=20), "stale", 0), (None, "unknown", 0), (NOW, "current", 1)])
def test_freshness_is_separate_from_access(monkeypatch, day, expected, count):
    monkeypatch.setattr(collector, "_fetch_url", lambda *a, **kw: collector._FetchResult(True, 200, feed(day), "u", None))
    items, stats = collector.collect_foamed_items([config(expected_interval_days=7)], NOW)
    row = stats["per_source"]["Example"]
    assert len(items) == count
    assert row["freshness"] == expected
    assert row["health"] == "ok_rss"
    assert row["selected_items"] is None  # Collector cannot claim selection.


@pytest.mark.parametrize("strategy", ["disabled", "audit_only", "reference"])
def test_skipped_sources_do_not_request_or_change_health(monkeypatch, strategy):
    monkeypatch.setenv("FOAMED_AUDIT", "1")
    monkeypatch.setattr(collector, "_fetch_url", lambda *a, **k: pytest.fail("skipped HTTP"))
    items, stats = collector.collect_foamed_items([config(extraction_strategy=strategy, homepage="https://example.org", disabled=True)], NOW)
    state = {"foamed_source_health": {"Example": {"consecutive_failures": 3, "disabled_until_utc": "2030-01-01T00:00:00+00:00"}}}
    before = copy.deepcopy(state)
    update_health(state, stats["per_source"])
    assert state == before
    assert not items
    assert stats["sources_failed"] == stats["sources_ok"] == 0
    assert stats["per_source"]["Example"]["skipped_reason"] == strategy


def update_health(state, rows):
    return main._update_foamed_health_state(state, rows, NOW, auto_disable_enabled=True,
        disable_after_403=2, disable_days_403=7, disable_after_404=2, disable_days_404=30)


@pytest.mark.parametrize("status,health", [(403, "blocked_403"), (502, "other")])
def test_real_http_failure_is_not_editorial_inactivity(monkeypatch, status, health):
    monkeypatch.setattr(collector, "_fetch_url", lambda *a, **kw: collector._FetchResult(False, status, b"", "u", None))
    _, stats = collector.collect_foamed_items([config()], NOW)
    row = stats["per_source"]["Example"]
    assert row["health"] == health
    assert row["freshness"] == "unknown"
    state = {}
    update_health(state, stats["per_source"])
    assert state["foamed_source_health"]["Example"]["consecutive_failures"] == 1


@pytest.mark.parametrize("row", [
    {"health": "ok_rss", "probe_only": True}, {"health": "budget_exhausted"},
    {"health": "other", "attempted": False},
])
def test_incomplete_or_probe_does_not_clear_or_increment_ban(row):
    state = {"foamed_source_health": {"Example": {"consecutive_failures": 3, "disabled_until_utc": "2030-01-01T00:00:00+00:00"}}}
    before = copy.deepcopy(state)
    update_health(state, {"Example": row})
    assert state == before


def article(pmid="123", day="2026-10-14", extra=""):
    return f'<article><h2>Critical care learning</h2><time datetime="{day}">{day}</time><p>A source commentary on an endpoint, not a new trial.</p><a href="https://pubmed.ncbi.nlm.nih.gov/{pmid}/">Original</a>{extra}</article>'


def test_ccr_entry_scopes_date_text_identity_and_primary_reference():
    body = '<footer><time datetime="2026-10-15">Tomorrow</time></footer>' + article() + article("456", "2026-10-13")
    rows, diag = ccr_items(body.encode(), "https://example.org/potd", "CCR", NOW, NOW-timedelta(hours=24))
    assert len(rows) == 2 and rows[0]["id"] != rows[1]["id"]
    assert rows[0]["published_at"].date().isoformat() == "2026-10-14"
    assert "Tomorrow" not in rows[0]["text"] and "456" not in rows[0]["text"]
    assert rows[0]["primary_references"][0]["value"] == "123"
    assert rows[0]["source"] == "foamed" and rows[0]["source_role"] == "commentary"
    assert rows[0]["published_precision"] == "date"
    later, _ = ccr_items(body.encode(), "https://example.org/potd", "CCR", NOW+timedelta(hours=1), NOW-timedelta(hours=24))
    assert [x["id"] for x in later] == [x["id"] for x in rows]
    assert diag["newest_entry_datetime"].startswith("2026-10-14")


@pytest.mark.parametrize("body", [
    '<main><h2>Listing</h2><time datetime="2026-10-14"></time><a href="https://pubmed.ncbi.nlm.nih.gov/123/">Original</a></main>',
    article(extra='<time datetime="2026-10-13"></time>'),
    article(extra='<a href="https://pubmed.ncbi.nlm.nih.gov/456/">Other</a>'),
    article(day="October 14"), article(day="2026-10-15"),
    article().replace('https://pubmed.ncbi.nlm.nih.gov/123/', 'https://secondary.example/article'),
])
def test_ccr_ambiguous_or_unsupported_blocks_are_not_page_wide_studies(body):
    rows, diag = ccr_items(body.encode(), "https://example.org/potd", "CCR", NOW, NOW-timedelta(hours=24))
    assert rows == []
    assert diag["adapter_result"] == "unsupported_structure" or diag["rejected_entries"] > 0


def test_ccr_collector_never_falls_back_to_generic_listing(monkeypatch):
    monkeypatch.setattr(collector, "_fetch_url", lambda *a, **kw: collector._FetchResult(True, 200, article().encode(), "https://example.org/potd", None))
    items, stats = collector.collect_foamed_items([config(feed_url="", homepage="https://example.org/potd", extraction_strategy="html_only", listing_adapter="ccr_dated_articles_v1")], NOW)
    assert len(items) == 1
    assert items[0]["id"].startswith("foamed:ccr:")
    assert stats["per_source"]["Example"]["adapter_result"] == "recognized_structure"


def test_old_ccr_config_and_observed_canonical_alias_preserve_legacy_guard(monkeypatch):
    body = '<html><head><link rel="canonical" href="https://example.org/old-canonical"></head><body>' + article() + '</body></html>'
    monkeypatch.setattr(collector, '_fetch_url', lambda *a, **kw: collector._FetchResult(True, 200, body.encode(), 'https://example.org/final', None))
    items, _ = collector.collect_foamed_items([config(name='Critical Care Reviews', feed_url='', homepage='https://example.org/potd', extraction_strategy='html_only', listing_page_as_item=True)], NOW)
    assert len(items) == 1 and items[0]['id'].startswith('foamed:ccr:')
    state = {'reports': {}}
    state_manager.mark_processed(state, 'cybermed', 'foamed', 'https://example.org/old-canonical')
    assert main._foamed_processed_guard(state, 'cybermed', items[0])
    saved = main._sanitize_cybermed_foamed_item(items[0])
    assert 'https://example.org/potd' in saved['legacy_listing_ids']


def test_disabled_boolean_does_not_make_an_audit_request(monkeypatch):
    monkeypatch.setenv('FOAMED_AUDIT', '1')
    monkeypatch.setattr(collector, '_fetch_url', lambda *a, **kw: pytest.fail('disabled request'))
    items, stats = collector.collect_foamed_items([config(disabled=True)], NOW)
    assert items == [] and stats['per_source']['Example']['attempted'] is False


def test_commentary_provenance_survives_daily_weekly_monthly_without_paper_receipt():
    rows, _ = ccr_items(article().encode(), "https://example.org/potd", "CCR", NOW, NOW-timedelta(hours=24))
    row = main._sanitize_cybermed_foamed_item(rows[0])
    digest = {"run_date": "2026-10-14", "items": {"foamed": [row], "pubmed": []}}
    weekly = aggregate_cybermed_digest_inputs([digest], pubmed_cap=10, foamed_cap=10, deep_dive_cap=2)
    assert weekly["foamed"][0]["primary_references"] == row["primary_references"]
    monthly = main._sanitize_cybermed_monthly_rollup_item(weekly["foamed"][0])
    assert monthly["source_role"] == "commentary"
    assert monthly["primary_references"] == row["primary_references"]
    assert report_papers({"finalization": {"overview_items": rows, "detail_items": []}}) == []


def test_same_landing_page_keeps_separate_commentaries_in_aggregates_and_original():
    body = article() + article("456")
    rows, _ = ccr_items(body.encode(), "https://example.org/potd", "CCR", NOW, NOW-timedelta(hours=24))
    sanitized = [main._sanitize_cybermed_foamed_item(row) for row in rows]
    original = {"source_type": "pubmed", "item_id": "123", "pmid": "123", "title": "Critical care learning", "url": "https://pubmed.ncbi.nlm.nih.gov/123/"}
    weekly = aggregate_cybermed_digest_inputs([{"run_date": "2026-10-14", "items": {"pubmed": [original], "foamed": sanitized}}], pubmed_cap=10, foamed_cap=10, deep_dive_cap=2)
    assert len(weekly["foamed"]) == 2 and len(weekly["pubmed"]) == 1
    assert {x['item_id'] for x in weekly['foamed']} == {x['id'] for x in rows}
    from newsagent2.cybermed_digest_store import _match_keys
    assert set(_match_keys(sanitized[0])).isdisjoint(_match_keys(sanitized[1]))
    diagnostics = {}
    rollups.render_yearly_markdown(report_title='Cybermed', report_language='en', year=2026,
        rollups=[{'month': '2026-10', 'cybermed_items': [main._sanitize_cybermed_monthly_rollup_item(x) for x in weekly['foamed']]}], diagnostics=diagnostics)
    assert diagnostics['cybermed_yearly_duplicates_suppressed_total'] == 0


def test_new_ccr_identity_does_not_bypass_legacy_or_other_newsletter_state():
    rows, _ = ccr_items(article().encode(), 'https://example.org/potd', 'CCR', NOW, NOW-timedelta(hours=24))
    state = {'reports': {}}
    state_manager.mark_processed(state, 'cybermed', 'foamed', rows[0]['legacy_listing_id'])
    before = copy.deepcopy(state)
    assert main._foamed_processed_guard(state, 'cybermed', rows[0]) is True
    assert main._foamed_processed_guard(state, 'cyberlurch', rows[0]) is False
    assert state == before


@pytest.mark.parametrize('raw', [b'{bad', b'{}', b'[{"name":"Example","extraction_strategy":[]}]', b'[{"name":"A"},{"name":"A"}]'])
def test_existing_invalid_source_configuration_stops_without_rewriting(tmp_path, raw):
    path = tmp_path / 'sources.json'
    path.write_bytes(raw)
    with pytest.raises(StoreError):
        main.load_foamed_sources_config(str(path))
    assert path.read_bytes() == raw
    missing = tmp_path / 'missing' / 'sources.json'
    assert load_sources(missing) == [] and not missing.parent.exists()


@pytest.mark.parametrize("mode", ["none", "test"])
@pytest.mark.parametrize("report_key", ["cybermed", "cyberlurch"])
def test_health_pipeline_preserves_all_productive_paths(monkeypatch, tmp_path, mode, report_key):
    production = tmp_path / 'production'
    production.mkdir()
    for variable, filename in PATHS.items():
        target = production / filename
        target.write_text(json.dumps({"reports": {}, "foamed_source_health": {"Example": {"consecutive_failures": 2}}} if variable == 'STATE_PATH' else {"legacy": "unchanged"}))
        monkeypatch.setenv(variable, str(target))
    monkeypatch.setenv('REPORT_KEY', report_key)
    monkeypatch.setenv('EMAIL_MODE', mode)
    before = file_hashes(production)
    monkeypatch.setattr(collector, '_fetch_url', lambda *a, **kw: collector._FetchResult(True, 200, feed(NOW), 'https://example.org', None))
    with execution_context():
        state = state_manager.load_state(str(path_for('STATE_PATH')))
        sources, _ = main._filter_disabled_foamed_sources([config()], state, NOW, auto_disable_enabled=True)
        _, stats = collector.collect_foamed_items(sources, NOW)
        update_health(state, stats['per_source'])
        state_manager.save_state(str(path_for('STATE_PATH')), state)
        assert state['foamed_source_health']['Example']['consecutive_failures'] == 0
    assert file_hashes(production) == before


@pytest.mark.parametrize('raw', [b'{"foamed_source_health":[]}', b'{"foamed_source_health":{"Bad":{"consecutive_failures":"bad"}}}'])
def test_invalid_health_store_stops_byte_identical(tmp_path, raw):
    state = tmp_path / 'processed.json'
    state.write_bytes(raw)
    with pytest.raises(StoreError, match='invalid_existing_store'):
        state_manager.load_state(str(state))
    assert state.read_bytes() == raw


def test_valid_legacy_health_fields_are_preserved_additively(tmp_path):
    old = {"reports": {}, "foamed_source_health": {"Example": {"consecutive_failures": 2, "operator_note": "keep", "last_health": "ok_rss"}}, "legacy": {"opaque": True}}
    state = tmp_path / 'processed.json'
    raw = json.dumps(old).encode()
    state.write_bytes(raw)
    loaded = state_manager.load_state(str(state))
    assert loaded['foamed_source_health'] == old['foamed_source_health']
    assert loaded['legacy'] == old['legacy'] and state.read_bytes() == raw


def test_good_feed_with_failed_article_fetch_keeps_dimensions_separate(monkeypatch):
    monkeypatch.setenv('FOAMED_ARTICLE_FETCH', '1')
    def mock_fetch(session, url, **kw):
        return collector._FetchResult(True, 200, feed(NOW), url, None) if url.endswith('/feed') else collector._FetchResult(False, 502, None, url, None)
    monkeypatch.setattr(collector, '_fetch_url', mock_fetch)
    items, stats = collector.collect_foamed_items([config(extraction_strategy='rss_then_article')], NOW)
    row = stats['per_source']['Example']
    assert row['health'] == 'ok_rss'
    assert row['article_fetch_failed'] == 1 and row['article_fetch_success'] == 0
    assert len(items) == 1


def local_files(tmp_path, strategy="audit_only"):
    cfg = tmp_path / "sources.json"
    cfg.write_text(json.dumps([config(extraction_strategy=strategy)]))
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"foamed_source_health": {"Example": {"consecutive_failures": 4}}}))
    return cfg, state


def file_hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file() and p.name != 'isolation-audit.log'}


def test_probe_default_is_local_and_missing_store_is_not_created(monkeypatch, tmp_path):
    cfg, state = local_files(tmp_path)
    before = file_hashes(tmp_path)
    monkeypatch.setattr(collector, "collect_foamed_items", lambda *a, **kw: pytest.fail("network collector"))
    out = foamed_probe.inspect_sources(["Example"], config=cfg, state_path=state)
    assert out["mode"] == "local_inspection"
    assert file_hashes(tmp_path) == before
    missing = tmp_path / "absent" / "state.json"
    out = foamed_probe.inspect_sources(["Example"], config=cfg, state_path=missing)
    assert out["state_status"] == "missing_new_store" and not missing.parent.exists()


@pytest.mark.parametrize("raw", [b'{broken', b'[]', b'{"foamed_source_health": []}'])
def test_probe_invalid_existing_store_is_byte_identical_and_stops_before_http(monkeypatch, tmp_path, raw):
    cfg, state = local_files(tmp_path)
    state.write_bytes(raw)
    monkeypatch.setattr(collector, "collect_foamed_items", lambda *a, **kw: pytest.fail("network collector"))
    with pytest.raises(StoreError):
        foamed_probe.inspect_sources(["Example"], config=cfg, state_path=state, probe=True)
    assert state.read_bytes() == raw


def response(url, status=200, body=b'', location=None):
    r = requests.Response()
    r.status_code, r.url, r._content = status, url, body
    r._content_consumed = True
    r.headers = {'Content-Type': 'application/rss+xml'}
    if location:
        r.headers['Location'] = location
    return r


@pytest.mark.parametrize("mode", ["none", "test"])
def test_explicit_probe_is_read_only_with_mocked_redirect_and_secret_headers(monkeypatch, tmp_path, mode):
    cfg, state = local_files(tmp_path)
    monkeypatch.setenv("EMAIL_MODE", mode)
    sources = json.loads(cfg.read_text())
    sources[0]["headers"] = {"Authorization": "SECRET-MARKER"}
    cfg.write_text(json.dumps(sources))
    before = file_hashes(tmp_path)
    calls = []
    def mock_get(self, url, **kw):
        assert kw['allow_redirects'] is False
        assert 'SECRET-MARKER' not in str(kw)
        calls.append(url)
        return response(url, 302, location='/current') if len(calls) == 1 else response(url, body=feed(NOW))
    monkeypatch.setattr(requests.Session, 'get', mock_get)
    out = foamed_probe.inspect_sources(["Example"], config=cfg, state_path=state, probe=True)
    assert out['requests_used'] == 2 and out['redirects_used'] == 1
    assert out['diagnostics']['Example']['probe_only'] is True
    assert file_hashes(tmp_path) == before
    assert 'SECRET-MARKER' not in str(out)


def test_probe_redirect_loop_stops_at_hard_request_budget(monkeypatch, tmp_path):
    cfg, state = local_files(tmp_path)
    calls = []
    def mock_get(self, url, **kw):
        calls.append(url)
        return response(url, 302, location='/loop')
    monkeypatch.setattr(requests.Session, 'get', mock_get)
    before = file_hashes(tmp_path)
    out = foamed_probe.inspect_sources(["Example"], config=cfg, state_path=state, probe=True, requests_max=2)
    assert len(calls) == out['requests_used'] == 2
    assert out['incomplete'] is True
    assert file_hashes(tmp_path) == before


def test_probe_wall_deadline_escapes_collector_without_statewrite(monkeypatch, tmp_path):
    cfg, state = local_files(tmp_path)
    before = file_hashes(tmp_path)
    def expire(*args, **kwargs):
        raise foamed_probe.ProbeDeadline()
    monkeypatch.setattr(collector, 'collect_foamed_items', expire)
    out = foamed_probe.inspect_sources(["Example"], config=cfg, state_path=state, probe=True)
    assert out['outcome'] == 'wall_deadline_exceeded'
    assert file_hashes(tmp_path) == before


def test_source_endpoint_and_reference_changes_keep_disabled_profiles():
    cfg = json.loads(Path('data/cybermed_foamed_sources.json').read_text())
    sources = {s['name']: s for s in cfg}
    assert sources['PulmCCM']['feed_url'] == 'https://www.pulmccm.org/feed'
    assert sources['The Resus Room']['feed_url'] == 'https://theresusroom.libsyn.com/rss'
    assert sources['OpenAnesthesia']['extraction_strategy'] == sources['Anesthesiology News']['extraction_strategy'] == 'reference'
    assert sources['ALiEM']['extraction_strategy'] == 'audit_only'
    state = {'foamed_source_health': {n: {'disabled_until_utc': '2030-01-01T00:00:00+00:00', 'consecutive_failures': 4}
             for n in ('LITFL', 'CanadiEM', 'PulmCCM')}}
    before = copy.deepcopy(state)
    selected, _ = main._filter_disabled_foamed_sources(cfg, state, NOW, auto_disable_enabled=True)
    assert not {'LITFL', 'CanadiEM', 'PulmCCM'} & {s['name'] for s in selected}
    assert state == before
