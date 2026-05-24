import json
import sys

from newsagent2 import main
from newsagent2.main import _foamed_72h_text_diagnostics, _pubmed_content_backfill_and_diagnostics


def test_pubmed_coverage_with_abstracts_and_backfill_reason():
    items = [{"abstract": "A"*300, "doi": "10.1/a"} for _ in range(3)]
    d = _pubmed_content_backfill_and_diagnostics(items)
    assert d["pubmed_items_with_abstract_or_oa_fulltext_total"] == 3
    assert d["pubmed_items_metadata_only_total"] == 0
    assert d["pubmed_post_state_content_coverage_pct"] == 100.0
    assert d["pubmed_content_backfill_attempted_total"] == 0
    assert d["pubmed_content_backfill_not_attempted_reason"] == "all_post_state_items_already_have_usable_content"
    assert d["pubmed_content_source_counts"]["pubmed_abstract"] == 3
    assert d["pubmed_content_retrieval_method_counts"]["efetch_xml"] == 3


def test_pubmed_coverage_threshold_and_metadata_only_mix():
    items = [
        {"abstract": "A" * 280, "doi": "10.1/a"},
        {"full_text_excerpt": "F" * 1300, "fulltext_source": "pmc:123", "pmcid": "PMC1"},
        {"title": "metadata only"},
        {"title": "metadata only 2"},
    ]
    d = _pubmed_content_backfill_and_diagnostics(items)
    assert d["pubmed_items_with_abstract_or_oa_fulltext_total"] == 2
    assert d["pubmed_items_metadata_only_total"] == 2
    assert d["pubmed_post_state_content_coverage_pct"] == 50.0


def test_foamed_pct_scaling_and_usable_text_threshold():
    items = [{"final_content_source": "article_full_text", "text": "x"*600} for _ in range(5)] + [{"final_content_source": "article_excerpt", "text": "y"*300} for _ in range(5)]
    d = _foamed_72h_text_diagnostics(items, min_chars=400)
    assert d["foamed_72h_items_total"] == 10
    assert d["foamed_72h_article_fulltext_total"] == 5
    assert d["foamed_72h_article_excerpt_total"] == 5
    assert d["foamed_72h_article_fulltext_pct"] == 50.0
    assert d["foamed_72h_usable_text_pct"] == 50.0
    assert d["foamed_72h_text_length_median"] > 0


def test_rolling_audit_fields_present(tmp_path, monkeypatch):
    report_dir = tmp_path / 'out'
    monkeypatch.setenv('REPORT_KEY', 'cybermed'); monkeypatch.setenv('REPORT_MODE', 'daily'); monkeypatch.setenv('REPORT_DIR', str(report_dir))
    monkeypatch.setenv('STATE_PATH', str(tmp_path / 'state.json')); (tmp_path / 'state.json').write_text('{}', encoding='utf-8')
    monkeypatch.setenv('SEND_EMAIL', '0'); monkeypatch.setenv('EMAIL_MODE', 'none'); monkeypatch.setenv('GITHUB_EVENT_NAME', 'workflow_dispatch')
    monkeypatch.setenv('FOAMED_ROLLING_AUDIT_DAYS', '30')
    monkeypatch.setenv('CYBERMED_HEAVY_AUDIT_MODE', '1')
    monkeypatch.setattr(sys, 'argv', ['newsagent2-main'])
    monkeypatch.setattr(main, 'load_channels_config', lambda _p: ([], {}, {}))
    monkeypatch.setattr(main, 'search_recent_pubmed', lambda *a, **k: [])
    cfg = [{'name': 'S1', 'homepage': 'https://x', 'feed_url': 'https://x/feed', 'domain_group': 'critical_care', 'priority_tier': '1 core', 'extraction_strategy': 'rss_then_article'}]
    monkeypatch.setattr(main, 'load_foamed_sources_config', lambda _p: cfg)

    def fake_collect(*a, **k):
        return ([{"foamed_source": "S1", "final_content_source": "article_full_text", "text": "z"*700}], {"sources_total": 1, "per_source": {}, "audit": {"enabled": True, "sources": {}}, "foamed_source_strategy_summary": [{"name": "S1", "source_status": "usable_fulltext", "domain_group": "critical_care", "priority_tier": "1 core", "extraction_strategy": "rss_then_article", "article_fetch_success": 1}]})
    monkeypatch.setattr(main, 'collect_foamed_items', fake_collect)
    main.main()
    diag = json.loads((report_dir / 'cybermed_daily_diagnostics.json').read_text())
    assert diag['foamed_rolling_audit_enabled'] is True
    assert diag['foamed_rolling_audit_days'] == 30
    assert diag['foamed_rolling_productive_sources_total'] >= 1


def test_rolling_audit_gated_when_heavy_mode_disabled(tmp_path, monkeypatch):
    report_dir = tmp_path / 'out'
    monkeypatch.setenv('REPORT_KEY', 'cybermed'); monkeypatch.setenv('REPORT_MODE', 'daily'); monkeypatch.setenv('REPORT_DIR', str(report_dir))
    monkeypatch.setenv('STATE_PATH', str(tmp_path / 'state.json')); (tmp_path / 'state.json').write_text('{}', encoding='utf-8')
    monkeypatch.setenv('SEND_EMAIL', '0'); monkeypatch.setenv('EMAIL_MODE', 'none'); monkeypatch.setenv('GITHUB_EVENT_NAME', 'workflow_dispatch')
    monkeypatch.setenv('FOAMED_ROLLING_AUDIT_DAYS', '30')
    monkeypatch.setenv('CYBERMED_HEAVY_AUDIT_MODE', '0')
    monkeypatch.setattr(sys, 'argv', ['newsagent2-main'])
    monkeypatch.setattr(main, 'load_channels_config', lambda _p: ([], {}, {}))
    monkeypatch.setattr(main, 'search_recent_pubmed', lambda *a, **k: [])
    cfg = [{'name': 'S1', 'homepage': 'https://x', 'feed_url': 'https://x/feed'}]
    monkeypatch.setattr(main, 'load_foamed_sources_config', lambda _p: cfg)
    calls = {"n": 0}
    def fake_collect(*a, **k):
        calls["n"] += 1
        return ([], {"sources_total": 1, "per_source": {}, "audit": {"enabled": True, "sources": {}}, "foamed_source_strategy_summary": []})
    monkeypatch.setattr(main, 'collect_foamed_items', fake_collect)
    main.main()
    diag = json.loads((report_dir / 'cybermed_daily_diagnostics.json').read_text())
    assert calls["n"] == 1
    assert diag['foamed_rolling_audit_requested_days'] == 30
    assert diag['foamed_rolling_audit_enabled'] is False
    assert diag['foamed_rolling_audit_skipped_reason'] == 'heavy_audit_mode_disabled'
    readiness = diag["cybermed_readiness"]
    assert readiness["foamed_ready_for_coverage"] == "not_evaluated"
    assert "foamed_rolling_productive_sources_below_8" not in readiness["blocking_reasons"]
    assert diag["foamed_ready_for_coverage_not_evaluated_reason"] == "heavy_audit_mode_disabled"
