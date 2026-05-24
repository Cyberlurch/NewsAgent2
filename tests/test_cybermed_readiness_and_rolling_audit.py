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


def test_foamed_pct_scaling_and_usable_text_threshold():
    items = [{"final_content_source": "article_full_text", "text": "x"*600} for _ in range(5)] + [{"final_content_source": "article_excerpt", "text": "y"*300} for _ in range(5)]
    d = _foamed_72h_text_diagnostics(items, min_chars=400)
    assert d["foamed_72h_article_fulltext_pct"] == 50.0
    assert d["foamed_72h_usable_text_pct"] == 50.0
    assert "foamed_72h_text_length_median" in d


def test_rolling_audit_fields_present(tmp_path, monkeypatch):
    report_dir = tmp_path / 'out'
    monkeypatch.setenv('REPORT_KEY', 'cybermed'); monkeypatch.setenv('REPORT_MODE', 'daily'); monkeypatch.setenv('REPORT_DIR', str(report_dir))
    monkeypatch.setenv('STATE_PATH', str(tmp_path / 'state.json')); (tmp_path / 'state.json').write_text('{}', encoding='utf-8')
    monkeypatch.setenv('SEND_EMAIL', '0'); monkeypatch.setenv('EMAIL_MODE', 'none'); monkeypatch.setenv('GITHUB_EVENT_NAME', 'workflow_dispatch')
    monkeypatch.setenv('FOAMED_ROLLING_AUDIT_DAYS', '30')
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
