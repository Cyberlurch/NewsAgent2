import json
import sys
from datetime import datetime, timezone

from newsagent2 import main


def test_top30_diagnostics_fields_present(tmp_path, monkeypatch):
    report_dir = tmp_path / 'out'
    monkeypatch.setenv('REPORT_KEY', 'cybermed')
    monkeypatch.setenv('REPORT_MODE', 'daily')
    monkeypatch.setenv('REPORT_DIR', str(report_dir))
    monkeypatch.setenv('STATE_PATH', str(tmp_path / 'state.json'))
    (tmp_path / 'state.json').write_text('{}', encoding='utf-8')
    monkeypatch.setenv('SEND_EMAIL', '0')
    monkeypatch.setenv('EMAIL_MODE', 'none')
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'workflow_dispatch')
    monkeypatch.setenv('FOAMED_AUDIT', '1')
    monkeypatch.setattr(sys, 'argv', ['newsagent2-main'])
    monkeypatch.setattr(main, 'load_channels_config', lambda _p: ([], {}, {}))
    monkeypatch.setattr(main, 'search_recent_pubmed', lambda *a, **k: [])
    cfg = [{'name': f'S{i}', 'homepage': 'https://x', 'feed_url': 'https://x/feed', 'domain_group': 'critical_care', 'priority_tier': '1 core', 'extraction_strategy': 'rss_then_article'} for i in range(30)]
    monkeypatch.setattr(main, 'load_foamed_sources_config', lambda _p: cfg)
    summary = [{
        'name': f'S{i}', 'source_status': 'usable_fulltext' if i < 20 else 'no_recent_content', 'extraction_strategy': 'rss_then_article',
        'domain_group': 'critical_care', 'priority_tier': '1 core', 'article_fetch_attempted': 0, 'article_fetch_success': 0,
        'article_fetch_improved_text': 0, 'discovery_content_mode': 'rss_excerpt', 'final_content_source': 'rss_excerpt',
        'extraction_method_counts': {}, 'candidates_found': 0, 'kept_in_window_count': 0, 'feed_status_code': 200,
        'homepage_status_code': 200, 'wp_rest_available': False, 'sitemap_available': False, 'notes_diagnostic': ''
    } for i in range(30)]
    monkeypatch.setattr(main, 'collect_foamed_items', lambda *a, **k: ([], {'sources_total': 30, 'per_source': {}, 'audit': {'enabled': True, 'sources': {}}, 'foamed_source_strategy_summary': summary}))
    main.main()
    diag = json.loads((report_dir / 'cybermed_daily_diagnostics.json').read_text(encoding='utf-8'))
    assert diag['foamed_sources_config_total'] == 30
    assert len(diag['foamed_source_strategy_summary']) == 30
    for key in ['foamed_top30_core_sources_total','foamed_top30_core_sources_usable_total','foamed_top30_sources_fulltext_capable_total','foamed_top30_sources_excerpt_only_total','foamed_top30_sources_blocked_total','foamed_top30_sources_broken_total','foamed_top30_sources_audit_only_total','foamed_top30_sources_no_recent_content_total','foamed_top30_domain_group_counts','foamed_top30_priority_tier_counts']:
        assert key in diag
    blob = json.dumps(diag)
    for banned in ['raw_html', 'full_text', 'SMTP_PASS', 'OPENAI_API_KEY', 'RECIPIENTS_CONFIG_JSON', '@']:
        assert banned not in blob
