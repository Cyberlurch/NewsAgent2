import json
from pathlib import Path


def _load():
    return json.loads(Path('data/cybermed_foamed_sources.json').read_text(encoding='utf-8'))


def test_top30_source_config_shape_and_count():
    cfg = _load()
    assert len(cfg) == 30
    for src in cfg:
        assert src.get('name')
        assert src.get('homepage') or src.get('feed_url')
        assert src.get('domain_group') in {'critical_care','emergency_medicine','anesthesiology','pediatric_emergency','mixed'}
        assert src.get('priority_tier') in {'1 core','2 important','3 audit_optional'}
        assert src.get('extraction_strategy') in {'rss_then_article','html_only','rss_only','audit_only','disabled'}


def test_journalfeed_and_rebel_and_aliem_profiles():
    by_name = {s['name']: s for s in _load()}
    jf = by_name['JournalFeed (Critical Care)']
    assert jf['extraction_strategy'] == 'html_only'
    assert jf['article_fetch_required'] is True
    rebel = by_name['REBEL EM']
    assert rebel['extraction_strategy'] in {'rss_then_article','html_only'}
    assert 'legacy' in rebel.get('notes_diagnostic','').lower()
    aliem = by_name['ALiEM']
    assert aliem['extraction_strategy'] == 'audit_only'
