import pathlib, sys
from datetime import datetime, timezone, timedelta
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))

from newsagent2.cyberlurch_cadence import cyberlurch_cadence_profile, classify_cyberlurch_item_temporality
from newsagent2.reporter import to_markdown
from newsagent2.cyberlurch_monthly import build_source_registry

def test_cadence_profile_modes():
    assert cyberlurch_cadence_profile('daily')['focus'] == 'new_items'
    assert cyberlurch_cadence_profile('weekly')['source_link_style'] == 'capped_top_videos'
    assert cyberlurch_cadence_profile('monthly')['source_link_style'] == 'representative_links_by_topic'
    assert cyberlurch_cadence_profile('yearly')['focus'] == 'annual_analysis'

def test_temporality_evergreen_for_apologetics():
    it = {'title':'Existence of God and Old Testament theology', 'channel':'WesHuff', 'topic_primary':'Christlicher Glaube, Bibel & Apologetik'}
    assert classify_cyberlurch_item_temporality(it) == 'evergreen'

def test_temporality_current_affairs_for_mainstream_news():
    it = {'title':'Breaking news today', 'channel':'tagesschau', 'topic_primary':'Mainstream DE/SE News'}
    assert classify_cyberlurch_item_temporality(it) == 'current_affairs'

def test_monthly_and_yearly_headings_present():
    now = datetime.now(timezone.utc)
    items = [{
        'id':'1','title':'Trend update','channel':'Channel','url':'https://x','published_at':now-timedelta(days=3),
        'topic_primary':'Geopolitik','content_status':'full_text','text_source':'managed_transcript','bottom_line':'x'
    }]
    registry = build_source_registry(items)
    synthesis = {'executive_summary':[{'synthesis':'Month.','source_refs':[registry[0]['ref_id']]}], 'trends':[], 'notable_developments':[{'heading':'Trend update','synthesis':'Channel recorded an isolated update.','source_refs':[registry[0]['ref_id']]}], 'month_in_brief':'One development was recorded.', 'source_refs_used':[registry[0]['ref_id']]}
    md_m = to_markdown(items, '', {'1':'detail'}, report_title='The Cyberlurch Report — Monthly', report_language='en', report_mode='monthly', monthly_synthesis=synthesis, monthly_source_registry=registry)
    assert 'Executive Summary' in md_m and 'Key Trends' in md_m and 'Month in Brief' in md_m and 'Sources' in md_m
    md_y = to_markdown(items, '## Executive Summary\n\nYear.', {'1':'detail'}, report_title='The Cyberlurch Report — Year in Review', report_language='en', report_mode='yearly')
    assert 'Key themes across the year' in md_y and 'Crisis trajectories' in md_y and 'Recurring narratives' in md_y and 'Evergreen highlights' in md_y
