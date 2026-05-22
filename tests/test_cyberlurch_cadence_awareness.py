import pathlib, sys
from datetime import datetime, timezone, timedelta
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))

from newsagent2.main import cyberlurch_cadence_profile
from newsagent2.cyberlurch_editorial import classify_cyberlurch_item_temporality
from newsagent2.reporter import to_markdown

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
    md_m = to_markdown(items, '## Executive Summary\n\nMonth.', {'1':'detail'}, report_title='The Cyberlurch Report — Monthly', report_language='en', report_mode='monthly')
    assert 'Monthly trend map' in md_m and 'Topic streams' in md_m and 'Evergreen / long-shelf-life items' in md_m
    md_y = to_markdown(items, '## Executive Summary\n\nYear.', {'1':'detail'}, report_title='The Cyberlurch Report — Year in Review', report_language='en', report_mode='yearly')
    assert 'Key themes across the year' in md_y and 'Crisis trajectories' in md_y and 'Recurring narratives' in md_y and 'Evergreen highlights' in md_y
