import pathlib, sys
from datetime import datetime, timezone
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))

from newsagent2.cyberlurch_editorial import (
    infer_channel_tone_profile,is_deep_dive_eligible,score_cyberlurch_deep_dive_candidate,build_trend_clusters
)

def _item(**kw):
    base={"channel":"X","text":"A"*4000,"content_status":"full_text","text_source":"managed_transcript","published_at":datetime.now(timezone.utc),"transcript_processing":"direct_full_transcript","transcript_full_summary":"sum"}
    base.update(kw)
    return base

def test_mainstream_not_deepdive_by_default():
    assert not is_deep_dive_eligible(_item(channel='tagesschau'), {})

def test_metadata_only_never_deepdive():
    assert not is_deep_dive_eligible(_item(content_status='metadata_only', text_source='metadata_only', text=''), {})

def test_short_text_not_eligible():
    assert not is_deep_dive_eligible(_item(text='short', transcript_full_summary=''), {})

def test_managed_long_item_eligible():
    assert is_deep_dive_eligible(_item(channel='Random'), {})

def test_priority_channel_boost():
    a=_item(channel='CanadianPrepper')
    b=_item(channel='Other')
    sa=score_cyberlurch_deep_dive_candidate(a,[a,b],{}, {})['score']
    sb=score_cyberlurch_deep_dive_candidate(b,[a,b],{}, {})['score']
    assert sa>sb

def test_trend_boost_marks_items():
    a=_item(title='NATO Russia energy crisis')
    b=_item(title='Russia NATO sanctions and energy')
    d=build_trend_clusters([a,b])
    assert d['trend_boosted_items_total'] >= 2

def test_tone_profiles():
    assert infer_channel_tone_profile('WesHuff', {})=='christian_apologetics'
    assert infer_channel_tone_profile('DoomDebates', {})=='fringe_absurd'
