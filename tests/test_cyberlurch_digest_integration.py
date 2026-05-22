import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
import json
from datetime import datetime, timezone
from newsagent2 import main


def test_digest_state_initializer_writes_minimal(tmp_path):
    p = tmp_path / 'state' / 'cyberlurch_digests.json'
    state = main._load_cyberlurch_digest_state(str(p))
    assert p.exists()
    assert state == {'version': 1, 'updated_at_utc': '', 'digests': []}


def test_rollup_items_preserve_safe_cyberlurch_fields():
    items = main._rollup_items_for_month(
        overview_items=[{
            'title':'t','url':'u','channel':'c','source':'youtube','published_at':datetime(2026,1,1,tzinfo=timezone.utc),
            'topic_primary':'malware','topics':['malware'],'text_source':'direct_digest','content_status':'ok','transcript_processing':'direct',
            'editorial_relevance':'high','transcript_full_summary':'x'*700,
        }],
        detail_items=[],
        foamed_overview_items=[],
    )
    it = items[0]
    assert it['topic_primary'] == 'malware'
    assert it['transcript_full_summary_short'] == 'x'*600


def test_digest_sanitize_no_full_text_payload():
    out = main._sanitize_cyberlurch_digest_record({'video_id':'v','_full_text_for_processing':'x','text':'raw transcript','title':'t'})
    assert '_full_text_for_processing' not in out
    assert 'text' not in out


def test_workflow_commits_digest_state_file():
    yml = open('.github/workflows/newsagent.yml', encoding='utf-8').read()
    assert 'Commit updated state files if changed' in yml
    assert 'state/cyberlurch_digests.json' in yml
