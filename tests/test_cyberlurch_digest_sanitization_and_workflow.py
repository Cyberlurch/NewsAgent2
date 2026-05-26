from __future__ import annotations
import json
from pathlib import Path
from newsagent2 import main
from newsagent2 import rollups


def test_state_digest_file_has_no_synthetic_records():
    data = json.loads(Path('state/cyberlurch_digests.json').read_text(encoding='utf-8'))
    banned_channels = {'Test Channel', 'C'}
    banned_ids = {'fallback123','v1','id1','id2','ok1','m1','new1','c2','c3','c9'}
    banned_titles = {'Metadata Only Title','t','A','B','T','Meta','Newer title','Short','Long','FailDirect'}
    for d in data.get('digests', []):
        assert d.get('channel') not in banned_channels
        assert d.get('video_id') not in banned_ids
        assert d.get('title') not in banned_titles


def test_sanitize_digest_state_removes_synthetic_records():
    state = {'version':1,'updated_at_utc':'','digests':[{'video_id':'fallback123','url':'https://www.youtube.com/watch?v=fallback123','channel':'Test Channel','title':'Newer title','topics':['test']},{'video_id':'realx','url':'https://www.youtube.com/watch?v=realx','channel':'tagesschau','title':'Real','topics':['news']}]}
    out, removed = main.sanitize_cyberlurch_digest_state(state)
    assert removed == 1
    assert len(out['digests']) == 1
    assert out['digests'][0]['video_id'] == 'realx'


def test_workflow_dispatch_all_modes_and_overrides_present():
    yml = Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')
    assert '- all' in yml
    assert 'run_modes="daily weekly monthly yearly"' in yml
    assert 'which_report=$run_which_report' in yml
    assert 'Manual email mode' in yml


def test_yearly_cyberlurch_no_clinical_fallback_strings():
    md = rollups.render_yearly_markdown(report_title='The Cyberlurch Report', report_language='en', year=2026, rollups=[{'month':'2026-01','executive_summary':['x'], 'top_items':[{'title':'Title','url':'https://www.youtube.com/watch?v=x','channel':'tagesschau'}]}])
    assert 'Mixed clinical topics' not in md
    assert 'BOTTOM LINE: (not available in rollup; re-run monthly to capture)' not in md


def test_schedule_crons_and_gate_target_0430_stockholm_and_order():
    yml = Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')
    assert '30 2 * * *' in yml
    assert '30 3 * * *' in yml
    assert '30 4 * * *' not in yml
    assert 'Gate scheduled runs to 04:30 Europe/Stockholm' in yml
    assert 'expected_cron="30 2 * * *"' in yml
    assert 'expected_cron="30 3 * * *"' in yml
    assert '05:30' not in yml
    assert 'Scheduled both-run order: Cybermed first, Cyberlurch second.' in yml
    marker = 'Scheduled both-run order: Cybermed first, Cyberlurch second.'
    idx = yml.find(marker)
    assert idx != -1
    tail = yml[idx:]
    assert tail.find('REPORT_KEY=cybermed') < tail.find('REPORT_KEY=cyberlurch')
