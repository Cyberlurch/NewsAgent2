"""Actual main/generation/render/journal/finalization paths, no external calls.

Daily includes the legitimate empty-report route; saved-source rollups contain
substance. Existing collector and medical-facts tests cover source ingestion.
"""
import json
import os
from pathlib import Path
import sys

import pytest

from newsagent2 import main, emailer, edition_plan as ep, delivery
from newsagent2.cybermed_digest_store import make_cybermed_weekly_digest
from newsagent2.run_context import PATHS, path_for
from newsagent2.store_io import atomic_store
from test_delivery_boundary import smtp  # Existing strict SMTP mock fixture.
from test_cyberlurch_mode_artifacts_and_digest_primary import _common


def seed(monkeypatch, key, cadence):
    days = ['2026-10-05', '2026-10-06', '2026-10-07', '2026-10-08', '2026-10-09', '2026-10-12', '2026-10-13']
    daily = []
    for day in days:
        item = {'item_id': 'foam-'+day, 'source_type': 'foamed', 'source': 'foamed',
            'foamed_source': 'Fixture commentary', 'title': 'Ventilation review '+day,
            'url': 'https://example.test/ventilation-'+day, 'published_at': day+'T01:00:00Z',
            'bottom_line': 'A saved clinical commentary discusses ventilation adjustments for adults in intensive care.'}
        daily.append({'digest_id': 'cybermed_daily_'+day, 'report_key': 'cybermed', 'cadence': 'daily',
            'run_date': day, 'generated_at_utc': day+'T02:00:00Z',
            'items': {'pubmed': [], 'foamed': [item]}, 'deep_dives': [], 'top_picks': []})
    week = make_cybermed_weekly_digest(period_start='2026-10-05', period_end='2026-10-09',
        generated_at_utc='2026-10-09T04:00:00Z', source_digests=daily[:5], pubmed_items=[],
        foamed_items=[d['items']['foamed'][0] for d in daily[:5]], deep_dives=[], top_picks=[])
    path_for('CYBERMED_DAILY_DIGEST_STATE_PATH').write_text(json.dumps({'digests': daily}))
    path_for('CYBERMED_WEEKLY_DIGEST_STATE_PATH').write_text(json.dumps({'digests': [week]}))
    videos = [{'video_id': 'video-'+str(i), 'title': 'Persisted concrete development '+str(i),
        'channel': 'tagesschau', 'url': 'https://www.youtube.com/watch?v=fixture'+str(i),
        'published_at': day+'T01:00:00Z', 'text_source': 'managed_transcript',
        'bottom_line': 'The report describes a concrete development and its documented economic consequences.',
        'transcript_full_summary': 'A persisted source describes the development and the resulting change in operating costs.',
        'editorial_relevance': 'high', 'temporality': 'current_affairs', 'topic_primary': 'economy', 'topics': ['economy']}
        for i, day in enumerate(['2026-10-08', '2026-10-12', '2026-10-13'])]
    path_for('CYBERLURCH_DIGEST_STATE_PATH').write_text(json.dumps({'digests': videos}))
    month = {'month': '2026-09', 'full_text_count': 1, 'top_items': [{
        'title': 'Persisted clinical commentary', 'url': 'https://example.test/clinical',
        'source': 'foamed', 'bottom_line': 'The saved commentary discusses ventilation practice.'}]}
    video_month = {'month': '2026-09', 'full_text_count': 1, 'representative_items': [{
        'video_id': 'sept', 'channel': 'tagesschau', 'date': '2026-09-10',
        'title': 'Persisted development', 'url': 'https://example.test/sept',
        'bottom_line': 'A persisted report describes a concrete change in operating costs.'}]}
    path_for('ROLLUPS_STATE_PATH').write_text(json.dumps({'reports': {'cybermed': [month], 'cyberlurch': [video_month]}}))
    monkeypatch.setattr(main, 'load_channels_config', lambda *a: ([], {}, {}))
    monkeypatch.setattr(main, 'load_foamed_sources_config', lambda *a: [])
    monkeypatch.setattr(main, 'collect_foamed_items', lambda *a, **k: ([], {}))
    monkeypatch.setattr(main, 'summarize', lambda *a, **k: '## Executive Summary\n\nPersisted developments and their concrete consequences.')
    monkeypatch.setattr(main, 'summarize_item_detail', lambda *a, **k: 'Saved source detail.')
    def annual(system, user):
        rows = json.loads(user.split('Authoritative source metadata (JSON):\n', 1)[1].split('\nYour prior', 1)[0])
        refs = [r['ref_id'] for r in rows]
        return json.dumps({'executive_summary': [{'heading': 'Saved developments',
            'synthesis': 'A persisted report documents a concrete change in operating costs.', 'source_refs': refs[:1]}],
            'annual_trends': [], 'turning_points': [], 'notable_developments': [],
            'timeline': [{'period': 'September', 'synthesis': 'The saved September report documents the development.', 'source_refs': refs[:1]}],
            'year_in_brief': 'The available report documents the change described above.', 'source_refs_used': refs[:1]})
    monkeypatch.setattr(main, 'synthesize_cyberlurch_yearly_json', annual)


@pytest.mark.parametrize('key', ['cybermed', 'cyberlurch'])
@pytest.mark.parametrize('cadence', ep.CADENCES)
@pytest.mark.parametrize('mode', ['real', 'none', 'test'])
def test_all_eight_real_application_paths_and_nonproduction_isolation(key, cadence, mode, smtp, monkeypatch, freeze_clock, tmp_path):
    cls, calls, _ = smtp
    _common(monkeypatch, tmp_path, cadence)
    for name, filename in PATHS.items():
        monkeypatch.setenv(name, str(tmp_path/filename))
    for name in ('DELIVERY_PERIOD_START', 'DELIVERY_PERIOD_END'):
        monkeypatch.delenv(name, raising=False)
    day = {'daily': '2026-10-16', 'weekly': '2026-10-16', 'monthly': '2026-11-06', 'yearly': '2027-01-15'}[cadence]
    freeze_clock(day+'T12:00:00+00:00')
    seed(monkeypatch, key, cadence)
    for name, value in {'REPORT_KEY': key, 'REPORT_PROFILE': 'medical' if key == 'cybermed' else 'general',
        'REPORT_MODE': cadence, 'EMAIL_MODE': mode, 'SEND_EMAIL': '0' if mode == 'none' else '1',
        'SEND_EMPTY_REPORT_EMAIL': '1', 'ROLLUP_MONTH_OVERRIDE': '2026-10', 'YEAR_IN_REVIEW_YEAR': '2026',
        'RECIPIENTS_CONFIG_JSON': '["production-dummy@example.test"]',
        'TEST_RECIPIENTS_CONFIG_JSON': '["test-only@example.test"]'}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(main, 'send_markdown', emailer.send_markdown)
    monkeypatch.setattr(sys, 'argv', ['main'])
    plan = ep.make_plan(key, created_at=day+'T12:00:00Z', job_started_at=day+'T12:01:00Z',
        trigger='workflow_dispatch', request=cadence, mode=mode, month_override='2026-10', year_override='2026')
    plan_path = tmp_path/'plan.json'
    atomic_store(plan_path, plan, kind='edition_plan', validate=ep.validate_plan)
    monkeypatch.setenv('NEWSAGENT_PLAN_PATH', str(plan_path))
    before = {name: path_for(name).read_bytes() if path_for(name).exists() else None for name in PATHS}
    main.main()
    delivery.verify_reports(Path(os.environ['REPORT_DIR']), key, [cadence], mode=mode, send_required=mode != 'none')
    result = json.loads((Path(os.environ['REPORT_DIR'])/f'{key}_{cadence}_delivery_result.json').read_text())
    assert result['status'] == ('intentionally_skipped' if mode == 'none' else 'accepted_by_smtp')
    if mode == 'real':
        assert result['state_persisted'] and len(calls) == 1
        first_id = result['message_id']
        main.main()
        assert len(calls) == 1  # No collection, generation or SMTP replay.
        again = delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][result['edition_id']]
        assert again['message_id'] == first_id
    else:
        assert before == {name: path_for(name).read_bytes() if path_for(name).exists() else None for name in PATHS}
        assert len(calls) == (1 if mode == 'test' else 0)
        if calls:
            assert calls[0][0] == ['test-only@example.test']
