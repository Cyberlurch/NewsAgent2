"""The same frozen dates and edition identities drive all eight cadences."""
from datetime import date, datetime, timedelta
import json
import sys

import pytest

from newsagent2 import delivery, edition_plan as ep, operational_readiness as op, run_timing as timing
from newsagent2.run_context import path_for
from newsagent2.store_io import atomic_store
from test_recovery_scope import result as receipt


@pytest.mark.parametrize('key,day,modes',[
    ('cybermed','2026-10-23',['daily','weekly','monthly']),
    ('cyberlurch','2026-10-23',['daily','weekly']),
    ('cybermed','2026-10-26',['daily','monthly']),
    ('cyberlurch','2026-10-26',['daily']),
    ('cyberlurch','2026-11-01',['monthly']),('cybermed','2026-11-01',[]),
    ('cybermed','2026-11-06',['daily','weekly','monthly']),
    ('cybermed','2026-12-23',['daily','weekly','monthly']),
    ('cybermed','2026-12-24',[]),('cybermed','2026-12-25',[]),
    ('cyberlurch','2027-01-01',['daily','weekly','monthly','yearly']),
    ('cybermed','2027-01-01',[]),('cybermed','2027-01-04',['daily','monthly','yearly'])])
def test_requested_calendar_cases_and_dependency_order_share_one_policy(key,day,modes):
    assert ep.planned_modes(key,date.fromisoformat(day))==modes
    assert modes==[c for c in ep.CADENCES if c in modes]


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('cadence',ep.CADENCES)
def test_finalized_scheduled_editions_are_removed_from_actual_plan_without_state_mutation(key,cadence,monkeypatch,tmp_path):
    day='2026-10-23' if cadence in {'daily','weekly'} else '2026-11-06' if key=='cybermed' and cadence=='monthly' else '2027-01-04' if key=='cybermed' else '2026-11-01' if cadence=='monthly' else '2027-01-01'
    entry=ep.entry_for(key,cadence,date.fromisoformat(day),'schedule','real')
    row=receipt(key,cadence=cadence,start=entry['period_start'],end=entry['period_end'],body='Saved source report.')
    row.update(status='accepted_by_smtp',state_persisted=True,smtp_phase='completed',accepted=['a'*64])
    atomic_store(path_for('DELIVERY_JOURNAL_PATH'),{'version':1,'editions':{row['edition_id']:row}},kind='journal',validate=delivery.journal_valid)
    before=path_for('DELIVERY_JOURNAL_PATH').read_bytes()
    monkeypatch.setenv('GITHUB_EVENT_NAME','schedule');monkeypatch.setenv('EMAIL_MODE','real')
    output=tmp_path/'plan.json';outputs=tmp_path/'outputs'
    monkeypatch.setenv('GITHUB_OUTPUT',str(outputs))
    monkeypatch.setattr(sys,'argv',['plan','--report-key',key,'--created-at',day+'T12:00:00Z',
        '--job-started-at',day+'T12:01:00Z','--output',str(output)])
    ep.main();plan=json.loads(output.read_text())
    assert row['edition_id'] in plan['already_finalized_editions']
    assert cadence not in [e['cadence'] for e in plan['entries']]
    assert ep.validate_plan(plan) and path_for('DELIVERY_JOURNAL_PATH').read_bytes()==before
    assert 'plan_proof='+ep.plan_proof(e['edition_id'] for e in plan['entries']) in outputs.read_text()


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('cadence',ep.CADENCES)
def test_frozen_plan_is_not_rebased_by_job_start_on_another_day_or_year(key,cadence,monkeypatch,tmp_path):
    day=date(2026,12,23) if key=='cybermed' else date(2027,1,1)
    created=datetime.combine(day,datetime.min.time(),ep.STO)+timedelta(hours=12)
    job=created+timedelta(days=10)
    plan=ep.make_plan(key,created_at=created.isoformat(),job_started_at=job.isoformat(),
        trigger='schedule',request=cadence,mode='none')
    output=tmp_path/'plan.json';atomic_store(output,plan,kind='plan',validate=ep.validate_plan)
    monkeypatch.setenv('NEWSAGENT_PLAN_PATH',str(output));monkeypatch.setenv('EMAIL_MODE','none')
    before=output.read_bytes()
    with ep.bind_entry(key,cadence) as entry:
        coordinator=delivery.Coordinator(key,cadence)
        assert coordinator.edition_id==entry['edition_id']
        assert entry['logical_date']==day.isoformat()
        assert entry['coverage_end']==ep.entry_for(key,cadence,day,'schedule','none')['coverage_end']
    assert output.read_bytes()==before


@pytest.mark.parametrize('key,created,seconds',[
    ('cybermed','2026-10-23T08:50:09+00:00',18609),
    ('cyberlurch','2026-10-23T08:42:23+00:00',18743)])
def test_observed_delay_before_job_is_distinct_from_generation_smtp_and_publication(key,created,seconds,monkeypatch,tmp_path,freeze_clock):
    stamp=datetime.fromisoformat(created)
    plan=ep.make_plan(key,created_at=created,job_started_at=(stamp+timedelta(minutes=1)).isoformat(),
        trigger='schedule',request='daily',mode='none')
    output=tmp_path/'plan.json';atomic_store(output,plan,kind='plan',validate=ep.validate_plan)
    monkeypatch.setenv('NEWSAGENT_PLAN_PATH',str(output));monkeypatch.setenv('EMAIL_MODE','none')
    fixed=freeze_clock((stamp+timedelta(minutes=2)).isoformat())
    monkeypatch.setattr(timing,'datetime',fixed)
    timing.record(key,'daily','application_started_at')
    path=path_for('STATE_PATH').parent/'out'/f'{key}_daily_timing_metadata.json'
    row=json.loads(path.read_text())
    assert row['latencies']['inferred_start_lateness_seconds']==seconds
    assert row['latencies']['run_to_job_seconds']==60 and row['latencies']['job_to_application_seconds']==60
    assert row['late_start_warning'] and row['smtp_accepted_at'] is None and row['git_publication_observed_at'] is None
    assert op.SCHEDULE_GRACE==timedelta(hours=8)
