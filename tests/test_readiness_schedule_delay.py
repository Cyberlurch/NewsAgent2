"""Delayed scheduler events are distinct from failed or missing service."""
from datetime import date, datetime, timedelta
import pytest

from newsagent2 import delivery, edition_plan as ep, operational_readiness as op
from newsagent2.run_context import path_for
from newsagent2.store_io import atomic_store
from test_operational_readiness import sources, snapshot
from test_recovery_scope import result as receipt


def accepted(key, day='2026-10-16', cadence='daily'):
    start=day if cadence=='daily' else '2026-10-12' if key=='cybermed' else '2026-10-09'
    row=receipt(key, start=start, end=day, cadence=cadence, body='Saved original report.')
    row.update(status='accepted_by_smtp', state_persisted=True, smtp_phase='completed', accepted=['a'*64])
    return row


def seed(key, *extra, prior_day='2026-10-16'):
    sources()
    rows=[accepted(key, prior_day), accepted(key, cadence='weekly'), *extra]
    atomic_store(path_for('DELIVERY_JOURNAL_PATH'), {'version':1, 'editions':{r['edition_id']:r for r in rows}},
                 kind='delivery_journal',validate=delivery.journal_valid)


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('stamp',['2026-10-19T07:35:50+00:00','2026-10-19T09:11:45+00:00'])
def test_actual_monday_alarm_times_do_not_mark_delayed_senders_as_failed(key,stamp):
    seed(key); before=snapshot()
    report=op.assess(key,as_of=datetime.fromisoformat(stamp))
    row=report['checks'][0]
    assert report['ok'] and row['status']=='awaiting_scheduled_run'
    assert row['last_daily_due']=='2026-10-16' and row['last_persisted_daily_date']=='2026-10-16'
    assert row['delivery_deadline']==f'2026-10-19T11:{"40" if key=="cybermed" else "30"}:00+02:00'
    assert 'delivery_status' not in row and snapshot()==before


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_noon_check_still_fails_for_a_missing_daily(key):
    seed(key); before=snapshot()
    report=op.assess(key,as_of=datetime.fromisoformat('2026-10-19T10:00:00+00:00'))
    assert not report['ok'] and report['checks'][0]['last_daily_due']=='2026-10-19'
    assert report['checks'][0]['categories']==['operational_daily_publication_stale']
    assert snapshot()==before


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_delay_window_does_not_hide_a_missing_previous_business_day(key):
    seed(key,prior_day='2026-10-15')
    report=op.assess(key,as_of=datetime.fromisoformat('2026-10-19T07:35:50+00:00'))
    assert not report['ok'] and report['checks'][0]['status']=='recovery_required'
    assert report['checks'][0]['last_daily_due']=='2026-10-16'


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_missing_history_is_not_success_even_during_the_delay_window(key):
    sources()
    report=op.assess(key,as_of=datetime.fromisoformat('2026-10-19T07:35:50+00:00'))
    assert not report['ok']
    assert 'operational_daily_outcome_unestablished' in report['checks'][0]['categories']


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('status',['failed','partially_accepted','outcome_unknown','accepted_by_smtp'])
def test_known_current_failure_or_unpersisted_acceptance_is_immediate_even_before_deadline(key,status):
    row=receipt(key,start='2026-10-19',body='Saved report.')
    row['status']=status
    if status=='failed': row['generation_validated']=False
    if status=='partially_accepted': row.update(smtp_phase='completed',accepted=['a'*64],rejected=['b'*64])
    if status=='outcome_unknown': row.update(smtp_phase='handoff_started',unknown=['a'*64])
    if status=='accepted_by_smtp': row.update(smtp_phase='completed',accepted=['a'*64])
    seed(key,row); before=snapshot()
    report=op.assess(key,as_of=datetime.fromisoformat('2026-10-19T07:35:50+00:00'))
    assert not report['ok'] and report['checks'][0]['status'] in {'blocked','recovery_required'}
    assert snapshot()==before


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_current_success_is_already_accepted_before_deadline_not_merely_awaiting(key):
    seed(key,accepted(key,'2026-10-19'))
    report=op.assess(key,as_of=datetime.fromisoformat('2026-10-19T09:25:00+00:00'))
    assert report['ok'] and report['checks'][0]['status']=='already_accepted'


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('cadence',ep.CADENCES)
def test_all_cadences_use_the_same_bounded_deadline_and_native_calendar(key,cadence):
    day=date.fromisoformat({'daily':'2026-10-19','weekly':'2026-10-23',
        'monthly':'2026-11-06' if key=='cybermed' else '2026-11-01',
        'yearly':'2027-01-04' if key=='cybermed' else '2027-01-01'}[cadence])
    deadline=op.delivery_deadline(key,day)
    before=op.last_due_entry(key,cadence,deadline-timedelta(seconds=1))
    expected=ep.entry_for(key,cadence,day,'schedule','real')
    assert before is None or before['edition_id']!=expected['edition_id']
    assert op.last_due_entry(key,cadence,deadline)['edition_id']==expected['edition_id']


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_deadline_tracks_stockholm_winter_time_and_holiday_business_days(key):
    day=date(2026,10,26)
    deadline=op.delivery_deadline(key,day)
    assert deadline.utcoffset()==timedelta(hours=1)
    assert op._last_due_daily(key,deadline-timedelta(seconds=1))=='2026-10-23'
    assert op._last_due_daily(key,deadline)=='2026-10-26'
    holiday=datetime.fromisoformat('2026-12-25T14:00:00+01:00')
    if key=='cybermed': assert op._last_due_daily(key,holiday)=='2026-12-23'
