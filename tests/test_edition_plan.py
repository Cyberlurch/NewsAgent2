from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from newsagent2 import edition_plan as ep, delivery, main, workflow_delivery
from newsagent2.store_io import atomic_store


def plan_env(monkeypatch,tmp_path,key='cybermed',day='2026-10-30',request='scheduled',mode='real',**kw):
    created=kw.pop('created_at',day+'T12:00:00Z')
    plan=ep.make_plan(key,created_at=created,job_started_at=day+'T23:59:00Z',trigger=kw.pop('trigger','schedule'),
                     request=request,mode=mode,run_id='local',**kw)
    path=tmp_path/'plan.json';atomic_store(path,plan,kind='test_plan',validate=ep.validate_plan)
    monkeypatch.setenv('NEWSAGENT_PLAN_PATH',str(path));monkeypatch.setenv('EMAIL_MODE',mode)
    monkeypatch.setenv('REPORT_KEY',key);monkeypatch.setenv('REPORT_MODE',plan['entries'][0]['cadence'] if plan['entries'] else 'daily')
    monkeypatch.setenv('GITHUB_EVENT_NAME',plan['trigger'])
    return plan


@pytest.mark.parametrize('day,weekly,monthly',[
 ('2026-06-18',True,True),('2026-06-19',False,False),
 ('2026-12-23',True,True),('2026-12-24',False,False),('2026-12-25',False,False),
 ('2026-12-30',True,True),('2026-12-31',False,False),
 ('2026-11-02',False,False),('2026-05-01',False,False),('2026-05-04',False,True),
 ('2027-01-01',False,False),('2027-01-04',False,True)])
def test_new_cybermed_policy_handles_multiple_holidays_and_bounded_closes(day,weekly,monthly):
    modes=ep.planned_modes('cybermed',date.fromisoformat(day))
    assert ('weekly' in modes)==weekly and ('monthly' in modes)==monthly
    if modes: assert modes[0]=='daily'
    if day=='2027-01-04': assert modes==['daily','monthly','yearly']


@pytest.mark.parametrize('day,modes',[
 ('2026-06-19',['daily','weekly']),('2026-06-20',[]),('2026-11-01',['monthly']),
 ('2027-01-01',['monthly','yearly','daily','weekly'])])
def test_cyberlurch_actual_workflow_policy_is_independent(day,modes):
    assert ep.planned_modes('cyberlurch',date.fromisoformat(day))==modes


@pytest.mark.parametrize('day,offset', [('2026-03-27','+01:00'),('2026-03-29','+02:00'),
 ('2026-10-23','+02:00'),('2026-10-25','+01:00'),('2026-10-30','+01:00')])
def test_stockholm_slots_have_real_dst_offset(day,offset):
    plan=ep.make_plan('cyberlurch',created_at=day+'T12:00:00Z',job_started_at=day+'T13:00:00Z',trigger='schedule')
    assert plan['expected_slot']==day+'T05:30:00'+offset
    assert plan['slot_source']=='inferred_run_creation_date' and plan['slot_certainty']=='inferred'


def test_native_created_before_local_slot_is_ambiguous_not_successful_noop():
    with pytest.raises(ValueError,match='ambiguous'):
        ep.make_plan('cybermed',created_at='2026-10-31T00:00:00Z',job_started_at='2026-10-31T07:00:00Z',trigger='schedule')


def test_logical_week_is_friday_identity_with_wednesday_coverage(monkeypatch,tmp_path):
    plan=plan_env(monkeypatch,tmp_path,day='2026-12-23')
    entry=next(e for e in plan['entries'] if e['cadence']=='weekly')
    assert (entry['period_start'],entry['period_end'],entry['coverage_end'])==('2026-12-21','2026-12-25','2026-12-23')
    with ep.bind_entry('cybermed','weekly'):
        coordinator=delivery.Coordinator('cybermed','weekly')
        assert coordinator.edition_id=='cybermed:weekly:2026-12-21:2026-12-25:production'
        from newsagent2.cybermed_digest_store import cybermed_weekly_reporting_period
        assert cybermed_weekly_reporting_period(date(2026,12,23))==(date(2026,12,21),date(2026,12,25))
    assert os.getenv('DELIVERY_PERIOD_START') is None


def test_plan_cannot_inject_private_fields_or_inconsistent_periods(monkeypatch,tmp_path):
    plan=plan_env(monkeypatch,tmp_path)
    plan['credentials']='private';assert not ep.validate_plan(plan);del plan['credentials']
    plan['entries'][0]['period_end']='2026-11-30';assert not ep.validate_plan(plan)


def test_plan_cli_to_main_and_verifier_keeps_edition_across_midnight(monkeypatch,tmp_path,freeze_clock):
    key='cybermed';path=tmp_path/'plan.json';output=tmp_path/'outputs'
    monkeypatch.setenv('NEWSAGENT_PLAN_PATH',str(path));monkeypatch.setenv('GITHUB_OUTPUT',str(output))
    monkeypatch.setenv('REPORT_MODE','monthly');monkeypatch.setenv('EMAIL_MODE','none');monkeypatch.setenv('GITHUB_EVENT_NAME','workflow_dispatch')
    monkeypatch.setenv('ROLLUP_MONTH_OVERRIDE','2026-09')
    # Actual public plan CLI in a guarded Python child; no live metadata fetch.
    result=subprocess.run([sys.executable,'-m','newsagent2.edition_plan','--report-key',key,
        '--created-at','2026-09-30T22:30:00Z','--job-started-at','2026-10-01T23:00:00Z'],capture_output=True,text=True)
    assert result.returncode==0,result.stderr
    plan=ep.load_plan();assert plan['logical_date']=='2026-10-01' # Stockholm differs from UTC.
    assert 'modes=monthly' in output.read_text()
    monkeypatch.setenv('REPORT_KEY',key)
    freeze_clock('2026-11-01T06:00:00+00:00')
    observed={}
    def generated():
        ctx=main.current();observed['period']=(ctx.coordinator.start,ctx.coordinator.end)
        observed['date']=main._edition_now().date().isoformat()
        main._record_generation('Test','Persisted source facts')
        main._queue_report('Test','Persisted source facts')
    monkeypatch.setattr(main,'_main',generated)
    main.main()
    assert observed=={'period':('2026-09-01','2026-09-30'),'date':'2026-10-01'}
    workflow_delivery.verify_reports(Path(os.environ['REPORT_DIR']),key,['monthly'],mode='none',send_required=False)
    receipt=json.loads((Path(os.environ['REPORT_DIR'])/'cybermed_monthly_delivery_result.json').read_text())
    assert receipt['period_start']=='2026-09-01' and receipt['status']=='intentionally_skipped'


def test_readiness_missing_daily_and_legacy_shift_stop_before_processing(monkeypatch,tmp_path):
    plan=plan_env(monkeypatch,tmp_path,day='2026-12-23')
    weekly=next(e for e in plan['entries'] if e['cadence']=='weekly')
    with pytest.raises(RuntimeError,match='daily_coverage'): ep.check_ready(weekly)
    daily=Path(os.environ['CYBERMED_DAILY_DIGEST_STATE_PATH'])
    daily.write_text(json.dumps({'schema_version':1,'digests':[{'digest_id':'cybermed_daily_'+d,'run_date':d} for d in ep.required_daily_dates(weekly['period_start'],weekly['coverage_end'])]}))
    ep.check_ready(weekly)
    store=Path(os.environ['CYBERMED_WEEKLY_DIGEST_STATE_PATH'])
    store.write_text(json.dumps({'schema_version':1,'digests':[{'digest_id':'old','report_key':'cybermed','cadence':'weekly',
      'period_start':'2026-12-21','period_end':'2026-12-23','items':{'pubmed':[],'foamed':[]},'deep_dives':[],'top_picks':[]}]}))
    with pytest.raises(RuntimeError,match='legacy_shifted'): ep.check_ready(weekly)


def test_monthly_missing_closing_week_is_partial_without_changing_id(monkeypatch,tmp_path):
    from newsagent2.cybermed_digest_store import prepare_cybermed_monthly_source_digests, make_cybermed_weekly_digest
    plan=plan_env(monkeypatch,tmp_path,day='2026-11-06')
    entry=next(e for e in plan['entries'] if e['cadence']=='monthly'); identity=entry['edition_id']
    path=Path(os.environ['CYBERMED_WEEKLY_DIGEST_STATE_PATH']);store={'schema_version':1,'digests':[]}
    _,diag=prepare_cybermed_monthly_source_digests(store,'2026-10')
    rows=[];all_daily=[]
    for label in diag['expected_weekly_periods']:
        start,end=label.split('..')
        daily=[{'digest_id':'cybermed_daily_'+d,'run_date':d} for d in ep.required_daily_dates(start,end,policy='legacy')]
        all_daily.extend(daily)
        rows.append(make_cybermed_weekly_digest(period_start=start,period_end=end,generated_at_utc='fixed',source_digests=daily,pubmed_items=[],foamed_items=[],deep_dives=[],top_picks=[],diagnostic_summary={}))
    store['digests']=rows[:-1];path.write_text(json.dumps(store))
    Path(os.environ['CYBERMED_DAILY_DIGEST_STATE_PATH']).write_text(json.dumps({'digests':all_daily}))
    before=path.read_bytes()
    ep.check_ready(entry)  # Explicit available-evidence policy, no invented closing week.
    assert path.read_bytes()==before
    Path(os.environ['CYBERMED_DAILY_DIGEST_STATE_PATH']).write_text(json.dumps({'digests':all_daily}))
    store['digests']=rows;path.write_text(json.dumps(store));ep.check_ready(entry)
    later=ep.make_plan('cybermed',created_at='2026-11-09T12:00:00Z',job_started_at='2026-11-09T12:01:00Z',trigger='schedule',mode='real')
    assert next(e for e in later['entries'] if e['cadence']=='monthly')['edition_id']==identity


def test_noop_plan_cli_and_safe_failure_metadata(monkeypatch,tmp_path):
    output=tmp_path/'plan.json';monkeypatch.setenv('NEWSAGENT_PLAN_PATH',str(output));monkeypatch.setenv('GITHUB_EVENT_NAME','schedule')
    monkeypatch.setattr(sys,'argv',['plan','--report-key','cybermed','--created-at','2026-12-25T12:00:00Z','--job-started-at','2026-12-25T12:01:00Z'])
    ep.main();assert ep.load_plan()['entries']==[]
    assert not Path(os.environ['STATE_PATH']).exists()
    monkeypatch.setattr(ep,'github_created_at',lambda: (_ for _ in ()).throw(RuntimeError('private-host-secret')))
    monkeypatch.setenv('GITHUB_RUN_ID','42');monkeypatch.setattr(sys,'argv',['plan','--report-key','cybermed'])
    with pytest.raises(RuntimeError,match='edition_plan_invalid'):ep.main()
    evidence=(Path(os.environ['REPORT_DIR'])/'cybermed_plan_failure_metadata.json').read_text()
    assert 'private-host-secret' not in evidence


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('status',['accepted_by_smtp','partially_accepted','outcome_unknown'])
def test_public_main_duplicate_with_frozen_plan_never_regenerates_or_resends(key,status,monkeypatch,tmp_path):
    from newsagent2 import emailer
    plan_env(monkeypatch,tmp_path,key=key)
    monkeypatch.setenv('REPORT_MODE','daily');monkeypatch.setenv('SEND_EMAIL','1')
    monkeypatch.setenv('DELIVERY_RECIPIENT_HMAC_KEY','offline-calendar-key-at-least-thirty-two-bytes')
    monkeypatch.setenv('RECIPIENTS_CONFIG_JSON','["one@example.test","two@example.test"]')
    generated=[];submitted=[]
    def generate():
        generated.append('generated')
        ctx=main.current();ctx.finalization_policy='no_markers'
        main._record_generation('Offline fixture','Original source facts')
        main._queue_report('Offline fixture','Original source facts')
    def transport(message,recipients,inflight,outcome):
        submitted.append(message);inflight()
        outcome(status,recipients if status=='accepted_by_smtp' else recipients[:1] if status=='partially_accepted' else [],
                recipients[1:] if status=='partially_accepted' else [],'')
    def send(subject,body,*,coordinator,finalization):
        return coordinator.deliver(body,['one@example.test','two@example.test'],transport,finalization=finalization)
    monkeypatch.setattr(main,'_main',generate);monkeypatch.setattr(main,'send_markdown',send)
    if status=='accepted_by_smtp': main.main()
    else:
        with pytest.raises(RuntimeError): main.main()
    if status=='accepted_by_smtp': main.main()
    else:
        with pytest.raises(RuntimeError): main.main()
    assert len(generated)==len(submitted)==1
    timing=json.loads((Path(os.environ['REPORT_DIR'])/f'{key}_daily_timing_metadata.json').read_text())
    assert timing['generated_at'] and bool(timing['smtp_accepted_at'])==(status=='accepted_by_smtp')
    assert bool(timing['state_persisted_at'])==(status=='accepted_by_smtp')
    assert timing['delivery_status']==status


def test_daily_success_does_not_mask_blocked_weekly_and_missing_receipt(monkeypatch,tmp_path):
    plan_env(monkeypatch,tmp_path,mode='none')
    with ep.bind_entry('cybermed','daily'):
        coordinator=delivery.Coordinator('cybermed','daily');coordinator.skip('fixture','disabled')
    directory=Path(os.environ['REPORT_DIR'])
    with pytest.raises(RuntimeError,match='missing_planned_delivery_result:cybermed:weekly'):
        delivery.verify_reports(directory,'cybermed',['daily','weekly','monthly'],mode='none',send_required=False)
    assert json.loads((directory/'cybermed_weekly_delivery_result.json').read_text())['status']=='failed'


def test_run_metadata_request_uses_only_actual_created_at_and_redacts_failure(monkeypatch):
    import requests
    seen=[]
    monkeypatch.setenv('GITHUB_REPOSITORY','Cyberlurch/NewsAgent2');monkeypatch.setenv('GITHUB_RUN_ID','42');monkeypatch.setenv('GITHUB_TOKEN','offline-test-token')
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'created_at':'2026-10-30T04:40:00Z','run_started_at':'2026-10-31T06:00:00Z'}
    def get(*args,**kwargs): seen.append(args[0]);return Response()
    monkeypatch.setattr(requests,'get',get)
    assert ep.github_created_at()=='2026-10-30T04:40:00+00:00' and seen==['https://api.github.com/repos/Cyberlurch/NewsAgent2/actions/runs/42']
    monkeypatch.setattr(requests,'get',lambda *a,**kw: (_ for _ in ()).throw(ValueError('token-host-private')))
    with pytest.raises(RuntimeError,match='^edition_run_metadata_unavailable$'): ep.github_created_at()


def test_actual_daily_pipeline_stores_logical_date_and_real_generation_time(monkeypatch,tmp_path,freeze_clock):
    from test_cybermed_daily_digest_store import _base_env
    _base_env(monkeypatch,tmp_path)
    freeze_clock('2026-10-02T12:00:00+00:00')
    plan_env(monkeypatch,tmp_path,day='2026-10-01',created_at='2026-09-30T22:30:00Z',mode='none',request='daily',trigger='workflow_dispatch')
    monkeypatch.setattr(main,'summarize',lambda *a,**kw: pytest.fail('Empty source must not invoke a model'))
    main.main()
    out=Path(os.environ['REPORT_DIR'])
    store=next(out.rglob('cybermed_daily_digests.json'))
    row=json.loads(store.read_text())['digests'][0]
    assert row['digest_id']=='cybermed_daily_2026-10-01' and row['run_date']=='2026-10-01'
    assert row['generated_at_utc'].startswith('2026-10-02T12:00:00')
    result=json.loads((out/'cybermed_daily_delivery_result.json').read_text())
    assert result['edition_id']=='cybermed:daily:2026-10-01:2026-10-01:none'
    assert not (tmp_path/'state'/'cybermed_daily_digests.json').exists()


def test_shifted_week_selects_only_observed_coverage_not_later_friday(monkeypatch,tmp_path):
    from newsagent2.cybermed_digest_store import select_cybermed_daily_digests_for_week
    plan_env(monkeypatch,tmp_path,day='2026-12-23')
    rows={'digests':[{'digest_id':'cybermed_daily_'+d,'run_date':d} for d in ['2026-12-21','2026-12-22','2026-12-23','2026-12-25']]}
    with ep.bind_entry('cybermed','weekly'):
        selected=select_cybermed_daily_digests_for_week(rows,date(2026,12,23))
    assert [r['run_date'] for r in selected]==['2026-12-21','2026-12-22','2026-12-23']


def test_actual_shifted_weekly_pipeline_writes_friday_identity_and_wednesday_proof(monkeypatch,tmp_path,freeze_clock):
    from test_cybermed_daily_digest_store import _base_env
    _base_env(monkeypatch,tmp_path)
    freeze_clock('2026-12-23T12:00:00+00:00')
    plan_env(monkeypatch,tmp_path,day='2026-12-23')
    monkeypatch.setenv('REPORT_MODE','weekly');monkeypatch.setenv('SEND_EMAIL','0')
    for function in ('search_recent_pubmed','collect_foamed_items','summarize','summarize_item_detail'):
        monkeypatch.setattr(main,function,lambda *a,**kw: pytest.fail('Stored Weekly must not collect or invoke models'))
    days=['2026-12-21','2026-12-22','2026-12-23']
    rows=[{'digest_id':'cybermed_daily_'+day,'report_key':'cybermed','cadence':'daily','run_date':day,
        'items':{'pubmed':[],'foamed':[{'item_id':'learning-'+day,'source':'foamed','source_type':'foamed',
          'source_name':'Offline education','title':'Clinical education case','url':'https://example.test/'+day,
          'bottom_line':'The source discussed a clinical education case with practical learning points.'}]},
        'deep_dives':[],'top_picks':[]} for day in days]
    Path(os.environ['CYBERMED_DAILY_DIGEST_STATE_PATH']).parent.mkdir(parents=True,exist_ok=True)
    Path(os.environ['CYBERMED_DAILY_DIGEST_STATE_PATH']).write_text(json.dumps({'schema_version':1,'digests':rows}))
    main.main()
    row=json.loads(Path(os.environ['CYBERMED_WEEKLY_DIGEST_STATE_PATH']).read_text())['digests'][0]
    assert row['digest_id']=='cybermed_weekly_2026-12-21_2026-12-25'
    assert row['coverage_end']=='2026-12-23' and row['source_run_dates']==days and row['expected_daily_dates']==days
    assert row['calendar_policy']==ep.POLICY and row['coverage_status']=='complete'


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_two_python_cli_runs_share_writer_lock_and_send_once(key,monkeypatch,tmp_path):
    plan_env(monkeypatch,tmp_path,key=key)
    monkeypatch.setenv('REPORT_MODE','daily');monkeypatch.setenv('SEND_EMAIL','1')
    monkeypatch.setenv('DELIVERY_RECIPIENT_HMAC_KEY','offline-calendar-key-at-least-thirty-two-bytes')
    monkeypatch.setenv('RECIPIENTS_CONFIG_JSON','["one@example.test"]')
    trace=tmp_path/'trace.txt'
    code=f'''import time
from pathlib import Path
from newsagent2 import main
trace=Path({str(trace)!r})
def generate():
    with trace.open('a') as f: f.write('generation\\n')
    time.sleep(.15)
    ctx=main.current();ctx.finalization_policy='no_markers'
    main._record_generation('Offline fixture','Original source facts')
    main._queue_report('Offline fixture','Original source facts')
def transport(message,recipients,inflight,outcome):
    inflight()
    with trace.open('a') as f: f.write('smtp_mock\\n')
    outcome('accepted_by_smtp',recipients,[],'')
def send(subject,body,*,coordinator,finalization):
    return coordinator.deliver(body,['one@example.test'],transport,finalization=finalization)
main._main=generate;main.send_markdown=send
main.main()
'''
    children=[subprocess.Popen([sys.executable,'-c',code],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(2)]
    for child in children:
        stdout,stderr=child.communicate(timeout=30)
        assert child.returncode==0,stdout+stderr
    assert trace.read_text().splitlines()==['generation','smtp_mock']


def test_publication_failure_remains_failure_and_has_no_success_time(monkeypatch,tmp_path):
    plan_env(monkeypatch,tmp_path,mode='real')
    path=Path(os.environ['STATE_PATH']);path.write_text('{}')
    monkeypatch.setenv('GITHUB_REF_NAME','test-branch')
    def git(args,**kw):
        action=args[1]
        if action=='status': return subprocess.CompletedProcess(args,0,' M store.json','')
        if action=='rev-parse':return subprocess.CompletedProcess(args,0,'a'*40,'')
        if action=='push': return subprocess.CompletedProcess(args,1,'','private push error')
        if action=='ls-remote':return subprocess.CompletedProcess(args,0,'b'*40+'\trefs/heads/test-branch','')
        return subprocess.CompletedProcess(args,0,'','')
    monkeypatch.setattr(workflow_delivery.subprocess,'run',git)
    with pytest.raises(RuntimeError,match='git_push_failed_or_unknown'):
        workflow_delivery.publish_state([str(path)],report_dir=Path(os.environ['REPORT_DIR']))
    value=json.loads((Path(os.environ['REPORT_DIR'])/'git_timing_metadata.json').read_text())
    assert value['publication_outcome']=='git_push_failed_or_unknown' and 'private push error' not in json.dumps(value)
    assert not list(Path(os.environ['REPORT_DIR']).glob('cybermed_*_timing_metadata.json'))


def test_completed_monthly_only_schedule_is_an_already_finalized_noop(monkeypatch,tmp_path):
    plan_env(monkeypatch,tmp_path,key='cyberlurch',day='2026-11-01')
    coordinator=delivery.Coordinator('cyberlurch','monthly')
    row=coordinator.generated('Saved original facts')
    row.status='accepted_by_smtp';row.accepted=['a'*64];row.smtp_phase='completed';row.state_persisted=True
    coordinator._persist(row)
    monkeypatch.setattr(sys,'argv',['plan','--report-key','cyberlurch','--created-at','2026-11-01T12:00:00Z','--job-started-at','2026-11-01T12:01:00Z'])
    ep.main()
    plan=ep.load_plan()
    assert plan['entries']==[] and plan['reason']=='already_finalized_no_op'
    assert plan['already_finalized_editions']==[row.edition_id]


def test_existing_other_cadence_receipt_finalizes_under_its_original_period(monkeypatch,tmp_path):
    plan_env(monkeypatch,tmp_path,key='cyberlurch',day='2026-11-01') # Only Monthly is planned.
    coordinator=delivery.Coordinator('cyberlurch','daily',mode='real',period_dates=('2026-10-30','2026-10-30'))
    row=delivery.DeliveryResult('cyberlurch','daily',coordinator.start,coordinator.end,coordinator.edition_id,
        'a'*64,'unused')
    import hashlib
    row.message_id='<'+hashlib.sha256((row.edition_id+':'+row.content_version).encode()).hexdigest()+'@newsagent2.invalid>'
    row.status='accepted_by_smtp';row.accepted=['a'*64];row.smtp_phase='completed'
    coordinator._persist(row)
    workflow_delivery.finalize_pending_receipts('cyberlurch')
    final=delivery.load_journal(coordinator.path)['editions'][row.edition_id]
    assert final['period_start']=='2026-10-30' and final['state_persisted']


@pytest.mark.parametrize('created,job,day,category',[
 ('2026-10-30T12:00:00Z','2026-10-30T11:00:00Z','', 'job_precedes'),
 ('2026-10-30T12:00:00Z','2026-10-30T13:00:00Z','2026-10-31','future_logical')])
def test_invalid_execution_dates_stop_before_a_plan(created,job,day,category):
    with pytest.raises(ValueError,match=category):
        ep.make_plan('cybermed',created_at=created,job_started_at=job,logical_date=day,trigger='workflow_dispatch',mode='real')


def test_manual_preview_has_same_week_identity_but_early_production_is_blocked(monkeypatch,tmp_path):
    plan=plan_env(monkeypatch,tmp_path,day='2026-10-28',request='weekly',trigger='workflow_dispatch')
    entry=plan['entries'][0]
    assert entry['period_end']=='2026-10-30' and entry['coverage_end']=='2026-10-28'
    with pytest.raises(RuntimeError,match='not_closed_preview_only'): ep.check_ready(entry)
    native=ep.entry_for('cybermed','weekly',date(2026,10,30),'schedule','real')
    assert native['edition_id']==entry['edition_id']


def test_other_week_identity_in_journal_cannot_license_a_new_send(monkeypatch,tmp_path):
    plan=plan_env(monkeypatch,tmp_path,day='2026-12-23')
    entry=next(e for e in plan['entries'] if e['cadence']=='weekly')
    older=delivery.Coordinator('cybermed','weekly',period_dates=('2026-12-21','2026-12-23'))
    row=older.generated('Original legacy week');row.status='outcome_unknown';older._persist(row)
    with pytest.raises(RuntimeError,match='other_week_identity_requires_review'): ep.check_ready(entry)


def test_plan_binding_cannot_change_from_default_none_to_implicit_scheduled_real(monkeypatch,tmp_path):
    plan_env(monkeypatch,tmp_path,mode='none')
    monkeypatch.delenv('EMAIL_MODE')
    with pytest.raises(RuntimeError,match='execution_binding_mismatch'):ep.load_plan()
    monkeypatch.delenv('NEWSAGENT_PLAN_PATH');monkeypatch.setenv('NEWSAGENT_LOGICAL_DATE','2026-10-30')
    with pytest.raises(RuntimeError,match='logical_date_requires_validated_plan'):
        with ep.bind_entry('cybermed','daily'):pytest.fail('Unbound context must not reach generation')


def test_verifier_uses_plan_even_with_conflicting_period_environment(monkeypatch,tmp_path):
    plan_env(monkeypatch,tmp_path,mode='none')
    coordinator=delivery.Coordinator('cybermed','daily',period_dates=('2026-10-29','2026-10-29'))
    coordinator.skip('Original output','disabled')
    monkeypatch.setenv('DELIVERY_PERIOD_START','2026-10-29');monkeypatch.setenv('DELIVERY_PERIOD_END','2026-10-29')
    with pytest.raises(RuntimeError,match='mismatched_planned_delivery_period'):
        delivery.verify_reports(Path(os.environ['REPORT_DIR']),'cybermed',['daily'],mode='none',send_required=False)
