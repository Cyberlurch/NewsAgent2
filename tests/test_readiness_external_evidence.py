"""An external check failure is distinct from a known delivery/state failure."""
from datetime import datetime
import json
from types import SimpleNamespace

import pytest
import requests

from newsagent2 import delivery, edition_plan as ep, operational_readiness as op, workflow_delivery as wd
from newsagent2.run_context import path_for, PATHS
from newsagent2.store_io import atomic_store
from _github_fakes import response, collection
from test_github_readonly import clock
from test_recovery_scope import job, result as receipt
from test_recovery_scope import packet, zipped, snapshot
from test_readiness_schedule_delay import seed

AS_OF = datetime.fromisoformat('2026-10-23T07:18:52+00:00')


def cli_inputs(monkeypatch,tmp_path):
    directory=tmp_path/'inputs';directory.mkdir()
    for name,filename in PATHS.items():monkeypatch.setenv(name,str(directory/filename))


def api(monkeypatch, handler):
    calls=[]
    class Session:
        def __init__(self):self.headers={}
        def get(self,url,**kwargs):
            calls.append((url,kwargs))
            return handler(url,kwargs,len(calls))
    monkeypatch.setattr(requests,'Session',Session)
    for name,value in {'GITHUB_REPOSITORY':'offline/repo','GITHUB_REF_NAME':'main',
                       'GITHUB_TOKEN':'private-token','NEWSAGENT_CHECKED_COMMIT':'a'*40}.items():
        monkeypatch.setenv(name,value)
    return calls


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('fault',['timeout','503','rate'])
def test_recovered_external_check_has_no_failure_annotation(key,fault,clock,monkeypatch,tmp_path,capsys):
    cli_inputs(monkeypatch,tmp_path)
    seed(key,prior_day='2026-10-22')
    def get(url,kwargs,index):
        if index==1:
            if fault=='timeout':raise requests.Timeout('private.test token-secret')
            return response(status=503 if fault=='503' else 429,headers={'Retry-After':'1'})
        return response(collection({'workflow_runs':[]}))
    calls=api(monkeypatch,get)
    output=tmp_path/'report.json';summary=tmp_path/'summary.md'
    monkeypatch.setenv('GITHUB_STEP_SUMMARY',str(summary))
    monkeypatch.setattr('sys.argv',['readiness','--report-key',key,'--check-actions',
        '--as-of',AS_OF.isoformat(),'--output',str(output)])
    assert op.main()==0
    report=json.loads(output.read_text());captured=capsys.readouterr()
    assert report['ok'] and report['status']=='prerequisites_pass' and report['errors']==[]
    assert report['checks'][0]['status']==report['checks'][1]['status']=='awaiting_scheduled_run'
    assert report['actions_check']['status']=='complete' and len(calls)==2
    assert report['actions_check']['diagnostics'][0]['attempts']==2
    assert report['state_view']=={'commit':'a'*40,'ref':'main'}
    assert 'prerequisites_pass' in summary.read_text() and '::error' not in captured.err


@pytest.mark.parametrize('status',[401,403,404,503])
def test_exhausted_external_check_is_unverified_not_delivery_failure(status,clock,monkeypatch,tmp_path,capsys):
    cli_inputs(monkeypatch,tmp_path)
    seed('cybermed',prior_day='2026-10-22')
    calls=api(monkeypatch,lambda *a:response({'message':'private.test token-secret'},status=status))
    output=tmp_path/'report.json';summary=tmp_path/'summary.md'
    monkeypatch.setenv('GITHUB_STEP_SUMMARY',str(summary))
    monkeypatch.setattr('sys.argv',['readiness','--report-key','cybermed','--check-actions',
        '--as-of',AS_OF.isoformat(),'--output',str(output)])
    assert op.main()==1
    report=json.loads(output.read_text());captured=capsys.readouterr()
    assert not report['ok'] and report['status']=='externally_unverified'
    assert report['checks'][0]['status']==report['checks'][1]['status']=='awaiting_scheduled_run'
    assert report['actions_check']['status']=='incomplete'
    assert report['actions_check']['failure']['http_status']==status
    assert len(calls)==(4 if status==503 else 1)
    assert report['errors']==['operational_actions_check_incomplete']
    assert 'externally_unverified' in summary.read_text() and 'operational_actions_check_incomplete' in captured.err
    assert all(private not in output.read_text()+summary.read_text()+captured.out+captured.err
        for private in ['private.test','token-secret','private-token'])


@pytest.mark.parametrize('status',['outcome_unknown','partially_accepted','accepted_by_smtp'])
def test_api_failure_keeps_known_smtp_or_finalization_error_visible(status,clock,monkeypatch):
    row=receipt(start='2026-10-23',body='Saved source report.')
    row.update(status=status,smtp_phase='completed' if status!='outcome_unknown' else 'handoff_started')
    if status=='outcome_unknown':row['unknown']=['a'*64]
    elif status=='partially_accepted':row.update(accepted=['a'*64],rejected=['b'*64])
    else:row['accepted']=['a'*64]
    seed('cybermed',row,prior_day='2026-10-22')
    api(monkeypatch,lambda *a:response(status=503))
    report=op.assess('cybermed',as_of=AS_OF,check_actions=True)
    assert report['status']=='blocked' and not report['ok']
    assert report['unresolved_delivery']==[dict(edition_id=row['edition_id'],status=status,
        required_action='state_recovery_no_resend' if status=='accepted_by_smtp' else 'reconcile_smtp_no_automatic_retry')]
    assert report['checks'][0]['status'] in {'blocked','recovery_required'}
    assert report['actions_check']['status']=='incomplete'


def plan_job(key,ids):
    value=job(key,mode='real')
    value['steps'].insert(1,{'name':f'NewsAgent2 plan v1 / {key} / '+ep.plan_proof(ids),
        'number':4,'started_at':'2026-10-23T07:00:00Z','conclusion':'success'})
    return value


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('receipts',['one','two','all','missing_plan','late_plan'])
def test_subset_of_receipts_never_proves_a_complete_multicadence_plan(key,receipts,clock,monkeypatch):
    rows=[receipt(key,cadence=c,run='71',start=s,end=e,body='Saved report '+c)
        for c,s,e in [('daily','2026-10-23','2026-10-23'),('weekly','2026-10-19','2026-10-23'),
                      ('monthly','2026-09-01','2026-09-30')]]
    for row in rows:row.update(status='accepted_by_smtp',state_persisted=True,smtp_phase='completed',accepted=['a'*64])
    present=rows[:1] if receipts=='one' else rows[:2] if receipts=='two' else rows
    journal={'version':1,'editions':{r['edition_id']:r for r in present}}
    jobs=[plan_job(key,[r['edition_id'] for r in rows])]
    if receipts=='missing_plan':jobs=[job(key,mode='real')]
    if receipts=='late_plan':jobs[0]['steps'][1]['number']=10
    def get(url,kwargs,index):
        return response(collection({'workflow_runs':[{'id':71,'event':'schedule','conclusion':'failure'}]}
            if url.endswith('/runs') else {'jobs':jobs}))
    api(monkeypatch,get)
    pending=op._pending_runs(key,journal)
    assert bool(pending)==(receipts!='all')
    assert wd._completed_plan_receipts({'id':71},jobs,key,journal)==(receipts=='all')
    if pending:assert pending[0]['planning_evidence']==('missing' if receipts in {'missing_plan','late_plan'} else 'present')


def test_pending_recovery_found_before_later_api_failure_is_retained(clock,monkeypatch):
    seed('cybermed',prior_day='2026-10-22')
    def get(url,kwargs,index):
        if url.endswith('/runs'):return response(collection({'workflow_runs':[
            {'id':71,'event':'schedule','conclusion':'failure'}, {'id':72,'event':'schedule','conclusion':'failure'}]}))
        if '/71/' in url:return response(collection({'jobs':[job(mode='real')]}))
        raise requests.Timeout('private.test')
    api(monkeypatch,get)
    report=op.assess('cybermed',as_of=AS_OF,check_actions=True)
    assert report['pending_recovery_runs'][0]['run_id']=='71'
    assert report['status']=='blocked' and report['actions_check']['status']=='incomplete'


def test_sender_on_second_jobs_page_cannot_be_lost(clock,monkeypatch):
    jobs=[{'id':i,'steps':[]} for i in range(1,101)]+[{'id':101,**job(mode='real')}]
    def get(url,kwargs,index):
        if url.endswith('/runs'):return response(collection({'workflow_runs':[{'id':71,'event':'schedule','conclusion':'failure'}]}))
        return response({'total_count':101,'jobs':jobs[:100] if kwargs['params']['page']==1 else jobs[100:]})
    calls=api(monkeypatch,get)
    assert op._pending_runs('cybermed',{'editions':{}})[0]['run_id']=='71'
    assert len(calls)==3 and calls[-1][1]['params']['filter']=='all'


def test_all_attempt_plans_can_match_pruned_subsets_without_hiding_a_missing_cadence():
    rows=[receipt(cadence=c,run='71',start='2026-10-23',body='Saved '+c) for c in ['daily','weekly','monthly']]
    for row in rows:row.update(status='accepted_by_smtp',state_persisted=True,smtp_phase='completed',accepted=['a'*64])
    ids=[r['edition_id'] for r in rows]
    jobs=[plan_job('cybermed',ids),plan_job('cybermed',ids[1:])]
    journal={'editions':{r['edition_id']:r for r in rows}}
    assert wd._completed_plan_receipts({'id':71},jobs,'cybermed',journal)
    del journal['editions'][ids[-1]]
    assert not wd._completed_plan_receipts({'id':71},jobs,'cybermed',journal)


def test_an_old_daily_and_later_weekly_acceptance_do_not_invent_original_plan_evidence(clock,monkeypatch):
    daily=receipt(run='38874103951',start='2026-10-16',body='Original Daily.')
    weekly=receipt(run='38894702613',start='2026-10-12',end='2026-10-16',cadence='weekly',body='Later reviewed Weekly.')
    for row in [daily,weekly]:row.update(status='accepted_by_smtp',state_persisted=True,smtp_phase='completed',accepted=['a'*64])
    journal={'editions':{r['edition_id']:r for r in [daily,weekly]}}
    def get(url,kwargs,index):
        data={'workflow_runs':[{'id':38874103951,'event':'schedule','conclusion':'failure'}]} if url.endswith('/runs') else {'jobs':[job(mode='real')]}
        return response(collection(data))
    api(monkeypatch,get)
    pending=op._pending_runs('cybermed',journal)
    assert pending[0]['planning_evidence']=='missing' and pending[0]['run_id']=='38874103951'
    assert pending[0]['required_action']=='inspect_frozen_plan_and_recovery_receipts_no_resend'


def test_real_plan_metadata_get_recovers_without_changing_the_creation_timestamp(clock,monkeypatch,capsys):
    api(monkeypatch,lambda *a:None)
    calls=[]
    def get(url,**kwargs):
        calls.append((url,kwargs))
        return response(status=503) if len(calls)==1 else response({
            'created_at':'2026-10-23T08:50:09Z','run_started_at':'2026-10-24T01:00:00Z'})
    monkeypatch.setattr(requests,'get',get);monkeypatch.setenv('GITHUB_RUN_ID','71')
    assert ep.github_created_at()=='2026-10-23T08:50:09+00:00'
    assert len(calls)==2 and all(c[0].endswith('/actions/runs/71') for c in calls)
    output=capsys.readouterr()
    assert '"attempts": 2' in output.out and '::error' not in output.err and 'private-token' not in output.out


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('fault',['metadata','archive'])
def test_recovery_retries_only_reads_and_imports_once(key,fault,clock,monkeypatch,tmp_path):
    import hashlib
    row=receipt(key,run='71',body='Validated saved source content.')
    row.update(status='accepted_by_smtp',smtp_phase='completed',state_persisted=True,accepted=['a'*64])
    directory=packet(tmp_path,[row],[(row,'Validated saved source content.')])
    content=zipped(directory);attempts={}
    def get(url,kwargs,index):
        attempts[url]=attempts.get(url,0)+1
        if (fault=='metadata' and url.endswith('/runs') or fault=='archive' and url.endswith('/zip')) and attempts[url]==1:
            return response(status=503)
        if url.endswith('/runs'):data={'workflow_runs':[{'id':71,'event':'schedule','conclusion':'failure'}]}
        elif url.endswith('/jobs'):data={'jobs':[job(key,mode='real')]}
        elif url.endswith('/artifacts'):data={'artifacts':[{'id':7,'name':f'{key}-delivery-recovery-71',
            'expired':False,'digest':'sha256:'+hashlib.sha256(content).hexdigest()}]}
        else:return response(content=content)
        return response(collection(data))
    api(monkeypatch,get)
    monkeypatch.setenv('GITHUB_RUN_ID','99');monkeypatch.setenv('REPORT_KEY',key);monkeypatch.setenv('EMAIL_MODE','real')
    imports=[];original=wd.import_recovery
    def imported(*args,**kwargs):imports.append('import');return original(*args,**kwargs)
    monkeypatch.setattr(wd,'import_recovery',imported)
    wd.recover_failed_runs(key,'cybermed.yml' if key=='cybermed' else 'newsagent.yml',report_dir=tmp_path/'out')
    saved=delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]
    assert imports==['import'] and saved['status']=='accepted_by_smtp' and not saved['state_persisted']
    assert max(attempts.values())==2 and saved['message_id']==row['message_id']


@pytest.mark.parametrize('defect',['digest','duplicate','invalid_zip','oversized'])
def test_recovery_archive_checks_fail_before_state_import(defect,clock,monkeypatch,tmp_path):
    import io
    import zipfile
    from newsagent2.store_io import StoreError
    row=receipt(body='Saved source content.')
    directory=packet(tmp_path,[row],[(row,'Saved source content.')])
    content=zipped(directory)
    if defect=='duplicate':
        buffer=io.BytesIO()
        with zipfile.ZipFile(buffer,'w') as archive:
            archive.writestr('a/journal.json',(directory/'journal.json').read_bytes())
            archive.writestr('b/journal.json',(directory/'journal.json').read_bytes())
        content=buffer.getvalue()
    if defect=='invalid_zip':content=b'not a zip'
    if defect=='oversized':monkeypatch.setattr('newsagent2.github_readonly.MAX_ARCHIVE',2)
    def get(url,kwargs,index):
        if url.endswith('/runs'):data={'workflow_runs':[{'id':71,'event':'schedule','conclusion':'failure'}]}
        elif url.endswith('/jobs'):data={'jobs':[job(mode='real')]}
        elif url.endswith('/artifacts'):data={'artifacts':[{'id':7,'name':'cybermed-delivery-recovery-71','expired':False,
            **({'digest':'sha256:'+'0'*64} if defect=='digest' else {})}]}
        else:return response(content=content)
        return response(collection(data))
    api(monkeypatch,get);monkeypatch.setenv('GITHUB_RUN_ID','99');monkeypatch.setenv('EMAIL_MODE','real')
    before=snapshot()
    with pytest.raises((RuntimeError,StoreError)):
        wd.recover_failed_runs('cybermed','cybermed.yml',report_dir=tmp_path/'out')
    assert snapshot()==before


def test_readiness_workflow_does_not_cancel_running_check_and_records_checkout():
    from pathlib import Path
    import yaml
    text=Path('.github/workflows/newsletter-readiness.yml').read_text()
    workflow=yaml.safe_load(text)
    assert workflow['concurrency']['cancel-in-progress'] is False
    assert 'ref: ${{ github.ref_name }}' in text and 'git rev-parse HEAD' in text
    assert '--check-actions' in text and 'continue-on-error' not in text
    for filename,key in [('cybermed.yml','cybermed'),('newsagent.yml','cyberlurch')]:
        producer=Path('.github/workflows',filename).read_text()
        assert f'NewsAgent2 plan v1 / {key} / ' in producer and 'outputs.plan_proof' in producer
        assert 'group: newsagent2-state-writers-${{ github.ref }}' in producer
        assert 'cancel-in-progress: false' in producer and 'ref: ${{ github.ref_name }}' in producer
