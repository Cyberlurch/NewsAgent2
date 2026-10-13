import hashlib
import json
import sys
import threading
from pathlib import Path

import pytest

from newsagent2 import main, emailer, store_io, workflow_delivery
from newsagent2 import pre_smtp_recovery as recovery
from newsagent2.delivery import load_journal
from newsagent2.run_context import PATHS, path_for
from newsagent2.store_io import StoreError, digest_store, rollups_store


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}


@pytest.fixture
def saved(monkeypatch, tmp_path):
    state = tmp_path / 'state'; state.mkdir()
    for variable, filename in PATHS.items(): monkeypatch.setenv(variable, str(state / filename))
    for variable, value in {'EMAIL_MODE':'real', 'SEND_EMAIL':'1', 'GITHUB_EVENT_NAME':'workflow_dispatch',
        'REPORT_KEY':'cybermed', 'REPORT_MODE':'daily', 'REPORT_DIR':str(tmp_path / 'out'),
        'DELIVERY_PERIOD_START':'2026-10-12', 'DELIVERY_PERIOD_END':'2026-10-12',
        'DELIVERY_RECIPIENT_HMAC_KEY':'offline-recovery-key-never-used-for-production',
        'RECIPIENTS_CONFIG_JSON':'["one@example.test","two@example.test"]',
        'SMTP_HOST':'smtp.example.test', 'SMTP_USER':'sender@example.test', 'SMTP_PASS':'offline-password',
        'SMTP_PORT':'587', 'EMAIL_FROM':'sender@example.test'}.items(): monkeypatch.setenv(variable,value)
    calls, generation = [], []
    class SMTP:
        error = None
        refused = {}
        def __init__(self,*a,**k): pass
        def starttls(self,**k): pass
        def login(self,*a): pass
        def sendmail(self,sender,audience,message):
            calls.append((sender,audience,message))
            if self.error: raise self.error
            return self.refused
        def quit(self): pass
    monkeypatch.setattr(emailer.smtplib,'SMTP',SMTP)
    for function in ('search_recent_pubmed','fetch_pubmed_abstracts','list_recent_videos','summarize'):
        monkeypatch.setattr(main,function,lambda *a,**k: pytest.fail('Recovery must not collect or call providers'))
    atomic = store_io.atomic_store
    def prepare(key='cybermed', cadence='daily', approved=True, markers=True, no_markers=False, two_stores=False):
        monkeypatch.setenv('REPORT_KEY',key); monkeypatch.setenv('REPORT_MODE',cadence)
        if cadence == 'monthly':
            monkeypatch.setenv('DELIVERY_PERIOD_START','2026-10-01'); monkeypatch.setenv('DELIVERY_PERIOD_END','2026-10-31')
        kind = 'rollups' if cadence == 'monthly' else ('cybermed_daily' if key=='cybermed' else 'cyberlurch_digests')
        target = path_for(recovery.BINDINGS[kind])
        original = {'version':1,'reports':{}} if kind=='rollups' else {'schema_version':1,'digests':[]}
        target.write_text(json.dumps(original))
        def generate():
            generation.append('generation')
            main._record_generation('Original newsletter subject', 'Validated original content.\n')
            value = {'version':1,'reports':{key:[{'month':'2026-10','executive_summary':['Validated.']}]}} if kind=='rollups' else {
                'schema_version':1,'digests':[{'digest_id':key+'_daily_2026-10-12','video_id':'video', 'run_date':'2026-10-12',
                    'processed_at_utc':'2026-10-12T05:00:00Z','bottom_line':'Validated.'}]}
            store_io.generated_store(target,value,kind=kind,validate=rollups_store if kind=='rollups' else digest_store)
            if two_stores:
                store_io.generated_store(path_for('ROLLUPS_STATE_PATH'), {'version':1,'reports':{key:[{'month':'2026-10'}]}},
                    kind='rollups',validate=rollups_store)
            if not approved: raise RuntimeError('medical_or_aggregate_quality_not_approved')
            if markers:
                rows=[{'source':'pubmed' if key=='cybermed' else 'youtube','id':'item'}]
                main._update_state_after_run(state_path=str(path_for('STATE_PATH')),state=main.load_state(str(path_for('STATE_PATH'))),
                    items_all_new=rows,overview_items=rows,detail_items=[],foamed_overview_items=[],report_key=key,report_mode=cadence,
                    now_utc_iso='2026-10-12T05:00:00Z',read_only=False)
            main._queue_report('Original newsletter subject','Validated original content.\n',no_markers=no_markers)
        monkeypatch.setattr(main,'_main',generate)
        def fail_target(path,*a,**k):
            if Path(path)==target: raise StoreError(kind,'persistence_failed')
            return atomic(path,*a,**k)
        monkeypatch.setattr(store_io,'atomic_store',fail_target)
        with pytest.raises(RuntimeError): main.main()
        monkeypatch.setattr(store_io,'atomic_store',atomic)
        row=next(iter(load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'].values()))
        return row,target
    def cli(edition,apply=False):
        args=['workflow_delivery','replay-pre-smtp','--edition-id',edition]
        if apply: args+=['--apply','--confirm-replay']
        monkeypatch.setattr(sys,'argv',args)
        workflow_delivery.main()
    return prepare,cli,SMTP,calls,generation,tmp_path


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('cadence',['daily','monthly'])
def test_public_main_and_cli_replay_after_ready_store_failure(saved,key,cadence,monkeypatch,capsys):
    prepare,cli,SMTP,calls,generation,root=saved
    row,target=prepare(key,cadence)
    assert row['category']==recovery.FAILURE and row['smtp_phase']=='not_handed_off' and row['recovery']
    assert row['finalization'] and len(generation)==1 and not calls
    before=hashes(root)
    cli(row['edition_id'])
    assert hashes(root)==before and not calls
    assert 'ready_for_explicit_replay' in capsys.readouterr().out
    with pytest.raises(RuntimeError,match='replay-pre-smtp --edition-id'): main.main()
    assert len(generation)==1 and not calls
    cli(row['edition_id'],True)
    assert len(calls)==1 and 'Subject: Original newsletter subject' in calls[0][2] and row['message_id'] in calls[0][2]
    final=load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]
    assert final['status']=='accepted_by_smtp' and final['state_persisted'] and final['smtp_phase']=='completed'
    assert json.loads(target.read_text()).get('reports' if cadence=='monthly' else 'digests')
    state=json.loads(path_for('STATE_PATH').read_text())
    assert 'item' in state['reports'][key]['pubmed' if key=='cybermed' else 'youtube']['processed']
    if cadence=='daily':
        clock=state['last_successful_daily_run_utc_by_report'][key] if key=='cybermed' else state['last_successful_daily_run_utc']
        assert clock=='2026-10-12T05:00:00Z'
    cli(row['edition_id'],True); main.main()
    assert len(calls)==1 and len(generation)==1


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_early_generation_without_late_quality_approval_never_replays(saved,key):
    prepare,cli,SMTP,calls,generation,root=saved
    row,target=prepare(key,approved=False)
    assert row['generation_validated'] and not row['recovery']
    before=hashes(root)
    workflow_delivery.restore_generated_records(root/'out/recovery',key)
    assert hashes(root)==before
    with pytest.raises(RuntimeError,match='legacy_recovery_readiness_missing'): cli(row['edition_id'],True)
    assert hashes(root)==before and not calls and json.loads(target.read_text())['digests']==[]


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_already_restored_deltas_are_supported(saved,key):
    prepare,cli,SMTP,calls,generation,root=saved
    row,target=prepare(key)
    workflow_delivery.restore_generated_records(root/'out/recovery',key)
    before=hashes(root)
    cli(row['edition_id'])
    assert hashes(root)==before and not calls
    cli(row['edition_id'],True)
    assert len(calls)==1


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_fresh_checkout_reads_artifact_and_imports_only_confirmed_edition(saved,key):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare(key)
    # Simulate the branch checkout after the failed runner never published.
    path_for('DELIVERY_JOURNAL_PATH').unlink()
    before=hashes(root)
    cli(row['edition_id'])
    assert hashes(root)==before and not path_for('DELIVERY_JOURNAL_PATH').exists() and not calls
    cli(row['edition_id'],True)
    final=load_journal(path_for('DELIVERY_JOURNAL_PATH'))
    assert list(final['editions'])==[row['edition_id']] and len(calls)==1


def test_explicit_no_marker_policy_is_bound_and_replayable(saved):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare(markers=False,no_markers=True)
    assert row['recovery']['finalization_policy']=='no_markers' and row['finalization']=={}
    cli(row['edition_id'],True)
    assert len(calls)==1 and not path_for('STATE_PATH').exists()


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_real_generation_pipeline_is_fully_approved_before_replay_checkpoint(saved,key,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    from datetime import datetime,timezone
    provider_calls=[]
    if key=='cyberlurch':
        from test_cyberlurch_monthly_safety import _setup
        target=_setup(root,monkeypatch)
        monkeypatch.setenv('DELIVERY_PERIOD_START',datetime.now(main.STO).strftime('%Y-%m-01'))
        monkeypatch.setenv('DELIVERY_PERIOD_END','2026-10-31')
    else:
        from test_pubmed_facts import paper,CORRECTION
        day=datetime.now(main.STO).date().isoformat()
        monkeypatch.setenv('DELIVERY_PERIOD_START',day); monkeypatch.setenv('DELIVERY_PERIOD_END',day)
        monkeypatch.setenv('REPORT_PROFILE','medical')
        monkeypatch.setenv('REPORT_LANGUAGE','en')
        monkeypatch.setenv('REPORT_TITLE','Cybermed Report')
        monkeypatch.setenv('REPORT_SUBJECT','Cybermed Report')
        monkeypatch.setattr(sys,'argv',['main'])
        item=paper(published_at=datetime.now(timezone.utc),text=CORRECTION['abstract'])
        monkeypatch.setattr(main,'load_channels_config',lambda *a: ([{'source':'pubmed','name':'Test','query':'mock'}],{},{}))
        def collect(*a,**k):
            return ([item],{}) if k.get('return_metadata') else [item]
        monkeypatch.setattr(main,'search_recent_pubmed',collect)
        monkeypatch.setattr(main,'collect_foamed_items',lambda *a,**k: ([],{}))
        monkeypatch.setattr(main,'_select_pubmed_deep_dives_with_content',lambda rows,**k: rows[:1])
        def summary(*a,**k):
            provider_calls.append('mocked summary')
            return '## Executive Summary\n\nThe intervention reduced delirium.'
        monkeypatch.setattr(main,'summarize',summary)
        monkeypatch.setattr(main,'summarize_pubmed_bottom_line',lambda *a,**k:'The intervention reduced delirium.')
        monkeypatch.setattr(main,'summarize_item_detail',lambda *a,**k:'BOTTOM LINE: No significant difference.\n\nYet the intervention reduced delirium.')
        target=path_for('CYBERMED_DAILY_DIGEST_STATE_PATH')
    monkeypatch.setenv('EMAIL_MODE','real'); monkeypatch.setenv('SEND_EMAIL','1')
    monkeypatch.setattr(main,'send_markdown',emailer.send_markdown)
    original=store_io.atomic_store
    def fail_output(path,*a,**k):
        if Path(path)==target: raise StoreError('generated_output','persistence_failed')
        return original(path,*a,**k)
    monkeypatch.setattr(store_io,'atomic_store',fail_output)
    with pytest.raises(RuntimeError,match=recovery.FAILURE): main.main()
    monkeypatch.setattr(store_io,'atomic_store',original)
    row=next(iter(load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'].values()))
    assert row['recovery']['approved_after_generation_checks'] and not calls and not target.exists()
    if key=='cybermed':
        body=next((root/'out/recovery').glob('*.md')).read_text()
        assert '225/641' in body and '201/614' in body and 'P=.37' in body
    assert row['recovery']['finalization_policy']=='markers'
    providers_before=list(provider_calls)
    before=hashes(root)
    cli(row['edition_id']); assert hashes(root)==before
    monkeypatch.setattr(main,'_main',lambda: pytest.fail('Replay must not regenerate'))
    cli(row['edition_id'],True); cli(row['edition_id'],True); main.main()
    assert len(calls)==1 and provider_calls==providers_before and target.exists()


def test_missing_finalization_approval_is_not_ship_ready(saved):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare(markers=False)
    assert not row['recovery']
    with pytest.raises(RuntimeError,match='legacy_recovery_readiness_missing'): cli(row['edition_id'],True)
    assert not calls


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
def test_every_required_delta_is_validated_before_restoring_any_store(saved,key):
    prepare,cli,SMTP,calls,generation,root=saved
    row,target=prepare(key,two_stores=True)
    manifest=row['recovery']['generated_deltas']
    assert len(manifest)==2
    (root/'out/recovery'/manifest[-1]['file']).unlink()
    before=hashes(root)
    with pytest.raises(RuntimeError,match='required_delta_missing'): cli(row['edition_id'],True)
    assert hashes(root)==before and not calls and not path_for('ROLLUPS_STATE_PATH').exists()


def test_additive_legacy_receipt_does_not_infer_non_submission_from_empty_arrays(saved):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare()
    journal=load_journal(path_for('DELIVERY_JOURNAL_PATH'))
    legacy=journal['editions'][row['edition_id']]
    legacy.pop('recovery'); legacy.pop('smtp_phase')
    assert not legacy['accepted'] and not legacy['unknown']
    path_for('DELIVERY_JOURNAL_PATH').write_text(json.dumps(journal))
    before=hashes(root)
    with pytest.raises(RuntimeError,match='legacy_recovery_readiness_missing'): cli(row['edition_id'],True)
    assert hashes(root)==before and not calls


@pytest.mark.parametrize('key',['cybermed','cyberlurch'])
@pytest.mark.parametrize('damage',['missing_body','changed_body','missing_delta','changed_delta','newer_state','audience','key','wrong_edition','intents','past_unknown','wrong_period','incomplete_acceptance'])
def test_replay_requires_every_original_binding_and_all_deltas(saved,key,damage,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    row,target=prepare(key)
    directory=root/'out/recovery'
    body=directory/(hashlib.sha256((row['edition_id']+':'+row['content_version']).encode()).hexdigest()+'.md')
    delta=directory/row['recovery']['generated_deltas'][0]['file']
    edition=row['edition_id']
    if damage=='missing_body': body.unlink()
    if damage=='changed_body': body.write_text('Changed output.')
    if damage=='missing_delta': delta.unlink()
    if damage=='changed_delta': delta.write_text('{}')
    if damage=='newer_state': target.write_text('{"digests":[{"digest_id":"newer","video_id":"newer"}]}')
    if damage=='audience': monkeypatch.setenv('RECIPIENTS_CONFIG_JSON','["other@example.test"]')
    if damage=='key': monkeypatch.setenv('DELIVERY_RECIPIENT_HMAC_KEY','a-different-offline-key-at-least-thirty-two-bytes')
    if damage=='wrong_edition': edition=edition.replace('2026-10-12','2026-10-11')
    if damage in {'intents','past_unknown','wrong_period','incomplete_acceptance'}:
        journal=load_journal(path_for('DELIVERY_JOURNAL_PATH'))
        if damage=='intents': journal['editions'][edition]['finalization']={}
        if damage=='past_unknown': journal['editions'][edition]['prior_attempts']=[{'status':'outcome_unknown','category':'smtp_in_flight','content_version':row['content_version']}]
        if damage=='wrong_period': journal['editions'][edition]['period_end']='2026-10-13'
        if damage=='incomplete_acceptance':
            journal['editions'][edition]['status']='accepted_by_smtp'
            journal['editions'][edition]['accepted']=[row['recovery']['audience'][0]]
        path_for('DELIVERY_JOURNAL_PATH').write_text(json.dumps(journal))
    before=hashes(root)
    with pytest.raises((RuntimeError,StoreError)): cli(edition,True)
    assert hashes(root)==before and not calls


@pytest.mark.parametrize('status',['outcome_unknown','partially_accepted'])
def test_uncertain_or_partial_replay_cannot_send_again(saved,status):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare()
    if status=='outcome_unknown': SMTP.error=TimeoutError('private SMTP error')
    else: SMTP.refused={'two@example.test':(550,b'private rejection')}
    with pytest.raises(RuntimeError): cli(row['edition_id'],True)
    final=load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]
    assert final['status']==status and final['prior_attempts']
    with pytest.raises(RuntimeError,match='not_a_proven'): cli(row['edition_id'],True)
    assert len(calls)==1


def test_acceptance_before_marker_failure_repairs_only_markers(saved,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare()
    original=main.save_state
    monkeypatch.setattr(main,'save_state',lambda *a,**k: (_ for _ in ()).throw(StoreError('processed_items','persistence_failed')))
    with pytest.raises(StoreError): cli(row['edition_id'],True)
    final=load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]
    assert final['status']=='accepted_by_smtp' and not final['state_persisted'] and len(calls)==1
    monkeypatch.setattr(main,'save_state',original)
    cli(row['edition_id'],True)
    assert len(calls)==1 and load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]['state_persisted']


def test_accepted_artifact_repairs_generation_and_markers_in_fresh_checkout(saved,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    row,target=prepare()
    original=main.save_state
    monkeypatch.setattr(main,'save_state',lambda *a,**k: (_ for _ in ()).throw(StoreError('processed_items','persistence_failed')))
    with pytest.raises(StoreError): cli(row['edition_id'],True)
    monkeypatch.setattr(main,'save_state',original)
    path_for('DELIVERY_JOURNAL_PATH').unlink()
    target.write_text(json.dumps({'schema_version':1,'digests':[]}))
    before=hashes(root)
    cli(row['edition_id']); assert hashes(root)==before
    cli(row['edition_id'],True); cli(row['edition_id'],True)
    final=load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]
    assert len(calls)==1 and final['state_persisted'] and json.loads(target.read_text())['digests']


def test_failed_restore_retains_attempt_history_and_can_resume_safely(saved,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    row,target=prepare()
    original=recovery.atomic_store
    def fail_output(path,*a,**k):
        if Path(path)==target: raise StoreError('generated_output','persistence_failed')
        return original(path,*a,**k)
    monkeypatch.setattr(recovery,'atomic_store',fail_output)
    with pytest.raises(StoreError): cli(row['edition_id'],True)
    failed=load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]
    assert failed['status']=='failed' and failed['smtp_phase']=='not_handed_off' and len(failed['prior_attempts'])==2
    assert failed['prior_attempts'][-1]['category']=='persistence_failed' and not calls
    monkeypatch.setattr(recovery,'atomic_store',original)
    cli(row['edition_id'],True)
    assert len(calls)==1


def test_apply_without_exact_explicit_confirmation_never_mutates(saved,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare()
    before=hashes(root)
    monkeypatch.setattr(sys,'argv',['workflow_delivery','replay-pre-smtp','--edition-id',row['edition_id'],'--apply'])
    with pytest.raises(RuntimeError,match='requires_confirmation'): workflow_delivery.main()
    assert hashes(root)==before and not calls


@pytest.mark.parametrize('mode',['none','test'])
def test_apply_requires_real_mode_and_cannot_modify_production(saved,mode,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare(); monkeypatch.setenv('EMAIL_MODE',mode)
    before=hashes(root)
    with pytest.raises(RuntimeError,match='requires_confirmation_and_real_mode'): cli(row['edition_id'],True)
    assert hashes(root)==before and not calls


def test_main_and_replay_share_lock_without_nested_deadlock(saved,monkeypatch):
    prepare,cli,SMTP,calls,generation,root=saved
    row,_=prepare()
    entered,release,replay_started=threading.Event(),threading.Event(),threading.Event()
    preflight=main._preflight_stores
    def held_preflight(key):
        if threading.current_thread().name=='main-run':
            entered.set(); assert release.wait(3)
        preflight(key)
    monkeypatch.setattr(main,'_preflight_stores',held_preflight)
    errors=[]
    def report():
        try: main.main()
        except RuntimeError: pass  # Normal main still requires explicit replay.
        except BaseException as error: errors.append(error)
    def apply():
        replay_started.set()
        try: recovery.replay(row['edition_id'],apply=True,confirm=True)
        except BaseException as error: errors.append(error)
    a=threading.Thread(target=report,name='main-run'); a.start(); assert entered.wait(3)
    b=threading.Thread(target=apply,name='replay'); b.start(); assert replay_started.wait(3)
    b.join(.1); assert b.is_alive() and not calls
    release.set(); a.join(3); b.join(3)
    assert not a.is_alive() and not b.is_alive() and not errors and len(calls)==1
