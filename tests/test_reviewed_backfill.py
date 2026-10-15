"""Offline review publication tests; all Apply inputs are synthetic temporary data."""
import copy
from datetime import date
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from contextlib import contextmanager

import pytest

from newsagent2.maintenance import reviewed_backfill as r
from newsagent2.maintenance.backfill_cybermed_weekly import build_audit, daily_audit
from newsagent2 import main, history_coverage as hc
from newsagent2.run_context import PATHS

STAMP='2026-10-15T12:00:00+00:00'


def put(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))


def rows():
    return [{'digest_id':'cybermed_daily_'+d,'report_key':'cybermed','cadence':'daily','run_date':d,
             'calendar_policy':hc.POLICY,'items':{'pubmed':[],'foamed':[]},'generated_at_utc':d+'T05:40:00+00:00'}
            for d in ('2026-09-07','2026-09-08','2026-09-09','2026-09-10','2026-09-11')]


@pytest.fixture
def setup(tmp_path,monkeypatch):
    state=tmp_path/'state'
    for key,filename in PATHS.items():monkeypatch.setenv(key,str(state/filename))
    source=state/'cybermed_daily_digests.json';target=state/'cybermed_weekly_digests.json'
    put(source,{'schema_version':1,'legacy_extra':{'keep':True},'digests':rows()})
    plan=build_audit(daily_path=source,weekly_path=target,from_date=date(2026,9,7),to_date=date(2026,9,11),reconstructed_at=STAMP)
    return plan,source,target,tmp_path.parent/(tmp_path.name+'-private-backup')


def apply(setup,**kw):
    plan,source,target,backup=setup
    return r.apply_plan(kw.pop('plan',plan),cadence='weekly',target=target,sources={'daily':source},
                        expected_sha256=kw.pop('expected',plan['plan_sha256']),backup_dir=backup,**kw)


def test_audit_no_state_lock_or_directory_creation_and_complete_valid_empty(setup,tmp_path):
    plan,source,target,_=setup
    assert not target.exists() and not source.with_name('delivery_journal.lock').exists()
    assert plan['candidates'][0]['history_coverage']['status']=='complete'
    assert plan['candidates'][0]['generated_at_utc']==STAMP
    assert plan['candidates'][0]['reconstructed_at']==STAMP
    assert plan['target']['pre_sha256'] is None
    before=source.read_bytes()
    r.write_plan(plan,tmp_path/'out'/'review.json')
    assert source.read_bytes()==before and not target.exists()


def test_insert_private_backup_and_idempotence(setup):
    plan,source,target,backup=setup;original=json.dumps(plan,sort_keys=True)
    assert apply(setup)['status']=='applied'
    assert hashlib.sha256(target.read_bytes()).hexdigest()==plan['target']['post_sha256']
    assert backup.stat().st_mode & 0o777 == 0o700
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in backup.iterdir())
    assert json.loads((backup/'manifest.json').read_text())['target_was_absent']
    assert not (backup/'preimage.json').exists()
    before=target.read_bytes();mtime=target.stat().st_mtime_ns
    assert apply(setup)['status']=='already_at_postimage'
    assert target.read_bytes()==before and target.stat().st_mtime_ns==mtime
    assert json.dumps(plan,sort_keys=True)==original


@pytest.mark.parametrize('change',['source','target','candidate','scope','postimage','path','candidate_cached_hash'])
def test_changes_block_before_state_write(setup,change):
    plan,source,target,backup=setup
    p=copy.deepcopy(plan);expected=plan['plan_sha256']
    if change=='source':source.write_text(source.read_text()+' ')
    if change=='target':put(target,{'digests':[]})
    if change=='candidate':p['candidates'][0]['top_picks']=['tampered']
    if change=='scope':p['report_key']='cyberlurch'
    if change=='postimage':p['target']['post_sha256']='0'*64
    if change=='path':p['target']['path']=str(source)
    if change=='candidate_cached_hash':
        p['candidate_sha256']='0'*64;p['plan_sha256']=r.plan_hash(p);expected=p['plan_sha256']
    before=target.read_bytes() if target.exists() else None
    with pytest.raises(r.ReviewError):apply(setup,plan=p,expected=expected)
    assert (target.read_bytes() if target.exists() else None)==before and not backup.exists()


def test_valid_existing_empty_and_nonempty_rows_are_preserved(setup):
    _,source,target,backup=setup
    p=build_audit(daily_path=source,weekly_path=target,from_date=date(2026,9,7),to_date=date(2026,9,11),reconstructed_at=STAMP)
    old=copy.deepcopy(p['candidates'][0]);old['legacy_extra']='original';old['items']['pubmed']=[{'title':'Existing summary'}]
    put(target,{'schema_version':1,'legacy':{'keep':True},'digests':[old]});before=target.read_bytes()
    plan=build_audit(daily_path=source,weekly_path=target,from_date=date(2026,9,7),to_date=date(2026,9,11),reconstructed_at=STAMP)
    assert plan['actions'][0]['action']=='preserve_existing'
    assert apply((plan,source,target,backup))['status']=='already_at_postimage'
    assert target.read_bytes()==before and not backup.exists()


@pytest.mark.parametrize('failure',['backup','write'])
def test_backup_or_persistence_failure_has_no_false_success(setup,monkeypatch,failure):
    plan,source,target,backup=setup
    def fail(*a,**k):raise OSError('private host and exception payload must not escape')
    monkeypatch.setattr(r,'_private_write' if failure=='backup' else 'atomic_text',fail)
    with pytest.raises(r.ReviewError,match='review_backup_failed|review_persistence_failed') as exc:apply(setup)
    assert 'private host' not in str(exc.value) and not target.exists()
    if failure=='write':assert (backup/'manifest.json').exists()


def test_checks_sources_after_acquiring_common_lock(setup,monkeypatch):
    _,source,target,_=setup
    @contextmanager
    def locked():
        source.write_text(source.read_text()+' ')
        yield
    monkeypatch.setattr(r,'writer_lock',locked)
    with pytest.raises(r.ReviewError,match='source_hash'):apply(setup)
    assert not target.exists()


def test_source_or_target_changed_during_backup_cannot_be_overwritten(setup,monkeypatch):
    _,source,target,_=setup;real=r._backup
    def backup(*a):
        out=real(*a);put(target,{'digests':[],'concurrent':True});return out
    monkeypatch.setattr(r,'_backup',backup)
    with pytest.raises(r.ReviewError,match='concurrent_target'):apply(setup)
    assert json.loads(target.read_text())['concurrent'] is True


def test_corrupt_existing_json_byte_identical(setup):
    _,source,target,_=setup;raw=b'{ broken';target.write_bytes(raw)
    from newsagent2.store_io import StoreError
    with pytest.raises(StoreError):build_audit(daily_path=source,weekly_path=target,from_date=date(2026,9,7),to_date=date(2026,9,11))
    assert target.read_bytes()==raw


@pytest.mark.parametrize('defect',['duplicate','competing'])
def test_duplicate_and_competing_periods_stop(setup,defect):
    p,source,target,_=setup
    if defect=='duplicate':
        value=json.loads(source.read_text());value['digests'].append(value['digests'][0]);put(source,value)
    else:
        row={**p['candidates'][0],'digest_id':'different-id'};put(target,{'digests':[row]})
    with pytest.raises(r.ReviewError,match='duplicate|competing'):build_audit(daily_path=source,weekly_path=target,from_date=date(2026,9,7),to_date=date(2026,9,11))


def test_missing_tuesday_and_unknown_policy_never_claim_complete(setup):
    _,source,target,_=setup
    value=json.loads(source.read_text());value['digests'].pop(1);put(source,value)
    def build():return build_audit(daily_path=source,weekly_path=target,from_date=date(2026,9,7),to_date=date(2026,9,11))
    assert build()['candidates'][0]['coverage_status']=='incomplete'
    for row in value['digests']:row.pop('calendar_policy')
    put(source,value)
    assert build()['candidates'][0]['coverage_status']=='unknown'


def test_shifted_holiday_has_friday_identity_and_wednesday_coverage(setup):
    _,source,target,_=setup
    ds=[{**row,'digest_id':'cybermed_daily_'+d,'run_date':d} for row,d in zip(rows(),('2026-12-21','2026-12-22','2026-12-23'))]
    put(source,{'digests':ds})
    plan=build_audit(daily_path=source,weekly_path=target,from_date=date(2026,12,21),to_date=date(2026,12,25))
    candidate=plan['candidates'][0]
    assert candidate['period_end']=='2026-12-25' and candidate['coverage_end']=='2026-12-23'
    assert candidate['coverage_status']=='complete'


def test_daily_offline_apply_and_existing_empty_protection(tmp_path,monkeypatch):
    state=tmp_path/'state';out=tmp_path/'out'
    for k,f in PATHS.items():monkeypatch.setenv(k,str(state/f))
    candidate={**rows()[0],'generated_at_utc':STAMP,'reconstructed_at':STAMP,'reconstruction_basis':'historical_window_generation'}
    source=out/'candidate.json';target=state/'cybermed_daily_digests.json';put(source,{'digests':[candidate]})
    plan=daily_audit(source,target,from_date=date(2026,9,7),to_date=date(2026,9,7))
    def forbidden(*a,**k):pytest.fail('collector/provider must not be called')
    monkeypatch.setattr(main,'search_recent_pubmed',forbidden);monkeypatch.setattr(main,'summarize',forbidden)
    backup=tmp_path.parent/(tmp_path.name+'-backup')
    result=r.apply_plan(plan,cadence='daily',target=target,sources={'candidate':source},expected_sha256=plan['plan_sha256'],backup_dir=backup)
    assert result['status']=='applied'
    original=target.read_bytes()
    candidate['items']['pubmed']=[{'title':'Changed alternative'}];put(source,{'digests':[candidate]})
    plan=daily_audit(source,target,from_date=date(2026,9,7),to_date=date(2026,9,7))
    assert plan['actions'][0]['action']=='preserve_existing' and target.read_bytes()==original


def test_old_direct_apply_stops_before_context_or_providers(monkeypatch):
    monkeypatch.setenv('CYBERMED_DIGEST_BACKFILL_APPLY','1')
    monkeypatch.setattr(main,'execution_context',lambda *a:pytest.fail('unbound apply reached context'))
    with pytest.raises(RuntimeError,match='reviewed_backfill_apply_required'):main.main()


def test_symlink_target_and_private_backup_inside_checkout_rejected(setup,tmp_path):
    plan,source,target,_=setup
    with pytest.raises(r.ReviewError,match='private_external'):apply((plan,source,target,source.parent/'backup'))
    target.symlink_to(source)
    with pytest.raises(r.ReviewError,match='symlink'):apply(setup)


def load_tool(name):
    path=Path(__file__).parents[1]/'tools'/f'{name}.py'
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def test_artifact_allowlist_and_no_traversal_or_symlinks(tmp_path,monkeypatch):
    for key,f in PATHS.items():monkeypatch.setenv(key,str(tmp_path/"state"/f))
    tool=load_tool('verify_reviewed_artifact');source=tmp_path/'download';source.mkdir()
    put(source/'cybermed_weekly_backfill_audit.json',{'test':'review'})
    tool.verify(source,tmp_path/'out','weekly')
    (source/'extra').write_text('untrusted')
    with pytest.raises(ValueError):tool.verify(source,tmp_path/'out2','weekly')
    (source/'extra').unlink();(source/'cybermed_weekly_backfill_audit.json').unlink()
    (source/'cybermed_weekly_backfill_audit.json').symlink_to(tmp_path/'out/cybermed_weekly_backfill_audit.json')
    with pytest.raises(ValueError):tool.verify(source,tmp_path/'out3','weekly')


def test_push_failure_propagates_and_only_target_is_staged(tmp_path,monkeypatch):
    tool=load_tool('publish_maintenance');monkeypatch.chdir(tmp_path)
    target=Path(tool.TARGETS['daily']);target.parent.mkdir();target.write_bytes(b'new')
    old=b'old';calls=[]
    def git(args,**kw):
        calls.append((args,kw))
        if args[1]=='push':raise subprocess.CalledProcessError(1,args)
        out=b' M state/cybermed_daily_digests.json\n' if args[1]=='status' else old if args[1]=='show' else b''
        return subprocess.CompletedProcess(args,1 if args[1]=='diff' else 0,stdout=out)
    monkeypatch.setattr(tool.subprocess,'run',git)
    plan={'cadence':'daily','report_key':'cybermed','target':{'pre_sha256':r.sha(old),'post_sha256':r.sha(b'new')}}
    with pytest.raises(subprocess.CalledProcessError):tool.publish(plan,'daily','main')
    assert next(args for args,kw in calls if args[1]=='add')==['git','add','--',str(target)]
    assert calls[-1][1]['check'] is True and '--force' not in calls[-1][0]


def test_actual_daily_generation_export_then_offline_apply_is_bound_and_honest(tmp_path,monkeypatch,freeze_clock):
    from test_cybermed_daily_digest_store import _base_env,_with_nonempty_selection
    _base_env(monkeypatch,tmp_path);_with_nonempty_selection(monkeypatch)
    monkeypatch.setattr(main,'_select_pubmed_deep_dives_with_content',lambda *a,**k:[])
    freeze_clock(STAMP)
    monkeypatch.setenv('CYBERMED_DIGEST_BACKFILL_MODE','1')
    monkeypatch.setenv('CYBERMED_DIGEST_BACKFILL_RUN_DATE','2026-09-01')
    monkeypatch.setenv('CYBERMED_DIGEST_BACKFILL_REFERENCE_UTC','2026-09-01T05:40:00+00:00')
    main.main()
    source=tmp_path/'out/cybermed_daily_candidate.json';target=tmp_path/'state/cybermed_daily_digests.json'
    plan_path=tmp_path/'out/cybermed_daily_reviewed_plan.json';plan=json.loads(plan_path.read_text())
    assert not target.exists() and plan['candidates'][0]['generated_at_utc']==STAMP
    assert plan['candidates'][0]['reference_utc']=='2026-09-01T05:40:00+00:00'
    def fail(*a,**k):pytest.fail('offline Apply must not regenerate')
    monkeypatch.setattr(main,'search_recent_pubmed',fail);monkeypatch.setattr(main,'summarize_pubmed_bottom_line',fail)
    protected={k:Path(os.environ[k]).read_bytes() if Path(os.environ[k]).exists() else None for k in PATHS if k!='CYBERMED_DAILY_DIGEST_STATE_PATH'}
    r.apply_plan(plan,cadence='daily',target=target,sources={'candidate':source},expected_sha256=plan['plan_sha256'],
                 backup_dir=tmp_path.parent/(tmp_path.name+'-backup'))
    assert target.exists()
    assert protected=={k:Path(os.environ[k]).read_bytes() if Path(os.environ[k]).exists() else None for k in protected}


def test_actual_offline_cli_consumes_plan_without_mutating_it(setup,tmp_path):
    plan,source,target,backup=setup
    review=tmp_path/'out'/'review.json';r.write_plan(plan,review);before=review.read_bytes()
    command=[sys.executable,'-m','newsagent2.maintenance.backfill_cybermed_weekly','--apply',
        '--reviewed-plan',str(review),'--expected-sha256',plan['plan_sha256'],'--backup-dir',str(backup),
        '--daily-path',str(source),'--weekly-path',str(target),'--output',str(tmp_path/'out/result.json')]
    proc=subprocess.run(command,text=True,capture_output=True)
    assert proc.returncode==0,proc.stderr
    assert review.read_bytes()==before and target.exists()
    assert json.loads((tmp_path/'out/result.json').read_text())['status']=='applied'


def test_backup_contains_exact_preimage_and_legacy_fields_retained(setup):
    _,source,target,backup=setup
    put(target,{'schema_version':1,'legacy_extra':{'old':'retain'},'digests':[]});before=target.read_bytes()
    plan=build_audit(daily_path=source,weekly_path=target,from_date=date(2026,9,7),to_date=date(2026,9,11),reconstructed_at=STAMP)
    apply((plan,source,target,backup))
    assert (backup/'preimage.json').read_bytes()==before
    assert json.loads(target.read_text())['legacy_extra']=={'old':'retain'}


def test_unexpected_staged_file_blocks_commit_and_push(tmp_path,monkeypatch):
    tool=load_tool('publish_maintenance');monkeypatch.chdir(tmp_path)
    calls=[]
    def git(args,**kw):
        calls.append(args);return subprocess.CompletedProcess(args,0,stdout=b'src/unrelated.py\n')
    monkeypatch.setattr(tool.subprocess,'run',git)
    with pytest.raises(ValueError,match='staged'):tool.publish({'cadence':'daily','report_key':'cybermed'},'daily','main')
    assert not any(a[1] in {'commit','push','add'} for a in calls)


def test_frozen_postimage_mismatch_is_checked_even_with_new_review_hash(setup):
    p,source,target,backup=setup;p=copy.deepcopy(p)
    p['target']['post_sha256']='0'*64;p['plan_sha256']=r.plan_hash(p)
    with pytest.raises(r.ReviewError,match='postimage'):apply((p,source,target,backup))
    assert not target.exists() and not backup.exists()


def test_existing_same_id_different_postimage_is_conflict_not_retry_success(setup):
    plan,source,target,_=setup
    put(target,{'digests':[plan['candidates'][0]],'unexpected':'same ID is not proof'})
    before=target.read_bytes()
    with pytest.raises(r.ReviewError,match='target_hash'):apply(setup)
    assert target.read_bytes()==before


def test_workflows_keep_review_binding_writer_lock_and_result_upload_allowlist():
    import yaml
    root=Path(__file__).parents[1]
    for cadence in ('daily','weekly'):
        text=(root/f'.github/workflows/cybermed-{cadence}-backfill.yml').read_text()
        data=yaml.load(text,Loader=yaml.BaseLoader)
        assert data['concurrency']['group']=='newsagent2-state-writers-${{ github.ref }}'
        steps=data['jobs']['audit-or-apply']['steps']
        checkout=next(s for s in steps if s.get('uses','').startswith('actions/checkout'))
        assert checkout['with']['ref']=='${{ github.ref_name }}'
        apply=next(s for s in steps if s.get('name','').startswith('Apply exact'))
        assert all(flag in apply['run'] for flag in ('--reviewed-plan','--expected-sha256','--backup-dir'))
        assert 'python -m src.newsagent2.main' not in apply['run']
        publisher=next(s for s in steps if s.get('name','').startswith('Commit and push'))
        assert 'publish_maintenance.py' in publisher['run'] and 'always()' not in publisher['if']
        for step in steps:
            if step.get('uses','').startswith('actions/upload-artifact'):
                assert 'private-maintenance-backup' not in step['with']['path']
        if cadence=='daily':
            generation=next(s for s in steps if s.get('name','').startswith('Generate audit'))
            assert generation['if']=='${{ !inputs.apply }}' and 'OPENAI_API_KEY' in generation['env']
            assert 'OPENAI_API_KEY' not in data['jobs']['audit-or-apply']['env']
    ci=yaml.load((root/'.github/workflows/offline-tests.yml').read_text(),Loader=yaml.BaseLoader)
    upload=next(s for s in ci['jobs']['offline-tests']['steps'] if s.get('uses','').startswith('actions/upload-artifact'))
    assert upload['with']['path'].split()==['.ci-results/pytest.xml','.ci-results/integrity.json']
    assert upload['with']['include-hidden-files']=='true' and upload['with']['if-no-files-found']=='error'


def test_state_changed_to_unsupported_schema_remains_unchanged(setup):
    _,source,target,_=setup;put(target,{'schema_version':99,'digests':[]});before=target.read_bytes()
    with pytest.raises(r.ReviewError,match='schema_unsupported'):apply(setup)
    assert target.read_bytes()==before
