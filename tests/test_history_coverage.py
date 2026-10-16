"""Synthetic edition provenance only: no copied articles and no live services."""
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from newsagent2 import history_coverage as hc, edition_plan as ep
from newsagent2.maintenance.history_audit import audit, write_report
from newsagent2.run_context import PATHS
from newsagent2.store_io import atomic_store, StoreError, digest_store, rollups_store


def daily(d):
    return {'digest_id': 'cybermed_daily_'+d, 'report_key': 'cybermed', 'cadence': 'daily',
            'run_date': d, 'items': {'pubmed': [], 'foamed': []}}


def week(start, end=None, policy=hc.POLICY):
    end = end or (date.fromisoformat(start)+timedelta(days=4)).isoformat()
    dates = hc.expected_dates(start, end, policy) or hc.expected_dates(start, end, hc.LEGACY_WEEKDAYS)
    rows = [daily(d) for d in dates]
    row = {'digest_id': f'cybermed_weekly_{start}_{end}', 'report_key': 'cybermed', 'cadence': 'weekly',
           'period_start': start, 'period_end': end, 'coverage_end': dates[-1] if dates else end,
           'source_daily_digest_ids': [d['digest_id'] for d in rows], 'source_run_dates': dates,
           'items': {'pubmed': [], 'foamed': []}}
    if policy:
        row['calendar_policy'] = policy
    return row, rows


def month(m='2026-09'):
    start, end = hc.month_bounds(m)
    cursor = date.fromisoformat(start)
    cursor -= timedelta(days=cursor.weekday())
    weekly, ds = [], {}
    while cursor.isoformat() <= end:
        row, rows = week(cursor.isoformat())
        weekly.append(row)
        ds.update({d['digest_id']: d for d in rows})
        cursor += timedelta(days=7)
    row = {'month': m, 'calendar_policy': hc.POLICY, 'source_weekly_digest_ids': [w['digest_id'] for w in weekly]}
    return row, weekly, list(ds.values())


def files(tmp_path, report='cybermed', *, daily_rows=(), weekly_rows=(), months=()):
    root = tmp_path/'state';root.mkdir()
    paths = {k: root/f for k, f in PATHS.items()}
    def put(k, value):
        paths[k].write_text(json.dumps(value))
    if report == 'cybermed':
        put('CYBERMED_DAILY_DIGEST_STATE_PATH', {'digests': list(daily_rows)})
        put('CYBERMED_WEEKLY_DIGEST_STATE_PATH', {'digests': list(weekly_rows)})
    else:
        put('CYBERLURCH_DIGEST_STATE_PATH', {'digests': [{'video_id': 'v', 'published_at': '2026-09-01'}]})
    put('ROLLUPS_STATE_PATH', {'reports': {report: list(months)}})
    return paths


def snapshot(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}


def test_interior_september_gap_propagates_and_unreferenced_present_daily_is_not_coverage():
    m, weeks, ds = month()
    assert hc.monthly_coverage(m, weeks, ds)['status'] == 'complete'
    target = next(w for w in weeks if w['period_start'] == '2026-08-31')
    target['source_daily_digest_ids'].remove('cybermed_daily_2026-09-01')
    target['source_run_dates'].remove('2026-09-01')
    for rows in (ds, [d for d in ds if d['run_date'] != '2026-09-01']):
        cov = hc.monthly_coverage(m, weeks, rows)
        assert cov['status'] == 'incomplete'
        assert '2026-09-01' in cov['expected'] and '2026-09-01' not in cov['observed']


def test_holiday_shift_and_valid_empty():
    row, ds = week('2026-12-21')
    assert row['coverage_end'] == '2026-12-23'
    assert hc.weekly_coverage(row, ds)['status'] == 'complete'
    assert all(not d['items']['pubmed'] for d in ds)
    row['coverage_end'] = '2026-12-22'
    assert hc.weekly_coverage(row, ds)['status'] == 'incomplete'


def test_historical_unknown_policy_is_not_silently_v2_and_known_defect_dominates():
    row, ds = week('2026-08-31', policy=None)
    assert hc.weekly_coverage(row, ds)['status'] == 'unknown'
    row['source_daily_digest_ids'].append('missing')
    assert hc.weekly_coverage(row, ds)['status'] == 'incomplete'


@pytest.mark.parametrize('defect', ['duplicate_id', 'duplicate_period', 'foreign', 'outside', 'orphan'])
def test_reference_defects(defect):
    m, ws, ds = month()
    if defect == 'duplicate_id': ds.append(ds[0].copy())
    if defect == 'duplicate_period': ws.append({**ws[0], 'digest_id': 'competing'})
    if defect == 'foreign': ds[0]['report_key'] = 'cyberlurch'
    if defect == 'outside': ws[0]['source_daily_digest_ids'].append('outside');ds.append({**daily('2026-01-01'), 'digest_id': 'outside'})
    if defect == 'orphan': m['source_weekly_digest_ids'].append('orphan')
    assert hc.monthly_coverage(m, ws, ds)['status'] == 'incomplete'


def test_boundary_does_not_count_outside_month_and_retains_valid_empty_day():
    m, ws, ds = month()
    cov = hc.monthly_coverage(m, ws, ds)
    assert cov['status'] == 'complete'
    assert '2026-08-31' not in cov['observed']
    assert '2026-09-01' in cov['observed']


def test_yearly_not_twelve_rows_equals_complete_and_running_horizon():
    rows = [{'month': f'2026-{m:02}'} for m in range(1, 13)]
    cov = hc.yearly_coverage('cybermed', 2026, rows, as_of='2027-01-15T12:00:00Z')
    assert cov['status'] == 'unknown' and not cov['final_year_ready']
    cov = hc.yearly_coverage('cybermed', 2026, rows, as_of='2026-10-15T12:00:00Z')
    assert cov['horizon']['due_months'][-1] == '2026-09'
    assert cov['horizon']['open_months'] == ['2026-10']
    assert cov['horizon']['future_months'] == ['2026-11', '2026-12']
    assert not cov['final_year_ready']
    cov = hc.yearly_coverage('cybermed', 2026, [], as_of='2027-01-15T12:00:00Z', required_policy=hc.POLICY)
    assert cov['status'] == 'incomplete'


def test_due_horizon_observes_close_slot_and_independent_newsletters():
    before = hc.month_horizon('cybermed', 2026, '2026-10-02T03:39:59Z')
    after = hc.month_horizon('cybermed', 2026, '2026-10-02T03:40:00Z')
    assert '2026-09' in before['awaiting_close'] and '2026-09' in after['due_months']
    assert '2026-09' in hc.month_horizon('cyberlurch', 2026, '2026-10-01T04:00:00Z')['due_months']


def test_audit_readonly_missing_vs_corrupt_and_newsletter_separation(tmp_path):
    row, ds = week('2026-08-31')
    paths = files(tmp_path, daily_rows=ds, weekly_rows=[row])
    paths['CYBERLURCH_DIGEST_STATE_PATH'].write_bytes(b'broken unrelated')
    before = snapshot(tmp_path/'state')
    p = audit('cybermed', '2026-08-31', '2026-09-04', '2026-10-15T12:00:00Z', paths=paths)
    assert p['inputs']['DELIVERY_JOURNAL_PATH']['status'] == 'missing_new_store'
    assert all(r['delivery']['status'] == 'unknown' for r in p['records'])
    assert next(r for r in p['records'] if r['kind'] == 'daily')['content_state'] == 'valid_empty'
    write_report(p, tmp_path/'audit', paths=paths)
    assert snapshot(tmp_path/'state') == before
    assert 'CYBERLURCH_DIGEST_STATE_PATH' not in p['inputs']
    paths['CYBERMED_DAILY_DIGEST_STATE_PATH'].write_bytes(b'{ broken')
    before = snapshot(tmp_path/'state')
    p = audit('cybermed', '2026-09-01', '2026-09-04', '2026-10-15T12:00:00Z', paths=paths)
    assert p['exit_code'] == 2 and p['inputs']['CYBERMED_DAILY_DIGEST_STATE_PATH']['status'] == 'corrupted_existing_store'
    assert snapshot(tmp_path/'state') == before


def test_cyberlurch_videos_are_not_editions_and_other_invalid_report_is_ignored(tmp_path):
    paths = files(tmp_path, 'cyberlurch', months=[{'month': '2026-09'}])
    paths['CYBERMED_DAILY_DIGEST_STATE_PATH'].write_bytes(b'bad')
    paths['ROLLUPS_STATE_PATH'].write_text(json.dumps({'reports': {'cyberlurch': [{'month': '2026-09'}], 'cybermed': 'invalid'}}))
    p = audit('cyberlurch', '2026-09-01', '2026-09-30', '2026-10-15T12:00:00Z', paths=paths)
    assert p['video_record_count'] == 1
    assert p['comparison_scenario']['absent_daily_dates'] is None
    assert p['records'][0]['coverage']['status'] == 'unknown'
    assert p['records'][0]['evidence_class'] == 'thin_legacy'
    assert 'CYBERMED_DAILY_DIGEST_STATE_PATH' not in p['inputs']


def test_duplicate_month_is_visible_in_audit(tmp_path):
    paths = files(tmp_path, months=[{'month':'2026-09'}, {'month':'2026-09', 'generated_at':'later'}])
    p = audit('cybermed','2026-09-01','2026-09-30','2027-01-15T12:00:00Z',paths=paths)
    assert any(f['category']=='duplicate_monthly_period' for f in p['findings'])
    assert p['years']['2026']['status'] == 'incomplete'


@pytest.mark.parametrize('payload', [b'{"digests":[],"digests":[]}', b'{"digests":[{"run_date":"invalid"}]}', b'{"digests":[{"run_date":"2026-09-01","items":[]}]}'])
def test_invalid_json_and_record_shapes_remain_byte_equal(tmp_path,payload):
    paths=files(tmp_path);target=paths['CYBERMED_DAILY_DIGEST_STATE_PATH'];target.write_bytes(payload)
    p=audit('cybermed','2026-09-01','2026-09-30','2026-10-15T12:00:00Z',paths=paths)
    assert p['exit_code']==2 and target.read_bytes()==payload


def test_output_overlap_and_naive_timestamp_fail_closed(tmp_path):
    paths=files(tmp_path)
    p=audit('cybermed','2026-09-01','2026-09-30','2026-10-15T12:00:00Z',paths=paths)
    with pytest.raises(ValueError,match='overlaps'):write_report(p,tmp_path/'state'/'audit',paths=paths)
    with pytest.raises(ValueError,match='timezone'):audit('cybermed','2026-09-01','2026-09-30','2026-10-15T12:00:00',paths=paths)


def test_optional_coverage_validated_at_store_and_recovery_boundary(tmp_path):
    row=daily('2026-09-01');row['history_coverage']=hc.result()
    target=tmp_path/'new.json'
    atomic_store(target,{'digests':[row]},kind='test',validate=digest_store)
    row['history_coverage']['status']='sent'
    with pytest.raises(StoreError):atomic_store(target,{'digests':[row]},kind='test',validate=digest_store)
    assert not rollups_store({'reports':{'cybermed':[row]}})


def test_actual_cli_is_read_only_and_apply_stays_refused(tmp_path):
    paths=files(tmp_path)
    env=dict(os.environ);env.update({k:str(v) for k,v in paths.items()})
    command=[sys.executable,'-m','newsagent2.maintenance.backfill_digest_stores','--report','cybermed','--from-date','2026-09-01','--to-date','2026-09-30','--as-of','2026-10-15T12:00:00Z','--output-dir',str(tmp_path/'audit')]
    before=snapshot(tmp_path/'state')
    proc=subprocess.run(command,env=env,text=True,capture_output=True)
    assert proc.returncode==1,proc.stderr
    assert (tmp_path/'audit/history_audit.json').exists()
    proc=subprocess.run(command+['--apply'],env=env,text=True,capture_output=True)
    assert proc.returncode!=0 and 'read_only' in proc.stderr
    assert snapshot(tmp_path/'state')==before


def test_real_monthly_readiness_checks_interior_dailys(tmp_path,monkeypatch):
    m, ws, ds=month('2026-10')
    paths=files(tmp_path,daily_rows=ds,weekly_rows=ws)
    for k,v in paths.items():monkeypatch.setenv(k,str(v))
    entry=ep.entry_for('cybermed','monthly',date(2026,11,6),'schedule','real')
    # Existing closing-week checks need their prior metadata, not invented history.
    for w in ws:w['coverage_status']='complete'
    paths['CYBERMED_WEEKLY_DIGEST_STATE_PATH'].write_text(json.dumps({'digests':ws}))
    ep.check_ready(entry)
    ds=[d for d in ds if d['run_date']!='2026-10-14']
    paths['CYBERMED_DAILY_DIGEST_STATE_PATH'].write_text(json.dumps({'digests':ds}))
    with pytest.raises(RuntimeError,match='history_coverage:incomplete'):ep.check_ready(entry)


def test_overlapping_distinct_weeks_do_not_establish_complete_month():
    m, ws, ds = month()
    overlapping = {**ws[1], 'period_start':'2026-09-08', 'digest_id':'overlap'}
    overlapping['source_daily_digest_ids'] = overlapping['source_daily_digest_ids'][1:]
    overlapping['source_run_dates'] = overlapping['source_run_dates'][1:]
    ws.append(overlapping);m['source_weekly_digest_ids'].append('overlap')
    assert 'overlapping_weekly_coverage' in hc.monthly_coverage(m,ws,ds)['reasons']


def test_yearly_known_interior_failure_survives_other_unknown_months():
    m, ws, ds=month()
    ws[0]['source_daily_digest_ids'].remove('cybermed_daily_2026-09-01')
    rows=[{'month':f'2026-{n:02}'} for n in range(1,13) if n!=9]+[m]
    cov=hc.yearly_coverage('cybermed',2026,rows,weekly_rows=ws,daily_rows=ds,as_of='2027-01-15T12:00:00Z')
    assert cov['status']=='incomplete' and not cov['final_year_ready']


def test_actual_monthly_main_blocks_before_generation_and_smtp(tmp_path,monkeypatch,freeze_clock):
    from newsagent2 import main
    from test_cybermed_weekly_fixture_safety import _run_digest_store_report
    freeze_clock('2026-10-02T12:00:00+00:00')
    m, ws, ds=month()
    for w in ws:w['coverage_status']='complete'
    ds=[d for d in ds if d['run_date']!='2026-09-01']
    Path(os.environ['CYBERMED_DAILY_DIGEST_STATE_PATH']).write_text(json.dumps({'digests':ds}))
    monkeypatch.setenv('ROLLUP_MONTH_OVERRIDE','2026-09')
    entry=ep.entry_for('cybermed','monthly',date(2026,10,2),'schedule','real')
    monkeypatch.setattr(ep,'planned_entry',lambda *a: entry)
    def forbidden(*a,**k):raise AssertionError('generation/SMTP must not be reached')
    monkeypatch.setattr(main,'summarize',forbidden)
    with pytest.raises(RuntimeError,match='history_coverage:incomplete'):
        _run_digest_store_report(monkeypatch,tmp_path,report_mode='monthly',digests=ws,send_func=forbidden)


@pytest.mark.parametrize('report', ['cybermed','cyberlurch'])
@pytest.mark.parametrize('mode',['none','test'])
def test_actual_yearly_main_none_test_hash_all_registered_paths(tmp_path,monkeypatch,freeze_clock,report,mode):
    from newsagent2 import main
    from conftest import accepted_sender_stub
    freeze_clock('2027-01-15T12:00:00+00:00')
    source=tmp_path/'protected';source.mkdir()
    paths={k:source/f for k,f in PATHS.items()}
    for k,p in paths.items():
        value={'reports':{}} if k=='ROLLUPS_STATE_PATH' else {'digests':[]} if 'DIGEST' in k else {'editions':{}} if k=='DELIVERY_JOURNAL_PATH' else {}
        if k=='YOUTUBE_CHANNEL_ID_CACHE_PATH':value={'channels':{}}
        if k=='YOUTUBE_TRANSCRIPT_ATTEMPTS_PATH':value={'attempts':{}}
        p.write_text(json.dumps(value));monkeypatch.setenv(k,str(p))
    monkeypatch.setenv('REPORT_KEY',report);monkeypatch.setenv('REPORT_MODE','yearly')
    monkeypatch.setenv('YEAR_IN_REVIEW_YEAR','2026');monkeypatch.setenv('EMAIL_MODE',mode)
    monkeypatch.setenv('GITHUB_EVENT_NAME','workflow_dispatch');monkeypatch.setenv('REPORT_DIR',str(tmp_path/'reports'))
    monkeypatch.setenv('SEND_EMAIL','1');monkeypatch.setattr(main,'send_markdown',accepted_sender_stub)
    monkeypatch.setattr(sys,'argv',['newsagent2-main'])
    before=snapshot(source)
    main.main()
    assert snapshot(source)==before
    text=(tmp_path/'reports'/f'{report}_yearly_review_2026.md').read_text()
    assert 'Teilrückblick' in text and 'nicht enthalten' in text


def test_receipts_export_without_audience_or_smtp_details(tmp_path,monkeypatch):
    from newsagent2.delivery import Coordinator
    paths=files(tmp_path)
    for k,v in paths.items():monkeypatch.setenv(k,str(v))
    monkeypatch.setenv('REPORT_KEY','cybermed');monkeypatch.setenv('REPORT_MODE','daily')
    monkeypatch.setenv('EMAIL_MODE','real');monkeypatch.setenv('DELIVERY_PERIOD_START','2026-09-01');monkeypatch.setenv('DELIVERY_PERIOD_END','2026-09-01')
    coordinator=Coordinator('cybermed','daily',mode='real')
    receipt=coordinator.generated('Synthetic historical edition')
    receipt.status='accepted_by_smtp';receipt.accepted=['a'*64];receipt.state_persisted=False
    coordinator._persist(receipt)
    p=audit('cybermed','2026-09-01','2026-09-30','2026-10-15T12:00:00Z',paths=paths)
    assert p['delivery_editions'][0]['status']=='accepted_by_smtp'
    assert p['delivery_editions'][0]['state_persisted'] is False
    assert 'accepted' not in p['delivery_editions'][0] and 'recovery' not in p['delivery_editions'][0]
    assert 'a'*64 not in json.dumps(p)


@pytest.mark.parametrize('defect', ['orphan','foreign','outside','duplicate'])
def test_cyberlurch_registry_defects_are_visible_without_fabricated_editions(defect):
    row={'month':'2026-09','source_registry':[{'ref_id':'S1','source_date':'2026-09-01'}],'source_refs_used':['S1']}
    if defect=='orphan':row['source_refs_used']=['missing']
    if defect=='foreign':row['source_registry'][0]['report_key']='cybermed'
    if defect=='outside':row['source_registry'][0]['source_date']='2026-08-31'
    if defect=='duplicate':row['source_registry'].append(row['source_registry'][0].copy())
    assert hc.cyberlurch_monthly_coverage(row)['status']=='incomplete'


def test_cyberlurch_valid_registry_does_not_prove_issue_calendar():
    row={'month':'2026-09','source_registry':[{'ref_id':'S1','source_date':'2026-09-01'}],'source_refs_used':['S1']}
    assert hc.cyberlurch_monthly_coverage(row)['status']=='unknown'


def test_cyberlurch_coverage_is_pure_and_optional_sections_cannot_mutate_refs():
    row={'month':'2026-09','source_registry':[{'ref_id':'S1','source_date':'2026-09-01'}],
         'source_refs_used':['S1'],'themes':[{'source_refs':['S1']}]}
    before=json.dumps(row,sort_keys=True)
    hc.cyberlurch_monthly_coverage(row)
    assert json.dumps(row,sort_keys=True)==before


def test_recovery_delta_cannot_bypass_coverage_validation(tmp_path):
    from newsagent2.pre_smtp_recovery import _delta_plan
    row={'report_key':'cybermed','cadence':'monthly','edition_id':'cybermed:monthly:2026-09-01:2026-09-30:production'}
    delta={'kind':'rollups',**row,'mode':'real','generation_validated':True,'production_target':True,
           'base_sha256':None,'removed_record_ids':[],
           'records':[{'month':'2026-09','history_coverage':{'status':'complete'}}]}
    artifact=tmp_path/'delta.json';artifact.write_text(json.dumps(delta))
    with pytest.raises(RuntimeError,match='recovery_delta_invalid_store'):_delta_plan(artifact,row)
    assert not Path(os.environ['ROLLUPS_STATE_PATH']).exists()


def test_as_of_audit_requires_explicit_output_dir_and_never_reports_exception_payload(tmp_path):
    proc=subprocess.run([sys.executable,'-m','newsagent2.maintenance.backfill_digest_stores','--report','cybermed',
         '--from-date','2026-09-01','--to-date','2026-09-30','--as-of','2026-10-15T12:00:00Z'],capture_output=True,text=True)
    assert proc.returncode==2 and 'explicit --output-dir' in proc.stderr


def test_unknown_store_version_stops_without_rewriting(tmp_path):
    paths=files(tmp_path);p=paths['CYBERMED_DAILY_DIGEST_STATE_PATH']
    raw=json.dumps({'schema_version':99,'digests':[daily('2026-09-01')]}).encode();p.write_bytes(raw)
    payload=audit('cybermed','2026-09-01','2026-09-30','2026-10-15T12:00:00Z',paths=paths)
    assert payload['exit_code']==2 and p.read_bytes()==raw
    assert payload['inputs']['CYBERMED_DAILY_DIGEST_STATE_PATH']['status']=='unsupported_existing_schema'


def test_custom_state_directory_cannot_be_used_for_audit_output(tmp_path):
    custom={k:tmp_path/'custom-history-db'/f for k,f in PATHS.items()}
    p=audit('cybermed','2026-09-01','2026-09-30','2026-10-15T12:00:00Z',paths=custom)
    with pytest.raises(ValueError,match='overlaps'):write_report(p,tmp_path/'custom-history-db'/'audit',paths=custom)
    assert not (tmp_path/'custom-history-db').exists()


def test_actual_cyberlurch_monthly_stops_known_reference_defect_before_publication(tmp_path,monkeypatch,freeze_clock):
    from newsagent2 import main
    from test_cyberlurch_monthly_safety import _setup
    target=_setup(tmp_path,monkeypatch)
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    monkeypatch.setenv('ROLLUP_MONTH_OVERRIDE', now.strftime('%Y-%m'))
    freeze_clock(((now.replace(day=28)+timedelta(days=4)).replace(day=6)).isoformat())
    monkeypatch.setenv('GITHUB_EVENT_NAME','workflow_dispatch');monkeypatch.setenv('SEND_EMAIL','1')
    monkeypatch.setattr(hc,'cyberlurch_monthly_coverage',lambda *a:hc.result([('incomplete','orphan_source_reference')]))
    monkeypatch.setattr(main,'send_markdown',lambda *a,**k:pytest.fail('SMTP reached'))
    with pytest.raises(RuntimeError,match='cyberlurch_monthly_history_coverage:incomplete'):main.main()
    assert not target.exists()
