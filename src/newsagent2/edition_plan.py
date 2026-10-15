"""Frozen workflow plans; edition identity remains owned by delivery.py."""
from __future__ import annotations

import argparse
import calendar
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from .run_context import path_for, email_mode
from .store_io import atomic_store, read_store
from .swedish_calendar import is_swedish_no_send_day, next_swedish_business_day, previous_swedish_business_day

STO = ZoneInfo('Europe/Stockholm')
POLICY = 'stockholm-calendar-v2'
CADENCES = ('daily', 'weekly', 'monthly', 'yearly')


def instant(raw):
    value = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('edition_timestamp_requires_timezone')
    return value


def monthly_due(year, month):
    first = date(year, month, 1)
    friday = first + timedelta(days=(4-first.weekday()) % 7)
    return next_swedish_business_day(friday) if is_swedish_no_send_day(friday) else friday


def weekly_dates(day):
    monday = day - timedelta(days=day.weekday())
    friday = monday + timedelta(days=4)
    last = previous_swedish_business_day(friday + timedelta(days=1)) if is_swedish_no_send_day(friday) else friday
    return monday, friday, last


def planned_modes(key, day):
    if key == 'cyberlurch':
        modes = ['monthly'] if day.day == 1 else []
        if day.month == 1 and day.day == 1: modes.append('yearly')
        if day.weekday() < 5: modes.append('daily')
        if day.weekday() == 4: modes.append('weekly')
        return modes
    if is_swedish_no_send_day(day): return []
    modes = ['daily']
    if day == weekly_dates(day)[2]: modes.append('weekly')
    if day >= monthly_due(day.year, day.month):
        modes.append('monthly')  # Bounded to the immediately preceding month.
        if day.month == 1: modes.append('yearly')
    return modes


def entry_for(key, cadence, day, trigger, mode, month_override='', year_override=''):
    # Monthly/yearly manual defaults are retained; Weekly shares one identity.
    coverage = day
    if cadence == 'daily': start = end = day
    elif cadence == 'weekly':
        if key == 'cybermed':
            start, end, closing = weekly_dates(day)
            coverage = min(day,closing)
        else: start, end = day-timedelta(days=7), day
    elif cadence == 'monthly':
        previous = trigger == 'schedule'
        first = day.replace(day=1)
        start = date.fromisoformat(month_override+'-01') if month_override else (
            (first-timedelta(days=1)).replace(day=1) if previous else first)
        end = start.replace(day=calendar.monthrange(start.year,start.month)[1]); coverage = end
    else:
        year = int(year_override) if year_override else day.year-(trigger=='schedule')
        start, end = date(year,1,1), date(year,12,31); coverage = end
    purpose = 'production' if mode == 'real' else mode
    return dict(report_key=key, cadence=cadence, logical_date=day.isoformat(), period_start=start.isoformat(),
                period_end=end.isoformat(), coverage_end=coverage.isoformat(), calendar_policy=POLICY,
                edition_id=f'{key}:{cadence}:{start}:{end}:{purpose}')


def make_plan(key, *, created_at, job_started_at, trigger, request='scheduled', mode='none', logical_date='',
              month_override='', year_override='', run_id='local', schedule=''):
    if key not in {'cybermed','cyberlurch'} or trigger not in {'schedule','workflow_dispatch','manual'} or mode not in {'real','none','test'}:
        raise ValueError('edition_plan_configuration_invalid')
    created, job = instant(created_at), instant(job_started_at)
    if job < created: raise ValueError("edition_job_precedes_run_creation")
    day = date.fromisoformat(logical_date) if logical_date else created.astimezone(STO).date()
    if mode=='real' and day > created.astimezone(STO).date(): raise ValueError('edition_future_logical_date')
    slot = datetime.combine(day, time(5,40 if key=='cybermed' else 30), STO)
    source = 'explicit_logical_date' if logical_date else ('inferred_run_creation_date' if trigger=='schedule' else 'manual_run_creation_date')
    if trigger=='schedule':
        expected_cron = '40 5 * * *' if key=='cybermed' else '30 5 * * *'
        if schedule and schedule != expected_cron: raise ValueError('edition_schedule_unrecognized')
        if not logical_date and created < slot:
            raise ValueError('edition_slot_ambiguous_explicit_date_required')
    if request != 'scheduled' and request != 'all' and request not in CADENCES:
        raise ValueError('edition_cadence_invalid')
    modes = planned_modes(key,day) if request=='scheduled' else list(CADENCES) if request=='all' else [request]
    entries = [entry_for(key,cadence,day,trigger,mode,month_override,year_override) for cadence in modes]
    return dict(schema_version=1, report_key=key, trigger=trigger, mode=mode, workflow_run_id=str(run_id),
                logical_date=day.isoformat(), slot_source=source, slot_certainty='explicit' if logical_date else 'inferred' if trigger=='schedule' else 'not_scheduled',
                expected_slot=slot.isoformat() if trigger=='schedule' or logical_date else None,
                run_created_at=created.isoformat(), job_started_at=job.isoformat(), entries=entries,
                already_finalized_editions=[],reason='' if entries else 'calendar_no_op')


def validate_plan(value):
    try:
        if set(value) != {'schema_version','report_key','trigger','mode','workflow_run_id','logical_date','slot_source','slot_certainty','expected_slot','run_created_at','job_started_at','entries','already_finalized_editions','reason'}: return False
        if value['schema_version'] != 1 or value['report_key'] not in {'cybermed','cyberlurch'}: return False
        key, day = value['report_key'], date.fromisoformat(value['logical_date'])
        if value['mode'] not in {'real','none','test'} or value['trigger'] not in {'schedule','manual','workflow_dispatch'}: return False
        if value['slot_source'] not in {'explicit_logical_date','inferred_run_creation_date','manual_run_creation_date'}: return False
        if value['slot_certainty'] not in {'explicit','inferred','not_scheduled'}: return False
        if not (value['workflow_run_id']=='local' or str(value['workflow_run_id']).isdigit()): return False
        for field in ('run_created_at','job_started_at'): instant(value[field])
        if value['expected_slot']: instant(value['expected_slot'])
        if not isinstance(value['entries'],list) or len(value['entries'])>4: return False
        seen = set()
        for entry in value['entries']:
            if set(entry) != {'report_key','cadence','logical_date','period_start','period_end','coverage_end','calendar_policy','edition_id'}: return False
            cadence = entry['cadence']; start,end,coverage=(date.fromisoformat(entry[k]) for k in ('period_start','period_end','coverage_end'))
            if cadence not in CADENCES or cadence in seen or entry['report_key']!=key or entry['logical_date']!=day.isoformat(): return False
            seen.add(cadence)
            if start>end or not start<=coverage<=end or entry['calendar_policy']!=POLICY: return False
            purpose='production' if value['mode']=='real' else value['mode']
            if entry['edition_id']!=f'{key}:{cadence}:{start}:{end}:{purpose}': return False
            rebuilt=entry_for(key,cadence,day,value['trigger'],value['mode'],start.isoformat()[:7] if cadence=='monthly' else '',str(start.year) if cadence=='yearly' else '')
            if entry != rebuilt: return False
        if not isinstance(value['already_finalized_editions'],list) or any(not isinstance(v,str) or not v.startswith(key+':') for v in value['already_finalized_editions']): return False
        return value['reason'] in {'','calendar_no_op','already_finalized_no_op'}
    except (KeyError,TypeError,ValueError): return False


def load_plan(path=None):
    location=path or os.getenv('NEWSAGENT_PLAN_PATH')
    if not location: return None
    plan=read_store(Path(location),{},kind='edition_plan',validate=validate_plan)
    if plan['mode'] != email_mode() or plan['workflow_run_id'] != os.getenv('GITHUB_RUN_ID','local'):
        raise RuntimeError('edition_plan_execution_binding_mismatch')
    return plan


def planned_entry(key,cadence):
    plan=load_plan()
    if plan is None: return None
    if plan['report_key']!=key: raise RuntimeError('edition_plan_newsletter_mismatch')
    matches=[e for e in plan['entries'] if e['cadence']==cadence]
    if len(matches)!=1: raise RuntimeError('edition_not_planned')
    return matches[0]


@contextmanager
def bind_entry(key,cadence):
    entry=planned_entry(key,cadence)
    if entry is None and os.getenv('NEWSAGENT_LOGICAL_DATE'):
        raise RuntimeError('logical_date_requires_validated_plan')
    changes={}
    if entry:
        changes={'NEWSAGENT_LOGICAL_DATE':entry['logical_date'], 'DELIVERY_PERIOD_START':entry['period_start'], 'DELIVERY_PERIOD_END':entry['period_end']}
        if cadence=='monthly': changes['ROLLUP_MONTH_OVERRIDE']=entry['period_start'][:7]
        if cadence=='yearly': changes['YEAR_IN_REVIEW_YEAR']=entry['period_start'][:4]
        if key=='cybermed' and cadence=='weekly':
            changes.update(CYBERMED_WEEKLY_PERIOD_START=entry['period_start'],CYBERMED_WEEKLY_PERIOD_END=entry['period_end'],CYBERMED_WEEKLY_COVERAGE_END=entry['coverage_end'])
        for name in ('DELIVERY_PERIOD_START','DELIVERY_PERIOD_END'):
            if os.getenv(name) and os.environ[name]!=changes[name]: raise RuntimeError('edition_period_override_conflict')
    old={name:os.environ.get(name) for name in changes}
    os.environ.update(changes)
    try: yield entry
    finally:
        for name,value in old.items():
            if value is None: os.environ.pop(name,None)
            else: os.environ[name]=value


def required_daily_dates(start,end,*,policy=POLICY):
    result=[];cursor=date.fromisoformat(start);last=date.fromisoformat(end)
    while cursor<=last:
        if not (is_swedish_no_send_day(cursor) if policy==POLICY else cursor.weekday()>=5): result.append(cursor.isoformat())
        cursor+=timedelta(days=1)
    return result


def check_ready(entry):
    """Read-only under the existing writer lock, before model generation."""
    if not entry or entry['report_key']!='cybermed' or entry['cadence']=='daily': return
    from .cybermed_digest_store import load_cybermed_daily_digest_store, load_cybermed_weekly_digest_store, prepare_cybermed_monthly_source_digests
    cadence=entry['cadence']
    if cadence=='weekly':
        if entry['coverage_end'] != weekly_dates(date.fromisoformat(entry['period_start']))[2].isoformat():
            raise RuntimeError('edition_weekly_not_closed_preview_only')
        from .delivery import load_journal
        for previous in load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'].values():
            if (previous['report_key']=='cybermed' and previous['cadence']=='weekly'
                and previous['edition_id'].endswith(':production') and previous['edition_id']!=entry['edition_id']
                and previous['period_start']<=entry['period_end'] and previous['period_end']>=entry['period_start']):
                raise RuntimeError('edition_other_week_identity_requires_review')
        rows=load_cybermed_daily_digest_store(str(path_for('CYBERMED_DAILY_DIGEST_STATE_PATH')))['digests']
        expected=required_daily_dates(entry['period_start'],entry['coverage_end'])
        found=[row.get('run_date') for row in rows if row.get('run_date') in expected]
        if sorted(found)!=expected: raise RuntimeError('edition_weekly_daily_coverage_incomplete')
        existing=load_cybermed_weekly_digest_store(str(path_for('CYBERMED_WEEKLY_DIGEST_STATE_PATH')))['digests']
        for row in existing:
            if row['period_start']==entry['period_start'] and row['period_end']!=entry['period_end']:
                raise RuntimeError('edition_legacy_shifted_week_requires_review')
    elif cadence=='monthly':
        store=load_cybermed_weekly_digest_store(str(path_for('CYBERMED_WEEKLY_DIGEST_STATE_PATH')))
        _,diag=prepare_cybermed_monthly_source_digests(store,entry['period_start'][:7])
        if not diag['coverage_complete']: raise RuntimeError('edition_monthly_closing_week_incomplete')
        # New policy weeks need explicit full business-day coverage. Legacy
        # closing weeks require actual Daily dates; no invented v2 metadata.
        keys=diag['expected_weekly_periods']; closing=keys[-1] if keys else ''
        for row in store['digests']:
            label=row['period_start']+'..'+row['period_end']
            if label not in keys: continue
            if row.get('calendar_policy')==POLICY:
                expected_end=weekly_dates(date.fromisoformat(row['period_start']))[2].isoformat()
                if row.get('coverage_end')!=expected_end or row.get('coverage_status')!='complete':
                    raise RuntimeError('edition_monthly_week_coverage_not_final')
            if row.get('calendar_policy')==POLICY or label==closing:
                expected=required_daily_dates(row['period_start'],row.get('coverage_end',row['period_end']),policy=row.get('calendar_policy','legacy'))
                if not set(expected)<=set(row.get('source_run_dates',[])):
                    raise RuntimeError('edition_monthly_daily_provenance_incomplete')
    else:
        from .rollups import load_rollups_state_readonly
        store=load_rollups_state_readonly(str(path_for('ROLLUPS_STATE_PATH')))
        december=entry['period_start'][:4]+'-12'
        if not any(row.get('month')==december for row in store.get('reports',{}).get('cybermed',[])):
            raise RuntimeError('edition_yearly_december_monthly_missing')

    if cadence in {'monthly', 'yearly'}:
        from .history_coverage import load_inputs, monthly_coverage, yearly_coverage, require_complete
        daily_rows, weekly_rows, months = load_inputs('cybermed')
        if cadence == 'monthly':
            selected, _ = prepare_cybermed_monthly_source_digests({'digests': weekly_rows}, entry['period_start'][:7])
            coverage = monthly_coverage({'month': entry['period_start'][:7], 'calendar_policy': entry['calendar_policy'],
                'source_weekly_digest_ids': [r['digest_id'] for r in selected]}, weekly_rows, daily_rows)
        else:
            assessed = yearly_coverage('cybermed', int(entry['period_start'][:4]), months, weekly_rows=weekly_rows,
                daily_rows=daily_rows, as_of=datetime.combine(date.fromisoformat(entry['logical_date']), time(5,40), STO))
            coverage = assessed
            if not assessed['final_year_ready']:
                raise RuntimeError('edition_yearly_history_not_final:'+assessed['status'])
        require_complete(coverage, 'edition_'+cadence+'_history_coverage')


def github_created_at():
    import requests
    try:
        repository=os.environ['GITHUB_REPOSITORY'];run_id=os.environ['GITHUB_RUN_ID']
        if repository.count('/')!=1 or not run_id.isdigit(): raise ValueError()
        response=requests.get('https://api.github.com/repos/'+repository+'/actions/runs/'+run_id,
            headers={'Authorization':'Bearer '+os.environ['GITHUB_TOKEN'],'Accept':'application/vnd.github+json'},timeout=20)
        response.raise_for_status()
        return instant(response.json()['created_at']).isoformat()
    except Exception:
        raise RuntimeError('edition_run_metadata_unavailable') from None


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report-key',choices=['cybermed','cyberlurch'],required=True)
    parser.add_argument('--created-at')
    parser.add_argument('--job-started-at',default=os.getenv('NEWSAGENT_JOB_STARTED_AT'))
    parser.add_argument('--logical-date',default=os.getenv('NEWSAGENT_REQUESTED_DATE',''))
    parser.add_argument('--output',default=os.getenv('NEWSAGENT_PLAN_PATH','out/plan_metadata.json'))
    args=parser.parse_args()
    from .run_timing import write_plan_failure
    try:
        trigger=os.getenv('GITHUB_EVENT_NAME','manual');now=datetime.now(timezone.utc).isoformat()
        created=args.created_at or (github_created_at() if os.getenv('GITHUB_RUN_ID') else now)
        plan=make_plan(args.report_key,created_at=created,job_started_at=args.job_started_at or now,trigger=trigger,
            request='scheduled' if trigger=='schedule' else os.getenv('REPORT_MODE','daily'),mode=os.getenv('EMAIL_MODE','none'),
            logical_date=args.logical_date,month_override=os.getenv('ROLLUP_MONTH_OVERRIDE',''),year_override=os.getenv('YEAR_IN_REVIEW_YEAR',''),
            run_id=os.getenv('GITHUB_RUN_ID','local'),schedule=os.getenv('NEWSAGENT_TRIGGER_SCHEDULE',''))
        # Already finalized recurring closes need no model/SMTP retry. Journal
        # read is strict, and sending still rechecks under the writer lock.
        if trigger=='schedule' and plan['mode']=='real':
            from .delivery import load_journal
            journal=load_journal(path_for('DELIVERY_JOURNAL_PATH'))
            completed=[e for e in plan['entries'] if e['cadence'] in {'monthly','yearly'} and
                journal['editions'].get(e['edition_id'],{}).get('status')=='accepted_by_smtp' and
                journal['editions'][e['edition_id']].get('state_persisted')]
            plan['already_finalized_editions']=[e['edition_id'] for e in completed]
            plan['entries']=[e for e in plan['entries'] if e not in completed]
            if not plan['entries']: plan['reason']='already_finalized_no_op' if completed else 'calendar_no_op'
        atomic_store(Path(args.output),plan,kind='edition_plan',validate=validate_plan)
        modes=' '.join(e['cadence'] for e in plan['entries'])
        outputs={'modes':modes,'run_modes':modes,'skip':str(not bool(modes)).lower(),'run_skip':str(not bool(modes)).lower(),'run_which_report':args.report_key}
        if os.getenv('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'],'a') as file:
                for k,v in outputs.items(): file.write(f'{k}={v}\n')
        print(json.dumps({'report_key':args.report_key,'modes':modes,'reason':plan['reason'],'slot_source':plan['slot_source']}))
    except Exception as exc:
        known={'edition_slot_ambiguous_explicit_date_required','edition_schedule_unrecognized','edition_run_metadata_unavailable','edition_cadence_invalid','edition_job_precedes_run_creation','edition_future_logical_date'}
        category=str(exc) if str(exc) in known else 'edition_plan_invalid'
        write_plan_failure(args.report_key,category)
        raise RuntimeError(category+'; inspect plan metadata and supply an explicit logical_date for reviewed manual recovery') from None


if __name__=='__main__': main()
