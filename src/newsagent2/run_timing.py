"""Safe observed timestamps, separate from receipt authority and content."""
from datetime import datetime, timezone
import os
from pathlib import Path
from .store_io import atomic_store, read_store

EVENTS=('application_started_at','generated_at','smtp_accepted_at','state_persisted_at','git_publication_observed_at')


def record(key,cadence,event=None,*,outcome=None,delivery_status=None,edition_id=None):
    if not os.getenv('NEWSAGENT_PLAN_PATH'): return
    from .edition_plan import load_plan, planned_entry, instant
    plan=load_plan()
    if edition_id is not None:
        matches=[entry for entry in plan['entries'] if entry['edition_id']==edition_id]
        if not matches: return  # Original receipt reconciliation is not this run's generation.
        entry=matches[0]
    else:
        entry=planned_entry(key,cadence)
    directory=Path(os.getenv('REPORT_DIR') or 'out')
    path=directory/f'{key}_{cadence}_timing_metadata.json'
    value=read_store(path,{},kind='run_timing')
    if not value:
        value={'schema_version':1,'report_key':key,'cadence':cadence,'edition_id':entry['edition_id'],
            'logical_date':entry['logical_date'],'period_start':entry['period_start'],'period_end':entry['period_end'],
            'coverage_end':entry['coverage_end'],'expected_slot':plan['expected_slot'],
            'slot_source':plan['slot_source'],'slot_certainty':plan['slot_certainty'],
            'workflow_run_id':plan['workflow_run_id'],'run_created_at':plan['run_created_at'],
            'job_started_at':plan['job_started_at'],**dict.fromkeys(EVENTS),'outcome':'not_started','delivery_status':None}
    if value.get('edition_id')!=entry['edition_id']: raise RuntimeError('timing_edition_conflict')
    if event:
        if event not in EVENTS: raise ValueError('timing_event_invalid')
        if not value[event]: value[event]=datetime.now(timezone.utc).isoformat()
    if outcome:
        if outcome not in {'generated','intentionally_skipped','accepted_by_smtp','partially_accepted','failed','outcome_unknown','already_accepted','blocked_basis'}:
            raise ValueError('timing_outcome_invalid')
        value['outcome']=outcome
    if delivery_status is not None:
        from .delivery import STATUSES
        if delivery_status not in STATUSES: raise ValueError('timing_delivery_status_invalid')
        value['delivery_status']=delivery_status
    pairs={'inferred_start_lateness_seconds':('expected_slot','run_created_at'),
           'run_to_job_seconds':('run_created_at','job_started_at'),
           'job_to_application_seconds':('job_started_at','application_started_at'),
           'state_to_publication_seconds':('state_persisted_at','git_publication_observed_at'),
           'application_to_generation_seconds':('application_started_at','generated_at'),
           'generation_to_smtp_seconds':('generated_at','smtp_accepted_at'),
           'smtp_to_state_seconds':('smtp_accepted_at','state_persisted_at')}
    value['latencies']={label:(instant(value[end])-instant(value[start])).total_seconds() if value.get(start) and value.get(end) else None
                       for label,(start,end) in pairs.items()}
    value['late_start_warning']=bool(value['latencies']['inferred_start_lateness_seconds'] is not None and
                                     value['latencies']['inferred_start_lateness_seconds']>900)
    atomic_store(path,value,kind='run_timing')


def publication(report_dir,category):
    if not os.getenv('NEWSAGENT_PLAN_PATH'): return
    from .edition_plan import load_plan
    plan=load_plan()
    for entry in plan['entries']:
        if category=='git_persisted': record(entry['report_key'],entry['cadence'],'git_publication_observed_at')
    atomic_store(report_dir/'git_timing_metadata.json',{'publication_outcome':category,
        'observed_at':datetime.now(timezone.utc).isoformat()},kind='publication_timing')


def write_plan_failure(key,category="edition_plan_invalid_or_metadata_unavailable"):
    path=Path(os.getenv('REPORT_DIR') or 'out')/f'{key}_plan_failure_metadata.json'
    if path.exists() and category=="workflow_failed_before_plan": return
    atomic_store(path,
        {'report_key':key,'outcome':'failed','category':category,
         'observed_at':datetime.now(timezone.utc).isoformat()},kind='plan_failure')
