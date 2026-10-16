"""Read-only checks for every newsletter/cadence, including future dependencies.

No dotenv, collection, models, SMTP, lock creation or State writes. Output uses
only edition IDs, dates, counts, hashes and fixed categories. This is a static
preflight, not proof that external providers or an SMTP server are available.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from .aggregate_policy import require_review_sources
from .delivery import load_journal
from .edition_plan import CADENCES, POLICY, entry_for, planned_modes
from .history_coverage import weekly_coverage, monthly_coverage, yearly_coverage, load_inputs
from .run_context import path_for
from .store_io import StoreError, read_store

STO = ZoneInfo('Europe/Stockholm')


def _category(exc):
    if isinstance(exc, StoreError):
        return f'store_error:{exc.kind}:{exc.category}'
    raw = str(exc)
    if raw.startswith(('store_error:', 'edition_', 'operational_')) and all(c.isalnum() or c in '_:' for c in raw):
        return raw
    return 'operational_input_invalid'


def next_entries(report_key, as_of):
    """One next scheduled edition per cadence, never fabricate future sources."""
    from .legacy_transition import FRESH_START
    day = as_of.astimezone(STO).date()
    found = {}
    for offset in range(400):
        candidate = day + timedelta(days=offset)
        for cadence in planned_modes(report_key, candidate):
            if cadence in found:
                continue
            entry = entry_for(report_key, cadence, candidate, 'schedule', 'real')
            # Pre-transition catch-ups have unknown delivery authority. Check
            # new service independently; retain the archive diagnosis below.
            if report_key == 'cybermed' and entry['period_end'] < FRESH_START:
                continue
            found[cadence] = entry
        if len(found) == 4:
            return [found[c] for c in CADENCES]
    raise RuntimeError('operational_calendar_horizon_invalid')


def _configuration(key, cadence):
    from .emailer import _delivery_recipients
    if os.getenv('REPORT_KEY') != key:
        return ['operational_newsletter_scope_mismatch']
    missing = [name for name in ('SMTP_HOST', 'SMTP_USER', 'SMTP_PASS') if not os.getenv(name)]
    categories = ['operational_smtp_configuration_missing'] if missing else []
    try:
        port = int(os.getenv('SMTP_PORT') or '587')
        if not 1 <= port <= 65535:
            raise ValueError()
    except ValueError:
        categories.append('operational_smtp_port_invalid')
    if len(os.getenv('DELIVERY_RECIPIENT_HMAC_KEY', '').encode()) < 32:
        categories.append('operational_recipient_key_missing')
    if (key == 'cyberlurch' or cadence != 'yearly') and not os.getenv('OPENAI_API_KEY'):
        categories.append('operational_model_key_missing')
    try:
        recipients, _ = _delivery_recipients(key, cadence, 'real')
        if not recipients:
            categories.append('operational_recipients_missing')
    except (ValueError, RuntimeError, OSError):
        categories.append('operational_recipients_invalid')
    return categories


def _last_due_daily(key, as_of):
    local = as_of.astimezone(STO)
    # Allow normal runner delay; the 04:00 check assesses yesterday's service.
    day = local.date() if (local.hour, local.minute) >= (7, 40 if key == 'cybermed' else 30) else local.date()-timedelta(days=1)
    for _ in range(14):
        if 'daily' in planned_modes(key, day):
            return day.isoformat()
        day -= timedelta(days=1)
    raise RuntimeError('operational_daily_calendar_invalid')


def last_due_entry(key, cadence, as_of):
    """Only the existing modern-service boundary; absence is not SMTP proof."""
    from .legacy_transition import FRESH_START
    local = as_of.astimezone(STO)
    for offset in range(370):
        day = local.date()-timedelta(days=offset)
        if cadence not in planned_modes(key, day):
            continue
        slot = datetime(day.year, day.month, day.day, 7, 40 if key == 'cybermed' else 30, tzinfo=STO)
        if local < slot:
            continue
        entry = entry_for(key, cadence, day, 'schedule', 'real')
        if entry['period_end'] >= FRESH_START:
            return entry
    return None


def _pending_runs(report_key, journal):
    """Read Actions metadata only; never download/import an artifact here."""
    import requests
    from .legacy_transition import REVIEWED
    from .workflow_delivery import _nonproduction_jobs
    repository, branch, token = (os.getenv(k, '') for k in ('GITHUB_REPOSITORY', 'GITHUB_REF_NAME', 'GITHUB_TOKEN'))
    if not repository or not branch or not token:
        raise RuntimeError('operational_actions_configuration_missing')
    workflow = 'cybermed.yml' if report_key == 'cybermed' else 'newsagent.yml'
    session = requests.Session()
    session.headers.update({'Authorization': 'Bearer '+token, 'Accept': 'application/vnd.github+json'})
    def get(path, **params):
        try:
            response = session.get('https://api.github.com/repos/'+repository+path, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError):
            raise RuntimeError('operational_actions_access_failed') from None
    pending = []
    acknowledged = set(os.getenv('DELIVERY_RECOVERY_ACK_RUN_IDS', '').split(','))
    runs = get('/actions/workflows/'+workflow+'/runs', branch=branch, status='completed', per_page=100)
    for run in runs['workflow_runs']:
        if run['conclusion'] not in {'failure', 'cancelled', 'timed_out', 'action_required'}:
            continue
        if str(run['id']) in acknowledged:
            continue  # Same individually reviewed acknowledgments as recovery.
        reviewed = REVIEWED.get(str(run['id']))
        if (repository == 'Cyberlurch/NewsAgent2' and branch == 'main' and reviewed
            and (report_key, workflow, run.get('head_sha')) == reviewed[:3]):
            continue
        own = [r for r in journal['editions'].values() if r['report_key'] == report_key
               and r['workflow_run_id'] == str(run['id']) and r['edition_id'].endswith(':production')]
        if own and all((r['status'] == 'accepted_by_smtp' and r['state_persisted']) or
                       (r['status'] == 'failed' and not r['generation_validated'] and r['smtp_phase'] == 'not_handed_off') for r in own):
            continue
        jobs = get('/actions/runs/'+str(run['id'])+'/jobs', per_page=100)['jobs']
        capable = [j for j in jobs if any(s.get('name') in {'Run reports', 'Run Cybermed reports'}
            and s.get('started_at') and s.get('conclusion') != 'skipped' for s in j.get('steps', []))]
        if capable and not _nonproduction_jobs(run, capable, report_key):
            pending.append({'run_id': str(run['id']), 'category': 'operational_prior_run_recovery_required'})
    return pending


def assess(report_key, *, as_of, check_config=False, check_actions=False):
    if report_key not in {'cybermed', 'cyberlurch'} or as_of.tzinfo is None:
        raise ValueError('operational_request_invalid')
    result = {'schema_version': 1, 'report_key': report_key, 'as_of': as_of.isoformat(),
              'checks': [], 'inputs': {}, 'historical_delivery': [], 'errors': [], 'pending_recovery_runs': []}
    names = ['STATE_PATH', 'ROLLUPS_STATE_PATH', 'DELIVERY_JOURNAL_PATH']
    names += (['CYBERMED_DAILY_DIGEST_STATE_PATH', 'CYBERMED_WEEKLY_DIGEST_STATE_PATH', 'CYBERMED_PUBMED_INGESTION_PATH']
              if report_key == 'cybermed' else ['CYBERLURCH_DIGEST_STATE_PATH', 'YOUTUBE_CHANNEL_ID_CACHE_PATH',
                  'YOUTUBE_CONTENT_CACHE_PATH', 'YOUTUBE_TRANSCRIPT_ATTEMPTS_PATH'])
    for name in names:
        path = path_for(name)
        try:
            raw = path.read_bytes() if path.exists() else None
            result['inputs'][name] = {'sha256': hashlib.sha256(raw).hexdigest() if raw is not None else None,
                                     'status': 'existing' if raw is not None else 'missing_new_store'}
            if raw is not None:
                read_store(path, {}, kind='operational_input')
        except (StoreError, OSError) as exc:
            result['errors'].append(_category(exc))
    try:
        from .state_manager import load_state
        state = load_state(str(path_for('STATE_PATH')))
        daily, weekly, months = load_inputs(report_key)
        from .maintenance.history_audit import _valid_row
        for kind, rows in [('daily', daily), ('weekly', weekly), ('monthly', months)]:
            if any(not _valid_row(row, kind) for row in rows):
                raise StoreError('operational_'+kind, 'invalid_existing_store')
        journal = load_journal(path_for('DELIVERY_JOURNAL_PATH'))
        videos = []
        if report_key == 'cyberlurch':
            from .youtube_source import digest_valid
            videos = read_store(path_for('CYBERLURCH_DIGEST_STATE_PATH'), {'digests': []},
                                kind='cyberlurch_digests', validate=digest_valid)['digests']
        else:
            from .pubmed_ingestion import load
            load()
    except (StoreError, RuntimeError, ValueError, OSError) as exc:
        result['errors'].append(_category(exc))
        daily, weekly, months, videos, journal, state = [], [], [], [], {'editions': {}}, {}
    for entry in next_entries(report_key, as_of):
        cadence = entry['cadence']
        check = {'cadence': cadence, 'edition_id': entry['edition_id'], 'scheduled_date': entry['logical_date'],
                 'period_start': entry['period_start'], 'period_end': entry['period_end'],
                 'status': 'ready', 'categories': [], 'available_sources': 0, 'known_missing_dates': []}
        try:
            if report_key == 'cybermed' and cadence == 'weekly':
                selected = [r for r in daily if entry['period_start'] <= r.get('run_date', '') <= entry['coverage_end']]
                coverage = weekly_coverage({**entry, 'source_daily_digest_ids': [r.get('digest_id', '') for r in selected]}, daily)
                check['available_sources'] = len(selected)
                check['known_missing_dates'] = sorted(set(coverage['expected']) - set(coverage['observed']))
                check['known_missing_dates'] = [d for d in check['known_missing_dates'] if d < as_of.astimezone(STO).date().isoformat()]
            elif report_key == 'cybermed' and cadence == 'monthly':
                from .cybermed_digest_store import prepare_cybermed_monthly_source_digests
                selected, diagnostic = prepare_cybermed_monthly_source_digests({'digests': weekly}, entry['period_start'][:7])
                if diagnostic['unsafe_boundary_items']:
                    raise RuntimeError('operational_unsafe_month_boundary')
                coverage = monthly_coverage({'month': entry['period_start'][:7], 'calendar_policy': POLICY,
                    'source_weekly_digest_ids': [r['digest_id'] for r in selected]}, weekly, daily)
                check['available_sources'] = len(selected)
                check['known_missing_dates'] = [d for d in sorted(set(coverage['expected']) - set(coverage['observed']))
                    if d < as_of.astimezone(STO).date().isoformat()]
            elif cadence == 'yearly':
                selected = [r for r in months if r.get('month', '').startswith(entry['period_start'][:4]+'-')]
                coverage = yearly_coverage(report_key, int(entry['period_start'][:4]), selected,
                    weekly_rows=weekly, daily_rows=daily, as_of=as_of)
                check['available_sources'] = len(selected)
                if report_key == 'cyberlurch':
                    from .cyberlurch_yearly import build_annual_evidence
                    _, registry, _ = build_annual_evidence(selected, int(entry['period_start'][:4]))
                    check['available_sources'] = len(registry)
            elif report_key == 'cyberlurch' and cadence in {'weekly', 'monthly'}:
                selected = []
                for row in videos:
                    stamp = row.get('published_at', '')
                    if not stamp:
                        continue
                    stamp = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
                    if stamp.tzinfo is None:
                        raise RuntimeError('operational_video_timestamp_invalid')
                    day = stamp.astimezone(STO).date().isoformat()
                    if entry['period_start'] <= day <= entry['period_end']:
                        selected.append(row)
                coverage = {'status': 'complete', 'reasons': [], 'expected': [], 'observed': []}
                check['available_sources'] = len(selected)
            else:
                coverage = {'status': 'complete', 'reasons': [], 'expected': [], 'observed': []}
                check['available_sources'] = len(videos) if cadence != 'daily' else 0
            if cadence != 'daily':
                if check['available_sources']:
                    require_review_sources(coverage, available=check['available_sources'], category='operational_source_chain')
                    check['status'] = 'partial_ready' if coverage['status'] != 'complete' else 'ready'
                else:
                    check['status'] = 'awaiting_inputs'
                    if entry['period_end'] < as_of.astimezone(STO).date().isoformat():
                        check['status'] = 'blocked'
                        check['categories'].append('operational_closed_period_has_no_sources')
                check['coverage'] = coverage
                check['source_status'] = check['status']
                if cadence in {'monthly', 'yearly'} and entry['logical_date'] > as_of.astimezone(STO).date().isoformat() and check['status'] != 'blocked':
                    check['status'] = 'awaiting_close'
            row = journal['editions'].get(entry['edition_id'])
            if row:
                check['delivery_status'] = row['status']
                if row['status'] in {'outcome_unknown', 'partially_accepted'}:
                    check['status'] = 'blocked'
                    check['categories'].append('operational_delivery_reconciliation_required')
                elif row['status'] == 'accepted_by_smtp':
                    check['status'] = 'already_accepted' if row['state_persisted'] else 'recovery_required'
                elif row['status'] == 'failed' and row['generation_validated']:
                    check['status'] = 'recovery_required'
                    check['categories'].append('operational_saved_generation_requires_review')
            due = last_due_entry(report_key, cadence, as_of)
            if cadence != 'daily' and due:
                prior = journal['editions'].get(due['edition_id'])
                check['last_due_edition'] = due['edition_id']
                if not prior or prior['status'] != 'accepted_by_smtp' or not prior['state_persisted']:
                    check['status'] = 'recovery_required'
                    check['categories'].append('operational_due_edition_not_finalized')
            if cadence == 'daily':
                accepted = [r['period_end'] for r in journal['editions'].values() if r['report_key'] == report_key
                    and r['cadence'] == 'daily' and r['edition_id'].endswith(':production')
                    and r['status'] == 'accepted_by_smtp' and r['state_persisted']]
                marker = state.get('last_successful_daily_run_utc_by_report', {}).get(report_key)
                if marker:
                    stamp = datetime.fromisoformat(marker.replace('Z', '+00:00'))
                    accepted.append(stamp.astimezone(STO).date().isoformat())
                    check['marker_is_not_smtp_proof'] = True
                due = _last_due_daily(report_key, as_of)
                check['last_daily_due'] = due
                check['last_persisted_daily_date'] = max(accepted) if accepted else None
                if accepted and max(accepted) < due:
                    check['status'] = 'recovery_required'
                    check['categories'].append('operational_daily_publication_stale')
            if check_config:
                check['categories'].extend(_configuration(report_key, cadence))
            if (check['categories'] and check['status'] != 'recovery_required') or result['errors']:
                check['status'] = 'blocked'
        except (StoreError, RuntimeError, ValueError, OSError) as exc:
            check['status'] = 'blocked'
            check['categories'].append(_category(exc))
        result['checks'].append(check)
    from .legacy_transition import REVIEWED
    result['historical_delivery'] = [{'run_id': run, 'finding': row[4], 'automatic_resend': False}
        for run, row in REVIEWED.items() if row[0] == report_key]
    from .legacy_transition import FRESH_START
    result['unresolved_delivery'] = [{'edition_id': r['edition_id'], 'status': r['status']}
        for r in journal['editions'].values() if r['report_key'] == report_key
        and r['edition_id'].endswith(':production') and r['period_end'] >= FRESH_START
        and (r['status'] in {'outcome_unknown', 'partially_accepted'} or
             (r['status'] == 'accepted_by_smtp' and not r['state_persisted']))]
    if check_actions:
        try:
            result['pending_recovery_runs'] = _pending_runs(report_key, journal)
        except (RuntimeError, KeyError, TypeError) as exc:
            result['errors'].append(_category(exc))
    for name, preimage in result['inputs'].items():
        path = path_for(name)
        raw = path.read_bytes() if path.exists() else None
        if (hashlib.sha256(raw).hexdigest() if raw is not None else None) != preimage['sha256']:
            result['errors'].append('operational_input_changed_during_check')
    result['ok'] = (not result['errors'] and not result['unresolved_delivery'] and not result['pending_recovery_runs']
        and not any(r['status'] in {'blocked', 'recovery_required'} for r in result['checks']))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report-key', choices=['cybermed', 'cyberlurch'], required=True)
    parser.add_argument('--as-of')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--check-config', action='store_true')
    parser.add_argument('--check-actions', action='store_true', help='Read Actions metadata using the scoped runner token')
    args = parser.parse_args()
    now = datetime.fromisoformat(args.as_of.replace('Z', '+00:00')) if args.as_of else datetime.now(STO)
    from .maintenance.reviewed_backfill import external_output
    output = external_output(args.output, target=path_for('STATE_PATH'), sources={})
    report = assess(args.report_key, as_of=now, check_config=args.check_config, check_actions=args.check_actions)
    from .store_io import atomic_text
    atomic_text(output, json.dumps(report, sort_keys=True, indent=2)+'\n')
    lines = [f"## {args.report_key}: all-cadence preflight", '', '| Cadence | Next date | Status |', '|---|---|---|']
    lines += [f"| {r['cadence']} | {r['scheduled_date']} | {r['status']} |" for r in report['checks']]
    lines += ['', '**Overall: '+('pass' if report['ok'] else 'action required')+'**']
    for row in report['checks']:
        if row['categories']:
            lines.append('- '+row['cadence']+': '+', '.join(row['categories']))
        if row['known_missing_dates']:
            lines.append('- '+row['cadence']+' partial source dates missing: '+', '.join(row['known_missing_dates']))
    for row in report['pending_recovery_runs']:
        lines.append('- Recovery required for run '+row['run_id'])
    for row in report['unresolved_delivery']:
        lines.append('- Reconcile '+row['edition_id']+': '+row['status'])
    lines += ['- '+category for category in report['errors']]
    if report['historical_delivery']:
        lines.append('- Reviewed pre-journal history remains archived; no automatic historical resend.')
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
            stream.write('\n'.join(lines)+'\n')
    print(json.dumps({'report_key': args.report_key, 'ok': report['ok'],
                      'cadences': {r['cadence']: r['status'] for r in report['checks']}}, sort_keys=True))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
