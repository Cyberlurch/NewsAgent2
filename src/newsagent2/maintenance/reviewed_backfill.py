"""One-store, insert-only Cybermed maintenance publication. Never SMTP/generation.

Review hashes bind a frozen candidate, source bytes, target pre/postimages and paths.
Backups are private; public plans contain only candidate records, never whole stores.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from ..history_coverage import days, coverage_fields_valid
from ..run_context import PATHS, path_for, writer_lock
from ..store_io import read_store, digest_store, atomic_text, StoreError
from .history_audit import _valid_row

BINDINGS = {'daily': 'CYBERMED_DAILY_DIGEST_STATE_PATH', 'weekly': 'CYBERMED_WEEKLY_DIGEST_STATE_PATH'}


class ReviewError(RuntimeError):
    """Only fixed categories, never exception payloads or source content."""


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def plan_hash(plan):
    return sha(canonical({k: v for k, v in plan.items() if k != 'plan_sha256'}))


def now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def direct_path(path):
    """Reject symlinks, including parent components, before resolving bindings."""
    if ".." in Path(path).parts:
        raise ReviewError("review_path_traversal")
    target = Path(os.path.abspath(path))
    if any(p.is_symlink() for p in (target, *target.parents)):
        raise ReviewError('review_path_symlink')
    return target


def external_output(path, *, target, sources):
    output = direct_path(path)
    protected = [direct_path(path_for(k)) for k in PATHS] + [direct_path(target)]
    source_files = [direct_path(p) for p in sources.values()]
    roots = [p.parent for p in protected]
    roots += [direct_path(p).parent for role,p in sources.items() if role == 'daily']
    if output in protected+source_files or any(output.is_relative_to(p) for p in roots):
        raise ReviewError('review_output_overlaps_inputs')
    return output


def read_input(path):
    target = direct_path(path)
    try:
        raw = target.read_bytes()
    except FileNotFoundError:
        return None, {'schema_version': 1, 'digests': []}
    except OSError:
        raise ReviewError('review_input_unreadable') from None
    value = read_store(target, {}, kind='reviewed_backfill', validate=digest_store)
    version = value.get('schema_version',value.get('version',1))
    if type(version) is not int or version != 1:
        raise ReviewError('review_store_schema_unsupported')
    try:
        if target.read_bytes() != raw:
            raise ReviewError('review_input_changed')
    except OSError:
        raise ReviewError('review_input_unreadable') from None
    return raw, value


def encoded(value, preimage):
    compact = preimage is not None and b'\n ' not in preimage
    return (json.dumps(value, ensure_ascii=False, indent=None if compact else 2,
                       separators=(',', ':') if compact else None, allow_nan=False)+'\n').encode()


def validate_rows(rows, cadence):
    ids, periods = set(), set()
    for row in rows:
        if not _valid_row(row, cadence) or not row.get('digest_id'):
            raise ReviewError('review_record_invalid')
        if row.get('report_key', 'cybermed') != 'cybermed' or row.get('cadence', cadence) != cadence:
            raise ReviewError('review_foreign_record')
        identity = row['digest_id']
        period = row['run_date'] if cadence == 'daily' else (row['period_start'], row['period_end'])
        if identity in ids or period in periods:
            raise ReviewError('review_duplicate_id_or_period')
        ids.add(identity); periods.add(period)


def validate_candidate(row, cadence, start, end):
    validate_rows([row], cadence)
    if row.get('report_key') != 'cybermed' or row.get('cadence') != cadence:
        raise ReviewError('review_candidate_scope_invalid')
    a = row['run_date'] if cadence == 'daily' else row['period_start']
    b = row['run_date'] if cadence == 'daily' else row['period_end']
    if not start <= a <= b <= end:
        raise ReviewError('review_candidate_outside_period')
    expected = 'cybermed_daily_'+a if cadence == 'daily' else f'cybermed_weekly_{a}_{b}'
    if row['digest_id'] != expected:
        raise ReviewError('review_candidate_identity_invalid')
    marker = row.get('reconstructed_at')
    try:
        parsed = datetime.fromisoformat(marker.replace('Z', '+00:00'))
        if parsed.tzinfo is None or row.get('generated_at_utc') != marker:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise ReviewError('review_reconstruction_timestamp_invalid') from None
    if not coverage_fields_valid(row):
        raise ReviewError('review_coverage_invalid')
    from ..cybermed_quality import assert_no_generation_error_text
    assert_no_generation_error_text(row, boundary='reviewed_candidate')
    def inspect(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key).lower() in {'abstract', 'full_text', 'transcript', 'recipients', 'accepted', 'rejected',
                        'smtp_host', 'smtp_password', 'sent', 'state_persisted', 'delivery_status'}:
                    raise ReviewError('review_private_or_raw_source_field')
                inspect(child)
        elif isinstance(value, list):
            for child in value: inspect(child)
    inspect(row)


def materialize(store, candidates, cadence):
    validate_rows(store['digests'], cadence)
    result = copy.deepcopy(store)
    actions = []
    for candidate in candidates:
        identity = candidate['digest_id']
        same = [r for r in result['digests'] if r['digest_id'] == identity]
        if same:
            actions.append({'id': identity, 'action': 'preserve_existing', 'fields': []})
            continue  # Valid-empty and nonempty both stay byte-for-byte in the view.
        period_keys = ('run_date',) if cadence == 'daily' else ('period_start', 'period_end')
        if any(any(r[k] == candidate[k] for k in period_keys) for r in result['digests']):
            raise ReviewError('review_competing_period')
        result['digests'].append(copy.deepcopy(candidate))
        actions.append({'id': identity, 'action': 'insert', 'fields': sorted(candidate)})
    return result, actions


def _scope(cadence, target, sources):
    if cadence not in BINDINGS or set(sources) != ({'daily'} if cadence == 'weekly' else {'candidate'}):
        raise ReviewError('review_scope_invalid')
    target = direct_path(target)
    if any(target == direct_path(path_for(k)) for k in PATHS if k != BINDINGS[cadence]):
        raise ReviewError('review_target_is_unrelated_store')
    if any(target == direct_path(p) for p in sources.values()):
        raise ReviewError('review_target_is_source')
    return target


def build_plan(cadence, candidates, *, target, sources, start, end, created_at=None):
    days(start, end)
    if len(candidates) > 53 or (cadence == 'daily' and len(candidates) != 1):
        raise ReviewError('review_candidate_count_invalid')
    target = _scope(cadence, target, sources)
    validate_rows(candidates, cadence)
    for row in candidates: validate_candidate(row, cadence, start, end)
    source_bindings = {}
    for role, source in sources.items():
        raw, _ = read_input(source)
        if raw is None: raise ReviewError('review_source_missing')
        source_bindings[role] = {'path': str(direct_path(source)), 'sha256': sha(raw)}
    raw, store = read_input(target)
    updated, actions = materialize(store, candidates, cadence)
    output = encoded(updated, raw) if any(a['action'] == 'insert' for a in actions) else raw
    plan = {'schema_version': 1, 'purpose': 'cybermed_insert_only_backfill', 'report_key': 'cybermed',
            'cadence': cadence, 'from_date': start, 'to_date': end, 'created_at': created_at or now(),
            'target': {'path': str(target), 'binding': BINDINGS[cadence],
                       'pre_sha256': sha(raw) if raw is not None else None,
                       'post_sha256': sha(output) if output is not None else None},
            'sources': source_bindings, 'candidate_sha256': sha(canonical(candidates)),
            'candidates': copy.deepcopy(candidates), 'actions': actions,
            'generation_only': True, 'smtp_authorized': False}
    plan['plan_sha256'] = plan_hash(plan)
    return plan


def write_plan(plan, output):
    output = external_output(output, target=plan['target']['path'], sources={k:v['path'] for k,v in plan['sources'].items()})
    atomic_text(output, json.dumps(plan, ensure_ascii=False, indent=2, allow_nan=False)+'\n')


def _private_write(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())


def _backup(path, plan, raw):
    backup = direct_path(path)
    checkout = Path(__file__).resolve().parents[3]
    roots = [checkout, *[direct_path(path_for(k)).parent for k in PATHS], Path(plan['target']['path']).parent]
    if any(backup.is_relative_to(p) for p in roots):
        raise ReviewError('review_backup_must_be_private_external')
    created_dirs=[]
    cursor=backup
    while not cursor.exists():
        created_dirs.append(cursor);cursor=cursor.parent
    backup.mkdir(mode=0o700, parents=True, exist_ok=False)
    if raw is not None: _private_write(backup/'preimage.json', raw)
    _private_write(backup/'reviewed-plan.json', canonical(plan))
    _private_write(backup/'manifest.json', canonical({'pre_sha256':plan['target']['pre_sha256'],
        'post_sha256':plan['target']['post_sha256'], 'target_was_absent':raw is None, 'plan_sha256':plan['plan_sha256']}))
    # Persist directory entries as well as each file before touching State.
    for directory in dict.fromkeys([backup]+[p.parent for p in created_dirs]):
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    return backup


def apply_plan(plan, *, cadence, target, sources, expected_sha256, backup_dir):
    """Never mutate the supplied plan. All IO categories are safe for logs."""
    target = _scope(cadence, target, sources)
    try:
        days(plan['from_date'],plan['to_date'])
        if len(plan['candidates']) > 53 or (cadence == 'daily' and len(plan['candidates']) != 1):
            raise ReviewError('review_candidate_count_invalid')
        if not expected_sha256 or plan_hash(plan) != expected_sha256 or plan.get('plan_sha256') != expected_sha256:
            raise ReviewError('review_plan_hash_mismatch')
        if (plan.get('purpose') != 'cybermed_insert_only_backfill' or plan.get('report_key') != 'cybermed'
                or plan.get('cadence') != cadence or plan['target']['path'] != str(target)
                or plan['target']['binding'] != BINDINGS[cadence] or plan.get('smtp_authorized') is not False
                or plan.get('generation_only') is not True or plan.get('schema_version') != 1
                or set(plan['sources']) != set(sources)):
            raise ReviewError('review_plan_scope_mismatch')
        if sha(canonical(plan['candidates'])) != plan['candidate_sha256']:
            raise ReviewError('review_candidate_hash_mismatch')
        validate_rows(plan['candidates'], cadence)
        for row in plan['candidates']: validate_candidate(row,cadence,plan['from_date'],plan['to_date'])
    except (KeyError, TypeError, ValueError):
        raise ReviewError('review_plan_invalid') from None
    with writer_lock():
        for role, path in sources.items():
            raw, _ = read_input(path)
            if plan['sources'][role] != {'path': str(direct_path(path)), 'sha256': sha(raw) if raw is not None else None}:
                raise ReviewError('review_source_hash_mismatch')
        raw, store = read_input(target)
        actual = sha(raw) if raw is not None else None
        if actual == plan['target']['post_sha256']:
            return {'status': 'already_at_postimage', 'inserted_total': 0, 'apply_executed': False}
        if actual != plan['target']['pre_sha256']:
            raise ReviewError('review_target_hash_mismatch')
        updated, actions = materialize(store, plan['candidates'], cadence)
        output = encoded(updated, raw)
        if actions != plan['actions'] or sha(output) != plan['target']['post_sha256']:
            raise ReviewError('review_postimage_mismatch')
        try:
            backup = _backup(backup_dir, plan, raw)
        except (OSError, TypeError):
            raise ReviewError('review_backup_failed_no_state_write') from None
        try:
            # Recheck after durable backup as well as after acquiring the lock.
            again, _ = read_input(target)
            if again != raw: raise ReviewError('review_concurrent_target_change')
            for role, path in sources.items():
                source, _ = read_input(path)
                if source is None or sha(source) != plan['sources'][role]['sha256']:
                    raise ReviewError('review_concurrent_source_change')
            atomic_text(target, output.decode('utf-8'))
            if target.read_bytes() != output:
                raise ReviewError('review_persisted_hash_mismatch_inspect_private_backup')
            _private_write(backup/'applied.json', canonical({'status':'applied', 'post_sha256':sha(output)}))
        except (OSError, StoreError):
            raise ReviewError('review_persistence_failed_inspect_private_backup') from None
    return {'status':'applied', 'apply_executed':True,
            'inserted_total': sum(a['action']=='insert' for a in actions)}
