"""Existing bounded Weekly audit plus offline reviewed Daily/Weekly publication."""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from ..cybermed_digest_store import build_weekly_backfill_candidates
from ..run_context import path_for
from ..store_io import read_store
from . import reviewed_backfill as review


def build_audit(*, daily_path, weekly_path, from_date, to_date, reconstructed_at=None):
    if not 0 <= (to_date-from_date).days <= 370:
        raise review.ReviewError("review_period_out_of_bounds")
    timestamp = reconstructed_at or review.now()
    _, store = review.read_input(daily_path)
    review.validate_rows(store['digests'], 'daily')
    source = [row for row in store['digests'] if from_date.isoformat() <= row['run_date'] <= to_date.isoformat()]
    candidates = build_weekly_backfill_candidates(source, generated_at_utc=timestamp)
    candidates = [row for row in candidates if from_date.isoformat() <= row['period_start'] <= row['period_end'] <= to_date.isoformat()]
    for candidate in candidates:
        candidate['reconstructed_at'] = timestamp
        candidate['reconstruction_basis'] = 'persisted_daily_editions'
        candidate['source_generated_at_utc'] = sorted({row['generated_at_utc'] for row in source if row.get('generated_at_utc') and row['digest_id'] in candidate['source_daily_digest_ids']})
    plan = review.build_plan('weekly', candidates, target=weekly_path, sources={'daily':daily_path},
                             start=from_date.isoformat(), end=to_date.isoformat(), created_at=timestamp)
    return plan


def apply_audit(audit, *, weekly_path, expected_sha256, backup_dir=None, daily_path=None):
    if backup_dir is None:
        raise SystemExit('reviewed apply requires --backup-dir and --reviewed-plan; old candidate hashes are insufficient')
    source = daily_path or path_for('CYBERMED_DAILY_DIGEST_STATE_PATH')
    return review.apply_plan(audit,cadence='weekly',target=weekly_path,sources={'daily':source},
                             expected_sha256=expected_sha256,backup_dir=backup_dir)


def daily_audit(candidate_path, target, *, from_date, to_date):
    _, store = review.read_input(candidate_path)
    return review.build_plan('daily', store['digests'], target=target, sources={'candidate':candidate_path},
                             start=from_date.isoformat(),end=to_date.isoformat())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cadence',choices=['daily','weekly'],default='weekly')
    parser.add_argument('--daily-path',type=Path)
    parser.add_argument('--weekly-path',type=Path)
    parser.add_argument('--candidate-path',type=Path,default=Path('out/cybermed_daily_candidate.json'))
    parser.add_argument('--from-date');parser.add_argument('--to-date')
    parser.add_argument('--output',type=Path)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--reviewed-plan',type=Path)
    parser.add_argument('--expected-sha256',default='')
    parser.add_argument('--backup-dir',type=Path)
    args=parser.parse_args()
    args.output = args.output or Path('out/'+(args.cadence+'_apply_result.json' if args.apply else 'cybermed_weekly_backfill_audit.json'))
    daily=args.daily_path or path_for('CYBERMED_DAILY_DIGEST_STATE_PATH')
    weekly=args.weekly_path or path_for('CYBERMED_WEEKLY_DIGEST_STATE_PATH')
    target=weekly if args.cadence=='weekly' else daily
    sources={'daily':daily} if args.cadence=='weekly' else {'candidate':args.candidate_path}
    try:
        if args.apply:
            if not args.reviewed_plan or not args.expected_sha256 or not args.backup_dir:
                parser.error('apply requires --reviewed-plan, --expected-sha256 and --backup-dir; no generation is performed')
            review.direct_path(args.reviewed_plan)
            if args.output.absolute() == args.reviewed_plan.absolute():
                raise review.ReviewError('review_output_overlaps_reviewed_plan')
            plan=read_store(args.reviewed_plan,{},kind='reviewed_plan')
            if (args.from_date and args.from_date != plan.get('from_date')) or (args.to_date and args.to_date != plan.get('to_date')):
                raise review.ReviewError('review_requested_period_mismatch')
            review.external_output(args.output,target=target,sources=sources)
            result=review.apply_plan(plan,cadence=args.cadence,target=target,sources=sources,
                expected_sha256=args.expected_sha256,backup_dir=args.backup_dir)
            from ..store_io import atomic_text
            import json
            atomic_text(args.output,json.dumps(result,sort_keys=True)+'\n')
            print('[reviewed-backfill] '+result['status'])
        else:
            if not args.from_date or not args.to_date:parser.error('audit requires --from-date and --to-date')
            start,end=date.fromisoformat(args.from_date),date.fromisoformat(args.to_date)
            plan=(build_audit(daily_path=daily,weekly_path=weekly,from_date=start,to_date=end) if args.cadence=='weekly'
                  else daily_audit(args.candidate_path,daily,from_date=start,to_date=end))
            review.write_plan(plan,args.output)
            print('[reviewed-backfill] plan_sha256='+plan['plan_sha256'])
    except (review.ReviewError, ValueError, OSError) as exc:
        category=str(exc) if isinstance(exc,review.ReviewError) else 'review_request_or_io_invalid'
        raise SystemExit(category) from None
    return 0


if __name__=='__main__':
    raise SystemExit(main())
