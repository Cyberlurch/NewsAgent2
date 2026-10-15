"""Copy only the small reviewed maintenance files from an isolated download.

Never extract an archive in State; never copy arbitrary artifact trees or backups.
"""
from pathlib import Path
import argparse
import json
import os
import sys


def verify(source, output, cadence):
    names = ({'cybermed_daily_candidate.json','cybermed_daily_reviewed_plan.json'} if cadence=='daily'
             else {'cybermed_weekly_backfill_audit.json'})
    if any(p.is_symlink() for p in (source,output,*source.parents,*output.parents)):
        raise ValueError('review_artifact_symlink')
    root=Path(__file__).resolve().parents[1]
    sys.path.insert(0,str(root/'src'))
    from newsagent2.run_context import PATHS,path_for
    protected=[path_for(k).absolute().parent for k in PATHS]+[root/'state',root/'data']
    if any(output.absolute().is_relative_to(p) for p in protected):
        raise ValueError('review_artifact_output_is_state')
    files=list(source.rglob('*'))
    if {str(p.relative_to(source)) for p in files} != names or any(not p.is_file() or p.is_symlink() for p in files):
        raise ValueError('review_artifact_file_set_invalid')
    if sum(p.stat().st_size for p in files)>20_000_000:
        raise ValueError('review_artifact_too_large')
    # The CLI subsequently performs strict duplicate-key/hash/scope validation.
    for p in files:
        if not isinstance(json.loads(p.read_bytes()),dict):raise ValueError('review_artifact_invalid')
    output.mkdir(parents=True,exist_ok=True)
    for p in files:
        target=output/p.name
        if target.exists() or target.is_symlink():raise ValueError('review_artifact_destination_exists')
        fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'wb') as f:f.write(p.read_bytes())


if __name__=='__main__':
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--source',type=Path,required=True);ap.add_argument('--output',type=Path,required=True)
    ap.add_argument('--cadence',choices=['daily','weekly'],required=True)
    args=ap.parse_args()
    try:verify(args.source,args.output,args.cadence)
    except (ValueError,OSError):raise SystemExit('review_artifact_rejected') from None
