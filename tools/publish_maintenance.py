"""Commit one reviewed postimage; Git errors stay errors, no force/rebase/retry."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

TARGETS={'daily':'state/cybermed_daily_digests.json','weekly':'state/cybermed_weekly_digests.json'}


def publish(plan,cadence,ref):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_./-]*',ref) or '..' in ref:
        raise ValueError('maintenance_ref_invalid')
    target=TARGETS[cadence]
    if any(p.is_symlink() for p in (Path(target),*Path(target).absolute().parents)):
        raise ValueError('maintenance_target_symlink')
    if plan['cadence']!=cadence or plan['report_key']!='cybermed':raise ValueError('maintenance_scope_invalid')
    def git(*args,check=True):
        return subprocess.run(['git',*args],check=check,capture_output=True)
    staged=git('diff','--cached','--name-only').stdout.decode().splitlines()
    if any(path!=target for path in staged):raise ValueError('maintenance_unexpected_staged_changes')
    status=git('status','--porcelain','--untracked-files=all','--','state','data').stdout.decode().splitlines()
    if any(line[3:]!=target for line in status):raise ValueError('maintenance_unexpected_changes')
    raw=Path(target).read_bytes() if Path(target).exists() else None
    actual=hashlib.sha256(raw).hexdigest() if raw is not None else None
    if actual!=plan['target']['post_sha256']:raise ValueError('maintenance_postimage_mismatch')
    # Hosted-runner private backups are ephemeral. Require a committed Git
    # preimage too; preserve that commit/hash in the public, text-free receipt.
    committed=git('show','HEAD:'+target,check=False)
    before=hashlib.sha256(committed.stdout).hexdigest() if committed.returncode==0 else None
    if before not in {plan['target']['pre_sha256'],plan['target']['post_sha256']}:
        raise ValueError('maintenance_git_preimage_unreviewed')
    if raw is None:return
    diff=git('diff','--quiet','--',target,check=False)
    if diff.returncode not in {0,1}:raise ValueError('maintenance_git_diff_failed')
    if diff.returncode==1:
        git('config','user.name','newsagent-bot')
        git('config','user.email','newsagent-bot@users.noreply.github.com')
        git('add','--',target)
        git('commit','-m','Publish reviewed Cybermed '+cadence+' reconstruction [skip ci]')
    git('push','origin','HEAD:'+ref)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan',type=Path,required=True);p.add_argument('--cadence',choices=TARGETS,required=True);p.add_argument('--ref',required=True)
    a=p.parse_args()
    try:publish(json.loads(a.plan.read_bytes()),a.cadence,a.ref)
    except (OSError,ValueError,KeyError,subprocess.CalledProcessError):raise SystemExit('maintenance_git_publication_failed; inspect reviewed plan and branch pre/postimages') from None
