"""Workflow receipt verification, failure recovery and visible Git publication.

Verification/recovery/publication do not call providers or SMTP. Only explicit
operator retry commands submit saved content to SMTP. GitHub artifact access
uses the runner's scoped token; downloaded data are validated before merging.
"""
from __future__ import annotations

import argparse
import hashlib
import io
from itertools import combinations
import json
import os
import re
import subprocess
import sys
import zipfile
from pathlib import Path

import requests

from .delivery import Coordinator, DeliveryResult, journal_valid, load_journal, verify_reports
from .run_context import email_mode, path_for, PATHS, send_enabled
from .store_io import StoreError, atomic_store, read_store, digest_store, rollups_store
from .github_readonly import GitHubReader, GitHubReadError


def import_recovery(directory: Path, *, journal_path: Path | None = None, recovery_dir: Path | None = None,
                    report_key: str | None = None, source_run_id: str | None = None) -> bool:
    if not (directory / "journal.json").is_file():
        raise StoreError("delivery_recovery", "missing_journal")
    target = journal_path or path_for("DELIVERY_JOURNAL_PATH")
    incoming = load_journal(directory / "journal.json")
    if report_key not in {None, "cybermed", "cyberlurch"}:
        raise StoreError("delivery_recovery", "invalid_report_scope")
    if source_run_id is not None:
        matching = [row for row in incoming["editions"].values()
                    if row["report_key"] == report_key and row["workflow_run_id"] == str(source_run_id)]
        purposes = {row["edition_id"].rsplit(":", 1)[1] for row in matching}
        if "production" in purposes and purposes != {"production"}:
            raise StoreError("delivery_recovery", "mixed_source_run_modes")
        if purposes and "production" not in purposes:
            return False  # Ignore inherited production receipts in a preview's copied journal too.
    eligible = {edition: row for edition, row in incoming["editions"].items()
                if edition.endswith(":production") and (report_key is None or row["report_key"] == report_key)}
    if not eligible:
        if source_run_id is not None:
            # An old unlabelled artifact is positive non-production evidence
            # only for its actual originating run/newsletter, not an empty or
            # foreign journal left behind before the failed sender started.
            raise StoreError("delivery_recovery", "production_scope_missing")
        return False  # No target/lock/export creation, even for a missing store.
    existing = load_journal(target)
    reports = {row["report_key"] for row in eligible.values()}
    content_files = {}
    for edition, row in eligible.items():
        filename = hashlib.sha256((edition + ":" + row["content_version"]).encode()).hexdigest() + ".md"
        content_files[filename] = row["content_version"]
        previous = existing["editions"].get(edition)
        if previous:
            if previous["content_version"] != row["content_version"]:
                if any(event.get("category") == "operator_confirmed_not_accepted" and event.get("content_version") == row["content_version"]
                       for event in previous.get("prior_attempts", [])):
                    continue
                raise StoreError("delivery_recovery", "content_conflict")
            if (previous.get("state_persisted") or previous["status"] == "accepted_by_smtp"
                    or previous["updated_at_utc"] > row["updated_at_utc"]
                    or (previous.get("category") == "operator_confirmed_not_accepted" and
                        len(previous.get("prior_attempts", [])) > len(row.get("prior_attempts", [])))):
                continue
        if row["generation_validated"]:
            source = directory / filename
            if not source.is_file() or hashlib.sha256(source.read_bytes()).hexdigest() != row["content_version"]:
                if previous == row:
                    continue
                raise StoreError("delivery_recovery", "missing_or_invalid_generated_content")
        row = dict(row)
        if row["status"] == "accepted_by_smtp":
            # The artifact's filesystem checkpoint belongs to the OLD runner;
            # a failed Git push does not establish markers in this checkout.
            row["state_persisted"] = False
        existing["editions"][edition] = row
    legacy_reports = {report: ids for report, ids in incoming.get("paper_legacy_unknown", {}).get("reports", {}).items()
                      if report in reports}
    if legacy_reports:
        retained = existing.setdefault("paper_legacy_unknown", {"version": 1, "reports": {}})
        for report, ids in legacy_reports.items():
            retained["reports"][report] = sorted(set(retained["reports"].get(report, [])) | set(ids))
    atomic_store(target, existing, kind="delivery_journal", validate=journal_valid)
    if recovery_dir:
        recovery_dir.mkdir(parents=True, exist_ok=True)
        for source in directory.glob("*.md"):
            if source.name in content_files:
                body = source.read_bytes()
                if hashlib.sha256(body).hexdigest() == content_files[source.name]:
                    (recovery_dir / source.name).write_bytes(body)
        for source in directory.glob("*_generated_delta.json"):
            value = read_store(source, {}, kind="generated_delta")
            if value.get("report_key") not in reports:
                continue
            if value.get("generation_validated") is not True or not isinstance(value.get("records"), list):
                raise StoreError("generated_delta", "invalid_recovery")
            if not value.get("production_target"):
                continue  # Never promote an ordinary none/test generation delta.
            atomic_store(recovery_dir / source.name, value, kind="generated_delta")
        candidate_artifact = directory / "cyberlurch_candidates.json"
        if "cyberlurch" in reports and candidate_artifact.is_file():
            from .cyberlurch_candidates import envelope_valid
            value = read_store(candidate_artifact, {}, kind="cyberlurch_candidates", validate=envelope_valid)
            atomic_store(recovery_dir / candidate_artifact.name, value, kind="cyberlurch_candidates", validate=envelope_valid)
        atomic_store(recovery_dir / "journal.json", existing, kind="delivery_recovery", validate=journal_valid)
    return True


def _job_recovery_scope(job, report_key):
    prefix = f"NewsAgent2 recovery v1 / {report_key} / "
    markers = [step for step in job.get("steps", []) if step.get("name", "").startswith(prefix)]
    if len(markers) != 1:
        return None
    marker = markers[0]
    senders = [step for step in job.get("steps", []) if step.get("name") in {"Run reports", "Run Cybermed reports"}
               and step.get("started_at") and step.get("conclusion") != "skipped"]
    if (marker.get("conclusion") != "success" or not marker.get("started_at")
            or type(marker.get("number")) is not int or marker["number"] < 1
            or not senders or any(type(s.get("number")) is not int or s["number"] <= marker["number"] for s in senders)):
        return None
    return marker["name"][len(prefix):]


def _nonproduction_jobs(run, jobs, report_key):
    """Positive runner step evidence, never infer mode from a free-text title."""
    if run.get("event") != "workflow_dispatch" or not jobs:
        return False
    return all(_job_recovery_scope(job, report_key) in {"none", "test"} for job in jobs)


def _run_plan_proofs(jobs, report_key):
    prefix = f"NewsAgent2 plan v1 / {report_key} / "
    proofs = set()
    for job in jobs:
        markers = [s for s in job['steps'] if s.get('name', '').startswith(prefix)]
        senders = [s for s in job['steps'] if s.get('name') in {'Run reports', 'Run Cybermed reports'}
                   and s.get('started_at') and s.get('conclusion') != 'skipped']
        if len(markers) != 1 or not senders:
            return None
        marker = markers[0]
        proof = marker['name'][len(prefix):]
        if (not re.fullmatch('[a-f0-9]{64}', proof) or marker.get('conclusion') != 'success'
            or not marker.get('started_at') or type(marker.get('number')) is not int or marker['number'] < 1
            or any(type(s.get('number')) is not int or s['number'] <= marker['number'] for s in senders)):
            return None
        proofs.add(proof)
    return proofs or None


def _completed_plan_receipts(run, jobs, report_key, journal):
    from .edition_plan import plan_proof
    proofs = _run_plan_proofs(jobs, report_key)
    own = [r for r in journal['editions'].values() if r['report_key'] == report_key
           and r['workflow_run_id'] == str(run['id']) and r['edition_id'].endswith(':production')]
    if not proofs or not own or len(own)>4:
        return False
    ids = {r['edition_id'] for r in own}
    # A rerun can prune an already-finalized cadence. With at most four
    # editions, checking its subset proof is bounded to 15 combinations.
    candidates = {plan_proof(part):set(part) for size in range(1,len(ids)+1)
                  for part in combinations(sorted(ids),size)}
    matched = [candidates.get(proof) for proof in proofs]
    return bool(all(matched) and set().union(*matched) == ids
        and all((r['status'] == 'accepted_by_smtp' and r['state_persisted']) or
                (r['status'] == 'failed' and not r['generation_validated']
                 and r['smtp_phase'] == 'not_handed_off') for r in own))


def _maintenance_month(run, jobs, report_key):
    if run.get("event") != "workflow_dispatch" or report_key != "cyberlurch" or not jobs:
        return None
    scopes = {_job_recovery_scope(job, report_key) for job in jobs}
    if len(scopes) != 1:
        return None
    scope = scopes.pop()
    if not isinstance(scope, str) or not re.fullmatch(r"maintenance:\d{4}-(?:0[1-9]|1[0-2])", scope):
        return None
    from datetime import date
    try:
        date.fromisoformat(scope.split(":")[1] + "-01")
    except ValueError:
        return None
    return scope.split(":")[1]


def recover_failed_runs(report_key: str, workflow: str, *, report_dir: Path) -> None:
    """Fail closed if a previous SMTP-capable failure has no recoverable receipt.

    We do not infer acceptance from old logs or silently ignore cancelled runs.
    An operator may acknowledge an individually investigated run ID via a repo
    variable, after reconciling SMTP and restoring the required receipts.
    """
    if email_mode() != "real":
        return
    repository = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GITHUB_TOKEN"]
    branch = os.environ["GITHUB_REF_NAME"]
    acknowledged = set(os.getenv("DELIVERY_RECOVERY_ACK_RUN_IDS", "").split(","))
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})

    reader = GitHubReader(session, repository, budget_seconds=300)
    runs = reader.collection(f"/actions/workflows/{workflow}/runs", 'workflow_runs', 'workflow_runs',
                             params={"branch": branch, "status": "completed"}, window=100)
    candidates = sorted([r for r in runs if str(r["id"]) != os.getenv("GITHUB_RUN_ID")
                         and r["conclusion"] in {"failure", "cancelled", "timed_out", "action_required"}], key=lambda r: r["id"])
    from .legacy_transition import reviewed_failure, POLICY_SHA256
    legacy_findings = []
    for run in candidates:
        if str(run["id"]) in acknowledged:
            continue
        finding = reviewed_failure(run, report_key, workflow, load_journal(path_for("DELIVERY_JOURNAL_PATH")))
        if finding:
            legacy_findings.append(finding)
            atomic_store(report_dir / f"{report_key}_legacy_plan_metadata.json",
                {"schema_version": 1, "report_key": report_key, "policy_sha256": POLICY_SHA256,
                 "workflow_run_id": os.getenv("GITHUB_RUN_ID", "local"), "reviewed_legacy_runs": legacy_findings,
                 "smtp_success_invented": False}, kind="legacy_transition_evidence")
            print(f"[recovery] reviewed legacy run {run['id']}: {finding['finding']}; no historical resend")
            continue
        jobs = reader.collection(f"/actions/runs/{run['id']}/jobs", 'jobs', 'run_jobs',
                                 params={'filter':'all'}, max_pages=5)
        capable_jobs = [job for job in jobs if any(
            step.get("name") in {"Run reports", "Run Cybermed reports"} and step.get("started_at")
            and step.get("conclusion") != "skipped" for step in job.get("steps", []))]
        if not capable_jobs:
            continue  # Positive evidence: no report/sender step started.
        if _nonproduction_jobs(run, capable_jobs, report_key):
            continue  # Failed previews/test mails cannot block production or import State.
        if _completed_plan_receipts(run, capable_jobs, report_key, load_journal(path_for('DELIVERY_JOURNAL_PATH'))):
            continue
        maintenance_month = _maintenance_month(run, capable_jobs, report_key)
        artifacts = reader.collection(f"/actions/runs/{run['id']}/artifacts", 'artifacts', 'run_artifacts', max_pages=5)
        matches = [a for a in artifacts if a["name"] == f"{report_key}-delivery-recovery-{run['id']}" and not a["expired"]]
        if not matches:
            raise RuntimeError(f"delivery_recovery_missing_for_run:{run['id']};operator_reconciliation_required")
        try:
            content = reader.archive(f"/actions/artifacts/{matches[0]['id']}/zip")
            expected_digest = matches[0].get('digest')
            if expected_digest is not None and (not isinstance(expected_digest,str)
                or not re.fullmatch(r'sha256:[a-f0-9]{64}',expected_digest)
                or expected_digest != 'sha256:'+hashlib.sha256(content).hexdigest()):
                raise StoreError('delivery_recovery','artifact_digest_mismatch')
            directory = report_dir / "recovered" / str(run["id"])
            directory.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                if sum(info.file_size for info in archive.infolist()) > 100 * 1024 * 1024:
                    raise ValueError("oversized_recovery")
                names = set()
                for info in archive.infolist():
                    if info.is_dir():
                        continue
                    name = Path(info.filename).name
                    if name not in {"journal.json", "git_persistence.json", "cybermed_pubmed_ingestion.json", "cyberlurch_candidates.json"} and not re.fullmatch(r"[0-9a-f]{64}\.md|(?:[a-f0-9]{16}_)?(?:cybermed|cyberlurch)_[a-z]+_[a-z_]+_generated_delta\.json", name):
                        continue
                    if name in names:
                        raise StoreError('delivery_recovery','duplicate_archive_entry')
                    names.add(name)
                    (directory / name).write_bytes(archive.read(info))
            if not (directory / "journal.json").is_file():
                raise ValueError("missing_journal")
            if maintenance_month:
                packet = load_journal(directory / "journal.json")
                own = [row for row in packet["editions"].values()
                       if row["workflow_run_id"] == str(run["id"]) and row["report_key"] == report_key]
                if not own or any(row["cadence"] != "monthly" or not row["edition_id"].endswith(":none")
                                  or row["period_start"][:7] != maintenance_month for row in own):
                    raise StoreError("delivery_recovery", "maintenance_scope_mismatch")
                # This gate authorizes one Monthly rollup, never delivery
                # receipts, candidate memory, caches or another newsletter.
                restore_generated_records(directory, report_key, maintenance_month=maintenance_month)
                continue
            imported = import_recovery(directory, recovery_dir=report_dir / "recovery",
                                       report_key=report_key, source_run_id=str(run["id"]))
            if not imported:
                if (run.get("event") != "workflow_dispatch" or
                        all(_job_recovery_scope(job, report_key) == "real" for job in capable_jobs)):
                    raise StoreError("delivery_recovery", "source_mode_conflict")
                continue  # Matching old none/test receipt: never restore any accompanying deltas/caches.
            restore_generated_records(directory, report_key)
            if report_key == "cyberlurch":
                from .cyberlurch_candidates import restore
                restore(directory)
            if report_key == "cybermed":
                from .pubmed_ingestion import restore
                restore(directory)
        except (ValueError, OSError, requests.RequestException, zipfile.BadZipFile):
            raise RuntimeError("delivery_recovery_artifact_invalid") from None
    for diagnostic in reader.diagnostics:
        if diagnostic['attempts'] > 1:
            print(json.dumps({'github_read':diagnostic}, sort_keys=True))


def restore_generated_records(directory: Path, report_key: str, *, maintenance_month: str | None = None) -> None:
    """Recover only validated record deltas with matching preimages; no resets."""
    bindings = {"cybermed_daily": "CYBERMED_DAILY_DIGEST_STATE_PATH", "cybermed_weekly": "CYBERMED_WEEKLY_DIGEST_STATE_PATH",
                "cyberlurch_digests": "CYBERLURCH_DIGEST_STATE_PATH", "rollups": "ROLLUPS_STATE_PATH"}
    from .cybermed_quality import assert_no_generation_error_text
    # Check every staged manifest before restoring any of its records.
    approved_staged = set()
    from .pre_smtp_recovery import staged_delta_approved
    if maintenance_month is not None:
        if report_key != "cyberlurch":
            raise StoreError("generated_delta", "maintenance_scope_mismatch")
        # Validate the entire maintenance scope before changing any file.
        for artifact in sorted(directory.glob("*_generated_delta.json")):
            delta = read_store(artifact, {}, kind="generated_delta")
            if not delta.get("production_target"):
                continue
            if (delta.get("report_key") != report_key or delta.get("kind") != "rollups"
                    or delta.get("generation_validated") is not True or not isinstance(delta.get("records"), list)
                    or any(not isinstance(r, dict) or r.get("month") != maintenance_month for r in delta["records"])
                    or delta.get("removed_record_ids")):
                raise StoreError("generated_delta", "maintenance_scope_mismatch")
    for artifact in sorted(directory.glob("*_generated_delta.json")):
        delta = read_store(artifact, {}, kind="generated_delta")
        if delta.get("requires_ready_checkpoint") and delta.get("report_key") == report_key:
            if staged_delta_approved(directory, delta, artifact.name):
                approved_staged.add(artifact.name)
    for artifact in sorted(directory.glob("*_generated_delta.json")):
        delta = read_store(artifact, {}, kind="generated_delta")
        if delta.get("requires_ready_checkpoint") and artifact.name not in approved_staged:
            continue
        if not delta.get("production_target") or delta.get("report_key") != report_key:
            continue
        kind = delta.get("kind")
        if kind not in bindings or delta.get("generation_validated") is not True:
            raise StoreError("generated_delta", "invalid_recovery")
        target = path_for(bindings[kind])
        validator = rollups_store if kind == "rollups" else digest_store
        if kind == "cyberlurch_digests":
            from .youtube_source import digest_valid
            validator = digest_valid
        default = {"version": 1, "reports": {}} if kind == "rollups" else {"schema_version": 1, "digests": []}
        if kind == "cyberlurch_digests": default = {"version": 1, "digests": []}
        value = read_store(target, default, kind=kind, validate=validator)
        rows = value.get("reports", {}).get(report_key, []) if kind == "rollups" else value["digests"]
        records = delta.get("records")
        removed = delta.get("removed_record_ids")
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records) or not isinstance(removed, list):
            raise StoreError("generated_delta", "invalid_recovery")
        def identity(row): return str(row.get("digest_id") or row.get("video_id") or row.get("month") or "")
        groups = {}
        for row in records:
            key = identity(row)
            if not key: raise StoreError("generated_delta", "invalid_record_identity")
            groups.setdefault(key, []).append(row)
        already = all([row for row in rows if identity(row) == key] == group for key, group in groups.items())
        if already and not any(identity(row) in removed for row in rows):
            continue  # Preserve newer unrelated writes; no duplicate application.
        current_hash = hashlib.sha256(target.read_bytes()).hexdigest() if target.exists() else None
        if current_hash != delta.get("base_sha256"):
            raise StoreError("generated_delta", "newer_state_requires_operator_review")
        merged, inserted = [], set()
        for row in rows:
            key = identity(row)
            if key in removed: continue
            if key in groups:
                if key not in inserted:
                    merged.extend(groups[key]); inserted.add(key)
            else: merged.append(row)
        for key, group in groups.items():
            if key not in inserted: merged.extend(group)
        if kind == "rollups": value.setdefault("reports", {})[report_key] = merged
        else: value["digests"] = merged
        assert_no_generation_error_text(value, boundary="generation_recovery")
        if kind == "cybermed_weekly":
            from .cybermed_digest_store import save_cybermed_weekly_digest_store
            save_cybermed_weekly_digest_store(str(target), value)
        else: atomic_store(target, value, kind=kind, validate=validator)


def publish_state(paths: list[str], *, report_dir: Path, backfill_apply: bool = False) -> None:
    """One ordinary push; failures leave recovery metadata, never force/rebase."""
    def git(*args):
        return subprocess.run(["git", *args], text=True, capture_output=True)
    selected = [p for p in paths if Path(p).is_file()]
    if not selected:
        return
    if any(Path(p).name not in PATHS.values() for p in selected):
        raise RuntimeError("publication_path_not_allowed")
    if email_mode() != "real" and not (backfill_apply and email_mode() == "none" and
                                      all(Path(p).name == "rollups.json" for p in selected)):
        raise RuntimeError("publication_mutation_policy_blocked")
    for path in selected:
        # Existing strict loaders enforce store-specific record guards too.
        if path.endswith("cybermed_weekly_digests.json"):
            from .cybermed_digest_store import load_cybermed_weekly_digest_store
            load_cybermed_weekly_digest_store(path)
        elif path.endswith("delivery_journal.json"):
            load_journal(Path(path))
        elif path.endswith("cybermed_pubmed_ingestion.json"):
            if os.getenv("REPORT_KEY") != "cybermed":
                raise RuntimeError("publication_newsletter_scope_mismatch")
            from .pubmed_ingestion import valid
            read_store(path, {}, kind="cybermed_pubmed_ingestion", validate=valid)
        elif Path(path).name == "cyberlurch_digests.json":
            from .main import _load_cyberlurch_digest_state
            _load_cyberlurch_digest_state(path)
        elif Path(path).name == "processed_items.json":
            from .state_manager import load_state
            load_state(path)
        else:
            read_store(path, {}, kind="publication_input")
    branch = os.getenv("GITHUB_REF_NAME", "")
    if not branch:
        raise RuntimeError("git_publication_branch_missing")
    status = git("status", "--porcelain", "--", *selected)
    if status.returncode:
        raise RuntimeError("git_status_failed")
    if not status.stdout.strip():
        return
    category = "git_commit_failed"
    sha = ""
    try:
        if git("add", "--", *selected).returncode or git("commit", "-m", "Update validated newsletter state [skip ci]").returncode:
            raise RuntimeError(category)
        head = git("rev-parse", "HEAD")
        if head.returncode:
            raise RuntimeError(category)
        sha = head.stdout.strip()
        category = "git_push_failed_or_unknown"
        pushed = git("push", "origin", f"HEAD:{branch}")
        # A lost push acknowledgement may still have updated the remote.
        if pushed.returncode:
            remote = git("ls-remote", "origin", f"refs/heads/{branch}")
            if remote.returncode or not remote.stdout.startswith(sha + "\t"):
                raise RuntimeError(category)
        category = "git_persisted"
    finally:
        atomic_store(report_dir / "recovery/git_persistence.json", {"category": category, "commit": sha, "branch": branch}, kind="git_recovery")
        from .run_timing import publication
        publication(report_dir, category)


def finalize_receipt(edition: str) -> None:
    journal = load_journal(path_for("DELIVERY_JOURNAL_PATH"))
    row = journal["editions"].get(edition)
    if not row or row["status"] != "accepted_by_smtp" or not edition.endswith(":production"):
        raise RuntimeError("known_full_smtp_acceptance_required")
    result = DeliveryResult(**row)
    intents = result.finalization
    if intents and (intents.get("report_key") != result.report_key or intents.get("report_mode") != result.cadence):
        raise RuntimeError("receipt_finalization_scope_mismatch")
    from .main import _update_state_after_run
    from .state_manager import load_state
    if intents and not result.state_persisted:
        state_path = str(path_for("STATE_PATH"))
        _update_state_after_run(state_path=state_path, state=load_state(state_path), read_only=False,
                                delivery_result=result, **intents)
    result.state_persisted = True
    result.category = ""
    coordinator = Coordinator(result.report_key, result.cadence, mode="real", period_dates=(result.period_start, result.period_end))
    coordinator._persist(result)


def finalize_pending_receipts(report_key: str) -> None:
    rows = load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"].values()
    pending = sorted((row for row in rows if row["report_key"] == report_key and row["status"] == "accepted_by_smtp"
                      and not row["state_persisted"] and row["edition_id"].endswith(":production")), key=lambda row: row["period_end"])
    for row in pending:
        finalize_receipt(row["edition_id"])


def confirm_pending_not_accepted(edition: str) -> None:
    """Explicit operator reconciliation, after verifying SMTP logs externally."""
    journal = load_journal(path_for("DELIVERY_JOURNAL_PATH"))
    row = journal["editions"].get(edition)
    if not row or row["status"] != "outcome_unknown":
        raise RuntimeError("unknown_outcome_required_for_reconciliation")
    result = DeliveryResult(**row)
    result.prior_attempts.append({"status": result.status, "category": result.category, "content_version": result.content_version})
    result.rejected = sorted(set(result.rejected + result.unknown))
    result.unknown = []
    result.status = "partially_accepted" if result.accepted else "failed"
    result.category = "operator_confirmed_not_accepted"
    coordinator = Coordinator(result.report_key, result.cadence, mode="real", period_dates=(result.period_start, result.period_end))
    coordinator._persist(result)


def retry_saved_rejected(edition: str, subject: str, *, failed: bool = False) -> None:
    row = load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"].get(edition)
    known_failures = {"all_recipients_rejected", "recipient_rejection", "smtp_explicit_rejection", "smtp_connection_failed", "smtp_configuration_missing", "recipient_hmac_key_missing"}
    eligible = row and (row["status"] == "partially_accepted" or (
        failed and row["status"] == "failed" and row["category"] in known_failures and not row["accepted"] and not row["unknown"]))
    if not eligible or not edition.endswith(":production"):
        raise RuntimeError("known_partial_acceptance_required")
    # No providers: use the exact saved body and original edition/Message-ID.
    name = hashlib.sha256((edition + ":" + row["content_version"]).encode()).hexdigest() + ".md"
    report_dir = Path(os.getenv("REPORT_DIR") or "out")
    body = (report_dir / "recovery" / name).read_text()
    if hashlib.sha256(body.encode()).hexdigest() != row["content_version"]:
        raise RuntimeError("recovery_content_hash_mismatch")
    os.environ.update({"REPORT_KEY": row["report_key"], "REPORT_MODE": row["cadence"],
                       "DELIVERY_PERIOD_START": row["period_start"], "DELIVERY_PERIOD_END": row["period_end"],
                       "EMAIL_MODE": "real", "SEND_EMAIL": "1"})
    from .emailer import send_markdown
    coordinator = Coordinator(row["report_key"], row["cadence"], mode="real")
    send_markdown(subject, body, coordinator=coordinator, finalization=row["finalization"], retry_rejected=not failed)
    finalize_receipt(edition)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["verify", "recover", "publish", "finalize-receipt", "finalize-pending", "confirm-pending-not-accepted", "retry-saved-rejected", "retry-saved-failed", "replay-pre-smtp"])
    parser.add_argument("--report-key", choices=["cybermed", "cyberlurch"])
    parser.add_argument("--modes", nargs="+", default=[])
    parser.add_argument("--workflow")
    parser.add_argument("--paths", nargs="+", default=[])
    parser.add_argument("--edition-id")
    parser.add_argument("--backfill-apply", action="store_true")
    parser.add_argument("--confirm-smtp-reviewed", action="store_true")
    parser.add_argument("--subject")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-replay", action="store_true")
    args = parser.parse_args()
    report_dir = Path(os.getenv("REPORT_DIR") or "out")
    if args.action == "replay-pre-smtp":
        from .pre_smtp_recovery import replay
        print(json.dumps(replay(args.edition_id, apply=args.apply, confirm=args.confirm_replay), sort_keys=True))
    elif args.action == "verify":
        verify_reports(report_dir, args.report_key, args.modes, mode=email_mode(),
                       send_required=email_mode() != "none" and send_enabled())
    elif args.action == "recover":
        recover_failed_runs(args.report_key, args.workflow, report_dir=report_dir)
    elif args.action == "publish":
        publish_state(args.paths, report_dir=report_dir, backfill_apply=args.backfill_apply)
    elif args.action == "finalize-receipt":
        finalize_receipt(args.edition_id)
    elif args.action == "finalize-pending":
        if not args.report_key or email_mode() != 'real' or os.getenv('REPORT_KEY') != args.report_key:
            raise RuntimeError('receipt_finalization_scope_mismatch')
        from .run_context import writer_lock
        with writer_lock():
            finalize_pending_receipts(args.report_key)
    else:
        if not args.confirm_smtp_reviewed or not args.edition_id:
            parser.error("operator reconciliation/retry requires --confirm-smtp-reviewed and --edition-id")
        if args.action == "confirm-pending-not-accepted":
            confirm_pending_not_accepted(args.edition_id)
        else:
            if not args.subject:
                parser.error("retry requires --subject")
            retry_saved_rejected(args.edition_id, args.subject, failed=args.action == "retry-saved-failed")


def cli():
    """Expose fixed failure categories through Actions metadata, never payloads.

    Logs/artifact archives may be unavailable to an operator's API client. A
    category annotation keeps recovery failures diagnosable without echoing an
    exception, downloaded record, address, hostname or credential.
    """
    try:
        main()
    except Exception as exc:
        category = "newsletter_operation_failed"
        known = {
            "recovery_recipient_key_missing", "recovery_readiness_binding_invalid",
            "recovery_output_binding_invalid", "recovery_required_delta_missing_or_changed",
            "delivery_recovery_github_access_failed", "delivery_recovery_artifact_invalid",
            "receipt_finalization_scope_mismatch", "known_full_smtp_acceptance_required",
            "publication_newsletter_scope_mismatch", "publication_mutation_policy_blocked",
            "git_publication_branch_missing", "git_status_failed", "git_commit_failed",
            "git_push_failed_or_unknown",
        }
        kinds = {"delivery_journal", "delivery_recovery", "generated_delta", "generated_content",
                 "processed_items", "cybermed_daily", "cybermed_weekly", "cyberlurch_digests",
                 "rollups", "cybermed_pubmed_ingestion"}
        store_categories = {
            "missing_journal", "corrupted_existing_store", "invalid_existing_store", "unreadable_existing_store",
            "invalid_recovery", "recovery_preimage_conflict", "newer_state_requires_operator_review",
            "production_scope_missing", "mixed_source_run_modes", "source_mode_conflict",
            "missing_or_invalid_generated_content", "content_conflict", "maintenance_scope_mismatch",
            "persistence_failed", "invalid_output", "invalid_record_identity", "recovery_mutation_policy",
            "marker_finalization_failed",
            "artifact_digest_mismatch", "duplicate_archive_entry",
        }
        if isinstance(exc, GitHubReadError):
            print(json.dumps({'github_read':exc.diagnostic},sort_keys=True),file=sys.stderr)
            category = exc.diagnostic['category']
        elif isinstance(exc, StoreError) and exc.kind in kinds and exc.category in store_categories:
            category = f"store_error:{exc.kind}:{exc.category}"
        elif type(exc) is RuntimeError and str(exc) in known:
            category = str(exc)
        elif type(exc) is RuntimeError and re.fullmatch(r"delivery_recovery_missing_for_run:[0-9]+;operator_reconciliation_required", str(exc)):
            category = str(exc)
        print(f"::error title=Newsletter operation failed::{category}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    cli()
