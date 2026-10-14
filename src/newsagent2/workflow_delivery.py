"""Workflow receipt verification, failure recovery and visible Git publication.

Verification/recovery/publication do not call providers or SMTP. Only explicit
operator retry commands submit saved content to SMTP. GitHub artifact access
uses the runner's scoped token; downloaded data are validated before merging.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import zipfile
from pathlib import Path

import requests

from .delivery import Coordinator, DeliveryResult, journal_valid, load_journal, verify_reports
from .run_context import email_mode, path_for, PATHS, send_enabled
from .store_io import StoreError, atomic_store, read_store, digest_store, rollups_store


def import_recovery(directory: Path, *, journal_path: Path | None = None, recovery_dir: Path | None = None) -> None:
    if not (directory / "journal.json").is_file():
        raise StoreError("delivery_recovery", "missing_journal")
    target = journal_path or path_for("DELIVERY_JOURNAL_PATH")
    existing = load_journal(target)
    incoming = load_journal(directory / "journal.json")
    for edition, row in incoming["editions"].items():
        if not edition.endswith(":production"):
            continue  # Test receipts can never become production delivery history.
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
            filename = hashlib.sha256((edition + ":" + row["content_version"]).encode()).hexdigest() + ".md"
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
    if incoming.get("paper_legacy_unknown") and any(key.endswith(":production") for key in incoming["editions"]):
        retained = existing.setdefault("paper_legacy_unknown", {"version": 1, "reports": {}})
        for report, ids in incoming["paper_legacy_unknown"]["reports"].items():
            retained["reports"][report] = sorted(set(retained["reports"].get(report, [])) | set(ids))
    atomic_store(target, existing, kind="delivery_journal", validate=journal_valid)
    if recovery_dir:
        recovery_dir.mkdir(parents=True, exist_ok=True)
        for source in directory.glob("*.md"):
            if re.fullmatch(r"[0-9a-f]{64}\.md", source.name):
                (recovery_dir / source.name).write_bytes(source.read_bytes())
        for source in directory.glob("*_generated_delta.json"):
            value = read_store(source, {}, kind="generated_delta")
            if value.get("generation_validated") is not True or not isinstance(value.get("records"), list):
                raise StoreError("generated_delta", "invalid_recovery")
            if not value.get("production_target"):
                continue  # Never promote an ordinary none/test generation delta.
            atomic_store(recovery_dir / source.name, value, kind="generated_delta")
        atomic_store(recovery_dir / "journal.json", existing, kind="delivery_recovery", validate=journal_valid)


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
    api = "https://api.github.com"
    branch = os.environ["GITHUB_REF_NAME"]
    acknowledged = set(os.getenv("DELIVERY_RECOVERY_ACK_RUN_IDS", "").split(","))
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})

    def get(path, **kwargs):
        try:
            response = session.get(api + path, timeout=30, **kwargs)
            response.raise_for_status()
            return response.json()
        except Exception:
            raise RuntimeError("delivery_recovery_github_access_failed") from None

    runs = get(f"/repos/{repository}/actions/workflows/{workflow}/runs", params={"branch": branch, "status": "completed", "per_page": 100})
    candidates = sorted([r for r in runs["workflow_runs"] if str(r["id"]) != os.getenv("GITHUB_RUN_ID")
                         and r["conclusion"] in {"failure", "cancelled", "timed_out", "action_required"}], key=lambda r: r["id"])
    for run in candidates:
        if str(run["id"]) in acknowledged:
            continue
        jobs = get(f"/repos/{repository}/actions/runs/{run['id']}/jobs", params={"per_page": 100})
        capable = any(step.get("name") in {"Run reports", "Run Cybermed reports"} and step.get("started_at")
                      and step.get("conclusion") != "skipped" for job in jobs["jobs"] for step in job.get("steps", []))
        if not capable:
            continue  # Positive evidence: no report/sender step started.
        artifacts = get(f"/repos/{repository}/actions/runs/{run['id']}/artifacts", params={"per_page": 100})
        matches = [a for a in artifacts["artifacts"] if a["name"] == f"{report_key}-delivery-recovery-{run['id']}" and not a["expired"]]
        if not matches:
            raise RuntimeError(f"delivery_recovery_missing_for_run:{run['id']};operator_reconciliation_required")
        try:
            response = session.get(api + f"/repos/{repository}/actions/artifacts/{matches[0]['id']}/zip", timeout=60)
            response.raise_for_status()
            directory = report_dir / "recovered" / str(run["id"])
            directory.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                if sum(info.file_size for info in archive.infolist()) > 100 * 1024 * 1024:
                    raise ValueError("oversized_recovery")
                for info in archive.infolist():
                    if info.is_dir():
                        continue
                    name = Path(info.filename).name
                    if name not in {"journal.json", "git_persistence.json", "cybermed_pubmed_ingestion.json"} and not re.fullmatch(r"[0-9a-f]{64}\.md|(?:[a-f0-9]{16}_)?(?:cybermed|cyberlurch)_[a-z]+_[a-z_]+_generated_delta\.json", name):
                        continue
                    (directory / name).write_bytes(archive.read(info))
            if not (directory / "journal.json").is_file():
                raise ValueError("missing_journal")
            import_recovery(directory, recovery_dir=report_dir / "recovery")
            restore_generated_records(directory, report_key)
            if report_key == "cybermed":
                from .pubmed_ingestion import restore
                restore(directory)
        except (ValueError, OSError, requests.RequestException, zipfile.BadZipFile):
            raise RuntimeError("delivery_recovery_artifact_invalid") from None


def restore_generated_records(directory: Path, report_key: str) -> None:
    """Recover only validated record deltas with matching preimages; no resets."""
    bindings = {"cybermed_daily": "CYBERMED_DAILY_DIGEST_STATE_PATH", "cybermed_weekly": "CYBERMED_WEEKLY_DIGEST_STATE_PATH",
                "cyberlurch_digests": "CYBERLURCH_DIGEST_STATE_PATH", "rollups": "ROLLUPS_STATE_PATH"}
    from .cybermed_quality import assert_no_generation_error_text
    # Check every staged manifest before restoring any of its records.
    approved_staged = set()
    from .pre_smtp_recovery import staged_delta_approved
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
    parser.add_argument("action", choices=["verify", "recover", "publish", "finalize-receipt", "confirm-pending-not-accepted", "retry-saved-rejected", "retry-saved-failed", "replay-pre-smtp"])
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
    else:
        if not args.confirm_smtp_reviewed or not args.edition_id:
            parser.error("operator reconciliation/retry requires --confirm-smtp-reviewed and --edition-id")
        if args.action == "confirm-pending-not-accepted":
            confirm_pending_not_accepted(args.edition_id)
        else:
            if not args.subject:
                parser.error("retry requires --subject")
            retry_saved_rejected(args.edition_id, args.subject, failed=args.action == "retry-saved-failed")


if __name__ == "__main__":
    main()
