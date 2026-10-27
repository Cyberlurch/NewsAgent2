"""Reviewed, finite transition from pre-journal runs; never invent receipts.

The five source log archives were supplied by the operator and matched to the
original commits. Merging this policy explicitly retires automatic recovery of
these legacy runs, not their uncertainty. All other failures keep fail-closed
recovery. Historical attempts are held separately from the delivery journal.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .store_io import atomic_store

REPOSITORY = "Cyberlurch/NewsAgent2"
FRESH_START = "2026-10-15"  # Both scheduled senders were proven not to start.
# run: (newsletter, workflow, source commit, log archive SHA-256, finding)
REVIEWED = {
    "33743120339": ("cyberlurch", "newsagent.yml", "818734b4d2d6d59e2dac6e7b33a23ef75e0c53bc",
        "82bff38af2176cfab29b157e4997511422a601cd4b9dd95e55c9c538f298aac6", "no_smtp_handoff"),
    "33752005796": ("cyberlurch", "newsagent.yml", "3e36775b29e82fe56a8d8a4058b77e9bbf43e542",
        "63bd0797d0c335466b2039eaf8317f68a10b79d6ed3a675b6c40811a83db74f8", "no_smtp_handoff"),
    "33755797975": ("cyberlurch", "newsagent.yml", "606e67155a4e6bc680b4e5cd8d07e9ca722aa18e",
        "99f921b3e7ae973fd552f15670b4ec0a513887694f6e7207d5caadb38eae49c0", "no_smtp_handoff"),
    "33186081165": ("cybermed", "cybermed.yml", "67167b634467f8578840adf18da42e249029a010",
        "5cdbc382849f74efa4d1ae7846d91abca163da1e1d985b60d9e7093254b9e3ca", "partial_acceptance_not_excludable"),
    "33489323708": ("cybermed", "cybermed.yml", "5f7b5afa5f62209033dd98f07acbe20b5de434b7",
        "5fd95d03b6b1daac28903cd4c2ed471a1a6bb5e5dd878a2ff219a4ddd9eb4a2b", "single_smtp_acceptance_state_unrecovered"),
}
POLICY_SHA256 = hashlib.sha256(json.dumps(
    {"repository": REPOSITORY, "fresh_start": FRESH_START, "reviewed": REVIEWED},
    sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def active() -> bool:
    # Local maintenance in this checkout shares the same protection. Tests use
    # their explicit synthetic repository, not the operator's real decisions.
    return os.getenv("GITHUB_REPOSITORY", REPOSITORY) == REPOSITORY


def reviewed_failure(run, report_key, workflow, journal):
    """Only exact, branch-scoped legacy identities; no time-based blanket ACK."""
    if not active() or os.getenv("GITHUB_REF_NAME") != "main":
        return None
    row = REVIEWED.get(str(run.get("id")))
    if not row or (report_key, workflow, run.get("head_sha")) != row[:3]:
        return None
    # Structured evidence, if subsequently recovered, always outranks this
    # legacy exception and must follow ordinary receipt recovery.
    if any(r.get("workflow_run_id") == str(run["id"]) for r in journal["editions"].values()):
        return None
    if report_key == "cybermed":
        from .edition_plan import load_plan
        plan = load_plan()
        if (not plan or plan["report_key"] != report_key or plan["mode"] != "real"
                or plan["logical_date"] < FRESH_START):
            return None
    return {"run_id": str(run["id"]), "source_commit": row[2],
            "log_archive_sha256": row[3], "finding": row[4]}


def hold_reason(report_key, cadence, start, end, entry=None):
    """Finite historical window; new missing inputs/errors are not downgraded."""
    if not active() or report_key != "cybermed":
        return None
    if end < FRESH_START:
        return "legacy_delivery_history_held"
    if cadence == "weekly" and (start, end) == ("2026-10-12", "2026-10-16"):
        if entry is None:
            return "legacy_overlap_requires_review"
        from .edition_plan import check_ready
        try:
            check_ready(entry)
        except RuntimeError as exc:
            category = str(exc)
            # Corruption, foreign/duplicate identities and unexpected failures
            # remain fatal. Only the reviewed transition's coverage gap defers.
            if category == "edition_weekly_daily_coverage_incomplete":
                return "legacy_source_coverage_held"
            raise
    return None


def _payload(coordinator, reason):
    return {"schema_version": 1, "kind": "reviewed_legacy_hold", "report_key": coordinator.report_key,
            "cadence": coordinator.cadence, "period_start": coordinator.start, "period_end": coordinator.end,
            "edition_id": coordinator.edition_id, "workflow_run_id": os.getenv("GITHUB_RUN_ID", "local"),
            "status": "intentionally_skipped", "category": reason, "policy_sha256": POLICY_SHA256,
            "historical_smtp_outcome": "unknown", "content_version": None, "message_id": None,
            "generation_validated": False, "smtp_attempted": False, "state_persisted": False}


def _no_handoff_receipt(row):
    return row is None or (row.get("status") == "failed" and row.get("generation_validated") is False
        and row.get("smtp_phase") == "not_handed_off" and not row.get("recovery")
        and not any(row.get(k) for k in ("accepted", "rejected", "unknown")))


def expected_hold(coordinator, entry=None):
    row = coordinator.journal["editions"].get(coordinator.edition_id)
    if coordinator.mode != "real" or not _no_handoff_receipt(row):
        return None  # Never downgrade generated evidence or a real SMTP receipt.
    reason = hold_reason(coordinator.report_key, coordinator.cadence, coordinator.start, coordinator.end, entry)
    return _payload(coordinator, reason) if reason else None


def warn_hold(value):
    text = (f"{value['edition_id']}: {value['category']}; this attempt was not generated or sent; "
            "historical SMTP outcome remains unknown")
    print("::warning title=Historical report held::" + text)
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write("\n- **Report held, no SMTP:** " + text + "\n")


def write_hold(coordinator, entry=None):
    value = expected_hold(coordinator, entry)
    if value is None:
        return False
    atomic_store(coordinator.report_dir / f"{coordinator.report_key}_{coordinator.cadence}_delivery_result.json",
                 value, kind="legacy_attempt_hold")
    from .run_timing import record
    record(coordinator.report_key, coordinator.cadence, outcome="intentionally_skipped",
           delivery_status="intentionally_skipped", edition_id=coordinator.edition_id)
    warn_hold(value)
    return True


def verify_hold(value, report_key, cadence, report_dir, mode):
    """Verify the specific policy/edition/run, not just a claimed skip status."""
    from .delivery import Coordinator
    from .edition_plan import planned_entry
    coordinator = Coordinator(report_key, cadence, mode=mode, report_dir=report_dir)
    expected = expected_hold(coordinator, planned_entry(report_key, cadence))
    if expected is None or value != expected:
        raise RuntimeError("legacy_hold_verification_failed")
    warn_hold(value)


def guard_submission(coordinator):
    if coordinator.mode != "real" or not _no_handoff_receipt(coordinator.initial_receipt):
        return
    from .edition_plan import planned_entry
    if hold_reason(coordinator.report_key, coordinator.cadence, coordinator.start, coordinator.end,
                   planned_entry(coordinator.report_key, coordinator.cadence)):
        raise RuntimeError("legacy_period_smtp_prohibited")


# One reviewed modern run, distinct from the pre-journal transition:
# https://github.com/Cyberlurch/NewsAgent2/actions/runs/38874103951
# On 2026-10-16 the frozen production plan was Daily+Weekly+September Monthly.
# The job log confirms Daily was accepted; Weekly and Monthly were explicitly
# held before SMTP by existing legacy guards. Publication alone failed with
# publication_newsletter_scope_mismatch. A later authorized Weekly run was accepted.
# This exact-case resolution never represents the held Monthly as delivered.
REVIEWED_OCT16_RUN_ID = "38874103951"
REVIEWED_OCT16_SHA = "b24a0f9f296eaee89b5cbc4a555477fb740ddad5"


def reviewed_oct16_publication_failure(run, jobs, report_key, workflow, journal, *, repository, branch):
    """Close only the audited failed *publication*, never an ambiguous SMTP handoff.

    Require immutable run identity, successful per-cadence verification, the
    publication-only failure, same-run persisted Daily receipt and a separate
    persisted Weekly completion. Other failed runs remain fail-closed.
    """
    if (repository, branch, report_key, workflow, str(run.get("id")),
            run.get("head_sha"), run.get("event"), run.get("conclusion")) != (
            REPOSITORY, "main", "cybermed", "cybermed.yml", REVIEWED_OCT16_RUN_ID,
            REVIEWED_OCT16_SHA, "schedule", "failure"):
        return False
    if len(jobs) != 1:
        return False
    steps = jobs[0].get("steps", [])
    required = {
        "Compute frozen cybermed run plan": "success",
        "Run Cybermed reports": "success",
        "Verify each planned cybermed delivery result": "success",
        "Commit and publish validated cybermed state": "failure",
    }
    if any(len([s for s in steps if s.get("name") == name and s.get("conclusion") == status]) != 1
           for name, status in required.items()):
        return False
    if (hold_reason("cybermed", "weekly", "2026-10-12", "2026-10-16")
            != "legacy_overlap_requires_review"
            or hold_reason("cybermed", "monthly", "2026-09-01", "2026-09-30")
            != "legacy_delivery_history_held"):
        return False
    rows = journal.get("editions", {})
    daily = rows.get("cybermed:daily:2026-10-16:2026-10-16:production")
    weekly = rows.get("cybermed:weekly:2026-10-12:2026-10-16:production")
    if not daily or not weekly:
        return False
    if any(r.get("workflow_run_id") == REVIEWED_OCT16_RUN_ID and r is not daily
           for r in rows.values()):
        return False
    return all((
        daily.get("report_key") == "cybermed",
        daily.get("workflow_run_id") == REVIEWED_OCT16_RUN_ID,
        daily.get("status") == "accepted_by_smtp",
        daily.get("state_persisted") is True,
        daily.get("smtp_phase") == "completed",
        weekly.get("report_key") == "cybermed",
        weekly.get("workflow_run_id") == "38894702613",
        weekly.get("status") == "accepted_by_smtp",
        weekly.get("state_persisted") is True,
        weekly.get("smtp_phase") == "completed",
    ))
