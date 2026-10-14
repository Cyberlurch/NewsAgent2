"""Per-edition SMTP receipts, with explicit uncertainty and safe resumption.

SMTP acceptance is not inbox delivery. The journal and Git are not a shared
transaction; an in-flight attempt is durably unknown before handing off DATA.
"""
from __future__ import annotations

import calendar
import hashlib
import hmac
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

from .run_context import path_for, email_mode
from .store_io import StoreError, atomic_store, read_store, atomic_text


STATUSES = {"generated", "intentionally_skipped", "accepted_by_smtp", "partially_accepted", "failed", "outcome_unknown"}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def period(cadence: str, today: date | None = None, report_key: str | None = None) -> tuple[str, str]:
    from .edition_plan import planned_entry
    entry = None if any((os.getenv("DELIVERY_PERIOD_START"), os.getenv("DELIVERY_PERIOD_END"))) else planned_entry(report_key or os.getenv("REPORT_KEY") or "cyberlurch", cadence)
    if entry and not any((os.getenv("DELIVERY_PERIOD_START"), os.getenv("DELIVERY_PERIOD_END"))):
        return entry["period_start"], entry["period_end"]
    today = today or datetime.now(ZoneInfo("Europe/Stockholm")).date()
    explicit = (os.getenv("DELIVERY_PERIOD_START"), os.getenv("DELIVERY_PERIOD_END"))
    if any(explicit):
        if not all(explicit):
            raise ValueError("delivery_period_requires_both_dates")
        start, end = (date.fromisoformat(v) for v in explicit)
    elif cadence == "daily":
        start = end = today
    elif cadence == "weekly":
        if (report_key or os.getenv("REPORT_KEY")) == "cybermed":
            from .cybermed_digest_store import cybermed_weekly_reporting_period
            start, end = cybermed_weekly_reporting_period(today)
        else:
            end = today
            start = end - timedelta(days=7)
    elif cadence == "monthly":
        month = os.getenv("ROLLUP_MONTH_OVERRIDE")
        previous = os.getenv("GITHUB_EVENT_NAME") == "schedule" and (today.day == 1 or (today.weekday() == 4 and today.day <= 7))
        start = date.fromisoformat(month + "-01") if month else (
            (today.replace(day=1) - timedelta(days=1)).replace(day=1) if previous else today.replace(day=1))
        end = start.replace(day=calendar.monthrange(start.year, start.month)[1])
    elif cadence == "yearly":
        manual = os.getenv("GITHUB_EVENT_NAME") in {"manual", "workflow_dispatch"}
        year = int(os.getenv("YEAR_IN_REVIEW_YEAR") or (today.year if manual else today.year - 1))
        start, end = date(year, 1, 1), date(year, 12, 31)
    else:
        raise ValueError("invalid_delivery_cadence")
    if end < start:
        raise ValueError("invalid_delivery_period")
    return start.isoformat(), end.isoformat()


@dataclass
class DeliveryResult:
    report_key: str
    cadence: str
    period_start: str
    period_end: str
    edition_id: str
    content_version: str
    message_id: str
    status: str = "generated"
    category: str = ""
    accepted: list[str] = field(default_factory=list)  # HMAC IDs only.
    rejected: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)
    state_persisted: bool = False
    updated_at_utc: str = field(default_factory=now_iso)
    recipient_key_id: str = ""
    generation_validated: bool = True
    finalization: dict = field(default_factory=dict)
    workflow_run_id: str = field(default_factory=lambda: os.getenv("GITHUB_RUN_ID") or "local")
    prior_attempts: list[dict] = field(default_factory=list)
    smtp_phase: str = "legacy_unknown"
    recovery: dict = field(default_factory=dict)

    def public(self) -> dict:
        return asdict(self)


class DeliveryError(RuntimeError):
    def __init__(self, result: DeliveryResult):
        self.result = result
        super().__init__(f"delivery_error:{result.status}:{result.category or 'smtp'}")


def journal_valid(value) -> bool:
    if not isinstance(value, dict) or not isinstance(value.get("editions"), dict):
        return False
    unknown = value.get("paper_legacy_unknown")
    if unknown is not None:
        if (not isinstance(unknown, dict) or unknown.get("version") != 1
                or not isinstance(unknown.get("reports"), dict)
                or any(key not in {"cybermed", "cyberlurch"} or not isinstance(ids, list)
                       or any(not isinstance(i, str) or not i.isdigit() for i in ids)
                       for key, ids in unknown["reports"].items())):
            return False
    for key, row in value["editions"].items():
        try:
            result = DeliveryResult(**row)
            from .pre_smtp_recovery import recovery_valid
            if result.smtp_phase not in {"legacy_unknown", "not_handed_off", "handoff_started", "completed"} or not recovery_valid(result.recovery):
                return False
            if result.edition_id != key or result.status not in STATUSES:
                return False
            if result.report_key not in {"cybermed", "cyberlurch"} or result.cadence not in {"daily", "weekly", "monthly", "yearly"}:
                return False
            identity = result.edition_id.split(":")
            if identity[:4] != [result.report_key, result.cadence, result.period_start, result.period_end] or len(identity) != 5 or identity[4] not in {"production", "none", "test"}:
                return False
            if not re.fullmatch(r"[a-z0-9_]*", result.category) or not re.fullmatch(r"local|\d+", result.workflow_run_id):
                return False
            if date.fromisoformat(result.period_start) > date.fromisoformat(result.period_end):
                return False
            if len(result.content_version) != 64 or any(c not in "0123456789abcdef" for c in result.content_version):
                return False
            for ids in (result.accepted, result.rejected, result.unknown):
                if not isinstance(ids, list) or any(not isinstance(v, str) or len(v) != 64 or any(c not in "0123456789abcdef" for c in v) for v in ids):
                    return False
            if set(result.accepted) & (set(result.rejected) | set(result.unknown)):
                return False
            if not isinstance(result.finalization, dict) or not isinstance(result.state_persisted, bool) or not isinstance(result.generation_validated, bool):
                return False
            expected = hashlib.sha256((result.edition_id + ":" + result.content_version).encode()).hexdigest()
            if result.message_id != f"<{expected}@newsagent2.invalid>":
                return False
            if not isinstance(result.updated_at_utc, str):
                return False
            datetime.fromisoformat(result.updated_at_utc.replace("Z", "+00:00"))
            if not isinstance(result.prior_attempts, list) or any(not isinstance(attempt, dict)
                or set(attempt) - {"status", "category", "content_version", "smtp_phase"}
                or attempt.get("smtp_phase", "legacy_unknown") not in {"legacy_unknown", "not_handed_off", "handoff_started", "completed"}
                or attempt.get("status") not in STATUSES
                or not re.fullmatch(r"[a-z0-9_]*", attempt.get("category", ""))
                or not re.fullmatch(r"[a-f0-9]{64}", attempt.get("content_version", "")) for attempt in result.prior_attempts):
                return False
            allowed = {"items_all_new", "overview_items", "detail_items", "foamed_overview_items", "report_key", "report_mode", "now_utc_iso"}
            if set(result.finalization) - allowed:
                return False
            for field_name in ("items_all_new", "overview_items", "detail_items", "foamed_overview_items"):
                rows = result.finalization.get(field_name, [])
                if not isinstance(rows, list) or any(not isinstance(row, dict) or set(row) - {"source", "id", "title", "channel", "url", "paper_delivery"} for row in rows):
                    return False
                from .paper_delivery import metadata_valid
                for item in rows:
                    if "paper_delivery" in item and (item.get("source") != "pubmed" or
                            not metadata_valid(item["paper_delivery"]) or item.get("id") != item["paper_delivery"]["pmid"]):
                        return False
            if result.recovery and any("paper_delivery" in item for field in
                    ("items_all_new", "overview_items", "detail_items", "foamed_overview_items")
                    for item in result.finalization.get(field, [])):
                from .paper_delivery import digest
                if result.recovery["finalization_hash"] != digest(result.finalization):
                    return False
            if result.status == "accepted_by_smtp" and (not result.accepted or result.rejected or result.unknown):
                return False
            if result.status == "partially_accepted" and (not result.accepted or not result.rejected):
                return False
        except (TypeError, ValueError, KeyError):
            return False
    return True


def load_journal(path: Path) -> dict:
    return read_store(path, {"version": 1, "editions": {}}, kind="delivery_journal", validate=journal_valid)


class Coordinator:
    def __init__(self, report_key: str, cadence: str, *, journal_path: Path | None = None,
                 report_dir: Path | None = None, mode: str | None = None, period_dates: tuple[str, str] | None = None):
        self.report_key, self.cadence = report_key, cadence
        self.mode = mode or email_mode()
        self.path = journal_path or path_for("DELIVERY_JOURNAL_PATH")
        self.report_dir = report_dir or Path(os.getenv("REPORT_DIR") or "reports")
        self.recovery = self.report_dir / "recovery"
        start, end = period_dates or period(cadence, report_key=report_key)
        if date.fromisoformat(end) < date.fromisoformat(start):
            raise ValueError("invalid_delivery_period")
        self.start, self.end = start, end
        purpose = "production" if self.mode == "real" else self.mode
        self.edition_id = f"{report_key}:{cadence}:{start}:{end}:{purpose}"
        self.journal = load_journal(self.path)
        self.result: DeliveryResult | None = None

    def _persist(self, result: DeliveryResult) -> None:
        result.updated_at_utc = now_iso()
        self.result = result
        self.journal["editions"][result.edition_id] = result.public()
        if not journal_valid(self.journal):
            raise StoreError("delivery_journal", "invalid_output")
        # Recovery receipt first: a subsequent journal/Git failure must not hide
        # known server acceptance. No whole intermediate state is exported.
        atomic_store(self.recovery / "journal.json", self.journal, kind="delivery_recovery", validate=journal_valid)
        atomic_store(self.path, self.journal, kind="delivery_journal", validate=journal_valid)
        atomic_store(self.report_dir / f"{self.report_key}_{self.cadence}_delivery_result.json",
                     result.public(), kind="delivery_result")

    def generated(self, body: str, finalization: dict | None = None) -> DeliveryResult:
        version = hashlib.sha256(body.encode()).hexdigest()
        message = hashlib.sha256((self.edition_id + ":" + version).encode()).hexdigest()
        result = DeliveryResult(self.report_key, self.cadence, self.start, self.end,
                                self.edition_id, version, f"<{message}@newsagent2.invalid>",
                                finalization=finalization or {}, smtp_phase="not_handed_off")
        old = self.journal["editions"].get(self.edition_id) if self.mode != "none" else None
        if old:
            previous = DeliveryResult(**old)
            self.result = previous
            if previous.content_version != version and not (previous.status == "failed" and not previous.generation_validated):
                # Retain the old receipt; changed content never licenses resend.
                raise DeliveryError(DeliveryResult(**{**result.public(), "status": "failed",
                                                     "category": "content_version_changed_requires_review"}))
            if previous.status in {"accepted_by_smtp", "partially_accepted", "outcome_unknown"}:
                self.result = previous
                return previous
            if previous.recovery:
                if finalization is not None and finalization != previous.finalization:
                    raise DeliveryError(DeliveryResult(**{**previous.public(), "status": "failed",
                        "category": "recovery_finalization_changed"}))
                self.result = previous
                return previous
        self.recovery.mkdir(parents=True, exist_ok=True)
        target = self.recovery / f"{message}.md"
        # The exact validated outgoing Markdown is retained for explicit replay.
        atomic_text(target, body)
        if old:
            result.prior_attempts = previous.prior_attempts
            if previous.status == "failed":
                result.prior_attempts.append({"status": previous.status, "category": previous.category,
                                              "content_version": previous.content_version, "smtp_phase": previous.smtp_phase})
        self._persist(result)
        from .run_timing import record
        record(self.report_key, self.cadence, "generated_at", outcome="generated", delivery_status="generated", edition_id=result.edition_id)
        return result

    def skip(self, body: str, reason: str, *, finalization: dict | None = None) -> DeliveryResult:
        result = self.generated(body, finalization)
        if result.status in {"partially_accepted", "outcome_unknown"}:
            # Disabling a later attempt cannot erase a prior SMTP handoff.
            # Preview editions have separate none/test identities.
            raise DeliveryError(result)
        if result.status == "accepted_by_smtp":
            # The current invocation skipped; keep the historical receipt in
            # the journal and do not rerun its finalization callbacks.
            skipped = DeliveryResult(**{**result.public(), "status": "intentionally_skipped",
                "category": reason, "state_persisted": False, "accepted": [],
                "rejected": [], "unknown": [], "finalization": {}})
            self.result = skipped
            atomic_store(self.report_dir / f"{self.report_key}_{self.cadence}_delivery_result.json",
                         skipped.public(), kind="delivery_result")
            return skipped
        result.status, result.category = "intentionally_skipped", reason
        result.generation_validated = bool(body)
        self._persist(result)
        return result

    def failure(self, category: str, *, generated: bool = False) -> DeliveryResult:
        result = self.result
        previous = self.journal["editions"].get(self.edition_id)
        if previous and (result is None or previous["status"] in {"accepted_by_smtp", "partially_accepted", "outcome_unknown"}):
            result = DeliveryResult(**previous)
        if result is None:
            version = hashlib.sha256(b"").hexdigest()
            message = hashlib.sha256((self.edition_id + ":" + version).encode()).hexdigest()
            result = DeliveryResult(self.report_key, self.cadence, self.start, self.end,
                self.edition_id, version, f"<{message}@newsagent2.invalid>", generation_validated=generated,
                smtp_phase="not_handed_off")
        from .pre_smtp_recovery import FAILURE
        if result.status == "failed" and result.recovery and result.category == FAILURE:
            # A blocked ordinary run/preflight must retain the proven original
            # persistence failure, rather than severing its explicit replay path.
            self._persist(result)
            return result
        # A known SMTP receipt survives generation/state/push failure unchanged.
        if result.status not in {"accepted_by_smtp", "partially_accepted", "outcome_unknown"}:
            result.status, result.category = "failed", category
        self._persist(result)
        return result

    def deliver(self, body: str, recipients: list[str], transport: Callable, *, finalization: dict | None = None,
                retry_rejected: bool = False) -> DeliveryResult:
        result = self.generated(body, finalization)
        if result.status == "outcome_unknown" or (result.status == "partially_accepted" and not retry_rejected):
            raise DeliveryError(result)  # Operator reconciliation, never blind retry.
        key = os.getenv("DELIVERY_RECIPIENT_HMAC_KEY", "")
        if len(key.encode()) < 32:
            raise DeliveryError(self.failure("recipient_hmac_key_missing", generated=True))
        refs = {r: hmac.new(key.encode(), r.strip().casefold().encode(), hashlib.sha256).hexdigest() for r in recipients}
        prior_accepted = set(result.accepted)
        if result.status == "partially_accepted":
            expected_key = hmac.new(key.encode(), b"newsagent2-key-id", hashlib.sha256).hexdigest()
            if expected_key != result.recipient_key_id or set(refs.values()) != set(result.accepted + result.rejected):
                raise DeliveryError(DeliveryResult(**{**result.public(), "status": "failed", "category": "recipient_set_changed_requires_review"}))
            recipients = [r for r in recipients if refs[r] in result.rejected]
            result.prior_attempts.append({"status": result.status, "category": result.category, "content_version": result.content_version})
            refs = {r: refs[r] for r in recipients}
        if result.status == "accepted_by_smtp":
            if set(refs.values()) != set(result.accepted):
                raise DeliveryError(DeliveryResult(**{**result.public(), "status": "failed",
                                                     "category": "recipient_set_changed_requires_review"}))
            return result  # No duplicate SMTP; persistence may need repair.
        result.recipient_key_id = hmac.new(key.encode(), b"newsagent2-key-id", hashlib.sha256).hexdigest()

        def inflight():
            result.status, result.category = "outcome_unknown", "smtp_in_flight"
            result.smtp_phase = "handoff_started"
            result.unknown = sorted(set(refs.values()))
            result.rejected = []
            self._persist(result)  # Must succeed BEFORE DATA/sendmail.

        def outcome(status: str, accepted: list[str], rejected: list[str], category: str):
            result.status, result.category = status, category
            if status != "outcome_unknown" and result.smtp_phase == "handoff_started":
                result.smtp_phase = "completed"
            result.accepted = sorted(prior_accepted | {refs[r] for r in accepted})
            result.rejected = sorted({refs[r] for r in rejected})
            result.unknown = sorted(set(refs.values()) - set(result.accepted) - set(result.rejected))
            if result.accepted and result.rejected and not result.unknown:
                result.status = "partially_accepted"
            self._persist(result)  # Before QUIT and before state finalization.
            from .run_timing import record
            record(self.report_key, self.cadence, "smtp_accepted_at" if result.status == "accepted_by_smtp" else None, outcome=result.status, delivery_status=result.status, edition_id=result.edition_id)

        transport(result.message_id, recipients, inflight, outcome)
        if result.status != "accepted_by_smtp":
            raise DeliveryError(result)
        return result

    def finalize(self, callbacks: list[Callable]) -> DeliveryResult:
        result = self.result
        if result is None:
            raise RuntimeError("delivery_finalization_without_result")
        if result.status == "accepted_by_smtp" and self.mode == "real":
            try:
                for callback in callbacks:
                    callback(result)
            except Exception:
                result.state_persisted = False
                result.category = "state_persistence_failed"
                self._persist(result)
                raise DeliveryError(result) from None
            result.state_persisted = True
            result.category = ""
            self._persist(result)
            from .run_timing import record
            record(self.report_key, self.cadence, "state_persisted_at", edition_id=result.edition_id)
        return result


def verify_reports(report_dir: Path, report_key: str, cadences: list[str], *, mode: str, send_required: bool) -> None:
    def expected_period(cadence):
        from .edition_plan import planned_entry
        entry = planned_entry(report_key, cadence)
        return (entry["period_start"], entry["period_end"]) if entry else period(cadence, report_key=report_key)
    errors = []
    for cadence in cadences:
        path = report_dir / f"{report_key}_{cadence}_delivery_result.json"
        data = read_store(path, {}, kind="planned_delivery_result")
        try:
            result = DeliveryResult(**data)
        except TypeError:
            start, end = expected_period(cadence)
            identity = f"{report_key}:{cadence}:{start}:{end}:{'production' if mode == 'real' else mode}"
            version = hashlib.sha256(b"").hexdigest()
            message = hashlib.sha256((identity + ":" + version).encode()).hexdigest()
            result = DeliveryResult(report_key, cadence, start, end, identity, version,
                f"<{message}@newsagent2.invalid>", status="failed", category="planned_result_missing", generation_validated=False)
            atomic_store(path, result.public(), kind="planned_delivery_result")
            errors.append(f"missing_planned_delivery_result:{report_key}:{cadence}")
            from .run_timing import record
            record(report_key, cadence, outcome="failed", delivery_status="failed")
            continue
        if result.report_key != report_key or result.cadence != cadence or result.status not in STATUSES:
            errors.append("mismatched_planned_delivery_result")
            continue
        expected_start, expected_end = expected_period(cadence)
        if (result.period_start, result.period_end) != (expected_start, expected_end):
            errors.append(f"mismatched_planned_delivery_period:{report_key}:{cadence}")
            continue
        from .run_timing import record
        record(report_key, cadence, outcome=result.status, delivery_status=result.status, edition_id=result.edition_id)
        if send_required:
            if result.status != "accepted_by_smtp" or (mode == "real" and not result.state_persisted):
                errors.append(f"planned_delivery_not_finalized:{report_key}:{cadence}")
        elif result.status != "intentionally_skipped":
            errors.append(f"planned_delivery_not_intentionally_skipped:{report_key}:{cadence}")
    if errors:
        raise RuntimeError(";".join(errors))
