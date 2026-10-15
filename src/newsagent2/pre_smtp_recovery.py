"""Explicit replay of a complete, sealed output which failed before SMTP.

No generation/provider calls. Legacy text-only receipts do not grant approval.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from pathlib import Path

from .run_context import email_mode, path_for, writer_lock
from .store_io import StoreError, atomic_store, read_store, digest_store, rollups_store


FAILURE = "generated_persistence_failed_before_smtp"
HEX = re.compile(r"[0-9a-f]{64}")
BINDINGS = {"cybermed_daily": "CYBERMED_DAILY_DIGEST_STATE_PATH",
    "cybermed_weekly": "CYBERMED_WEEKLY_DIGEST_STATE_PATH",
    "cyberlurch_digests": "CYBERLURCH_DIGEST_STATE_PATH", "rollups": "ROLLUPS_STATE_PATH"}


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def recovery_valid(value) -> bool:
    if value == {}:
        return True  # Additive legacy read; never an approval.
    if not isinstance(value, dict):
        return False
    required = {"version", "edition_id", "subject", "subject_hash", "audience", "recipient_key_id",
        "finalization_hash", "finalization_policy", "content_version", "message_id", "history_clear",
        "generated_deltas", "seal", "approved_after_generation_checks"}
    if set(value) != required or value.get("version") != 1 or value.get("approved_after_generation_checks") is not True:
        return False
    if not isinstance(value["subject"], str) or not value["subject"] or "\n" in value["subject"] or "\r" in value["subject"]:
        return False
    if value["finalization_policy"] not in {"markers", "no_markers"} or not isinstance(value["history_clear"], bool):
        return False
    if any(not isinstance(value[key], str) or not HEX.fullmatch(value[key]) for key in
        ("subject_hash", "recipient_key_id", "finalization_hash", "content_version", "seal")):
        return False
    if not isinstance(value["audience"], list) or not value["audience"] or any(not isinstance(v, str) or not HEX.fullmatch(v) for v in value["audience"]):
        return False
    if not isinstance(value["edition_id"], str) or not isinstance(value["message_id"], str):
        return False
    if not isinstance(value["generated_deltas"], list):
        return False
    return all(isinstance(row, dict) and set(row) == {"file", "sha256"} and isinstance(row["file"], str)
        and re.fullmatch(r"[0-9a-f]{16}_(cybermed|cyberlurch)_(daily|weekly|monthly|yearly)_[a-z_]+_generated_delta\.json", row["file"])
        and isinstance(row["sha256"], str) and HEX.fullmatch(row["sha256"]) for row in value["generated_deltas"])


def _key():
    key = (os.getenv("DELIVERY_RECIPIENT_HMAC_KEY") or "").encode()
    if len(key) < 32:
        raise RuntimeError("recovery_recipient_key_missing")
    return key


def _seal(value):
    return hmac.new(_key(), digest({k: v for k, v in value.items() if k != "seal"}).encode(), hashlib.sha256).hexdigest()


def checkpoint(coordinator, subject: str, body: str, intents: dict, candidates: dict, *, markers_required: bool):
    """Called ONLY after _main returns via its final approved queue point."""
    from .emailer import current_audience_refs, _extract_run_metadata_for_email
    clean, _, _ = _extract_run_metadata_for_email(body)
    if not clean.strip() or (markers_required and not intents) or (not markers_required and intents):
        raise RuntimeError("recovery_readiness_incomplete")
    result = coordinator.generated(clean, intents)
    if result.status != "generated" or result.smtp_phase != "not_handed_off":
        raise RuntimeError("recovery_readiness_requires_fresh_generation")
    manifest = [{"file": candidate["artifact"].name, "sha256": hashlib.sha256(candidate["artifact"].read_bytes()).hexdigest()}
                for _, candidate in sorted(candidates.items(), key=lambda item: str(item[0]))]
    proof = {"version": 1, "edition_id": result.edition_id, "subject": subject, "subject_hash": digest(subject),
        "audience": sorted(current_audience_refs(result.report_key, result.cadence)),
        "recipient_key_id": hmac.new(_key(), b"newsagent2-key-id", hashlib.sha256).hexdigest(),
        "finalization_hash": digest(intents), "finalization_policy": "markers" if markers_required else "no_markers",
        "content_version": result.content_version, "message_id": result.message_id,
        "history_clear": all(a.get("smtp_phase") == "not_handed_off" and a.get("status") == "failed" for a in result.prior_attempts),
        "generated_deltas": manifest, "approved_after_generation_checks": True}
    proof["seal"] = _seal(proof)
    if not recovery_valid(proof):
        raise RuntimeError("recovery_readiness_invalid")
    result.recovery = proof
    result.recipient_key_id = proof["recipient_key_id"]
    coordinator._persist(result)  # Recovery and journal are durable before production writes.


def _delta_plan(artifact: Path, row: dict):
    delta = read_store(artifact, {}, kind="generated_delta")
    kind = delta.get("kind")
    if (kind not in BINDINGS or delta.get("generation_validated") is not True or delta.get("production_target") is not True
        or delta.get("mode") != "real" or any(delta.get(k) != row[k] for k in ("report_key", "cadence", "edition_id"))):
        raise RuntimeError("recovery_delta_scope_mismatch")
    target = path_for(BINDINGS[kind])
    validate = rollups_store if kind == "rollups" else digest_store
    if kind == "cyberlurch_digests":
        from .youtube_source import digest_valid
        validate = digest_valid
    default = {"version": 1, "reports": {}} if kind == "rollups" else {"schema_version": 1, "digests": []}
    value = read_store(target, default, kind=kind, validate=validate)
    rows = value.get("reports", {}).get(row["report_key"], []) if kind == "rollups" else value["digests"]
    records, removed = delta.get("records"), delta.get("removed_record_ids")
    if not isinstance(records, list) or not all(isinstance(v, dict) for v in records) or not isinstance(removed, list) or not all(isinstance(v, str) for v in removed):
        raise RuntimeError("recovery_delta_incomplete")
    def identity(record): return str(record.get("digest_id") or record.get("video_id") or record.get("month") or "")
    groups = {}
    for record in records:
        identity_key = identity(record)
        if not identity_key: raise RuntimeError("recovery_delta_identity_missing")
        groups.setdefault(identity_key, []).append(record)
    if all([v for v in rows if identity(v) == key] == group for key, group in groups.items()) and not any(identity(v) in removed for v in rows):
        return target, value, kind, validate, False
    actual = hashlib.sha256(target.read_bytes()).hexdigest() if target.exists() else None
    if actual != delta.get("base_sha256"):
        raise RuntimeError("recovery_generated_preimage_conflict")
    merged, inserted = [], set()
    for record in rows:
        identity_key = identity(record)
        if identity_key in removed: continue
        if identity_key in groups:
            if identity_key not in inserted:
                merged.extend(groups[identity_key]); inserted.add(identity_key)
        else: merged.append(record)
    for identity_key, group in groups.items():
        if identity_key not in inserted: merged.extend(group)
    if kind == "rollups": value.setdefault("reports", {})[row["report_key"]] = merged
    else: value["digests"] = merged
    from .cybermed_quality import assert_no_generation_error_text
    assert_no_generation_error_text(value, boundary="pre_smtp_recovery")
    if kind == "cybermed_weekly":
        from datetime import date
        for record in value["digests"]:
            if not record.get("digest_id"): raise RuntimeError("recovery_weekly_invalid")
            date.fromisoformat(record["period_start"]); date.fromisoformat(record["period_end"])
    return target, value, kind, validate, True


def _validate_saved(row: dict, directory: Path, *, marker_repair: bool = False):
    proof = row.get("recovery")
    if not proof:
        raise RuntimeError("legacy_recovery_readiness_missing_operator_review_required")
    if not recovery_valid(proof) or not hmac.compare_digest(proof["seal"], _seal(proof)):
        raise RuntimeError("recovery_readiness_binding_invalid")
    if (proof["edition_id"] != row["edition_id"] or proof["content_version"] != row["content_version"] or proof["message_id"] != row["message_id"]
        or proof["subject_hash"] != digest(proof["subject"]) or proof["finalization_hash"] != digest(row["finalization"])
        or (proof["finalization_policy"] == "markers" and not row["finalization"])
        or (proof["finalization_policy"] == "no_markers" and row["finalization"])):
        raise RuntimeError("recovery_output_binding_invalid")
    from .emailer import current_audience_refs
    if set(proof["audience"]) != current_audience_refs(row["report_key"], row["cadence"]):
        raise RuntimeError("recovery_audience_changed")
    if proof["recipient_key_id"] != hmac.new(_key(), b"newsagent2-key-id", hashlib.sha256).hexdigest():
        raise RuntimeError("recovery_recipient_key_changed")
    if marker_repair and set(row["accepted"]) != set(proof["audience"]):
        raise RuntimeError("recovery_incomplete_acceptance_receipt")
    if not marker_repair and (not proof["history_clear"] or any(a.get("smtp_phase") != "not_handed_off" or a.get("status") != "failed" for a in row["prior_attempts"])):
        raise RuntimeError("recovery_prior_attempt_uncertain")
    name = hashlib.sha256((row["edition_id"] + ":" + row["content_version"]).encode()).hexdigest() + ".md"
    source = directory / name
    if not source.is_file() or hashlib.sha256(source.read_bytes()).hexdigest() != row["content_version"]:
        raise RuntimeError("recovery_content_missing_or_changed")
    body = source.read_text(encoding="utf-8")
    from .cybermed_quality import assert_no_generation_error_text
    assert_no_generation_error_text(body, boundary="pre_smtp_replay")
    plans = []
    if not proof["generated_deltas"]:
        raise RuntimeError("recovery_required_deltas_missing")
    for manifest in proof["generated_deltas"]:
        artifact = directory / manifest["file"]
        if not artifact.is_file() or hashlib.sha256(artifact.read_bytes()).hexdigest() != manifest["sha256"]:
            raise RuntimeError("recovery_required_delta_missing_or_changed")
        plans.append(_delta_plan(artifact, row))
    if len({str(plan[0].resolve()) for plan in plans}) != len(plans):
        raise RuntimeError("recovery_duplicate_store_target")
    return body, plans


def staged_delta_approved(directory: Path, delta: dict, filename: str) -> bool:
    """Stage candidates never become production records without late approval."""
    from .delivery import load_journal
    row = load_journal(directory / "journal.json")["editions"].get(delta["edition_id"], {})
    proof = row.get("recovery")
    if not proof:
        return False
    if not recovery_valid(proof) or not hmac.compare_digest(proof["seal"], _seal(proof)):
        raise RuntimeError("recovery_readiness_binding_invalid")
    if proof["edition_id"] != row["edition_id"] or proof["content_version"] != row["content_version"] or proof["finalization_hash"] != digest(row["finalization"]):
        raise RuntimeError("recovery_output_binding_invalid")
    if filename not in {item["file"] for item in proof["generated_deltas"]}:
        return False
    for manifest in proof["generated_deltas"]:
        artifact = directory / manifest["file"]
        if not artifact.is_file() or hashlib.sha256(artifact.read_bytes()).hexdigest() != manifest["sha256"]:
            raise RuntimeError("recovery_required_delta_missing_or_changed")
    return True


def replay(edition: str, *, apply: bool = False, confirm: bool = False) -> dict:
    if not edition or not edition.endswith(":production"):
        raise RuntimeError("recovery_production_edition_required")
    if apply and (not confirm or email_mode() != "real"):
        raise RuntimeError("recovery_apply_requires_confirmation_and_real_mode")
    with writer_lock(write=apply):
        from .delivery import Coordinator, DeliveryResult, load_journal
        row = load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"].get(edition)
        from_artifact = row is None
        directory = Path(os.getenv("REPORT_DIR") or "out") / "recovery"
        if row is None:
            # A failed runner may never have pushed its receipt. Read the
            # artifact without importing it during preview; Apply persists ONLY
            # this sealed, validated edition and preserves unrelated history.
            row = load_journal(directory / "journal.json")["editions"].get(edition)
        if not row: raise RuntimeError("recovery_edition_missing")
        from .main import _preflight_stores
        _preflight_stores(row["report_key"])
        if row["status"] == "accepted_by_smtp":
            # Filesystem success in an old artifact is not publication to the
            # current branch. Restore only this edition, then repair markers.
            marker_repair = from_artifact or not row["state_persisted"]
            plans = []
            if marker_repair and (from_artifact or row.get("recovery")):
                _, plans = _validate_saved(row, directory, marker_repair=True)
            if apply and marker_repair:
                coordinator = Coordinator(row["report_key"], row["cadence"], mode="real")
                result = DeliveryResult(**row)
                result.state_persisted = False
                coordinator._persist(result)
                for target, value, kind, validate, change in plans:
                    if change: atomic_store(target, value, kind=kind, validate=validate)
                from .workflow_delivery import finalize_receipt
                finalize_receipt(edition)  # Marker repair only; never SMTP.
            return {"edition_id": edition, "status": "already_accepted_no_smtp", "marker_repair_required": not apply and marker_repair}
        if row["status"] == "failed" and not row.get("recovery"):
            raise RuntimeError("legacy_recovery_readiness_missing_operator_review_required")
        if row["status"] != "failed" or not row["generation_validated"] or row.get("smtp_phase", "legacy_unknown") != "not_handed_off" or row["category"] != FAILURE:
            raise RuntimeError("recovery_not_a_proven_pre_smtp_persistence_failure")
        body, plans = _validate_saved(row, directory)
        preview = {"edition_id": edition, "status": "ready_for_explicit_replay", "subject": row["recovery"]["subject"],
            "content_version": row["content_version"], "period_start": row["period_start"], "period_end": row["period_end"],
            "audience_count": len(row["recovery"]["audience"]), "stores_to_restore": [kind for _, _, kind, _, change in plans if change]}
        if not apply: return preview
        # Keep the original period even when replay happens on a later date.
        old = {k: os.environ.get(k) for k in ("REPORT_KEY", "REPORT_MODE", "DELIVERY_PERIOD_START", "DELIVERY_PERIOD_END", "SEND_EMAIL")}
        os.environ.update({"REPORT_KEY": row["report_key"], "REPORT_MODE": row["cadence"], "DELIVERY_PERIOD_START": row["period_start"],
            "DELIVERY_PERIOD_END": row["period_end"], "SEND_EMAIL": "1"})
        try:
            coordinator = Coordinator(row["report_key"], row["cadence"], mode="real")
            result = DeliveryResult(**row)
            result.prior_attempts.append({"status": "failed", "category": FAILURE, "content_version": result.content_version,
                "smtp_phase": "not_handed_off"})
            coordinator._persist(result)
            for target, value, kind, validate, change in plans:
                if change: atomic_store(target, value, kind=kind, validate=validate)
            from .emailer import send_markdown
            send_markdown(row["recovery"]["subject"], body, coordinator=coordinator, finalization=row["finalization"])
            from .workflow_delivery import finalize_receipt
            finalize_receipt(edition)
            preview["status"] = "accepted_by_smtp"
            return preview
        except Exception as exc:
            if "coordinator" in locals() and coordinator.result is not None:
                from .delivery import DeliveryError
                result = coordinator.result
                category = exc.category if isinstance(exc, StoreError) else (
                    exc.result.category if isinstance(exc, DeliveryError) else "recovery_attempt_failed")
                result.prior_attempts.append({"status": result.status, "category": category,
                    "content_version": result.content_version, "smtp_phase": result.smtp_phase})
                coordinator._persist(result)
            raise
        finally:
            for key, value in old.items():
                if value is None: os.environ.pop(key, None)
                else: os.environ[key] = value
