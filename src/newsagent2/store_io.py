"""Strict, non-mutating JSON readers and validated atomic writes."""
from __future__ import annotations

import copy
import json
import os
import tempfile
import hashlib
from pathlib import Path
from typing import Callable, Any


class StoreError(RuntimeError):
    def __init__(self, kind: str, category: str):
        self.kind, self.category = kind, category
        super().__init__(f"store_error:{kind}:{category}")


def atomic_text(path: Path, text: str) -> None:
    """Durably retain exact generated content before sealing its checkpoint."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(text); stream.flush(); os.fsync(stream.fileno())
            os.replace(name, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(directory)
            finally: os.close(directory)
        finally:
            if os.path.exists(name): os.unlink(name)
    except OSError:
        raise StoreError("generated_content", "persistence_failed") from None


def object_store(value: Any) -> bool:
    return isinstance(value, dict)


def _coverage_valid(row):
    from .history_coverage import coverage_fields_valid
    return coverage_fields_valid(row)


def digest_store(value: Any) -> bool:
    return (isinstance(value, dict) and isinstance(value.get("digests"), list)
            and all(isinstance(row, dict) and _coverage_valid(row) for row in value["digests"]))


def reports_store(value: Any) -> bool:
    return isinstance(value, dict) and isinstance(value.get("reports", {}), dict)


def processed_store(value: Any) -> bool:
    if not reports_store(value) or not all(isinstance(row, dict) for row in value.get("reports", {}).values()):
        return False
    if "foamed_source_health" in value and not foamed_health_store(value["foamed_source_health"]):
        return False
    if "cyberlurch_candidates" in value:
        from .cyberlurch_candidates import valid
        if not valid(value["cyberlurch_candidates"]):
            return False
    for report in value.get("reports", {}).values():
        for name in ("pubmed", "youtube", "foamed"):
            if name in report and (not isinstance(report[name], dict) or not isinstance(report[name].get("processed", {}), dict)):
                return False
    return True


def foamed_health_store(value: Any) -> bool:
    """Validate only known Health fields; retain additive legacy metadata."""
    if not isinstance(value, dict) or any(not isinstance(row, dict) for row in value.values()):
        return False
    for row in value.values():
        failures = row.get("consecutive_failures")
        if failures is not None and (type(failures) is not int or failures < 0):
            return False
        if any(key in row and not isinstance(row[key], str) for key in ("disabled_until_utc", "last_health", "health_epoch")):
            return False
    return True


def rollups_store(value: Any) -> bool:
    return reports_store(value) and all(isinstance(rows, list) and all(isinstance(row, dict) and _coverage_valid(row) for row in rows)
                                       for rows in value.get("reports", {}).values())


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def read_store(path: str | Path, default: dict, *, kind: str,
               validate: Callable[[Any], bool] = object_store) -> dict:
    if not str(path):
        raise StoreError(kind, "empty_path")
    target = Path(path)
    from .run_context import current
    ctx = current()
    candidate = ctx.generated_candidates.get(target.resolve()) if ctx else None
    if candidate is not None:
        data = copy.deepcopy(candidate["data"])
        if not validate(data):
            raise StoreError(kind, "invalid_staged_store")
        return data
    try:
        raw = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return copy.deepcopy(default)  # Missing-new-store: no file creation.
    except UnicodeError:
        raise StoreError(kind, "corrupted_existing_store") from None
    except OSError:
        raise StoreError(kind, "unreadable_existing_store") from None
    try:
        def nonfinite(_):
            raise ValueError("nonfinite_json_number")
        data = json.loads(raw, object_pairs_hook=_pairs, parse_constant=nonfinite)
    except (ValueError, UnicodeError):
        raise StoreError(kind, "corrupted_existing_store") from None
    if not validate(data):
        raise StoreError(kind, "invalid_existing_store")
    return data


def atomic_store(path: str | Path, data: dict, *, kind: str,
                 validate: Callable[[Any], bool] = object_store) -> None:
    if not str(path) or not validate(data):
        raise StoreError(kind, "invalid_output")
    target = Path(path)
    # Never replace a corrupt preimage, even when a caller supplied fresh data.
    read_store(target, {}, kind=kind, validate=validate)
    try:
        compact = target.is_file() and b"\n " not in target.read_bytes()
        payload = json.dumps(data, ensure_ascii=False, indent=None if compact else 2,
                             separators=(",", ":") if compact else None, allow_nan=False) + "\n"
        if not validate(json.loads(payload)):
            raise ValueError("invalid_roundtrip")
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, target)
            directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(name):
                os.unlink(name)
    except (OSError, ValueError, TypeError):
        raise StoreError(kind, "persistence_failed") from None


def generated_store(path: str | Path, data: dict, *, kind: str, validate=object_store) -> None:
    """Export validated generated RECORD deltas, never the intermediate state."""
    from .run_context import current
    ctx = current()
    if ctx is None:
        atomic_store(path, data, kind=kind, validate=validate)
        return
    if not validate(data):
        raise StoreError(kind, "invalid_output")
    target = Path(path)
    previous = ctx.generated_candidates.pop(target.resolve(), None)
    old = read_store(target, {}, kind=kind, validate=validate)
    if previous is not None:
        ctx.generated_candidates[target.resolve()] = previous
    report_key = os.getenv("REPORT_KEY") or "cyberlurch"
    cadence = os.getenv("REPORT_MODE") or "daily"
    def rows(value):
        return value.get("reports", {}).get(report_key, []) if kind == "rollups" else value.get("digests", [])
    def identity(row):
        return str(row.get("digest_id") or row.get("video_id") or row.get("month") or "")
    def grouped(value):
        groups = {}
        for row in rows(value):
            groups.setdefault(identity(row), []).append(row)
        return groups
    before, after = grouped(old), grouped(data)
    edition = ctx.coordinator.edition_id if ctx.coordinator is not None else f"{report_key}:{cadence}:local"
    delta = {"kind": kind, "report_key": report_key, "cadence": cadence, "mode": ctx.mode,
             "requires_ready_checkpoint": ctx.defer_generation,
             "production_target": target.resolve() in ctx.production_paths,
             "edition_id": edition,
             "base_sha256": hashlib.sha256(target.read_bytes()).hexdigest() if target.exists() else None,
             "generation_validated": True, "filesystem_persisted": False,
             "records": [row for key, group in after.items() if before.get(key) != group for row in group],
             "removed_record_ids": sorted(set(before) - set(after))}
    prefix = hashlib.sha256(edition.encode()).hexdigest()[:16]
    artifact = Path(os.getenv("REPORT_DIR") or "reports") / "recovery" / f"{prefix}_{report_key}_{cadence}_{kind}_generated_delta.json"
    atomic_store(artifact, delta, kind="generated_delta")
    if ctx.defer_generation:
        # The existing store verification/quality paths read this validated
        # candidate. Production publication waits until _main fully approves.
        json.dumps(data, ensure_ascii=False, allow_nan=False)
        ctx.generated_candidates[target.resolve()] = {"data": copy.deepcopy(data), "kind": kind,
            "validate": validate, "artifact": artifact, "base_sha256": delta["base_sha256"]}
        return
    atomic_store(target, data, kind=kind, validate=validate)
    delta["filesystem_persisted"] = True
    atomic_store(artifact, delta, kind="generated_delta")


def store_exists(path: str | Path) -> bool:
    from .run_context import current
    ctx = current()
    return bool(ctx and Path(path).resolve() in ctx.generated_candidates) or Path(path).exists()


def commit_generated_candidates() -> None:
    from .run_context import current
    ctx = current()
    if ctx is None:
        return
    candidates = dict(ctx.generated_candidates)
    # Validate ALL preimages before any write, and stop using the staged view.
    ctx.generated_candidates.clear()
    for target, candidate in candidates.items():
        actual = hashlib.sha256(target.read_bytes()).hexdigest() if target.exists() else None
        if actual != candidate["base_sha256"]:
            raise StoreError(candidate["kind"], "generated_preimage_conflict")
        read_store(target, {}, kind=candidate["kind"], validate=candidate["validate"])
    for target, candidate in candidates.items():
        atomic_store(target, candidate["data"], kind=candidate["kind"], validate=candidate["validate"])
