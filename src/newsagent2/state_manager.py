from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at_utc": _utc_now_iso(),
        "reports": {},
    }


def load_state(path: str) -> Dict[str, Any]:
    """Load JSON state from disk.

    Defensive behaviour:
    - If the file does not exist: start fresh.
    - If the file is corrupt: rename it aside and start fresh.
    """
    if not path:
        print("[state] WARN: empty state path -> starting with fresh state")
        return _new_state()

    if not os.path.exists(path):
        print(f"[state] No state file found at {path!r} -> starting fresh")
        return _new_state()

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            print(f"[state] WARN: state file {path!r} is empty -> starting fresh")
            return _new_state()
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("state root is not an object")
        data.setdefault("version", 1)
        data.setdefault("updated_at_utc", _utc_now_iso())
        data.setdefault("reports", {})
        if not isinstance(data.get("reports"), dict):
            data["reports"] = {}
        return data
    except Exception as e:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        corrupt = f"{path}.corrupt.{ts}"
        try:
            os.replace(path, corrupt)
            print(
                f"[state] ERROR: failed to parse state file {path!r}: {e!r}. "
                f"Renamed to {corrupt!r} and starting fresh."
            )
        except Exception as e2:
            print(
                f"[state] ERROR: failed to parse state file {path!r}: {e!r}. "
                f"Also failed to rename corrupt file: {e2!r}. Starting fresh."
            )
        return _new_state()


def save_state(path: str, state: Dict[str, Any]) -> None:
    """Persist JSON state atomically."""
    if not path:
        print("[state] WARN: empty state path -> not saving")
        return

    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    except Exception as e:
        print(f"[state] ERROR: cannot create state directory for {path!r}: {e!r}")
        return

    try:
        state["updated_at_utc"] = _utc_now_iso()
        payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)
            f.write("\n")
        os.replace(tmp_path, path)
        print(f"[state] Saved state to {path!r}")
    except Exception as e:
        print(f"[state] ERROR: failed to save state to {path!r}: {e!r}")
        try:
            if os.path.exists(f"{path}.tmp"):
                os.remove(f"{path}.tmp")
        except Exception:
            pass


def _ensure_bucket(state: Dict[str, Any], report_key: str, source: str) -> Dict[str, Any]:
    reports = state.setdefault("reports", {})
    if not isinstance(reports, dict):
        state["reports"] = {}
        reports = state["reports"]

    rep = reports.setdefault(report_key, {})
    if not isinstance(rep, dict):
        reports[report_key] = {}
        rep = reports[report_key]

    src = rep.setdefault(source, {})
    if not isinstance(src, dict):
        rep[source] = {}
        src = rep[source]

    processed = src.setdefault("processed", {})
    if not isinstance(processed, dict):
        src["processed"] = {}
        processed = src["processed"]

    return processed


def is_processed(state: Dict[str, Any], report_key: str, source: str, item_id: str) -> bool:
    if not item_id:
        return False
    try:
        processed = (
            state.get("reports", {})
            .get(report_key, {})
            .get(source, {})
            .get("processed", {})
        )
        return isinstance(processed, dict) and item_id in processed
    except Exception:
        return False


def mark_processed(
    state: Dict[str, Any],
    report_key: str,
    source: str,
    item_id: str,
    meta: Dict[str, Any] | None = None,
) -> None:
    if not item_id:
        return
    processed = _ensure_bucket(state, report_key, source)
    processed[item_id] = {
        "processed_at_utc": _utc_now_iso(),
        **(meta or {}),
    }


def prune_state(
    state: Dict[str, Any],
    retention_days: int,
    max_entries_per_bucket: int = 0,
) -> Tuple[int, int]:
    """Prune old entries.

    Returns:
      (removed_by_age, removed_by_cap)
    """
    removed_age = 0
    removed_cap = 0

    if retention_days <= 0 and (max_entries_per_bucket or 0) <= 0:
        return (0, 0)

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(retention_days, 0))

    reports = state.get("reports")
    if not isinstance(reports, dict):
        return (0, 0)

    for rep_key, rep_val in list(reports.items()):
        if not isinstance(rep_val, dict):
            continue
        for src_key, src_val in list(rep_val.items()):
            if not isinstance(src_val, dict):
                continue
            processed = src_val.get("processed")
            if not isinstance(processed, dict):
                continue

            # 1) Age-based pruning
            if retention_days > 0:
                for item_id, meta in list(processed.items()):
                    if not isinstance(meta, dict):
                        continue
                    ts = meta.get("processed_at_utc")
                    if not isinstance(ts, str) or not ts:
                        continue
                    try:
                        dt_obj = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if dt_obj.tzinfo is None:
                            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                    except Exception:
                        continue

                    if dt_obj < cutoff:
                        processed.pop(item_id, None)
                        removed_age += 1

            # 2) Hard cap pruning (keep newest by processed_at_utc)
            if max_entries_per_bucket and max_entries_per_bucket > 0:
                if len(processed) > max_entries_per_bucket:
                    sortable = []
                    for item_id, meta in processed.items():
                        if isinstance(meta, dict):
                            ts = meta.get("processed_at_utc")
                        else:
                            ts = None
                        sortable.append((str(ts or ""), item_id))

                    # Oldest first (empty timestamps first)
                    sortable.sort(key=lambda t: t[0])
                    to_remove = len(processed) - max_entries_per_bucket
                    for _, item_id in sortable[:to_remove]:
                        processed.pop(item_id, None)
                        removed_cap += 1

    return (removed_age, removed_cap)
