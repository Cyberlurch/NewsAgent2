from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple


# Delimiter for compound keys (report_key || source || item_id)
_ITEM_KEY_DELIM = "||"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at_utc": _utc_now_iso(),
        "reports": {},
    }


def load_state(path: str) -> Dict[str, Any]:
    """Load JSON state from disk (defensive)."""
    if not path:
        print("[state] WARN: empty state path -> starting fresh")
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
            print(f"[state] WARN: invalid JSON root type in {path!r} -> starting fresh")
            return _new_state()

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
                f"[state] ERROR: failed to parse {path!r}: {e!r}. "
                f"Renamed to {corrupt!r} and starting fresh."
            )
        except Exception as e2:
            print(
                f"[state] ERROR: failed to parse {path!r}: {e!r}. "
                f"Also failed to rename corrupt file: {e2!r}. Starting fresh."
            )
        return _new_state()


def save_state(path: str, state: Dict[str, Any]) -> None:
    """Persist JSON state atomically (defensive, never silent)."""
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
        raise


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


def _sanitize_key_part(value: str) -> str:
    if _ITEM_KEY_DELIM in value:
        print(f"[state] WARN: delimiter {_ITEM_KEY_DELIM!r} found in key part; sanitizing.")
        return value.replace(_ITEM_KEY_DELIM, "_")
    return value


def make_item_key(
    report_key: str,
    source: str,
    item_id: str,
    url: Optional[str] = None,
    title: Optional[str] = None,
    channel: Optional[str] = None,
) -> str:
    """
    Build a stable, parseable key (report_key||source||item_id).

    url/title/channel are accepted for forward compatibility but not encoded in the key.
    Uniqueness is defined by (report_key, source, item_id).
    """
    rk = _sanitize_key_part(str(report_key or "").strip())
    src = _sanitize_key_part(str(source or "").strip())
    iid = _sanitize_key_part(str(item_id or "").strip())
    return f"{rk}{_ITEM_KEY_DELIM}{src}{_ITEM_KEY_DELIM}{iid}"


def _parse_item_key(item_key: str) -> Optional[Tuple[str, str, str]]:
    if not isinstance(item_key, str):
        return None
    parts = item_key.split(_ITEM_KEY_DELIM)
    if len(parts) != 3:
        return None
    rk, src, iid = (p.strip() for p in parts)
    if not rk or not src or not iid:
        return None
    return rk, src, iid


def is_processed(
    state: Dict[str, Any],
    report_key_or_item_key: str,
    source: Optional[str] = None,
    item_id: Optional[str] = None,
) -> bool:
    """
    Backwards-compatible lookup.

    Supports:
      - is_processed(state, report_key, source, item_id)  [legacy]
      - is_processed(state, item_key)                     [new]
    """
    if source is None and item_id is None:
        parsed = _parse_item_key(report_key_or_item_key)
        if not parsed:
            print(f"[state] WARN: is_processed called with unparseable item_key={report_key_or_item_key!r}")
            return False
        report_key, source, item_id = parsed
    else:
        report_key = report_key_or_item_key

    if not item_id:
        return False

    try:
        processed = (
            state.get("reports", {})
            .get(report_key, {})
            .get(source or "", {})
            .get("processed", {})
        )
        return isinstance(processed, dict) and str(item_id) in processed
    except Exception as e:
        print(f"[state] ERROR: is_processed failed: {e!r}")
        return False


def mark_processed(
    state: Dict[str, Any],
    report_key_or_item_key: str,
    source_or_processed_at: Optional[str] = None,
    item_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    processed_at_utc: Optional[str] = None,
) -> None:
    """
    Backwards-compatible writer.

    Supports:
      - mark_processed(state, report_key, source, item_id, meta=?, processed_at_utc=?)  [legacy]
      - mark_processed(state, item_key, processed_at_utc, meta=?)                       [new]
    """
    # New-style positional call: (state, item_key, processed_at_utc)
    if item_id is None and source_or_processed_at is not None and processed_at_utc is None:
        # Heuristic: timestamp contains 'T' and '+' (e.g. 2025-12-12T13:22:38+00:00)
        if "T" in source_or_processed_at and "+" in source_or_processed_at:
            processed_at_utc = source_or_processed_at
            source_or_processed_at = None

    if item_id is None:
        parsed = _parse_item_key(report_key_or_item_key)
        if not parsed:
            print(f"[state] WARN: mark_processed called with unparseable item_key={report_key_or_item_key!r} -> skipping")
            return
        report_key, source, item_id = parsed
    else:
        report_key = report_key_or_item_key
        source = source_or_processed_at or ""

    if not item_id:
        return

    processed = _ensure_bucket(state, str(report_key), str(source))
    processed[str(item_id)] = {
        "processed_at_utc": processed_at_utc or _utc_now_iso(),
        **(meta or {}),
    }


def prune_state(
    state: Dict[str, Any],
    retention_days: int = 120,
    max_entries_per_bucket: int = 0,
) -> Tuple[int, int]:
    """
    Prune old entries and optionally cap the size per bucket.

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

            # Age-based pruning
            if retention_days > 0:
                for iid, meta in list(processed.items()):
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
                        processed.pop(iid, None)
                        removed_age += 1

            # Hard cap
            if max_entries_per_bucket and max_entries_per_bucket > 0:
                if len(processed) > max_entries_per_bucket:
                    sortable = []
                    for iid, meta in processed.items():
                        ts = meta.get("processed_at_utc") if isinstance(meta, dict) else None
                        sortable.append((str(ts or ""), iid))
                    sortable.sort(key=lambda t: t[0])  # oldest first
                    to_remove = len(processed) - max_entries_per_bucket
                    for _, iid in sortable[:to_remove]:
                        processed.pop(iid, None)
                        removed_cap += 1

    return (removed_age, removed_cap)
