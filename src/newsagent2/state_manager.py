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

    if not isinstance(state, dict):
        raise TypeError(f"save_state expects dict state, got {type(state)!r}")

    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    except Exception as e:
        print(f"[state] ERROR: cannot create state directory for {path!r}: {e!r}")
        raise

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


def _parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


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
    if not isinstance(state, dict):
        print(f"[state] WARN: is_processed got non-dict state={type(state)!r}")
        return False

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
    if not isinstance(state, dict):
        raise TypeError(f"mark_processed expects dict state, got {type(state)!r}")

    # New-style positional call: (state, item_key, processed_at_utc)
    if item_id is None and source_or_processed_at is not None and processed_at_utc is None:
        if "T" in source_or_processed_at:
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
    existing = processed.get(str(item_id)) if isinstance(processed, dict) else {}
    base = existing if isinstance(existing, dict) else {}
    merged = {
        **base,
        "processed_at_utc": processed_at_utc or base.get("processed_at_utc") or _utc_now_iso(),
        **(meta or {}),
    }
    processed[str(item_id)] = merged


def get_processed_meta(state: Dict[str, Any], report_key: str, source: str, item_id: str) -> Dict[str, Any]:
    """
    Safe metadata accessor; returns an empty dict when missing.
    """
    try:
        processed = (
            state.get("reports", {})
            .get(report_key, {})
            .get(source or "", {})
            .get("processed", {})
        )
        if isinstance(processed, dict):
            meta = processed.get(str(item_id))
            return meta if isinstance(meta, dict) else {}
    except Exception as e:
        print(f"[state] WARN: get_processed_meta failed: {e!r}")
    return {}


def mark_screened(state: Dict[str, Any], report_key: str, source: str, item_id: str, meta: Optional[Dict[str, Any]] = None) -> None:
    """
    Record that an item was screened (but not necessarily sent).
    """
    meta = meta or {}
    meta.setdefault("screened_at_utc", _utc_now_iso())
    mark_processed(state, report_key, source, item_id, meta=meta)


def mark_sent(
    state: Dict[str, Any],
    report_key: str,
    source: str,
    item_id: str,
    *,
    sent_overview: bool = False,
    sent_deep_dive: bool = False,
    meta: Optional[Dict[str, Any]] = None,
    when_utc: Optional[str] = None,
) -> None:
    """
    Record that an item was included in the overview and/or deep dive output.
    """
    ts = when_utc or _utc_now_iso()
    meta = meta or {}
    meta.setdefault("screened_at_utc", meta.get("screened_at_utc") or ts)
    if sent_overview:
        meta["sent_overview_at_utc"] = ts
    if sent_deep_dive:
        meta["sent_deep_dive_at_utc"] = ts
    mark_processed(state, report_key, source, item_id, meta=meta)


def should_skip_pubmed_item(
    state: Dict[str, Any],
    report_key: str,
    item_id: str,
    *,
    overview_cooldown_hours: int = 48,
    reconsider_unsent_hours: int = 36,
) -> Tuple[bool, str]:
    """
    Cybermed-specific skip helper: only skip when an item was already sent recently.

    If an item was previously screened but never sent, it will be reconsidered (no skip).
    """
    meta = get_processed_meta(state, report_key, "pubmed", item_id)
    if not meta:
        return False, "new"

    now = datetime.now(timezone.utc)

    sent_overview = _parse_iso_utc(meta.get("sent_overview_at_utc"))
    if sent_overview:
        if overview_cooldown_hours > 0:
            delta = now - sent_overview
            if delta < timedelta(hours=overview_cooldown_hours):
                return True, "sent_overview_recent"
        return False, "sent_overview_stale"

    screened = _parse_iso_utc(meta.get("screened_at_utc") or meta.get("processed_at_utc"))
    if screened:
        age_hours = (now - screened).total_seconds() / 3600.0
        if reconsider_unsent_hours > 0 and age_hours < reconsider_unsent_hours:
            # Considered "freshly screened"; allow reconsideration instead of skipping.
            return False, "screened_only_recent"
        return False, "screened_only_stale"

    return False, "no_meta"


def prune_state(
    state: Dict[str, Any],
    retention_days: int = 120,
    max_entries_per_bucket: int = 0,
) -> Dict[str, Any]:
    """
    Prune old entries and optionally cap the size per bucket.

    IMPORTANT: Must return the (possibly modified) state dict for compatibility with main.py:
        state = prune_state(state, ...)
    """
    if not isinstance(state, dict):
        print(f"[state] WARN: prune_state got non-dict state={type(state)!r} -> resetting")
        return _new_state()

    removed_age = 0
    removed_cap = 0

    if retention_days <= 0 and (max_entries_per_bucket or 0) <= 0:
        return state

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(retention_days, 0))

    reports = state.get("reports")
    if not isinstance(reports, dict):
        return state

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

            # Hard cap per bucket
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

    if removed_age or removed_cap:
        print(f"[state] Pruned state: removed_by_age={removed_age}, removed_by_cap={removed_cap}")

    return state
