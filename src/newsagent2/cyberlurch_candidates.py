"""Bounded Cyberlurch discovery memory; SMTP history remains in the journal.

No provider calls here. Unknown legacy processed IDs remain blocked. Candidate
checkpoints recover only this section, never the surrounding intermediate state.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import copy
import hashlib
import json
import math
import re
from pathlib import Path

from .cyberlurch_editorial import normalize_channel_name
from .state_manager import is_processed, save_state, load_state
from .store_io import StoreError, atomic_store, read_store
from .run_context import current, path_for

FIELD = "cyberlurch_candidates"
LIMIT = 500
MAX_ATTEMPTS = 3
CHECKPOINT_LIMIT = 4096
EMPTY = {"version": 1, "records": {}}


def instant(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        return instant(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def valid(value):
    if (not isinstance(value, dict) or set(value) - {"version", "records", "checkpoints"}
            or value.get("version") != 1 or "records" not in value):
        return False
    checkpoints = value.get("checkpoints", [])
    if (not isinstance(checkpoints, list) or len(checkpoints) > CHECKPOINT_LIMIT
            or any(not isinstance(i, str) or not re.fullmatch(r"[a-f0-9]{64}", i) for i in checkpoints)):
        return False
    records = value["records"]
    if not isinstance(records, dict) or len(records) > LIMIT:
        return False
    fields = {"id", "source", "title", "channel", "url", "description", "topics", "policy", "published_at",
              "screened_at", "expires_at", "attempts", "last_attempt_day", "stage"}
    for key, row in records.items():
        if not isinstance(row, dict) or set(row) - fields - {"channel_identity"} or not fields <= set(row) or row.get("id") != key or row.get("source") != "youtube":
            return False
        if "channel_identity" in row:
            from .youtube_source import identity_valid
            if not identity_valid(row["channel_identity"]):
                return False
        if any(not isinstance(row.get(f), str) or len(row[f]) > n for f, n in
               (("id", 128), ("title", 500), ("channel", 200), ("url", 1000), ("description", 1500), ("last_attempt_day", 10))):
            return False
        if (not key or not isinstance(row["policy"], str) or not isinstance(row["stage"], str)
                or row["policy"] not in {"news", "evergreen"} or row["stage"] not in {"screened", "selected", "content_processed"}):
            return False
        if type(row["attempts"]) is not int or not 0 <= row["attempts"] <= MAX_ATTEMPTS:
            return False
        if (not isinstance(row["topics"], list) or len(row["topics"]) > 8
                or any(not isinstance(t, str) or len(t) > 200 for t in row["topics"])):
            return False
        times = [instant(row[f]) for f in ("published_at", "screened_at", "expires_at")]
        if (any(t is None for t in times) or times[2] - times[0] != timedelta(days=14 if row["policy"] == "evergreen" else 3)
                or bool(row["attempts"]) != bool(row["last_attempt_day"])
                or (not row["attempts"] and row["stage"] != "screened")):
            return False
        if row["last_attempt_day"]:
            try:
                from datetime import date
                date.fromisoformat(row["last_attempt_day"])
            except ValueError:
                return False
    return True


def preselect(candidates, *, priority_channels, selected_max, deep_dive_max):
    """Cover configured topics first, then balance channels; relax only to fill.

    Topic tags describe channel configuration, not verified video subject matter.
    Within a balance tier: priority, newest publication, stable ID.
    """
    cap = min(14, max(1, min(12, selected_max)) + max(0, min(2, deep_dive_max)))
    priorities = {normalize_channel_name(c) for c in priority_channels}
    def channel(row):
        return normalize_channel_name(row.get("channel") or "unknown")
    def topics(row):
        return set(row.get("topics") or ["unknown"])
    def stable(row):
        dt = instant(row.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc)
        return (channel(row) not in priorities, -dt.timestamp() if dt.year > 1 else float("inf"), str(row["id"]))
    unique = {}
    for row in sorted(candidates, key=lambda r: (str(r.get("id", "")), json.dumps(r, default=str, sort_keys=True))):
        if row.get("id"):
            unique.setdefault(str(row["id"]), row)
    pool = list(unique.values())
    selected, counts, covered = [], Counter(), set()
    channel_cap = max(1, math.ceil(cap / 3))
    while pool and len(selected) < cap:
        bounded = [r for r in pool if counts[channel(r)] < channel_cap]
        relaxed = not bounded
        choices = bounded or pool
        row = min(choices, key=lambda r: (not bool(topics(r) - covered), counts[channel(r)], *stable(r)))
        reason = "channel_cap_relaxed" if relaxed else ("topic_coverage" if topics(row) - covered else "channel_balance")
        selected.append({**row, "selection_reason": reason})
        covered.update(topics(row)); counts[channel(row)] += 1
        pool.remove(row)
    return selected, cap


def held_ids(journal):
    """Derive holds from validated receipts/intents, across editions/cadences.

    Old broad intents are conservatively held too. No recipient data duplicated.
    Saved ready reports need explicit replay; a new edition is not that replay.
    """
    held = set()
    for row in journal.get("editions", {}).values():
        if row["report_key"] != "cyberlurch" or not row["edition_id"].endswith(":production"):
            continue
        if row["status"] not in {"accepted_by_smtp", "partially_accepted", "outcome_unknown"} and not row.get("recovery"):
            continue
        intents = row.get("finalization", {})
        for field in ("items_all_new", "overview_items", "detail_items"):
            held.update(str(i["id"]) for i in intents.get(field, []) if i.get("source") == "youtube" and i.get("id"))
    return held


class Candidates:
    def __init__(self, state, now, journal):
        self.state, self.now = state, instant(now)
        self.base = digest(state.get(FIELD))
        self.value = copy.deepcopy(state.get(FIELD, EMPTY))
        if not valid(self.value):
            raise StoreError(FIELD, "invalid_existing_store")
        self.held = held_ids(journal)
        self.counts = Counter()

    def collect(self, discovered):
        records = self.value["records"]
        for item in sorted(discovered, key=lambda r: str(r.get("id", ""))):
            iid = str(item.get("id") or "")
            if not iid or len(iid) > 128 or iid in records:
                continue  # Rediscovery never extends TTL or resets attempt history.
            pub = instant(item.get("published_at"))
            if pub is None or pub > self.now:
                self.counts["date_unknown_or_future"] += 1
                continue
            policy = "evergreen" if item.get("candidate_policy") == "evergreen" else "news"
            expires = pub + timedelta(days=14 if policy == "evergreen" else 3)
            if expires <= self.now:
                self.counts["expired"] += 1
                continue
            records[iid] = {
                **({"channel_identity": item["channel_identity"]} if "channel_identity" in item else {}),
                "id": iid, "source": "youtube", "title": str(item.get("title") or "")[:500],
                "channel": str(item.get("channel") or "")[:200], "url": str(item.get("url") or "")[:1000],
                "description": str(item.get("description") or "")[:1500],
                "topics": sorted(set(item.get("topics") or ["unknown"]))[:8], "policy": policy,
                "published_at": pub.isoformat(), "screened_at": self.now.isoformat(), "expires_at": expires.isoformat(),
                "attempts": 0, "last_attempt_day": "", "stage": "screened"}
        eligible = []
        for iid, row in list(records.items()):
            if is_processed(self.state, "cyberlurch", "youtube", iid) or iid in self.held:
                self.counts["legacy_or_receipt_hold"] += 1
                del records[iid]
            elif instant(row["expires_at"]) <= self.now:
                self.counts["expired"] += 1
                del records[iid]
            elif row["attempts"] >= MAX_ATTEMPTS or row["last_attempt_day"] == self.now.date().isoformat():
                self.counts["attempt_limit_or_same_day"] += 1
            else:
                eligible.append(row)
        # Cap total storage including exhausted candidates, newest first; TTL is
        # publication-bound even when an evicted candidate is later rediscovered.
        keep = sorted(records, key=lambda iid: (-instant(records[iid]["published_at"]).timestamp(), iid))[:LIMIT]
        self.counts["capacity_dropped"] += len(records) - len(keep)
        self.value["records"] = {iid: records[iid] for iid in keep}
        self.counts["eligible"] = sum(r["id"] in self.value["records"] for r in eligible)
        return [dict(r) for r in eligible if r["id"] in self.value["records"]]

    def attempted(self, iid):
        row = self.value["records"][iid]
        row["attempts"] += 1
        row["last_attempt_day"] = self.now.date().isoformat()
        row["stage"] = "selected"
        self.checkpoint()

    def content_processed(self, iid, available):
        if available:
            self.value["records"][iid]["stage"] = "content_processed"
        self.checkpoint()

    def checkpoint(self):
        # A published checkpoint is already known even if the workflow failed
        # after its push. Retain bounded IDs so older failure artifacts cannot
        # overwrite (or permanently block) later candidate progress.
        checkpoint_id = digest(self.value["records"])
        history = self.value.setdefault("checkpoints", [])
        if checkpoint_id not in history:
            history.append(checkpoint_id)
        self.value["checkpoints"] = history[-CHECKPOINT_LIMIT:]
        if not valid(self.value):
            raise StoreError(FIELD, "invalid_output")
        self.state[FIELD] = copy.deepcopy(self.value)
        ctx = current()
        if ctx and ctx.production:
            envelope = {"version": 1, "base_sha256": self.base, "data": self.value, "sha256": digest(self.value)}
            atomic_store(ctx.coordinator.recovery / "cyberlurch_candidates.json", envelope, kind=FIELD, validate=envelope_valid)
        save_state(str(path_for("STATE_PATH")), self.state)


def envelope_valid(value):
    return (isinstance(value, dict) and set(value) == {"version", "base_sha256", "data", "sha256"}
            and value["version"] == 1 and valid(value["data"])
            and isinstance(value["base_sha256"], str) and bool(re.fullmatch(r"[a-f0-9]{64}", value["base_sha256"]))
            and value["sha256"] == digest(value["data"]))


def restore(directory: Path):
    artifact = directory / "cyberlurch_candidates.json"
    if not artifact.exists():
        return
    value = read_store(artifact, {}, kind=FIELD, validate=envelope_valid)
    state = load_state(str(path_for("STATE_PATH")))
    existing = digest(state.get(FIELD))
    checkpoint_id = digest(value["data"]["records"])
    if checkpoint_id in state.get(FIELD, {}).get("checkpoints", []):
        return  # Already imported OR published, possibly followed by progress.
    if existing == value["sha256"]:
        return
    if existing != value["base_sha256"]:
        raise StoreError(FIELD, "recovery_preimage_conflict")
    state[FIELD] = value["data"]
    history = state[FIELD].setdefault("checkpoints", [])
    if checkpoint_id not in history:
        history.append(checkpoint_id)
    state[FIELD]["checkpoints"] = history[-CHECKPOINT_LIMIT:]
    save_state(str(path_for("STATE_PATH")), state)


class MetadataBudget:
    """A single run budget, fair remaining-channel reservations, no retry.

    IDs are charged before requesting, including failed/missing results. A used
    ID is never requested again. Unused reservations flow to later channels.
    """
    def __init__(self, limit, channels, diagnostics):
        self.remaining = max(0, limit)
        self.channels = max(1, channels)
        self.seen = set()
        self.diag = diagnostics

    def start_channel(self):
        quota = math.ceil(self.remaining / self.channels)
        self.channels = max(1, self.channels - 1)
        return quota

    def fetch(self, ids, quota, api_key, fetcher):
        unique = list(dict.fromkeys(str(i).strip() for i in ids if str(i).strip()))
        fresh = [i for i in unique if i not in self.seen]
        chosen = fresh[:min(quota, self.remaining)]
        self.seen.update(chosen)
        self.remaining -= len(chosen)
        self.diag["youtube_api_budget_skipped_total"] = self.diag.get("youtube_api_budget_skipped_total", 0) + len(fresh) - len(chosen)
        self.diag["youtube_api_budget_requested_ids_total"] = len(self.seen)
        result = {}
        for offset in range(0, len(chosen), 50):
            batch = chosen[offset:offset + 50]
            self.diag["youtube_api_budget_requests_total"] = self.diag.get("youtube_api_budget_requests_total", 0) + 1
            snippets = fetcher(batch, api_key, self.diag)
            result.update({i: s for i, s in snippets.items() if i in batch})
        self.diag["youtube_api_budget_enriched_ids_total"] = self.diag.get("youtube_api_budget_enriched_ids_total", 0) + len(result)
        return result
