"""Bounded Cybermed ingestion; this store contains no delivery claims."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import collectors_pubmed as collector
from .run_context import current, path_for, email_mode
from .store_io import StoreError, atomic_store, read_store

KIND = "cybermed_pubmed_ingestion"
EMPTY = {"schema_version": 1, "queries": {}, "rotation": 0}
RECOVERY = "cybermed_pubmed_ingestion.json"


def fingerprint(term):
    return hashlib.sha256(("crdt-v1\n" + term.strip()).encode()).hexdigest()


def _day(value):
    return isinstance(value, str) and date.fromisoformat(value).isoformat() == value


def valid(value):
    try:
        if (not isinstance(value, dict) or value.get("schema_version") != 1
                or not isinstance(value.get("queries"), dict)
                or type(value.get("rotation")) is not int or value["rotation"] < 0
                or not isinstance(value.get("recovered", []), list)
                or any(not isinstance(i, str) or len(i) != 64 or any(c not in "0123456789abcdef" for c in i)
                       for i in value.get("recovered", []))):
            return False
        for key, row in value["queries"].items():
            if (not isinstance(row, dict) or key != fingerprint(row["term"])
                    or not isinstance(row["name"], str) or not row["term"].strip()
                    or not _day(row["initial_start"]) or not _day(row["next_date"])
                    or row["next_date"] < row["initial_start"]
                    or not isinstance(row["recent_done"], dict)):
                return False
            for day, ids in row["recent_done"].items():
                if not _day(day) or not _ids(ids):
                    return False
            active = row["active"]
            if active is not None:
                if (not isinstance(active, dict) or not _day(active["day"])
                        or not row["initial_start"] <= active["day"] <= row["next_date"]
                        or not _ids(active["ids"]) or not _ids(active["done"])
                        or not _ids(active.get("parked", []))
                        or not set(active.get("parked", [])) <= set(active["ids"])
                        or set(active.get("parked", [])) & set(active["done"])
                        or not set(active["done"]) <= set(active["ids"])
                        or type(active["search_count"]) is not int or not 0 <= active["search_count"] <= 10000
                        or type(active["closed_day_snapshot"]) is not bool
                        or not isinstance(active["snapshot_reference"], str)
                        or not isinstance(active["records"], dict)
                        or not set(active["records"]) <= set(active["ids"]) - set(active["done"]) - set(active.get("parked", []))):
                    return False
                observed = datetime.fromisoformat(active["snapshot_reference"])
                if observed.tzinfo is None or observed.astimezone(timezone.utc).date() < date.fromisoformat(active["day"]):
                    return False
                if active["closed_day_snapshot"] and observed.astimezone(timezone.utc).date() <= date.fromisoformat(active["day"]):
                    return False
                for pmid, record in active["records"].items():
                    if (not isinstance(record, dict) or record.get("id") != pmid
                            or not isinstance(record.get("text"), str)
                            or not isinstance(record.get("title"), str)):
                        return False
                    if record.get("published_at") is not None:
                        datetime.fromisoformat(record["published_at"])
            if not isinstance(row.get("review", {}), dict):
                return False
            for review_key, review in row.get("review", {}).items():
                if (not isinstance(review, dict) or not _day(review["day"])
                        or review.get("status") != "review_required" or not isinstance(review.get("record"), dict)
                        or not str(review["record"].get("id") or "").isdigit()
                        or not isinstance(review.get("category"), str) or not re.fullmatch(r"[a-z0-9_]+", review["category"])
                        or not isinstance(review.get("edition_ids"), list)
                        or any(not isinstance(i, str) or not re.fullmatch(r"(?:cybermed|cyberlurch):daily:\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2}:production", i) for i in review["edition_ids"])
                        or not isinstance(review["record"].get("text"), str) or not isinstance(review["record"].get("title"), str)
                        or review_key != str(review["record"]["id"]) + ":" + review["source_version"]):
                    return False
                from .paper_delivery import paper_metadata, metadata_valid
                metadata = paper_metadata(review["record"])
                if not metadata_valid(metadata) or metadata["source_version"] != review["source_version"]:
                    return False
                if review["record"].get("published_at") is not None:
                    datetime.fromisoformat(review["record"]["published_at"])
            for day, receipts in row.get("processing_receipts", {}).items():
                if not _day(day) or not isinstance(receipts, list):
                    return False
                handled = set(row["recent_done"].get(day, []))
                if active and active["day"] == day:
                    handled.update(active["done"])
                for receipt in receipts:
                    if (not isinstance(receipt, dict) or not _ids(receipt["generated"])
                            or not _ids(receipt["screened_out"])
                            or not _ids(receipt.get("deduplicated", []))
                            or not (set(receipt["generated"]) | set(receipt["screened_out"]) | set(receipt.get("deduplicated", []))) <= handled
                            or receipt.get("delivery_claim") is not False
                            or not isinstance(receipt.get("edition_id"), (str, type(None)))):
                        return False
                    version = receipt.get("content_version")
                    if version is not None and (not isinstance(version, str) or len(version) != 64
                            or any(c not in "0123456789abcdef" for c in version)):
                        return False
        json.dumps(value, allow_nan=False)
        return True
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _ids(ids):
    return (isinstance(ids, list) and len(ids) <= 10000 and len(ids) == len(set(ids))
            and all(isinstance(i, str) and i.isdigit() for i in ids))


def load():
    return read_store(path_for("CYBERMED_PUBMED_INGESTION_PATH"), EMPTY, kind=KIND, validate=valid)


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def _seal(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def restore(directory: Path):
    """Strict preimage restoration only, never an SMTP authorization."""
    artifact = directory / RECOVERY
    if not artifact.exists():
        return
    if os.getenv("REPORT_KEY") != "cybermed" or email_mode() != "real" or (current() and not current().production):
        raise StoreError(KIND, "recovery_mutation_policy")
    envelope = read_store(artifact, {}, kind=KIND)
    data = envelope.get("data")
    if (envelope.get("kind") != KIND or envelope.get("mode") != "real"
            or not valid(data) or envelope.get("sha256") != _seal(data)):
        raise StoreError(KIND, "invalid_recovery")
    target = path_for("CYBERMED_PUBMED_INGESTION_PATH")
    existing = load()
    recovery_id = hashlib.sha256((str(envelope.get("base_sha256")) + envelope["sha256"]).encode()).hexdigest()
    if recovery_id in existing.get("recovered", []):
        return
    if existing != data and _hash(target) != envelope.get("base_sha256"):
        raise StoreError(KIND, "recovery_preimage_conflict")
    data["recovered"] = list(dict.fromkeys(data.get("recovered", []) + [recovery_id]))
    atomic_store(target, data, kind=KIND, validate=valid)


def _limit(name, default, low, high):
    try:
        value = int(os.getenv(name) or default)
    except ValueError:
        raise collector.PubMedError("invalid_budget_configuration") from None
    if not low <= value <= high:
        raise collector.PubMedError("invalid_budget_configuration")
    return value


class IngestionSession:
    def __init__(self, reference, *, budget=None):
        if not isinstance(reference, datetime) or reference.tzinfo is None:
            raise collector.PubMedError("invalid_reference_time")
        self.reference = reference.astimezone(timezone.utc)
        self.today = self.reference.date()
        self.path = path_for("CYBERMED_PUBMED_INGESTION_PATH")
        self.data = load()
        self.base_hash = _hash(self.path)
        self.budget = budget or collector.RetrievalBudget(
            seconds=_limit("CYBERMED_PUBMED_RETRIEVAL_SECONDS", 120, 1, 120),
            max_requests=_limit("CYBERMED_PUBMED_REQUESTS_MAX", 60, 1, 60),
            max_records=_limit("CYBERMED_PUBMED_RECORDS_MAX", 650, 1, 650))
        self.page_size = _limit("CYBERMED_PUBMED_PAGE_SIZE", 500, 1, 10000)
        self.initial_days = _limit("CYBERMED_PUBMED_INITIAL_DAYS", 3, 1, 7)
        self.metadata = {}
        self.failed = False
        self.consumed = set()
        self.terminal = set()
        self.deduplicated = set()
        self.paper_decisions = {}
        self.paper_ledger = None
        self.offered = set()
        self.used_queries = set()

    def order(self, channels):
        """Rotate only PubMed slots so a busy first query cannot starve others."""
        slots = [i for i, ch in enumerate(channels) if ch.get("source") == "pubmed"]
        if not slots:
            return channels
        ordered = list(channels)
        queries = [channels[i] for i in slots]
        shift = self.data["rotation"] % len(queries)
        for i, ch in zip(slots, queries[shift:] + queries[:shift]):
            ordered[i] = ch
        self.data["rotation"] += 1
        self.save()
        return ordered

    def save(self):
        if not valid(self.data):
            raise StoreError(KIND, "invalid_output")
        ctx = current()
        if ctx is None:
            raise StoreError(KIND, "execution_context_required")
        production = bool(ctx and ctx.production and self.path.resolve() in ctx.production_paths)
        # Separate, validated ingestion store. No processed/health/delivery state.
        if production:
            envelope = {"kind": KIND, "mode": "real", "base_sha256": self.base_hash,
                        "data": self.data, "sha256": _seal(self.data)}
            artifact = Path(os.getenv("REPORT_DIR") or "reports") / "recovery" / RECOVERY
            atomic_store(artifact, envelope, kind=KIND)
        atomic_store(self.path, self.data, kind=KIND, validate=valid)

    def collect(self, name, term, limit):
        key = fingerprint(term)
        self.used_queries.add(key)
        start = self.today - timedelta(days=self.initial_days)
        row = self.data["queries"].setdefault(key, {
            "name": name, "term": term.strip(), "initial_start": start.isoformat(),
            "next_date": start.isoformat(), "recent_done": {}, "active": None})
        meta = {"query_version": key, "date_basis": "crdt", "status": "incomplete",
                "category": "open_window", "retmax": limit, "esearch_count_total": 0,
                "idlist_count": 0, "fetched_xml_count": 0, "parsed_article_count": 0,
                "pages": 0, "possibly_truncated": True, "requests": 0, "fetched_this_run": 0}
        requests_before = self.budget.requests
        # Oldest unresolved day first. At most three empty/previously processed
        # days per run. One-day overlap does not move the frontier backwards.
        day = max(date.fromisoformat(row["initial_start"]), date.fromisoformat(row["next_date"]) - timedelta(days=1))
        records = []
        try:
            with collector.retrieval_budget(self.budget):
                for _ in range(3):
                    active = row["active"]
                    if active:
                        day = date.fromisoformat(active["day"])
                    if day > self.today:
                        break
                    if active is None or (not active["closed_day_snapshot"] and
                            (day < self.today or active["snapshot_reference"] != self.reference.isoformat())):
                        ids, search_meta = collector.discover_pubmed_ids(term, day, day, page_size=self.page_size)
                        meta.update(search_meta)
                        done = set(row["recent_done"].get(day.isoformat(), []))
                        if active:
                            done.update(active["done"])
                        saved = active["records"] if active else {}
                        parked = set(active.get("parked", [])) if active else set()
                        parked.update(review["record"]["id"] for review in row.get("review", {}).values() if review["day"] == day.isoformat())
                        # Result disappearance is not safe evidence to discard an
                        # already fetched candidate; retain its identity too.
                        ids = list(dict.fromkeys(ids + list(saved) + list(done) + sorted(parked)))
                        active = row["active"] = {"day": day.isoformat(), "ids": ids,
                            "done": sorted(done), "parked": sorted(parked), "records": saved,
                            "closed_day_snapshot": day < self.today,
                            "snapshot_reference": self.reference.isoformat(), "search_count": search_meta["esearch_count_total"]}
                        self.save()
                    records = [copy.deepcopy(active["records"][i]) for i in active["ids"]
                               if i in active["records"]][:limit]
                    missing = [i for i in active["ids"] if i not in active["done"] and i not in active.get("parked", []) and i not in active["records"]]
                    capacity = min(max(0, limit - len(active["records"])), self.budget.max_records - self.budget.records)
                    if missing and capacity > 0:
                        batch = missing[:capacity]
                        fetched = collector.fetch_pubmed_records(batch)
                        self.budget.records += len(fetched)
                        meta["fetched_this_run"] += len(fetched)
                        for record in fetched:
                            encoded = copy.deepcopy(record)
                            if isinstance(encoded.get("published_at"), datetime):
                                encoded["published_at"] = encoded["published_at"].isoformat()
                            encoded["query_provenance"] = [key]
                            encoded["ingestion_window"] = day.isoformat()
                            encoded["newly_ingested"] = True
                            encoded["older_publication"] = bool(encoded.get("published_at") and
                                datetime.fromisoformat(encoded["published_at"]).date() < start)
                            active["records"][record["id"]] = encoded
                        self.save()
                    records = [copy.deepcopy(active["records"][i]) for i in active["ids"]
                               if i in active["records"]][:limit]
                    meta.update(window_start=day.isoformat(), window_end=day.isoformat(),
                        esearch_count_total=active["search_count"], idlist_count=len(active["ids"]),
                        fetched_xml_count=len(active["records"]) + len(active["done"]),
                        parsed_article_count=len(active["records"]) + len(active["done"]))
                    if records or set(active["ids"]) != set(active["done"]) | set(active.get("parked", [])):
                        meta["category"] = "pending_processing" if records else "record_budget"
                        break
                    if day == self.today:
                        # Today's crdt range is provisional. It must be searched
                        # again after the day ends before certifying its coverage.
                        meta["category"] = "provisional_current_day"
                        break
                    self._close(row)
                    day += timedelta(days=1)
                else:
                    meta["category"] = "day_budget"
        except collector.PubMedError as exc:
            meta["category"] = exc.category
            if exc.category not in {"time_budget", "request_budget"}:
                meta["status"] = "failed"
                self.failed = True
        self.save()
        meta["requests"] = self.budget.requests - requests_before
        meta["open_candidates"] = len(row["active"]["ids"]) - len(row["active"]["done"]) if row["active"] else 0
        meta["retrieved_through"] = ((date.fromisoformat(row["next_date"]) - timedelta(days=1)).isoformat()
                                     if row["next_date"] > row["initial_start"] else None)
        meta["processed_through"] = self.processed_through(row)
        meta["review_required"] = len(row.get("review", {}))
        meta["reference_utc"] = self.reference.isoformat()
        meta["date_granularity"] = "day"
        meta["initial_start"] = row["initial_start"]
        if (meta["status"] != "failed" and date.fromisoformat(row["next_date"]) >= self.today
                and row["active"] and row["active"]["day"] == self.today.isoformat()
                and not records and not meta["open_candidates"] and not row.get("review")):
            meta.update(status="complete", category="closed_days_complete", possibly_truncated=False)
        self.metadata[name] = meta
        for record in records:
            if record.get("published_at"):
                record["published_at"] = datetime.fromisoformat(record["published_at"])
        self.offered.update(record["id"] for record in records)
        return records, meta

    def processed_through(self, row):
        boundary = date.fromisoformat(row["next_date"])
        if row.get("review"):
            boundary = min(boundary, min(date.fromisoformat(r["day"]) for r in row["review"].values()))
        return (boundary - timedelta(days=1)).isoformat() if boundary > date.fromisoformat(row["initial_start"]) else None

    def quarantine(self, pmid, record, decision):
        from .paper_delivery import paper_metadata
        metadata = paper_metadata(record)
        if metadata is None:
            raise StoreError(KIND, "paper_identity_invalid")
        for key in self.used_queries:
            row = self.data["queries"][key]
            active = row["active"]
            if not active or pmid not in active["records"]:
                continue
            review_key = pmid + ":" + metadata["source_version"]
            row.setdefault("review", {}).setdefault(review_key, {
                "status": "review_required", "day": active["day"], "source_version": metadata["source_version"],
                "record": copy.deepcopy(active["records"][pmid]), "category": decision.category,
                "edition_ids": list(decision.edition_ids)})
            del active["records"][pmid]
            active["parked"] = sorted(set(active.get("parked", [])) | {pmid})
        self.save()

    def refresh_reviews(self, ledger):
        """Only an authoritative duplicate resolution consumes held work.

        Failed/unknown/source-change work has no automatic send release. Explicit
        delivery recovery must first establish full SMTP acceptance.
        """
        for row in self.data["queries"].values():
            for key, review in list(row.get("review", {}).items()):
                from .paper_delivery import paper_metadata
                decision = ledger.decide(paper_metadata(review["record"]))
                if decision.action != "duplicate":
                    review["category"] = decision.category
                    review["edition_ids"] = list(decision.edition_ids)
                    continue
                pmid, day = review["record"]["id"], review["day"]
                active = row["active"]
                if active and active["day"] == day:
                    active["parked"] = [i for i in active.get("parked", []) if i != pmid]
                    active["done"] = sorted(set(active["done"]) | {pmid})
                else:
                    row["recent_done"][day] = sorted(set(row["recent_done"].get(day, [])) | {pmid})
                del row["review"][key]
        self.save()

    def _close(self, row):
        active = row["active"]
        day = date.fromisoformat(active["day"])
        row["recent_done"][active["day"]] = active["done"]
        frontier = max(date.fromisoformat(row["next_date"]), day + timedelta(days=1))
        row["next_date"] = frontier.isoformat()
        row["recent_done"] = {k: v for k, v in row["recent_done"].items()
                              if date.fromisoformat(k) >= frontier - timedelta(days=2)}
        row["processing_receipts"] = {k: v for k, v in row.get("processing_receipts", {}).items()
                                      if k in row["recent_done"]}
        row["active"] = None
        self.save()

    def acknowledge_generation(self):
        """Called only after approved generation and generated-store persistence.

        This consumes generated or explicitly rejected candidates, not sent
        candidates. Editorial capacity omissions remain open across restarts.
        """
        ctx = current()
        if (ctx is None or not getattr(ctx, "delivery_approved", False) or ctx.coordinator is None
                or ctx.coordinator.result is None or not ctx.coordinator.result.generation_validated):
            raise StoreError(KIND, "generation_approval_required")
        handled = (self.consumed | self.terminal | self.deduplicated) & self.offered
        for key in self.used_queries:
            row = self.data["queries"][key]
            active = row["active"]
            if not active:
                continue
            selected = set(active["records"]) & handled
            active["done"] = sorted(set(active["done"]) | selected)
            if selected:
                coordinator = current().coordinator
                result = coordinator.result if coordinator else None
                receipt = {"generated": sorted(selected & self.consumed),
                           "screened_out": sorted(selected - self.consumed - self.deduplicated),
                           "deduplicated": sorted(selected & self.deduplicated), "delivery_claim": False,
                           "edition_id": coordinator.edition_id if coordinator else None,
                           "content_version": result.content_version if result else None}
                row.setdefault("processing_receipts", {}).setdefault(active["day"], []).append(receipt)
            for pmid in selected:
                del active["records"][pmid]
            if set(active["ids"]) == set(active["done"]) | set(active.get("parked", [])) and active["closed_day_snapshot"]:
                self._close(row)
        self.save()
        diagnostics = {}
        for name, metadata in self.metadata.items():
            row = self.data["queries"][metadata["query_version"]]
            active = row["active"]
            diagnostics[name] = dict(metadata,
                open_candidates=len(active["ids"]) - len(active["done"]) if active else 0,
                processed_through=self.processed_through(row), review_required=len(row.get("review", {})))
        atomic_store(Path(os.getenv("REPORT_DIR") or "reports") / "cybermed_daily_ingestion_diagnostics.json",
                     {"queries": diagnostics, "requests": self.budget.requests, "records_fetched": self.budget.records,
                      "progress_is_delivery": False, "paper_decisions": self.paper_decisions}, kind=KIND)

    def reader_note(self):
        # Operational capture/continuation status stays in the saved ledger
        # and diagnostics. No generic capture disclaimer in any reader edition.
        return ""
