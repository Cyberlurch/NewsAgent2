"""Paper delivery is a projection of production Daily SMTP receipts, not a TTL."""
from __future__ import annotations

import hashlib
import html
import json
import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import unquote

from .pubmed_facts import sections, result_direction, build_pubmed_facts


def normalized(value):
    value = html.unescape(re.sub(r"</?[A-Za-z][^>]*>", "", str(value or "")))
    value = " ".join(unicodedata.normalize("NFC", value).split())
    def decimal_spelling(match):
        whole, fraction = match[0].split(".")
        whole, fraction = whole.lstrip("0") or "0", fraction.rstrip("0")
        return whole + ("." + fraction if fraction else "")
    return re.sub(r"(?<![\w.])\d*\.\d+(?!\w|\.\d)", decimal_spelling, value)


def normalize_doi(value):
    value = unquote(str(value or "").strip()).casefold()
    value = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", value)
    return value if re.fullmatch(r"10\.\d{4,9}/\S+", value) else ""


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                      separators=(",", ":"), allow_nan=False).encode()).hexdigest()


NOTICE_TYPES = {"retraction of publication": "retraction_notice", "published erratum": "correction_notice",
                "expression of concern": "expression_of_concern_notice",
                "corrected and republished article": "corrected_republication"}
NOTICE_REFS = {"RetractionOf": "retraction_notice", "ErratumFor": "correction_notice",
               "ExpressionOfConcernFor": "expression_of_concern_notice"}
STATUS_REFS = {"RetractionIn", "ErratumIn", "ExpressionOfConcernIn", "CorrectedandRepublishedIn"}


def paper_metadata(item):
    """Metadata v1 is captured from collector source, before model enrichment."""
    pmid = str(item.get("pmid") or item.get("id") or "").strip()
    if not pmid.isdigit():
        return None  # Non-PubMed fixtures/legacy IDs cannot establish identity.
    types = sorted(set(normalized(t).casefold() for t in item.get("publication_types", [])))
    relations = sorted({(str(r.get("ref_type") or ""), str(r.get("pmid") or ""))
                        for r in item.get("comments_corrections", []) if isinstance(r, dict)})
    notice_kinds = {NOTICE_REFS[t] for t, _ in relations if t in NOTICE_REFS} | {NOTICE_TYPES[t] for t in types if t in NOTICE_TYPES}
    kind = ("ambiguous_notice" if len(notice_kinds) > 1 else next(iter(notice_kinds), "original"))
    if kind == "original" and "retracted publication" in types:
        kind = "retracted_original"
    abstract = normalized(item.get("abstract"))
    parsed = sections(str(item.get("abstract") or ""))
    results = normalized(parsed.get("results"))
    # Structured results, not goals or a model Bottom Line, support comparison.
    facts = build_pubmed_facts(item)
    primary = normalized(facts.primary_results)
    numbers = re.findall(r"(?<!\w)\d*\.\d+|(?<!\w)\d+(?:/\d+)?%?", primary)
    fields = {"title": digest(normalized(item.get("title"))), "abstract": digest(abstract),
              "types": digest(types), "relations": digest(relations)}
    evidence = {"results": digest(results), "results_known": bool(facts.primary_endpoint and parsed.get("methods") and numbers and not facts.input_omitted),
                "numbers": digest(numbers), "endpoint": digest(normalized(facts.primary_endpoint)),
                "methods": digest(normalized(parsed.get("methods"))),
                "direction": result_direction(primary)}
    return {"version": 1, "pmid": pmid, "doi": normalize_doi(item.get("doi")), "kind": kind,
            "source_version": digest(fields), "fields": fields, "evidence": evidence,
            "relations": [{"ref_type": t, "pmid": p} for t, p in relations]}


def _hex(value):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def metadata_valid(value):
    try:
        if not isinstance(value, dict) or set(value) != {"version", "pmid", "doi", "kind", "source_version", "fields", "evidence", "relations"}:
            return False
        if (type(value["version"]) is not int or value["version"] != 1 or not isinstance(value["pmid"], str) or not value["pmid"].isdigit()
                or not isinstance(value["doi"], str) or normalize_doi(value["doi"]) != value["doi"]
                or value["kind"] not in {"original", "ambiguous_notice", "retracted_original", *NOTICE_TYPES.values(), *NOTICE_REFS.values()}):
            return False
        fields, evidence = value["fields"], value["evidence"]
        if set(fields) != {"title", "abstract", "types", "relations"} or not all(_hex(v) for v in fields.values()):
            return False
        if (value["source_version"] != digest(fields) or set(evidence) != {"results", "results_known", "numbers", "endpoint", "methods", "direction"}
                or not _hex(evidence["results"]) or not _hex(evidence["numbers"]) or not _hex(evidence["endpoint"]) or not _hex(evidence["methods"])
                or type(evidence["results_known"]) is not bool
                or evidence["direction"] not in {"positive", "negative_or_null", "noninferiority", "unclear", "unknown", "mixed_or_unclear", "harm"}):
            return False
        return (isinstance(value["relations"], list) and all(isinstance(r, dict)
                and set(r) == {"ref_type", "pmid"} and isinstance(r["ref_type"], str)
                and isinstance(r["pmid"], str) and (not r["pmid"] or r["pmid"].isdigit())
                for r in value["relations"]) and fields["relations"] == digest(sorted(
                    {(r["ref_type"], r["pmid"]) for r in value["relations"]})))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


@dataclass(frozen=True)
class Decision:
    action: str  # include, duplicate, review
    category: str
    edition_ids: tuple[str, ...] = ()


def report_papers(row):
    """Only shown overview/deep-dive items; never every screened candidate."""
    intents = row.get("finalization", {})
    return [{**item, "id": str(item["id"])} for field in ("overview_items", "detail_items") for item in intents.get(field, [])
            if str(item.get("source") or "").strip().lower() == "pubmed" and str(item.get("id") or "").isdigit()]


class PaperLedger:
    def __init__(self, journal, *, report_key="cybermed", legacy_digests=None, state=None,
                 audience=None, key_id=None):
        self.report_key = report_key
        self.audience, self.key_id = audience, key_id
        self.rows = [row for row in journal["editions"].values()
                     if row["report_key"] == report_key and row["cadence"] == "daily"
                     and row["edition_id"].endswith(":production")]
        self.rows.sort(key=lambda r: (r["period_end"], r["period_start"], r["edition_id"]))
        self.legacy = set(journal.get("paper_legacy_unknown", {}).get("reports", {}).get(report_key, []))
        for row in (legacy_digests or {}).get("digests", []):
            if row.get("report_key", report_key) != report_key or row.get("cadence", "daily") != "daily" or row.get("email_mode") in {"none", "test"} or row.get("qa_replay"):
                continue
            self.legacy.update(str(item.get("pmid") or item.get("item_id") or "") for item in row.get("items", {}).get("pubmed", []))
        bucket = (state or {}).get("reports", {}).get(report_key, {}).get("pubmed", {}).get("processed", {})
        self.legacy.update(pmid for pmid, meta in bucket.items()
                           if isinstance(meta, dict) and (meta.get("sent_overview_at_utc") or meta.get("sent_deep_dive_at_utc")))

    def decide(self, paper):
        if not metadata_valid(paper):
            return Decision("review", "paper_metadata_unknown")
        if paper["kind"] == "ambiguous_notice":
            return Decision("review", "source_notice_ambiguous")
        related = []
        alias_conflict = False
        for row in self.rows:
            shown = report_papers(row)
            if row["status"] in {"partially_accepted", "outcome_unknown"} and not shown:
                return Decision("review", "unmapped_delivery_uncertain", (row["edition_id"],))
            for item in shown:
                old = item.get("paper_delivery")
                same_id = item["id"] == paper["pmid"]
                same_doi = bool(old and paper["doi"] and old["doi"] == paper["doi"])
                # Notices remain independent documents even with a shared DOI.
                doi_alias = same_doi and old["kind"] in {"original", "retracted_original"} and paper["kind"] in {"original", "retracted_original"}
                if not same_id and not doi_alias:
                    continue
                if same_id and old and old["doi"] and paper["doi"] and old["doi"] != paper["doi"]:
                    alias_conflict = True
                if doi_alias and not same_id and old["source_version"] != paper["source_version"]:
                    alias_conflict = True
                related.append((row, old))
        uncertain = [r for r, _ in related if r["status"] in {"partially_accepted", "outcome_unknown"}]
        if uncertain:
            return Decision("review", "paper_delivery_uncertain", tuple(sorted({r["edition_id"] for r in uncertain})))
        saved = [r for r, _ in related if r["status"] in {"generated", "failed"} and r["generation_validated"]]
        if saved:
            return Decision("review", "saved_generation_requires_recovery", tuple(sorted({r["edition_id"] for r in saved})))
        if alias_conflict:
            return Decision("review", "paper_alias_conflict")
        accepted = [(r, p) for r, p in related if r["status"] == "accepted_by_smtp"]
        if accepted:
            references = tuple(sorted({r["edition_id"] for r, _ in accepted}))
            if callable(self.audience):
                self.audience, self.key_id = self.audience()
            if self.audience is not None and any(set(r["accepted"]) != self.audience or r["recipient_key_id"] != self.key_id for r, _ in accepted):
                return Decision("review", "paper_audience_changed", references)
            if any(p and p["source_version"] == paper["source_version"] for _, p in accepted):
                return Decision("duplicate", "paper_already_accepted", references)
            previous = accepted[-1][1]
            if previous is None:
                return Decision("review", "accepted_paper_version_unknown", references)
            old_rel = {(r["ref_type"], r["pmid"]) for r in previous["relations"]}
            new_rel = {(r["ref_type"], r["pmid"]) for r in paper["relations"]}
            if (any(t in STATUS_REFS for t, _ in new_rel - old_rel)
                    or paper["kind"] == "retracted_original" and previous["kind"] != "retracted_original"):
                return Decision("include", "source_status_update", references)
            old, new = previous["evidence"], paper["evidence"]
            if (old["results_known"] and new["results_known"] and old["numbers"] != new["numbers"]
                    and old["endpoint"] == new["endpoint"] and old["methods"] == new["methods"]
                    and previous["fields"]["title"] == paper["fields"]["title"]
                    and previous["fields"]["types"] == paper["fields"]["types"]):
                return Decision("include", "reported_results_update", references)
            return Decision("review", "source_change_requires_review", references)
        if paper["pmid"] in self.legacy:
            return Decision("review", "legacy_delivery_unknown")
        if any(r["ref_type"] in STATUS_REFS for r in paper["relations"]):
            return Decision("include", "source_status_update")
        return Decision("include", paper["kind"] if paper["kind"] != "original" else "new_paper")


def describe_update(item, category):
    paper = item["paper_delivery"]
    labels = {"retraction_notice": "Retraction notice", "correction_notice": "Correction notice",
              "expression_of_concern_notice": "Expression of Concern", "corrected_republication": "Corrected republication",
              "source_status_update": "PubMed source-status update", "reported_results_update": "Reported results changed"}
    label = ("Original record labelled Retracted Publication by PubMed" if paper["kind"] == "retracted_original"
             else labels.get(category, "Source update"))
    refs = ", ".join(f"{r['ref_type']}: PMID {r['pmid']}" for r in paper["relations"] if r["pmid"])
    return f"{label}. Primary record: PMID {paper['pmid']}. {refs}".strip()


def notice_markdown(items):
    if not items:
        return ""
    lines = ["", "## PubMed source updates", "",
             "Source notices are reported as recorded by PubMed; no independent reassessment of the original findings is claimed.", ""]
    for item in items:
        paper = item["paper_delivery"]
        lines.append(f"- [PMID {paper['pmid']}](https://pubmed.ncbi.nlm.nih.gov/{paper['pmid']}/): {describe_update(item, item['paper_update'])}")
    return "\n".join(lines) + "\n"


def audit_review():
    """Read-only inspection. Does not clear holds or authorize SMTP."""
    from .delivery import load_journal
    from .pubmed_ingestion import load
    from .run_context import path_for
    journal = load_journal(path_for("DELIVERY_JOURNAL_PATH"))
    state = load()
    ledger = PaperLedger(journal)
    return {"report_key": "cybermed", "read_only": True, "items": [
        {"pmid": record["record"]["id"], "source_version": record["source_version"],
         "captured_day": record["day"], "category": record["category"],
         "receipt_projection": ledger.decide(paper_metadata(record["record"])).category,
         "audience_verified": False, "edition_ids": record["edition_ids"]}
        for query in state["queries"].values() for record in query.get("review", {}).values()]}


if __name__ == "__main__":
    print(json.dumps(audit_review(), sort_keys=True, indent=2))
