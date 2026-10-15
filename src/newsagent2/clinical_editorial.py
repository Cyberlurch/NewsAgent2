"""Bounded, deterministic Cybermed editorial policy; no I/O or model calls.

These are source/format hints, not GRADE, causal inference or medical validation.
"""
from __future__ import annotations

import math
import re

from .pubmed_facts import build_pubmed_facts, evidence_input

CONTEXT_FIELDS = ("clinical_context", "top_pick_decision", "stored_top_pick")


def clinical_text(item: dict) -> str:
    """Use the existing results-first budget, never journal/channel keywords."""
    text, _, _ = evidence_input(item)
    return "\n".join(str(x) for x in (item.get("title") or "", text)).lower()


def _hits(text: str, pattern: str) -> bool:
    return bool(re.search(pattern, text, re.I))


def context_for(item: dict) -> dict:
    stored = item.get("clinical_context")
    if isinstance(stored, dict) and stored.get("version") == 1:
        return dict(stored)  # Aggregate readers do not re-score historical facts.
    facts = build_pubmed_facts(item)
    title = str(item.get("title") or "")
    hay = clinical_text(item)
    # Explicit setting takes precedence over incidental intervention mentions.
    setting = " ".join((title, facts.population, str(item.get("population_setting") or ""))).lower()
    cc = r"\bicu\b|intensive care|critical care|sepsis|septic|\bards\b|\becmo\b|resuscitat|vasopressor|cardiac arrest|\bcrrt\b|\bemergency\b|\btrauma\b|mechanical ventilat|respiratory failure"
    ana = r"anaesth|anesth|perioper|postoper|neuraxial|epidural|nerve block|\bpain\b|analgesi"
    if _hits(setting, cc):
        track = "Critical Care"
    elif _hits(setting, ana):
        track = "Anaesthesiology"
    elif _hits(hay, cc):
        track = "Critical Care"
    elif _hits(hay, ana):
        track = "Anaesthesiology"
    else:
        track = "unknown"
    sub = "Other Critical Care" if track == "Critical Care" else "Other Anaesthesiology"
    groups = (
        [("Circulation", r"shock|vasopressor|norepinephrine|hemodynamic|haemodynamic|cardiac arrest"),
         ("Respiration", r"ventilat|ards|oxygen|respir|intubat|airway|ecmo"),
         ("Infection/Sepsis", r"sepsis|septic|infection|antibiotic"),
         ("Renal/CRRT", r"renal|kidney|crrt|dialysis"),
         ("Neuro", r"neuro|brain|delirium|seizure|cerebr")]
        if track == "Critical Care" else
        [("Pain/Regional Anesthesia", r"nerve block|regional anesth|regional anaesth|neuraxial|epidural|\bpain\b|analgesi"),
         ("Perioperative Medicine", r"perioper|postoper|surgery|surgical"),
         ("General Anesthesia", r"induction|maintenance|airway|propofol|volatile|anaesth|anesth")]
    )
    for label, pattern in groups:
        if _hits(hay, pattern):
            sub = label
            break
    # Prefer an explicit population/setting over abstract background mentions.
    pop_text = facts.population or str(item.get("population_setting") or "") or title
    pop_text = re.sub(r"\b(?:excluded|excluding|without|no)\s+(?:children|pediatric patients|neonates)\b", "", pop_text, flags=re.I)
    neo = _hits(pop_text, r"\bneonat|\bnewborn|\bpreterm|\bnicu\b")
    child = neo or _hits(pop_text, r"paediatr|pediatr|\bchildren\b|\binfants?\b|adolescen")
    adult = _hits(pop_text, r"\badults?\b|\belderly\b|older patients")
    population = "mixed" if adult and child else "neonatal" if neo else "pediatric" if child else "adult" if adult else "unknown"
    source_type = str(item.get("source_type") or item.get("source") or "").lower()
    foamed = source_type == "foamed"
    source = str(item.get("final_content_source") or item.get("content_source") or "")
    raw = str(item.get("abstract") or item.get("text") or "").strip()
    basis = (source or "excerpt") if foamed and raw else facts.source_basis if raw else "unknown"
    primary_known = bool(facts.primary_results and facts.primary_endpoint)
    guideline = any(str(x).lower() in {"practice guideline", "guideline"} for x in item.get("publication_types", []))
    recommendation_known = bool(guideline and facts.conclusions and _hits(facts.conclusions, r"recommend|suggest|conditional"))
    coverage = "unknown" if not raw else "bounded_excerpt" if facts.input_omitted or foamed else "results_reported" if primary_known else "results_unknown"
    return {"version": 1, "track": track, "subcategory": sub, "population": population,
            "domain_basis": "source_content" if track != "unknown" else "unknown",
            "text_basis": basis, "text_coverage": coverage,
            "result_basis": "primary_reported" if primary_known else "recommendation_reported" if recommendation_known else "unknown",
            "result_direction": facts.direction if primary_known and not foamed else "unknown",
            "uncertainty": ("Primary endpoint/results mapping unknown; do not infer a primary benefit"
                            if raw and not foamed and facts.primary_results and not facts.primary_endpoint else
                            facts.uncertainty if raw and not foamed else "Source basis limited; no independent validation")}


def attach_context(item: dict, *, refresh: bool = False) -> None:
    source = {k: v for k, v in item.items() if k != 'clinical_context'} if refresh else item
    item["clinical_context"] = context_for(source)


def _score(item: dict, field: str) -> float:
    value = item.get(field)
    if isinstance(value, bool):
        return 0
    try:
        number = float(value)
        return number if math.isfinite(number) and 1 <= number <= 5 else 0
    except (ValueError, TypeError):
        return 0


def favorite_eligible(item: dict) -> tuple[bool, str]:
    context = item.get("clinical_context")
    if not isinstance(context, dict) or context.get("version") != 1:
        return False, "stored_basis_unknown"
    if context.get("domain_basis") != "source_content":
        return False, "clinical_domain_unknown"
    if str(item.get("source_type") or item.get("source") or "").lower() == "foamed":
        if not str(item.get("bottom_line") or "").strip():
            return False, "learning_summary_unavailable"
        if _score(item, "clinical_usefulness_1_5") < 3 or _score(item, "practice_relevance_1_5") < 3:
            return False, "display_score_floor"
        if item.get("source_quality_label") not in {"core", "important"} or item.get("text_confidence_label") not in {"high", "moderate"} or context.get("text_basis") == "unknown":
            return False, "source_text_floor"
    else:
        if _score(item, "clinical_relevance_1_5") < 3 or _score(item, "practice_change_potential_1_5") < 3:
            return False, "display_score_floor"
        if item.get("evidence_strength_label") not in {"A", "B", "C"} or item.get("low_evidence_radar") or item.get("floor_rejection_reason"):
            return False, "design_floor"
        if context.get("result_basis") not in {"primary_reported", "recommendation_reported"}:
            facts = build_pubmed_facts(item)
            # A clinically important null/harm/noninferiority finding may be an
            # editorial priority even if an abstract does not label a primary
            # endpoint. This never supplies a missing primary-benefit claim.
            clinical_result = re.search(r"mortality|survival|delirium|complications|pain|hypotension|"
                                        r"ventilat|hospital stay|icu stay|quality of life", facts.primary_results, re.I)
            if not clinical_result or facts.direction not in {"negative_or_null", "harm", "noninferiority"}:
                return False, "results_unknown"
    return True, "clinical_priority_not_proven_benefit"


def finalize_favorite(item: dict, *, cadence: str, candidate: bool | None = None, legacy_period: bool = False) -> None:
    requested = item.get("top_pick") is True if candidate is None else candidate
    previous = item.get("top_pick_decision")
    if isinstance(previous, dict) and previous.get("version") == 1 and previous.get("cadence") != cadence:
        item.setdefault("stored_top_pick", {"cadence": previous.get("cadence"), "selected": bool(item.get("top_pick"))})
    elif not previous and item.get("top_pick") and cadence != "daily":
        item.setdefault("stored_top_pick", {"cadence": "legacy_unknown", "selected": True})
    allowed, reason = favorite_eligible(item)
    # Explicit stored-period reconstruction only. These flags describe the
    # historical ranking policy, never a newly validated clinical endorsement.
    if legacy_period and not isinstance(item.get("clinical_context"), dict):
        allowed, reason = True, "legacy_period_ranking_not_revalidated"
    item["top_pick"] = bool(requested and allowed)
    item["top_pick_decision"] = {"version": 1, "cadence": cadence, "selected": item["top_pick"],
                                 "reason": reason if requested else "not_selected_by_cadence_policy"}


def favorite_label(item: dict) -> str:
    return "⭐ Top pick"


def context_line(item: dict) -> str:
    context = context_for(item)
    domain_note = " · Topic placement uncertain" if context.get("domain_basis") == "unknown" else ""
    if domain_note and item.get('journal'):
        domain_note += " (journal fallback only)"
    uncertainty = context.get('uncertainty', 'Result uncertainty unknown')
    basis = {'abstract': 'Abstract', 'abstract_and_fulltext_excerpt': 'Abstract + full-text excerpt',
             'pubmed_abstract': 'Abstract', 'article_full_text': 'article text', 'rss_full_content': 'feed text',
             'html_content': 'extracted website text', 'html_excerpt': 'website excerpt',
             'rss_excerpt': 'feed excerpt', 'article_excerpt': 'article excerpt', 'unknown': 'unknown'}.get(context.get('text_basis'), 'excerpt')
    coverage = {'bounded_excerpt': 'bounded excerpt; completeness unknown', 'results_reported': 'results available',
                'results_unknown': 'results unavailable', 'unknown': 'unknown'}.get(context.get('text_coverage'), 'unknown')
    return f"Population {context.get('population', 'unknown')} · Text basis {basis} · Coverage {coverage}{domain_note} · {uncertainty}"


def reference_line(item: dict) -> str:
    links = []
    for ref in (item.get('primary_references') or [])[:2]:
        if not isinstance(ref, dict):
            continue
        kind, value, url = str(ref.get('kind', '')), str(ref.get('value', '')), str(ref.get('url', ''))
        if kind == 'pmid' and value.isdigit() and url == f'https://pubmed.ncbi.nlm.nih.gov/{value}/':
            links.append(f'[PMID {value}]({url})')
        elif kind == 'doi' and re.fullmatch(r'10\.\d{4,9}/[^\s<>]+', value) and url == 'https://doi.org/' + value:
            links.append(f'[DOI](<{url}>)')
    role = 'Commentary' if item.get('source_role') == 'commentary' else 'FOAMed context'
    return role + (' · Primary references (source supplied): ' + '; '.join(links) if links else ' · Primary reference unknown')


def limitation_value(value):
    """Retain typed lists and valid legacy strings; never serialize list repr."""
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return str(value or "").strip()
