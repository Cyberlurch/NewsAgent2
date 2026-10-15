"""Grounded reader summaries, separate from the full PubMed audit core.

One existing generation call proposes a short narrative and an exact result
quote. Local checks bind the narrative to the source facts; failed proposals
fall back to contextual source excerpts, without another provider call.
"""
from __future__ import annotations

import json
import re
from dataclasses import fields

from .cybermed_quality import contains_generation_error_text, safe_generated_text
from .pubmed_facts import PubMedFacts, result_direction, sentences


_COMMON = set("a an the and or of to in on for with from by as at is was were are this these those study trial clinical primary secondary outcome outcomes results result reported reports finding findings implication implications remains remain uncertain uncertainty patients patient participants adults adult children treatment treatments group groups compared comparison versus control intervention interventions randomized randomised abstract source evidence authors evaluated evaluate effect effects benefit benefits demonstrated significant statistically available examined investigate investigated including routine usual care synthetic illustrative".split())
_STATS = re.compile(r"\b(?:IQR|CI|SD)\b|\bp\s*[=<>≤≥]|\d\s*%|\d\s*/\s*\d", re.I)


def _normal(text: str) -> str:
    return " ".join(text.split()).casefold()


def _terms(text: str) -> set[str]:
    words = set(re.findall(r"[a-z][a-z-]{1,}", text.casefold())) - _COMMON
    words |= {re.sub(r"\s+", "_", x.casefold()) for x in re.findall(r"\b(?:Drug|Treatment|Group|Arm)\s+[A-Z]\b", text)}
    return words


def _direction(text: str) -> str:
    text = re.sub(r"(?:does not|do not|cannot)\s+(?:establish|prove|imply)\s+(?:superiority|equivalence|causality)|"
                  r"(?:superiority|equivalence|causality)\s+(?:was|is)\s+not\s+(?:established|demonstrated|proven)", "", text, flags=re.I)
    if re.search(r"(?:do|does|did) not (?:establish|demonstrate|show)|no clear (?:benefit|difference|improvement)|"
                 r"(?:difference|benefit|improvement) was not demonstrated|did not differ", text, re.I):
        return "negative_or_null"
    direction = result_direction(text)
    if direction == "unclear" and re.search(r"\b(?:meaningful|significant|clinically relevant) (?:improvement|reduction)\b", text, re.I):
        return "positive"
    return direction


def source_comparison(text: str) -> tuple[str, str] | None:
    """Extract named arms only from an explicit source comparison."""
    for sentence in sentences(text):
        explicit = bool(re.search(r"\b(?:comparing|compared|comparison of)\s+", sentence, re.I))
        value = re.sub(r".*?\b(?:comparing|compared|comparison of)\s+", "", sentence, flags=re.I)
        separator = r"versus|vs\.?|against|with|to" if explicit else r"versus|vs\.?|against"
        match = re.search(r"^(.+?)\s+(?:" + separator + r")\s+(.+?)[.;]?$", value, re.I)
        if match:
            left, right = (part.strip(" .;,") for part in match.groups())
            if 1 <= len(left.split()) <= 14 and 1 <= len(right.split()) <= 14 and not re.search(r"\b(?:was|were|randomi[sz]ed|patients|trial)\b", left, re.I):
                return left, right
    return None


def _unstructured_result(facts: PubMedFacts, quote: str) -> bool:
    # A parser's inability to identify a labelled primary endpoint is not proof
    # that an unstructured abstract contains no reported findings. Bind an
    # attributed measurement to an exact result-bearing source sentence, while
    # keeping the canonical primary-outcome mapping unknown.
    if facts.reported_results or "Abstract / unstructured:" not in facts.evidence or "Fulltext excerpt /" in facts.evidence:
        return False
    if not quote or len(quote) > 500 or _normal(quote) not in _normal(facts.evidence):
        return False
    if re.search(r"objectives?|aim(?:ed|s)?\b|hypothes|protocol|will (?:measure|assess)|previous|prior stud|background", quote, re.I):
        return False
    return bool(re.search(r"\bwe (?:found|observed|report)|\bour (?:results|data)|\bp\s*[=<>≤≥]|\d\s*%|\d\s*/\s*\d", quote, re.I))


def _winner(text: str, aliases: set[str]) -> set[str]:
    found = set()
    for alias in aliases:
        escaped = re.escape(alias)
        if (re.search(escaped + r"\s+(?:was\s+|were\s+)?(?:superior|noninferior|reduced|lowered|improved|decreased)", text, re.I)
                or re.search(r"(?:lower|fewer|better|improved|reduced)\b[^.;]{0,45}\b(?:in|with|for)\s+(?:the\s+)?" + escaped, text, re.I)):
            found.add(alias.casefold())
    return found


def reader_summary_issues(item: dict, facts: PubMedFacts, text: str, quote: str, *, finish_reason=None) -> list[str]:
    issues = []
    parts = sentences(text)
    if finish_reason not in {None, "stop"} or not re.search(r"[.!?][\"’”']?$", text.strip()) or not 2 <= len(parts) <= 3:
        issues.append("incomplete_reader_summary")
    if not 18 <= len(text.split()) <= 105 or contains_generation_error_text(text):
        issues.append("unsafe_or_unreadable_reader_summary")
    if _STATS.search(text) or len(re.findall(r"\b\d+(?:\.\d+)?", text)) > 2 or re.search(r"https?://|<[a-z/!]|[\w.+-]+@[\w.-]+|^\s*(?:#|[-*]\s)", text, re.I):
        issues.append("reader_detail_wall")
    context = " ".join((str(item.get("title") or ""), facts.population, facts.intervention_comparator))
    context_terms = _terms(context)
    topic_terms = context_terms | _terms(facts.primary_endpoint) | _terms(facts.primary_results)
    words = _terms(text)
    if context_terms and not context_terms & words:
        issues.append("missing_study_context")
    if len(topic_terms & words) < min(2, len(topic_terms)) or not topic_terms:
        issues.append("missing_specific_topic")
    endpoints = _terms(facts.primary_endpoint)
    if endpoints and not endpoints & words:
        issues.append("missing_reported_outcome")
    source_numbers = set(re.findall(r"\d+(?:\.\d+)?", facts.evidence))
    if set(re.findall(r"\d+(?:\.\d+)?", text)) - source_numbers:
        issues.append("unsupported_reader_numbers")
    population = str((item.get("clinical_context") or {}).get("population") or facts.population).lower()
    population = re.sub(r"\b(?:excluded|excluding|without|no)\s+(?:children|pediatric patients|neonates)\b", "", population)
    if ((re.search(r"\badult", population) and not re.search(r"child|pediatr|paediatr|mixed", population)
         and re.search(r"\bchildren|pediatric|paediatric|neonat", text, re.I))
            or (re.search(r"neonat|pediatr|paediatr|children", population) and not re.search(r"adult|mixed", population)
                and re.search(r"\badults?\b", text, re.I))):
        issues.append("reader_population_mismatch")
    known_scope = " ".join((population, facts.evidence, str(item.get("title") or "")))
    if re.search(r"\badults?\b", text, re.I) and not re.search(r"\badults?\b", known_scope, re.I):
        issues.append("unsupported_adult_population")
    if facts.observational and parts and re.search(r"randomi[sz]ed (?:trial|comparison|study)", parts[0], re.I):
        issues.append("unsupported_reader_design")

    recommendation = (item.get("clinical_context") or {}).get("result_basis") == "recommendation_reported"
    source = facts.primary_results or (facts.conclusions if recommendation else "")
    if source:
        if not quote.strip() or len(quote) > 600 or _normal(quote) not in _normal(source):
            issues.append("unbound_result_quote")
    elif not _unstructured_result(facts, quote) and (quote.strip() or re.search(r"\b(?:reduced|improved|beneficial|effective|superior|noninferior)\b", " ".join(parts[1:]), re.I)):
        issues.append("results_unavailable")
    finding = " ".join(parts[1:])
    if not source and not _unstructured_result(facts, quote) and not re.search(
            r"(?:results?|findings?|outcomes?) (?:are |were |was |is )?(?:not (?:reported|available|provided)|unavailable)|no (?:reported )?results", finding, re.I):
        issues.append("missing_reported_finding")
    direction, quoted_direction = _direction(finding), _direction(quote)
    known = facts.direction if facts.direction not in {"unclear", "mixed_or_unclear"} else quoted_direction
    first = parts[0] if parts else ""
    question = re.search(r"\b(?:examined|assessed|investigated|evaluated|tested|compared|comparison|whether)\b", first, re.I)
    context_claim = not question or re.search(r"\b(?:showed|demonstrated|found|reported|led to|resulted in)\b", first, re.I)
    if known == "negative_or_null" and context_claim and _direction(first) == "positive":
        issues.append("unsupported_context_benefit")
    if known == "positive" and context_claim and _direction(first) in {"negative_or_null", "harm"}:
        issues.append("reader_result_reversal")
    if not source and not _unstructured_result(facts, quote) and context_claim and _direction(first) in {"positive", "harm"}:
        issues.append("results_unavailable")
    if known == "negative_or_null" and direction in {"positive", "harm"}:
        issues.append("unsupported_reader_benefit")
    if known in {"positive", "negative_or_null", "noninferiority", "harm"} and direction == "unclear" and not recommendation:
        issues.append("missing_reported_finding")
    if known == "positive" and direction in {"negative_or_null", "harm"}:
        issues.append("reader_result_reversal")
    if known == "harm" and direction in {"positive", "negative_or_null"}:
        issues.append("reader_result_reversal")
    unqualified = re.sub(r"(?:does not|do not|cannot|not)\s+(?:establish|demonstrate|prove|imply)\s+(?:superiority|equivalence|causality)|"
                         r"(?:superiority|equivalence|causality)\s+(?:was|is)\s+not\s+(?:established|demonstrated|proven)", "", text, flags=re.I)
    if facts.direction == "noninferiority" and re.search(r"superior|equivalen|as effective", unqualified, re.I):
        issues.append("reader_noninferiority_overclaim")
    if re.search(r"noninferior", quote, re.I) and re.search(r"not (?:shown|demonstrated|established)|failed|did not", quote, re.I):
        if re.search(r"noninferior", finding, re.I) and not re.search(r"not (?:shown|demonstrated|established)|failed|did not", finding, re.I):
            issues.append("reader_failed_noninferiority_upgraded")
    if facts.observational and re.search(r"\bcaused?\b|\bprevent(?:s|ed)\b|\bproves?\b|will reduce|led to", unqualified, re.I):
        issues.append("reader_causal_overclaim")
    if not facts.primary_endpoint and re.search(r"\bprimary (?:outcome|endpoint)\s+(?:improved|was reduced|benefit)", finding, re.I):
        issues.append("unmapped_primary_claim")
    if not facts.primary_endpoint and direction == "positive" and not recommendation:
        if not re.search(r"reported|observed|associated", finding, re.I) or re.search(r"clinical benefit|change practice|routine use|should replace", finding, re.I):
            issues.append("unmapped_clinical_benefit")
    if recommendation and re.search(r"conditional|suggest", facts.conclusions, re.I) and not re.search(r"conditional|suggest", text, re.I):
        issues.append("guideline_strength_upgraded")
    if re.search(r"subgroup|post[- ]hoc|exploratory|only (?:in|among)", quote, re.I) and not re.search(r"subgroup|post[- ]hoc|exploratory|only (?:in|among)", finding, re.I):
        issues.append("reader_subgroup_generalization")
    aliases = set(re.findall(r"\b(?:Drug|Treatment|Group|Arm)\s+[A-Z]\b", facts.evidence))
    comparison = source_comparison(facts.intervention_comparator) or source_comparison(str(item.get("title") or ""))
    if comparison:
        aliases.update(comparison)
        left, right = map(_terms, comparison)
        aliases.update((left ^ right) - {"high", "low", "dose", "oral", "intravenous", "daily", "therapy"})
    source_winner, reader_winner = _winner(quote, aliases), _winner(text, aliases)
    if reader_winner and (not source_winner or not reader_winner <= source_winner):
        issues.append("unverified_reader_group_direction")
    return list(dict.fromkeys(issues))


def capture_reader_summary(item: dict, facts: PubMedFacts, content: str, *, language: str, finish_reason=None) -> None:
    item.pop("clinical_summary", None)
    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate_reader_key")
            result[key] = value
        return result
    try:
        data = json.loads(content, object_pairs_hook=unique_keys)
        if not isinstance(data, dict) or set(data) != {"summary", "result_quote"} or not all(isinstance(v, str) for v in data.values()):
            raise ValueError
    except (ValueError, TypeError):
        item["_reader_summary_issues"] = ["invalid_reader_summary_json"]
        return
    text = re.sub(r"^(?:\*\*)?BOTTOM LINE:(?:\*\*)?\s*", "", data["summary"].strip(), flags=re.I)
    quote = data["result_quote"].strip()
    issues = reader_summary_issues(item, facts, text, quote, finish_reason=finish_reason)
    item["_reader_summary_issues"] = issues
    if not issues:
        item["clinical_summary"] = {"version": 1, "text": text, "result_quote": quote,
                                    "facts_version": facts.version, "language": language}


def verified_reader_summary(item: dict, *, facts: PubMedFacts | None = None, language: str = "en", rebind: bool = False) -> str:
    value = item.get("clinical_summary")
    if not isinstance(value, dict) or set(value) != {"version", "text", "result_quote", "facts_version", "language"} or value.get("version") != 1 or value.get("language") != language:
        return ""
    if not all(isinstance(value.get(k), str) for k in ("text", "result_quote", "facts_version")):
        return ""
    if facts is None:
        stored = item.get("pubmed_facts")
        if not isinstance(stored, dict) or set(stored) != {f.name for f in fields(PubMedFacts)}:
            return ""
        if not all(isinstance(v, bool) if k in {"observational", "input_omitted"} else isinstance(v, str) for k, v in stored.items()):
            return ""
        facts = PubMedFacts(**stored)
    identity = str(item.get("pmid") or item.get("id") or item.get("item_id") or "").removeprefix("pubmed:")
    if identity and facts.pmid and identity != facts.pmid:
        return ""
    if (value["facts_version"] != facts.version and not rebind) or reader_summary_issues(item, facts, value["text"], value["result_quote"]):
        return ""
    if rebind and value["facts_version"] != facts.version:
        item["clinical_summary"] = {**value, "facts_version": facts.version}
    return safe_generated_text(value["text"])
