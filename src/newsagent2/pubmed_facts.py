"""Conservative, source-based PubMed facts shared by Cybermed's two views.

This is an extractive guard, not a medical adjudicator. Unlabelled/ambiguous
primary outcomes stay unknown. Model prose cannot upgrade the source evidence.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any

from .cybermed_quality import contains_generation_error_text


_HEADINGS = re.compile(
    r"(?im)(?:^|\b)(background|objectives?|aims?|design|study design|methods?|"
    r"setting|subjects|participants|patients|interventions?|comparators?|"
    r"measurements and main results|measurements|primary (?:endpoints?|outcomes?)|"
    r"main results|results|conclusions?|limitations?)\s*(?::|\n)\s*"
)
_ALIASES = {
    "objective": "objectives", "aim": "objectives", "aims": "objectives",
    "study design": "design", "method": "methods", "participants": "population",
    "patients": "population", "subjects": "population", "intervention": "interventions",
    "comparator": "comparators", "main results": "results",
    "measurements and main results": "results", "conclusion": "conclusions",
    "limitation": "limitations", "primary endpoint": "endpoint",
    "primary endpoints": "endpoint", "primary outcome": "endpoint", "primary outcomes": "endpoint",
}


def sections(text: str) -> dict[str, str]:
    matches = list(_HEADINGS.finditer(text or ""))
    out: dict[str, str] = {}
    for i, match in enumerate(matches):
        key = _ALIASES.get(match[1].lower(), match[1].lower())
        value = text[match.end():matches[i + 1].start() if i + 1 < len(matches) else len(text)].strip()
        if value:
            out[key] = (out.get(key, "") + " " + value).strip()
    return out


def sentences(text: str) -> list[str]:
    # Never split decimal P-values such as P=.37 or 35.1%.
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+(?=[A-Z])|\n+", text) if s.strip()]


def _bounded(text: str, limit: int) -> str:
    """Keep complete source sentences; signal omission rather than cut words."""
    if len(text) <= limit:
        return text
    kept: list[str] = []
    for sentence in sentences(text):
        if len(" ".join(kept + [sentence])) <= limit:
            kept.append(sentence)
    return " ".join(kept)


def evidence_input(item: dict[str, Any], *, budget: int = 6000) -> tuple[str, str, bool]:
    """Results/conclusions first, complete sentences, bounded fulltext sections.

    Abstract is an independent source even when the enriched text starts with a
    long fulltext prefix. A results-less excerpt is not upgraded to a full study.
    """
    abstract = str(item.get("abstract") or "").strip()
    text = str(item.get("text") or "").strip()
    full = str(item.get("full_text_excerpt") or "").strip()
    if not full:
        marker = re.search(r"\[(?:PMC Open Access full text|Unpaywall OA full text)[^]]*\]", text)
        if marker:
            full, text = text[marker.end():], text[:marker.start()]
    if not abstract:
        # Collector metadata precedes a blank line; do not make it evidence.
        abstract = text.split("\n\n", 1)[-1] if "\n\n" in text else text
    chunks: list[str] = []
    omitted = False
    for label, source in (("Abstract", abstract), ("Fulltext excerpt", full)):
        if not source:
            continue
        parsed = sections(source)
        ordered = ["results", "conclusions", "limitations", "endpoint", "methods", "design",
                   "population", "setting", "interventions", "comparators", "measurements",
                   "objectives", "background"]
        if parsed:
            for key in ordered:
                if key in parsed:
                    # Do not let a single long background displace results.
                    limit = 3500 if key in {"results", "conclusions"} else 1200
                    value = _bounded(parsed[key], limit)
                    omitted |= value != parsed[key]
                    if value:
                        chunks.append(f"{label} / {key}: {value}")
        else:
            # No labelled results: retain bounded whole sentences, flag limits.
            value = _bounded(source, 5000 if label == "Abstract" else 2500)
            omitted |= value != source
            if value:
                chunks.append(f"{label} / unstructured: {value}")
    kept: list[str] = []
    # Results from BOTH sources precede background from either source.
    chunks.sort(key=lambda s: 0 if re.match(r".* / (results|conclusions|limitations):", s) else 1)
    for chunk in chunks:
        if len("\n".join(kept + [chunk])) <= budget:
            kept.append(chunk)
        else:
            omitted = True
    return "\n".join(kept), "abstract_and_fulltext_excerpt" if full else "abstract", omitted


def result_direction(text: str) -> str:
    low = (text or "").lower()
    if re.search(r"non[- ]?inferior", low):
        return "noninferiority" if not re.search(r"not (?:shown|demonstrated|established)|failed", low) else "unclear"
    if re.search(r"no (?:statistically )?significant|not (?:statistically )?significant|did not|"
                 r"no (?:difference|benefit)|not significantly|nicht signifikant", low):
        return "negative_or_null"
    p = re.search(r"\bp\s*(=|>|<|≥|≤)\s*(0?\.\d+|\d+\.\d+)", low)
    if p and ((p[1] in {"=", ">", "≥"} and float(p[2]) >= .05)):
        return "negative_or_null"
    if re.search(r"mixed|inconclusive|uncertain|borderline|individual components", low):
        return "mixed_or_unclear"
    if re.search(r"reduced|improved|beneficial|benefit|lower|significant improvement", low):
        return "positive"
    if re.search(r"increased harm|worse|higher adverse|more adverse", low):
        return "harm"
    return "unclear"


@dataclass(frozen=True)
class PubMedFacts:
    pmid: str
    design: str
    population: str
    intervention_comparator: str
    primary_endpoint: str
    reported_results: str
    primary_results: str
    secondary_results: str
    conclusions: str
    limitations: str
    uncertainty: str
    source_basis: str
    evidence: str
    input_omitted: bool
    direction: str
    observational: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True).encode()).hexdigest()

    def core(self, language: str = "en") -> str:
        if not self.primary_results:
            return ("Primary endpoint results are unknown from the supplied evidence; no benefit can be inferred."
                    if language == "en" else
                    "Das Ergebnis des primären Endpunkts ist aus der vorliegenden Quelle unbekannt; ein Nutzen lässt sich nicht ableiten.")
        intro = {
            "negative_or_null": "A statistically significant benefit was not demonstrated; this does not prove absence of an effect. Interpret secondary signals cautiously.",
            "noninferiority": "The reported noninferiority result does not establish superiority or equivalence.",
            "positive": "The source reports the following result; its scope is limited to the reported endpoint and population.",
        }.get(self.direction, "The primary result remains uncertain; no unqualified benefit can be inferred.")
        if language != "en":
            intro = {
                "negative_or_null": "Ein statistisch signifikanter Nutzen wurde nicht nachgewiesen; dies beweist keine Wirkungslosigkeit.",
                "noninferiority": "Das berichtete Nichtunterlegenheitsergebnis belegt weder Überlegenheit noch Gleichwertigkeit.",
                "positive": "Die Quelle berichtet folgendes Ergebnis; die Aussage gilt nur für den berichteten Endpunkt und die Population.",
            }.get(self.direction, "Das primäre Ergebnis bleibt unsicher; ein uneingeschränkter Nutzen lässt sich nicht ableiten.")
        caution = (" Observational associations do not establish causality." if self.observational else "")
        limitation = f" Limitations: {self.limitations}" if self.limitations else ""
        if self.input_omitted:
            limitation += " Only a bounded evidence excerpt was available."
        # Source sentences preserve groups, denominators, negation and P-values.
        return f"{intro} Reported results: {self.primary_results}{caution}{limitation}"

    def deep_dive(self, language: str = "en") -> str:
        fields = [
            ("Study type", self.design), ("Population/setting", self.population),
            ("Intervention/exposure & comparator", self.intervention_comparator),
            ("Primary endpoints", self.primary_endpoint), ("Key results", self.primary_results),
        ]
        rows = [f"BOTTOM LINE: {self.core(language)}", ""]
        rows += [f"- {label}: {value or 'Not reported'}" for label, value in fields]
        if self.secondary_results:
            rows.append(f"- Secondary results (do not replace the primary outcome): {self.secondary_results}")
        rows += ["- Limitations:", f"- {self.limitations or 'Not explicitly reported in the supplied evidence.'}",
                 f"- Source basis: {self.source_basis}",
                 "- Why this matters: Interpretation is limited to these source facts; clinical benefit is not inferred from the study objective."]
        return "\n".join(rows)


def build_pubmed_facts(item: dict[str, Any]) -> PubMedFacts:
    evidence, basis, omitted = evidence_input(item)
    # Parse the complete Abstract separately, not the reordered input labels.
    raw = str(item.get("abstract") or item.get("text") or "")
    marker = re.search(r"\[(?:PMC Open Access full text|Unpaywall OA full text)[^]]*\]", raw)
    if marker:
        raw = raw[:marker.start()]
    s = sections(raw)
    full = sections(str(item.get("full_text_excerpt") or ""))
    # Explicit fulltext results can supplement, but never silently reverse an
    # abstract result. Divergence yields unknown until reviewed by an operator.
    results = s.get("results", "")
    conclusions = s.get("conclusions", "")
    methods = s.get("methods", "")
    result_parts = re.split(r"(?=\b[Ss]econdary\b)|;\s*(?=\b[Ss]econdary\b)", results)
    primary = " ".join(x.strip() for x in result_parts if not re.match(r"secondary\b", x, re.I)).strip()
    secondary = " ".join(x.strip() for x in result_parts if re.match(r"secondary\b", x, re.I)).strip()
    if not primary and full.get("results"):
        primary = _bounded(full["results"], 3500)
    if primary and full.get("results") and result_direction(primary) != result_direction(full["results"]):
        primary = ""
    endpoint = s.get("endpoint", "")
    if not endpoint:
        endpoint = " ".join(x for x in sentences(methods + " " + s.get("measurements", ""))
                            if re.search(r"\bprimary (?:endpoint|outcome)|main outcome", x, re.I))
    if not endpoint:
        endpoint = " ".join(x for x in sentences(primary) if re.search(r"\bprimary (?:endpoint|outcome)", x, re.I))
    # Unstructured conclusions/background alone never establish an outcome.
    design = s.get("design", "") or " ".join(x for x in sentences(methods) if re.search(r"randomi[sz]|cohort|observational|trial|noninferior", x, re.I))
    if not design:
        design = ", ".join(str(x) for x in item.get("publication_types", [])[:2])
    population = " ".join(filter(None, [s.get("population"), s.get("setting")]))
    if not population:
        population = " ".join(x for x in sentences(methods + " " + results) if re.search(r"\bpatients|participants|randomi[sz]ed", x, re.I))
    intervention = " ".join(filter(None, [s.get("interventions"), s.get("comparators")]))
    if not intervention:
        intervention = " ".join(x for x in sentences(methods) if re.search(r"versus|\bvs\b|control|intervention", x, re.I))
    # Bind primary interpretation to explicit endpoint sentences where possible.
    # Do not let an unrelated secondary/safety endpoint determine polarity.
    endpoint_terms = set(re.findall(r"\b(?:mortality|survival|delirium|infection|complications|composite|pain)\b", endpoint, re.I))
    if endpoint_terms:
        selected = [x for x in sentences(primary) if any(re.search(r"\b" + re.escape(t) + r"\b", x, re.I) for t in endpoint_terms)]
        other = [x for x in sentences(primary) if x not in selected]
        if selected:
            primary = " ".join(selected)
            secondary = " ".join(filter(None, [secondary, " ".join(other)]))
    primary = _bounded(primary, 3500)
    direction = result_direction(primary)
    limitation = s.get("limitations", "") or " ".join(x for x in sentences(results + " " + conclusions)
        if re.search(r"missing|documented|assessed in|limited|uncertain|only \d|incomplete", x, re.I))
    uncertainty = "Primary results unknown" if not primary else (
        "No statistically significant benefit demonstrated; no-effect/equivalence not established"
        if direction == "negative_or_null" else "Interpret within the reported design, population and outcome")
    return PubMedFacts(str(item.get("pmid") or item.get("id") or ""), _bounded(design, 1200), _bounded(population, 1600),
                       _bounded(intervention, 1200), _bounded(endpoint, 1200), _bounded(results, 3500), primary, _bounded(secondary, 1800),
                       _bounded(conclusions, 1800), _bounded(limitation, 1600), uncertainty,
                       basis, evidence, omitted, direction, bool(re.search(r"observational|cohort", design, re.I)))


def candidate_issues(facts: PubMedFacts, text: str, *, finish_reason: str | None = None) -> list[str]:
    issues: list[str] = []
    if finish_reason not in {None, "stop"} or not text.strip() or not re.search(r"[.!?]\s*$", text):
        issues.append("incomplete_response")
    if contains_generation_error_text(text):
        issues.append("unsafe_generation_text")
    direction = result_direction(text)
    if facts.direction in {"negative_or_null", "mixed_or_unclear", "unclear"} and direction == "positive":
        issues.append("unsupported_benefit")
    if facts.direction == "positive" and direction in {"negative_or_null", "harm"}:
        issues.append("contradicts_reported_result")
    if facts.direction == "noninferiority" and re.search(r"superior|equivalent|as effective", text, re.I):
        issues.append("noninferiority_overclaim")
    if facts.observational and re.search(r"\bcaused|prevents?|proves?|will reduce|led to", text, re.I):
        issues.append("causal_overclaim")
    source_numbers = set(re.findall(r"\d+(?:\.\d+)?", facts.evidence))
    if set(re.findall(r"\d+(?:\.\d+)?", text)) - source_numbers:
        issues.append("unsupported_numbers")
    # Free-form model wording has no machine-verifiable group/endpoint binding.
    # Use the source-derived core even when a simple polarity check passes.
    if text.strip() != facts.core():
        issues.append("unverified_paraphrase")
    return list(dict.fromkeys(issues))
