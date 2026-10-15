"""Short, deterministic reader views of already-validated PubMed facts.

The detailed facts, consistency checks and stored assertions stay intact. A
reader sentence is either a qualitative source sentence with the same primary
finding, or a bounded description of that finding. No provider calls or I/O.
"""
from __future__ import annotations

import re
from types import SimpleNamespace

from .pubmed_facts import build_pubmed_facts, result_direction, sentences
from .cybermed_quality import safe_generated_text
from .clinical_reader import verified_reader_summary, source_comparison


_STATISTICS = re.compile(r"\b(?:IQR|CI|SD|HR|RR|(?-i:OR))\b|\bp\s*[=<>≤≥]|\d\s*%|\d\s*/\s*\d", re.I)
_BOILERPLATE = re.compile(r"does not prove absence|not proof of no effect|interpret secondary signals|"
                          r"no unqualified benefit|no benefit can be inferred|reported results:|"
                          r"limited to these source facts|bounded evidence excerpt", re.I)
_FIELDS = ("design", "population", "intervention_comparator", "primary_endpoint", "primary_results",
           "conclusions", "direction", "observational", "evidence")


def _lower_start(text: str) -> str:
    return text if text.split()[0].isupper() else text[0].lower() + text[1:]


def _legacy(item: dict) -> bool:
    return not isinstance(item.get("pubmed_facts"), dict) and (
        bool(item.get("digest_derived")) or not (item.get("abstract") or item.get("text")))


def _legacy_prose(value) -> str:
    text = safe_generated_text(value)
    prefix = re.compile(r"^\s*(?:\*\*)?bottom\s+line\s*:\s*(?:\*\*)?\s*", re.I)
    while prefix.match(text):
        text = prefix.sub("", text, count=1).strip()
    return text if text and len(text.split()) <= 60 and not _STATISTICS.search(text) and not _BOILERPLATE.search(text) else ""


def _facts(item: dict):
    stored = item.get("pubmed_facts")
    if isinstance(stored, dict):
        return SimpleNamespace(**{k: stored.get(k, False if k == "observational" else "") for k in _FIELDS})
    if not _legacy(item) and (item.get("abstract") or item.get("text")):
        return build_pubmed_facts(item)
    # Aggregates project persisted assertions; they never regenerate evidence.
    return SimpleNamespace(design=item.get("study_type") or "", population=item.get("population_setting") or "",
        intervention_comparator=" versus ".join(str(v) for v in (item.get("intervention_or_exposure"), item.get("comparator")) if v),
        primary_endpoint=item.get("primary_endpoint") or "", primary_results=item.get("key_results") or "",
        conclusions="", evidence="", direction=item.get("primary_result_direction") or "unclear",
        observational=bool(re.search(r"cohort|observational", str(item.get("study_type") or ""), re.I)))


def _population(facts, item: dict) -> str:
    for sentence in sentences(str(facts.population or "")):
        sentence = re.sub(r"^(?:population|participants|setting)\s*:\s*", "", sentence, flags=re.I)
        value = re.sub(r"^(?:a total of\s+)?[\d,]+\s+", "", sentence, flags=re.I)
        value = re.split(r"\b(?:were|was|are|underwent|received|randomi[sz]ed|enrolled|assigned)\b", value, maxsplit=1, flags=re.I)[0].strip(" .;,")
        if (re.search(r"\bpatients|participants|adults|children|infants|neonates\b", value, re.I)
                and len(value.split()) <= 14 and not re.search(r"\d", value)):
            return _lower_start(value)
    return {"adult": "adults", "pediatric": "children", "neonatal": "neonates", "mixed": "adults and children"}.get(
        (item.get("clinical_context") or {}).get("population"), "")


def _endpoint(facts) -> str:
    value = str(facts.primary_endpoint or "").strip()
    value = re.sub(r"^(?:the\s+)?(?:primary|main)\s+(?:clinical\s+)?(?:endpoints?|outcomes?)\s*(?:(?:was|were|is|are)\s+|:\s*)", "", value, flags=re.I)
    value = value.strip(" .:;")
    if not value or len(value.split()) > 15 or _STATISTICS.search(value):
        return "the primary clinical outcome"
    return _lower_start(value)


def _qualitative(sentence: str) -> str:
    sentence = re.sub(r"^(?:CONCLUSIONS AND RELEVANCE|CONCLUSIONS?|INTERPRETATION|RESULTS|FINDINGS)\s*:\s*", "", sentence, flags=re.I)
    # Discard statistical parentheses, not clinical parentheticals or negation.
    def statistical_parenthesis(match):
        inner = match[0][1:-1].strip()
        pure_statistics = re.match(r"(?:IQR|CI|SD|HR|RR|OR|\d+%\s*CI|p\s*[=<>≤≥]|n\s*=)", inner, re.I) and re.search(r"\d", inner)
        words = {word.lower() for word in re.findall(r"[a-z]+", inner, re.I)}
        pure_statistics = pure_statistics and words <= {"iqr", "ci", "sd", "hr", "rr", "or", "p", "n", "to", "vs", "versus", "adjusted", "unadjusted", "median", "mean", "and"}
        numeric_only = re.fullmatch(r"[\d\s.,%/–−+<>=-]+", inner)
        qualifier = re.search(r"except|only|subgroup|without|not\b", inner, re.I)
        return "" if (pure_statistics or numeric_only) and not qualifier else match[0]
    value = re.sub(r"\([^()]*\)", statistical_parenthesis, sentence)
    value = re.sub(r"\s+from\s+\d+(?:\.\d+)?%\s+to\s+\d+(?:\.\d+)?%", "", value, flags=re.I)
    value = re.sub(r"\s+by\s+\d+(?:\.\d+)?\s*(?:%|percentage points?)", "", value, flags=re.I)
    value = re.sub(r"\s+([,.;])", r"\1", re.sub(r"\s+", " ", value)).strip()
    if _STATISTICS.search(value) or _BOILERPLATE.search(value):
        return ""
    # Times in outcome names (e.g. 28-day mortality) are meaningful; dense
    # counts, ranges, ratios and estimates belong in Key results.
    without_times = re.sub(r"\b\d+[- ](?:day|week|month|year)s?\b", "", value, flags=re.I)
    if re.search(r"\d", without_times) or len(value.split()) > 46 or len(value) > 320:
        return ""
    return value if re.search(r"[.!?]$", value) else value.rstrip(";,") + "."


def _direction(text: str) -> str:
    if re.search(r"(?:do|does|did) not (?:establish|demonstrate|show)|(?:difference|benefit|improvement) was not demonstrated", text, re.I):
        return "negative_or_null"
    return result_direction(text)


def _source_finding(item: dict, *, language: str = "en") -> str:
    if (item.get("source") or item.get("source_type")) == "foamed":
        return re.sub(r"^(?:\*\*)?BOTTOM LINE:(?:\*\*)?\s*", "", str(item.get("bottom_line") or ""), flags=re.I).strip()
    if _legacy(item) and (legacy := _legacy_prose(item.get("bottom_line"))):
        return legacy
    facts = _facts(item)
    population, endpoint = _population(facts, item), _endpoint(facts)
    primary = str(facts.primary_results or "")
    subgroup_only = bool(re.search(r"subgroup|post[- ]hoc|exploratory|only (?:in|among)", primary, re.I))
    # English is the Cybermed publication language. Other languages receive a
    # short safe fallback rather than an untranslated source paraphrase.
    if not language.lower().startswith("en"):
        return "Das primäre Ergebnis ist unbekannt; Einzelheiten stehen im Deep Dive."
    if not primary and (item.get("clinical_context") or {}).get("result_basis") == "recommendation_reported":
        for sentence in sentences(str(facts.conclusions or "")):
            recommendation = _qualitative(sentence)
            if recommendation and re.search(r"\brecommend|\bsuggest", recommendation, re.I):
                # A reported recommendation is not a demonstrated trial effect.
                # Retain conditional wording and the source's stated scope.
                return re.sub(r"^We\b", "The guideline authors", recommendation)
    anchors = set(re.findall(r"[a-z]{4,}", endpoint.lower())) - {
        "primary", "clinical", "outcome", "outcomes", "endpoint", "endpoints", "days", "weeks", "months", "years"}
    if primary and facts.primary_endpoint and facts.direction not in {"unclear", "mixed_or_unclear"}:
        for source in (primary, str(facts.conclusions or "")):
            for sentence in sentences(source):
                candidate = _qualitative(sentence)
                if not candidate or _direction(candidate) != facts.direction:
                    continue
                if anchors and not any(word in candidate.lower() for word in anchors):
                    continue
                if source != primary and not anchors:
                    continue
                if subgroup_only and not re.search(r"subgroup|post[- ]hoc|exploratory|only (?:in|among)", candidate, re.I):
                    continue
                if facts.observational and (not re.search(r"associat|correlat|linked", candidate, re.I)
                        or re.search(r"\bcaused?|\bprevents?|\bproves?|will reduce|led to", candidate, re.I)):
                    continue
                if facts.direction == "noninferiority" and re.search(r"superior|equivalen|as effective", candidate, re.I):
                    continue
                if population and not re.search(r"patients|participants|adults|children|neonat", candidate, re.I):
                    candidate = f"In {population}, " + _lower_start(candidate)
                if len(candidate.split()) <= 60:
                    return candidate
    if primary and not facts.primary_endpoint and facts.direction not in {"unclear", "mixed_or_unclear"}:
        for sentence in sentences(primary):
            candidate = _qualitative(sentence)
            if not candidate or _direction(candidate) != facts.direction:
                continue
            if facts.observational and (not re.search(r"associat|correlat|linked", candidate, re.I)
                    or re.search(r"\bcaused?|\bprevents?|\bproves?|will reduce|led to", candidate, re.I)):
                continue
            if facts.direction == "noninferiority" and re.search(r"superior|equivalen|as effective", candidate, re.I):
                continue
            if subgroup_only and not re.search(r"subgroup|post[- ]hoc|exploratory|only (?:in|among)", candidate, re.I):
                continue
            # Report what the source actually says, without inventing which
            # endpoint was primary or deriving a practice recommendation.
            return "The authors reported: " + candidate
    if not primary or not facts.primary_endpoint:
        finding = ("The clinical implication of the reported findings remains uncertain." if primary else
                   "The primary clinical result is not reported in the available source.")
    elif facts.direction == "negative_or_null":
        finding = (f"No clear association with {endpoint} was demonstrated." if facts.observational else
                   f"No clear improvement in {endpoint} was demonstrated.")
    elif facts.direction == "noninferiority":
        finding = f"Noninferiority was reported for {endpoint}; superiority was not established."
    elif facts.direction == "positive" and subgroup_only:
        finding = f"An overall benefit for {endpoint} remains uncertain."
    elif facts.direction == "positive":
        finding = (f"An association with improved {endpoint} was reported." if facts.observational else
                   f"An improvement in {endpoint} was reported.")
    elif facts.direction == "harm":
        finding = f"Worse {endpoint} was reported."
    else:
        finding = f"The finding for {endpoint} remains inconclusive."
    return f"In {population}, {finding[0].lower() + finding[1:]}" if population else finding


def _study_context(item: dict, facts, language: str) -> str:
    population = _population(facts, item)
    design = str(facts.design or "")
    kind = ("noninferiority trial" if re.search(r"noninferior", design, re.I) else
            "randomized trial" if re.search(r"randomi[sz]|\bRCT\b", design, re.I) else
            "observational study" if facts.observational else "study")
    comparison = source_comparison(str(facts.intervention_comparator or ""))
    if comparison:
        context = f"The {kind} compared {comparison[0]} with {comparison[1]}"
        if population and not re.search(r"patients|participants|adults|children|neonat", context, re.I):
            context = f"In {population}, " + _lower_start(context)
        return context.rstrip(" .") + "."
    # Prefer an actual methods sentence, not a sentence of stock uncertainty.
    method = str(facts.intervention_comparator or "")
    if not method:
        match = re.search(r"Abstract / methods: (.+)", str(facts.evidence or ""))
        method = match[1] if match else ""
    for sentence in sentences(method):
        if len(sentence.split()) <= 48 and len(sentence.split()) >= 8 and re.search(r"compar|randomi[sz]|cohort|patients|participants", sentence, re.I) and not _STATISTICS.search(sentence):
            return sentence if sentence.endswith(".") else sentence + "."
    title = str(item.get("title") or "").strip(" .")
    if title:
        if re.match(r"(?:effects?|impact|association|comparison|use|management|treatment)\b", title, re.I):
            return f"The {kind} examined the {_lower_start(title)}."
        return f"The {kind} addressed “{title}”."
    endpoint = _endpoint(facts)
    return f"The {kind} examined {endpoint}" + (f" in {population}." if population else ".")


def reader_bottom_line(item: dict, *, language: str = "en") -> str:
    if text := verified_reader_summary(item, language=language):
        return text
    finding = _source_finding(item, language=language)
    if (item.get("source") or item.get("source_type")) == "foamed" or _legacy(item):
        return finding
    facts = _facts(item)
    context = _study_context(item, facts, language)
    # Keep source-grounded findings; restore the named study question before
    # an uncertainty statement instead of publishing uncertainty on its own.
    population = _population(facts, item)
    if population and context.lower().startswith("in " + population.lower() + ", "):
        finding = re.sub(r"^In " + re.escape(population) + r",\s*", "", finding, flags=re.I)
        if finding:
            finding = finding[0].upper() + finding[1:]
    if context.casefold().rstrip(" .") in finding.casefold():
        return finding
    return context + " " + finding


def reader_rating_line(item: dict) -> str:
    """Classic compact labels; retain actual values, never invent a high score."""
    fields = (("Source quality", "source_quality_label"), ("Usefulness", "clinical_usefulness_1_5"),
              ("Practice relevance", "practice_relevance_1_5"), ("Confidence", "text_confidence_label")) if (item.get("source") or item.get("source_type")) == "foamed" else (
              ("Evidence", "evidence_strength_label"), ("Relevance", "clinical_relevance_1_5"),
              ("Practice impact", "practice_change_potential_1_5"), ("Confidence", "text_confidence_label"))
    parts = []
    for label, key in fields:
        value = item.get(key)
        if value is None or value == "" or isinstance(value, bool):
            continue
        if key.endswith("_1_5"):
            if not isinstance(value, (float, int)) or not 1 <= value <= 5:
                continue
            shown = f"{int(value)}/5"
        else:
            shown = str(value).capitalize()
        parts.append(f"{label} {shown}")
    return " · ".join(parts)


_DETAIL_LABELS = {
    label.casefold(): label for label in (
        "Study type", "Population/setting", "Population", "Intervention/exposure & comparator",
        "Intervention/exposure", "Primary endpoints", "Key results", "Secondary results",
        "Limitations", "Why this matters", "Source", "Source basis", "Reported result (saved report)",
        "Background", "Importance", "Objectives", "Design", "Methods", "Setting", "Participants",
        "Interventions", "Main outcomes and measures", "Results", "Findings", "Conclusions",
        "Conclusions and relevance", "Interpretation", "Design, setting, and participants",
    )}
_DETAIL_LABELS.update({"primary endpoint": "Primary endpoints", "primary outcome": "Primary endpoints",
    "objective": "Objectives", "secondary results (do not replace the primary outcome)": "Secondary results"})
_DETAIL_FIELD = re.compile(r"^\s*(?:(?:[-*•]|\d+[.)])\s+)?(?:\*\*)?(" +
    "|".join(re.escape(label) for label in sorted(_DETAIL_LABELS, key=len, reverse=True)) +
    r")(?:\*\*)?\s*:(?:\*\*)?\s*(.*)$", re.I)
_SOURCE_SECTIONS = re.compile(r"(?<!\S)(?:(?:[-*•]|\d+[.)]|#{1,6})\s+)?(?:\*\*)?(" + "|".join(re.escape(label.upper()) for label in (
    "Conclusions and relevance", "Design, setting, and participants", "Main outcomes and measures",
    "Conclusions", "Interpretation", "Results", "Findings", "Background", "Importance", "Objective",
    "Objectives", "Methods", "Setting", "Participants", "Interventions")) +
    r"|(?i:Conclusions and relevance))\s*:(?:\*\*)?\s*")


def format_reader_detail(detail: str) -> str:
    """Reader typography only; preserve result wording, numbers and uncertainty.

    Fold a label and its continuation bullets into one row, without changing
    canonical facts or any saved Markdown. Source metadata remains internal;
    actual reference links are retained when present.
    """
    def section_break(match):
        line_start = detail.rfind("\n", 0, match.start()) + 1
        prefix = "\n" if detail[line_start:match.start()].strip() else ""
        return prefix + "- " + _DETAIL_LABELS[match[1].casefold()] + ": "
    detail = _SOURCE_SECTIONS.sub(section_break, detail)
    detail = re.sub(r"(?i)\bSecondary results \(do not replace the primary outcome\):\s*",
                    "\n- Secondary results: ", detail)
    rows, values, label = [], [], ""

    def flush():
        if label and values:
            text = " ".join(values).strip().rstrip(";")
            if label not in {"Source", "Source basis"}:
                rows.append(f"- **{label}:** {text}")
            elif re.search(r"https?://", text):
                rows.extend(["", text, ""])  # Keep a real citation, omit its redundant label.

    for raw in detail.splitlines():
        match = _DETAIL_FIELD.match(raw)
        if match:
            flush()
            label, values = _DETAIL_LABELS[match[1].casefold()], []
            if match[2].strip():
                values.append(match[2].strip())
        elif not raw.strip():
            if label == "Limitations" or label and not values:
                continue
            flush()
            label, values = "", []
            if rows and rows[-1]:
                rows.append("")
        elif label and not re.match(r"\s*(?:#{1,6}\s|\*\*BOTTOM LINE:)", raw, re.I):
            values.append(re.sub(r"^\s*(?:[-*•]|\d+[.)])\s+", "", raw).strip())
        else:
            flush()
            label, values = "", []
            rows.append(raw)
    flush()
    return "\n".join(rows).strip()


def reader_deep_dive(item: dict, detail: str, *, language: str = "en") -> str:
    """Keep detailed primary results and limitations, with one short core."""
    rows, old_core = [], str(item.get("_reader_original_bottom_line") or item.get("bottom_line") or "")
    for line in detail.splitlines():
        if re.match(r"\s*(?:\*\*)?BOTTOM LINE:(?:\*\*)?", line, re.I):
            old_core = re.sub(r"^\s*(?:\*\*)?BOTTOM LINE:(?:\*\*)?\s*", "", line, flags=re.I)
            continue
        if "Interpretation is limited to these source facts" in line:
            continue
        rows.append(line)
    if _STATISTICS.search(old_core) and not _STATISTICS.search("\n".join(rows)):
        reported = old_core.split("Reported results:", 1)[-1].strip()
        rows.extend(["", "- Reported result (saved report): " + reported])
    brief = _legacy_prose(old_core) if _legacy(item) else ""
    return f"**BOTTOM LINE:** {brief or reader_bottom_line(item, language=language)}\n\n" + format_reader_detail("\n".join(rows))
