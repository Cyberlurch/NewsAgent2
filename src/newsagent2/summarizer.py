# src/newsagent2/summarizer.py
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from openai import OpenAI

# Default model used for all summaries (override via OPENAI_MODEL env var)
OPENAI_MODEL = (os.getenv("OPENAI_MODEL") or "gpt-4.1").strip()
OPENAI_MODEL_PUBMED_DEEPDIVE = (os.getenv("OPENAI_MODEL_PUBMED_DEEPDIVE") or OPENAI_MODEL).strip()
OPENAI_MODEL_PUBMED_DEEPDIVE_FALLBACK = (os.getenv("OPENAI_MODEL_PUBMED_DEEPDIVE_FALLBACK") or "").strip()


@dataclass
class _PubmedDeepDiveModels:
    primary: str
    fallback: str


def _pubmed_deep_dive_models() -> _PubmedDeepDiveModels:
    primary = OPENAI_MODEL_PUBMED_DEEPDIVE or OPENAI_MODEL
    fallback = OPENAI_MODEL_PUBMED_DEEPDIVE_FALLBACK.strip()
    if fallback and fallback == primary:
        fallback = ""
    return _PubmedDeepDiveModels(primary=primary, fallback=fallback)


def _norm_language(lang: Optional[str]) -> str:
    l = (lang or "").strip().lower()
    if l in ("en", "eng", "english"):
        return "en"
    if l in ("de", "deu", "german", "deutsch"):
        return "de"
    return "de"


def _norm_profile(profile: Optional[str]) -> str:
    p = (profile or "").strip().lower()
    if p in ("medical", "med", "medicine", "health"):
        return "medical"
    return "general"


# ----------------------------
# Prompts
# ----------------------------

_SYS_OVERVIEW_DE = (
    "You are a careful, neutral summarizer. Write the output strictly in German.\n"
    "You will receive multiple items (YouTube videos, articles, PubMed abstracts) with title, date, and text.\n"
    "Goal: Create a compact daily overview for a newsletter reader.\n\n"
    "Output rules:\n"
    "- Output must be valid Markdown.\n"
    "- Start with exactly this section header: '## Kurzüberblick'.\n"
    "- Summarize only what is supported by the provided text; do not invent facts.\n"
    "- If claims are speculative/uncertain, say so explicitly.\n"
    "- Prefer concrete statements (who/what/where/when) if present.\n"
    "- Keep it readable: short paragraphs or short bullet lists.\n"
    "- After the overview, optionally add a subsection '### Kurz notiert' with 2–6 short bullets.\n"
)

_SYS_OVERVIEW_EN = (
    "You are a careful, neutral summarizer. Write the output strictly in English.\n"
    "You will receive multiple items (YouTube videos, articles, PubMed abstracts) with title, date, and text.\n"
    "Goal: Create a compact daily overview for a newsletter reader.\n\n"
    "Output rules:\n"
    "- Output must be valid Markdown.\n"
    "- Start with exactly this section header: '## Executive Summary'.\n"
    "- Summarize only what is supported by the provided text; do not invent facts.\n"
    "- If claims are speculative/uncertain, say so explicitly.\n"
    "- Prefer concrete statements (who/what/where/when) if present.\n"
    "- Keep it readable: short paragraphs or short bullet lists.\n"
    "- After the overview, optionally add a subsection '### In brief' with 2–6 short bullets.\n"
)

_SYS_OVERVIEW_CYBERLURCH_EN = (
    "You are a careful, neutral summarizer. Section headers must be in English.\n"
    "You will receive multiple items (YouTube videos, articles, PubMed abstracts) with title, date, and text.\n"
    "Goal: Create a compact daily overview for a newsletter reader.\n\n"
    "Language policy (per item):\n"
    "- Detect the dominant language using the item's title and text.\n"
    "- If the item language is English, German, or Swedish, write that item's summary in that language.\n"
    "- Otherwise, translate/summarize that item into English.\n"
    "- Keep report-level section headers in English.\n\n"
    "Output rules:\n"
    "- Output must be valid Markdown.\n"
    "- Start with exactly this section header: '## Executive Summary'.\n"
    "- Summarize only what is supported by the provided text; do not invent facts.\n"
    "- If claims are speculative/uncertain, say so explicitly.\n"
    "- Prefer concrete statements (who/what/where/when) if present.\n"
    "- Keep it readable: short paragraphs or short bullet lists.\n"
    "- After the overview, optionally add a subsection '### In brief' with 2–6 short bullets.\n"
)

# NOTE:
# Reporter now renders a Cybermed "paper-first" list per category.
# This overview prompt additionally asks for a small clickable paper list inside the summary.
# Keep it for now (safe/minimal). If you later feel it is redundant, we can remove it.
_MEDICAL_OVERVIEW_APPEND_DE = (
    "\nMedizinischer Fokus:\n"
    "- Fokussiere auf klinisch/praktisch relevante Inhalte (Anästhesie, Intensivmedizin, Schmerztherapie, Akut-/Notfallmedizin, Reanimation, KI im Gesundheitswesen).\n"
    "- Gruppiere, wenn sinnvoll, nach: Anästhesie, Intensivmedizin, Schmerztherapie, KI, Sonstige.\n"
    "- Wenn Studienlage schwach ist (kleine Stichprobe, rein beobachtend, nur Hypothese), benenne das klar.\n"
    "- Füge einen Abschnitt mit exakt diesem Titel hinzu: \"### Papers (klickbare Quellen)\".\n"
    "  - Darunter 5–12 der relevantesten Items als Bulletpoints listen.\n"
    "  - Jedes Bullet MUSS genau einen Markdown-Link mit der URL aus dem JSON enthalten (keine neuen Links).\n"
    "  - Empfohlenes Format: - [Kurztitel](URL) — Journal/Jahr falls vorhanden — 1 kurzer klinischer Takeaway.\n"
)

_MEDICAL_OVERVIEW_APPEND_EN = (
    "\nMedical focus:\n"
    "- Prioritize clinically/practically relevant information (anesthesia, intensive care, acute/emergency medicine, resuscitation, pain medicine, AI in healthcare).\n"
    "- If useful, group by: Anesthesia, Intensive Care, Pain, AI, Other.\n"
    "- If evidence is weak (small sample, observational only, hypothesis), state that clearly.\n"
    "- Add a section titled exactly: \"### Papers (clickable sources)\".\n"
    "  - Under that, list 5–12 of the most relevant items as bullet points.\n"
    "  - Each bullet MUST include exactly one Markdown link using the item URL from the JSON (no new links).\n"
    "  - Recommended format: - [Short title](URL) — Journal/Year if available — 1 short clinical takeaway.\n"
)

_SYS_DETAIL_YOUTUBE_DE = (
    "You are a careful summarizer. Write strictly in German.\n"
    "You will receive one YouTube item with title, channel, date, URL and transcript/description text.\n\n"
    "Return Markdown with this structure:\n"
    "Kernaussagen:\n"
    "- 3–6 bullets (präzise, keine Spekulation)\n\n"
    "Details & Argumentation:\n"
    "- 1–3 kurze Absätze.\n"
)

_SYS_DETAIL_YOUTUBE_EN = (
    "You are a careful summarizer. Write strictly in English.\n"
    "You will receive one YouTube item with title, channel, date, URL and transcript/description text.\n\n"
    "Return Markdown with this structure:\n"
    "Key takeaways:\n"
    "- 3–6 bullets (precise, no speculation)\n\n"
    "Details & reasoning:\n"
    "- 1–3 short paragraphs.\n"
)

_SYS_DETAIL_YOUTUBE_CYBERLURCH_EN = (
    "You are a careful summarizer. Section headers must be in English.\n"
    "You will receive one YouTube item with title, channel, date, URL and transcript/description text.\n\n"
    "Language policy:\n"
    "- Detect the dominant language using the title and text provided.\n"
    "- If the item language is English, German, or Swedish, write the summary in that language.\n"
    "- Otherwise, translate/summarize the item into English.\n"
    "- Keep the structural labels in English.\n\n"
    "Return Markdown with this structure:\n"
    "Key takeaways:\n"
    "- 3–6 bullets (precise, no speculation)\n\n"
    "Details & reasoning:\n"
    "- 1–3 short paragraphs.\n"
)

_SYS_DETAIL_PUBMED_DE = (
    "You are a careful clinical summarizer. Write strictly in German.\n"
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract/full-text excerpt when available).\n"
    "Use ONLY the provided text; do not add outside facts. Extract what is explicitly or implicitly present; only use 'nicht berichtet' when there is truly no hint in the text.\n"
    "Capture study type, population/setting, intervention/comparator, endpoints, key results, limitations, and why it matters. Nutze vorhandene Volltext-Auszüge (falls vorhanden) bevorzugt vor dem Abstract, priorisiere Methoden/Ergebnisse und nenne konkrete Zahlen. Gib nach jedem Label mindestens einen kurzen Satz in derselben Zeile an; zusätzliche Bullet-Points dürfen in eingerückten Zeilen folgen.\n\n"
    "Return Markdown that renders cleanly in HTML email:\n"
    "- Start with one paragraph: 'BOTTOM LINE: …'\n"
    "- Then a bullet list with bold labels exactly like this:\n"
    "  - **Study type:** …\n"
    "  - **Population/setting:** …\n"
    "  - **Intervention/exposure & comparator:** …\n"
    "  - **Primary endpoints:** …\n"
    "  - **Key results:** …\n"
    "  - **Limitations:**\n"
    "    - …\n"
    "  - **Why this matters:** …\n"
)

_SYS_DETAIL_PUBMED_EN = (
    "You are a careful clinical summarizer. Write strictly in English.\n"
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract/full-text excerpt when available).\n"
    "Use ONLY the provided text; do not add outside facts. Extract what is explicitly or implicitly present; only use 'Not reported' when there is truly no hint in the text.\n"
    "Capture study type, population/setting, intervention/comparator, endpoints, key results, limitations, and why it matters. Prefer any full-text excerpt provided (especially Methods/Results) over the abstract when present; include concrete numbers when available. Put at least one short sentence on the SAME LINE after each label; additional indented bullets may follow.\n\n"
    "Return Markdown that renders cleanly in HTML email:\n"
    "- Start with one paragraph: 'BOTTOM LINE: …'\n"
    "- Then a bullet list with bold labels exactly like this:\n"
    "  - **Study type:** …\n"
    "  - **Population/setting:** …\n"
    "  - **Intervention/exposure & comparator:** …\n"
    "  - **Primary endpoints:** …\n"
    "  - **Key results:** …\n"
    "  - **Limitations:**\n"
    "    - …\n"
    "  - **Why this matters:** …\n"
)

_SYS_DETAIL_PUBMED_CYBERLURCH_EN = (
    "You are a careful clinical summarizer. Section headers must be in English.\n"
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract/full-text excerpt when available).\n"
    "Use ONLY the provided text; do not add outside facts. Extract what is explicitly or implicitly present; only use 'Not reported' when there is truly no hint in the text. Prefer any full-text excerpt provided (especially Methods/Results) over the abstract when present; include concrete numbers when available.\n\n"
    "Language policy:\n"
    "- Detect the dominant language using the title and abstract text.\n"
    "- If the item language is English, German, or Swedish, write the summary in that language.\n"
    "- Otherwise, translate/summarize the item into English.\n"
    "- Keep the structural labels in English.\n\n"
    "Return Markdown that renders cleanly in HTML email:\n"
    "- Start with one paragraph: 'BOTTOM LINE: …'\n"
    "- Then a bullet list with bold labels exactly like this:\n"
    "  - **Study type:** …\n"
    "  - **Population/setting:** …\n"
    "  - **Intervention/exposure & comparator:** …\n"
    "  - **Primary endpoints:** …\n"
    "  - **Key results:** …\n"
    "  - **Limitations:**\n"
    "    - …\n"
    "  - **Why this matters:** …\n"
    "If the text contains enough information to infer the study type/population/endpoints/results, do so; only use 'Not reported' when there is truly no hint in the provided text. Prioritize Methods/Results, include concrete numbers when present, and place at least one short sentence on the SAME LINE after each label (additional indented bullets are allowed).\n"
)


_PUBMED_REQUIRED_HEADINGS = [
    "BOTTOM LINE:",
    "Study type:",
    "Population/setting:",
    "Intervention/exposure & comparator:",
    "Primary endpoints:",
    "Key results:",
    "Limitations:",
    "Why this matters:",
]

_PUBMED_LABEL_ALIASES = {
    "study design": "Study type:",
    "design": "Study type:",
    "methods": "Study type:",
    "population": "Population/setting:",
    "setting": "Population/setting:",
    "participants": "Population/setting:",
    "patients": "Population/setting:",
    "cohort": "Population/setting:",
    "intervention": "Intervention/exposure & comparator:",
    "exposure": "Intervention/exposure & comparator:",
    "comparator": "Intervention/exposure & comparator:",
    "control": "Intervention/exposure & comparator:",
    "outcome": "Primary endpoints:",
    "outcomes": "Primary endpoints:",
    "endpoint": "Primary endpoints:",
    "endpoints": "Primary endpoints:",
    "results": "Key results:",
    "findings": "Key results:",
    "key findings": "Key results:",
    "limitations": "Limitations:",
    "strengths/limitations": "Limitations:",
    "implications": "Why this matters:",
    "significance": "Why this matters:",
    "clinical relevance": "Why this matters:",
    "why it matters": "Why this matters:",
    "importance": "Why this matters:",
    "bottom line": "BOTTOM LINE:",
}

_PUBMED_JSON_KEYS = [
    "bottom_line",
    "study_type",
    "population_setting",
    "intervention_comparator",
    "primary_endpoints",
    "key_results",
    "limitations",
    "why_this_matters",
]

_PUBMED_REQUIRED_JSON_KEYS = {
    "study_type",
    "population_setting",
    "intervention_comparator",
    "primary_endpoints",
    "key_results",
    "limitations",
    "why_this_matters",
}


def _pubmed_json_system_prompt(lang: str) -> str:
    placeholder = "Not reported" if lang == "en" else "nicht berichtet"
    lang_label = "English" if lang == "en" else "German"
    return (
        "You are a careful clinical summarizer. Use ONLY the provided abstract/full text; do not invent facts.\n"
        f"Respond with a single valid JSON object (no Markdown, no commentary) using these keys: {', '.join(_PUBMED_JSON_KEYS)}.\n"
        "- Each field must be concise (<=2 sentences) and written in the requested language.\n"
        f"- If a field is truly absent, use the placeholder '{placeholder}'.\n"
        "- Field semantics:\n"
        "  - bottom_line: the core takeaway.\n"
        "  - study_type: design/methods.\n"
        "  - population_setting: who/where the study was done.\n"
        "  - intervention_comparator: exposure/intervention and comparator.\n"
        "  - primary_endpoints: primary outcomes/endpoints.\n"
        "  - key_results: main findings with numbers when available.\n"
        "  - limitations: array of short bullet strings.\n"
        "  - why_this_matters: clinical/practical relevance.\n"
        f"- Output language: {lang_label} for the values; keep JSON keys in English."
    )


def _strip_json_markers(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
    if text.endswith("```"):
        text = text[: -3].rstrip()
    return text


def _parse_pubmed_json_output(raw: str) -> Optional[Dict[str, Any]]:
    cleaned = _strip_json_markers(raw)
    try:
        data = json.loads(cleaned)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _validate_pubmed_json_output(data: Dict[str, Any]) -> None:
    missing = [k for k in _PUBMED_REQUIRED_JSON_KEYS if k not in data]
    if missing:
        raise ValueError(f"missing required keys: {', '.join(sorted(missing))}")


def extract_pubmed_abstract(raw_text: str) -> tuple[str, bool]:
    """
    Extract the abstract portion from a PubMed text blob.

    Returns (abstract_text, header_was_present).
    """
    text = (raw_text or "").strip()
    if not text:
        return "", False

    lines = text.splitlines()
    header_seen = False
    abstract_lines: List[str] = []
    found_blank = False

    for line in lines:
        if not found_blank and line.strip() == "":
            found_blank = True
            header_seen = True
            continue
        if found_blank:
            abstract_lines.append(line)

    if not abstract_lines:
        return text, header_seen

    return "\n".join(abstract_lines).strip(), header_seen


def _normalize_pubmed_field_values(md: str, *, lang: str, fallback_bottom_line: str = "") -> tuple[str, int]:
    placeholder = "Not reported" if lang == "en" else "nicht berichtet"
    lower_placeholder = placeholder.lower()
    label_line_pat = re.compile(r"(?im)^\s*(?:[-*•]\s*)?(?:\*\*)?([^:]+)(?:\*\*)?\s*:?\s*(.*?)\s*$")

    def _clean_value(val: str) -> str:
        cleaned = (val or "").strip()
        cleaned = re.sub(r"^\*+|\*+$", "", cleaned).strip()
        cleaned = re.sub(r"^[\s>*-]*[-*•]\s+", "", cleaned)
        if cleaned.lower() in {"not reported", "nicht berichtet"}:
            return placeholder
        return cleaned

    def _canonical_label(label: str) -> str:
        raw = (label or "").lower()
        raw = re.sub(r"[\s:]+", " ", raw).strip()
        if raw in _PUBMED_LABEL_ALIASES:
            return _PUBMED_LABEL_ALIASES[raw]
        for heading in _PUBMED_REQUIRED_HEADINGS:
            lbl = heading.rstrip(":").lower()
            if raw == lbl:
                return heading
        return ""

    def _join_parts(parts: List[str]) -> str:
        cleaned = [_clean_value(p) for p in parts if _clean_value(p)]
        return "; ".join(cleaned).strip()

    def _parse_labeled_blocks(text: str) -> Dict[str, List[str]]:
        blocks: Dict[str, List[str]] = {}
        lines = text.splitlines()
        idx = 0
        while idx < len(lines):
            raw_line = lines[idx]
            normalized_line = re.sub(r"^\s*[-*•]\s*", "", raw_line or "")
            normalized_line = re.sub(r"^\*+|\*+$", "", normalized_line).strip()
            normalized_line = normalized_line.replace("**", "").strip()
            if not normalized_line:
                idx += 1
                continue
            m = label_line_pat.match(normalized_line)
            if not m:
                idx += 1
                continue
            canon = _canonical_label(m.group(1))
            if not canon:
                idx += 1
                continue

            value_parts: List[str] = []
            first_val = _clean_value(m.group(2) or "")
            if first_val:
                value_parts.append(first_val)

            lookahead = idx + 1
            while lookahead < len(lines):
                nxt_raw = lines[lookahead]
                nxt_norm = re.sub(r"^\s*[-*•]\s*", "", nxt_raw or "")
                nxt_norm = re.sub(r"^\*+|\*+$", "", nxt_norm).rstrip()
                nxt_norm = nxt_norm.replace("**", "").strip()
                if not (nxt_norm or "").strip():
                    if value_parts:
                        break
                    lookahead += 1
                    continue
                next_label_match = label_line_pat.match(nxt_norm.strip())
                if next_label_match and _canonical_label(next_label_match.group(1)):
                    break
                if nxt_raw.startswith((" ", "\t")) or nxt_raw.lstrip().startswith(("-", "*", "•", ">")):
                    cleaned_part = _clean_value(nxt_norm)
                    if cleaned_part:
                        value_parts.append(cleaned_part)
                    lookahead += 1
                    continue
                if value_parts:
                    break
                lookahead += 1

            next_idx = max(lookahead, idx + 1)
            if value_parts:
                blocks[canon] = value_parts
            idx = next_idx
        return blocks

    alias_blocks = _parse_labeled_blocks(md)

    def _extract_alias_fields(text: str) -> Dict[str, str]:
        return {k: _join_parts(v) for k, v in alias_blocks.items() if _join_parts(v)}

    def _extract_bottom_line(text: str) -> str:
        for line in text.splitlines():
            stripped = re.sub(r"^[\s>*-]+", "", line or "")
            stripped = re.sub(r"^\*+|\*+$", "", stripped).strip()
            if stripped.lower().startswith("bottom line"):
                return stripped.split(":", 1)[-1].strip()
        return ""

    def _extract_field(text: str, label: str) -> str:
        if label in alias_blocks:
            return _join_parts(alias_blocks[label])
        pat = rf"(?im)^\s*(?:[-*•]\s*)?(?:\*\*)?{re.escape(label)}(?:\*\*)?\s*:?\s*(?:\*\*)?\s*(.+)$"
        m = re.search(pat, text)
        if m:
            return _clean_value(m.group(1))
        return ""

    def _extract_limitations(text: str) -> List[str]:
        if "Limitations:" in alias_blocks:
            return [_clean_value(x) for x in alias_blocks.get("Limitations:", []) if _clean_value(x)]
        inline_lim = _extract_field(text, "Limitations")
        if inline_lim:
            return [_clean_value(inline_lim)]
        pat = re.compile(r"(?im)^\s*(?:[-*•]\s*)?(?:\*\*)?(limitations|strengths/limitations)(?:\*\*)?\s*:?\s*$")
        lines = text.splitlines()
        start_idx = None
        for i, ln in enumerate(lines):
            if pat.match(ln):
                start_idx = i + 1
                break
        if start_idx is None:
            return []
        collected: List[str] = []
        for ln in lines[start_idx:]:
            stripped = ln.strip()
            if not stripped:
                if collected:
                    break
                continue
            if re.match(r"(?i)^\s*(?:[-*•]\s*)?(?:\*\*)?(study type|population/setting|intervention/exposure|primary endpoints|key results|why this matters)(?:\*\*)?\s*:", stripped):
                break
            if stripped.startswith(("-", "*", "•")):
                stripped = stripped.lstrip("-*• ").strip()
            collected.append(_clean_value(stripped))
        return collected

    def _salvage_unlabeled_bullets(text: str) -> List[str]:
        bullets: List[str] = []
        stop_at_limitations = False
        for ln in text.splitlines():
            stripped = ln.strip()
            if not stripped:
                continue
            if stripped.lower().startswith("bottom line"):
                continue
            if re.match(r"(?im)^\s*(?:[-*•]\s*)?(?:\*\*)?(limitations|strengths/limitations)(?:\*\*)?\s*:?\s*$", stripped):
                stop_at_limitations = True
                continue
            if stop_at_limitations:
                continue
            if stripped.startswith(("-", "*", "•")):
                bullets.append(_clean_value(stripped.lstrip("-*• ")))
        return bullets

    alias_fields = _extract_alias_fields(md)
    bottom_line = _clean_value(alias_fields.get("BOTTOM LINE:", "") or _extract_bottom_line(md) or fallback_bottom_line or placeholder)
    study_type = _clean_value(alias_fields.get("Study type:", "") or _extract_field(md, "Study type"))
    population = _clean_value(alias_fields.get("Population/setting:", "") or _extract_field(md, "Population/setting"))
    intervention = _clean_value(
        alias_fields.get("Intervention/exposure & comparator:", "")
        or _extract_field(md, "Intervention/exposure & comparator")
    )
    endpoints = _clean_value(alias_fields.get("Primary endpoints:", "") or _extract_field(md, "Primary endpoints"))
    key_results = _clean_value(alias_fields.get("Key results:", "") or _extract_field(md, "Key results"))
    why_matters = _clean_value(alias_fields.get("Why this matters:", "") or _extract_field(md, "Why this matters"))

    limitations_list = [_clean_value(x) for x in _extract_limitations(md) if _clean_value(x)]
    alias_lim = _clean_value(alias_fields.get("Limitations:", ""))
    if alias_lim and "Limitations:" not in alias_blocks:
        limitations_list = [alias_lim] + limitations_list

    not_reported_fields = 0
    fields_map = {
        "study_type": study_type,
        "population": population,
        "intervention": intervention,
        "endpoints": endpoints,
        "key_results": key_results,
        "why_matters": why_matters,
    }

    salvage_candidates = iter([b for b in _salvage_unlabeled_bullets(md) if b and b.lower() != lower_placeholder])
    for key in fields_map:
        if (fields_map[key] or "").strip():
            continue
        try:
            candidate = next(salvage_candidates)
        except StopIteration:
            break
        if candidate:
            fields_map[key] = candidate

    for val in fields_map.values():
        cleaned = (val or "").strip()
        if not cleaned or cleaned.lower() == lower_placeholder:
            not_reported_fields += 1

    if not limitations_list:
        limitations_list = [placeholder]
        not_reported_fields += 1
    elif all((li or "").strip().lower() == lower_placeholder for li in limitations_list):
        not_reported_fields += 1

    fields = [
        f"- Study type: {fields_map['study_type'] or placeholder}",
        f"- Population/setting: {fields_map['population'] or placeholder}",
        f"- Intervention/exposure & comparator: {fields_map['intervention'] or placeholder}",
        f"- Primary endpoints: {fields_map['endpoints'] or placeholder}",
        f"- Key results: {fields_map['key_results'] or placeholder}",
        "- Limitations:",
    ]
    fields.extend(f"- {li or placeholder}" for li in limitations_list)
    fields.append(f"- Why this matters: {fields_map['why_matters'] or placeholder}")

    normalized = "\n".join([f"BOTTOM LINE: {bottom_line or placeholder}", ""] + fields).strip()
    return normalized, not_reported_fields


def normalize_pubmed_deep_dive(md: str, *, lang: str, fallback_bottom_line: str = "") -> str:
    normalized, _ = _normalize_pubmed_field_values(md, lang=lang, fallback_bottom_line=fallback_bottom_line)
    return normalized


def _ensure_pubmed_deep_dive_template(detail_md: str, *, lang: str = "en", fallback_bottom_line: str = "") -> str:
    return normalize_pubmed_deep_dive(detail_md, lang=lang, fallback_bottom_line=fallback_bottom_line)


def _render_pubmed_deep_dive_from_json(
    data: Dict[str, Any], *, lang: str, fallback_bottom_line: str = ""
) -> tuple[str, int]:
    placeholder = "Not reported" if lang == "en" else "nicht berichtet"

    def _as_str(key: str) -> str:
        val = data.get(key, "")
        if isinstance(val, (list, dict)):
            return ""
        return str(val or "").strip()

    def _as_list(key: str) -> List[str]:
        val = data.get(key, [])
        if isinstance(val, list):
            return [str(v or "").strip() for v in val if str(v or "").strip()]
        if isinstance(val, str):
            parts = [p.strip() for p in re.split(r"[\n;]+", val) if p.strip()]
            return parts
        return []

    fields_map = {
        "bottom_line": _as_str("bottom_line") or fallback_bottom_line or placeholder,
        "study_type": _as_str("study_type"),
        "population": _as_str("population_setting"),
        "intervention": _as_str("intervention_comparator"),
        "endpoints": _as_str("primary_endpoints"),
        "key_results": _as_str("key_results"),
        "why_matters": _as_str("why_this_matters"),
    }
    limitations = _as_list("limitations")
    if not limitations:
        limitations = [placeholder]

    lines = [
        f"BOTTOM LINE: {fields_map['bottom_line'] or placeholder}",
        "",
        f"- Study type: {fields_map['study_type'] or placeholder}",
        f"- Population/setting: {fields_map['population'] or placeholder}",
        f"- Intervention/exposure & comparator: {fields_map['intervention'] or placeholder}",
        f"- Primary endpoints: {fields_map['endpoints'] or placeholder}",
        f"- Key results: {fields_map['key_results'] or placeholder}",
        "- Limitations:",
    ]
    lines.extend(f"- {li or placeholder}" for li in limitations)
    lines.append(f"- Why this matters: {fields_map['why_matters'] or placeholder}")

    normalized, missing_fields = _normalize_pubmed_field_values(
        "\n".join(lines).strip(), lang=lang, fallback_bottom_line=fallback_bottom_line
    )
    return normalized, missing_fields


def _run_pubmed_markdown_deep_dive(
    client: OpenAI,
    *,
    model: str,
    sys_prompt: str,
    user_prompt: str,
    lang: str,
    fallback_bottom_line: str,
) -> tuple[str, int]:
    messages = [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user_prompt},
    ]
    r = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.2,
    )
    raw_md = (r.choices[0].message.content or "").strip()
    return _normalize_pubmed_field_values(raw_md, lang=lang, fallback_bottom_line=fallback_bottom_line)


def _is_sparse_pubmed_deep_dive(not_reported_fields: int) -> bool:
    return not_reported_fields >= 4


def _get_client() -> OpenAI:
    # OPENAI_API_KEY is expected to be available via env (GitHub Actions Secret or local .env)
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _slim_items(items: List[Dict[str, Any]], max_text_chars: int = 2000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        text = (it.get("text") or "").strip()
        if len(text) > max_text_chars:
            text = text[:max_text_chars].rstrip()

        published = it.get("published_at")
        if isinstance(published, datetime):
            published_str = published.replace(microsecond=0).isoformat()
        else:
            published_str = str(published) if published else ""

        out.append(
            {
                "source": (it.get("source") or "").strip(),
                "channel": (it.get("channel") or "").strip(),
                "title": (it.get("title") or "").strip(),
                "url": (it.get("url") or "").strip(),
                "published_at": published_str,
                "text": text,
            }
        )
    return out


def summarize(items: List[Dict[str, Any]], *, language: str = "de", profile: str = "general") -> str:
    """
    Create the overview section (Markdown), using multiple items.
    """
    lang = _norm_language(language)
    prof = _norm_profile(profile)
    report_key = (os.getenv("REPORT_KEY") or "").strip().lower()
    is_cyberlurch = report_key == "cyberlurch"

    if is_cyberlurch:
        sys_prompt = _SYS_OVERVIEW_CYBERLURCH_EN
    else:
        sys_prompt = _SYS_OVERVIEW_EN if lang == "en" else _SYS_OVERVIEW_DE
        if prof == "medical":
            sys_prompt += _MEDICAL_OVERVIEW_APPEND_EN if lang == "en" else _MEDICAL_OVERVIEW_APPEND_DE

    items_json = json.dumps(_slim_items(items), ensure_ascii=False, indent=2)

    user_prompt = (
        "Items (JSON):\n"
        f"{items_json}\n\n"
        "Now write the requested overview."
    )

    try:
        client = _get_client()
        r = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
        return (r.choices[0].message.content or "").strip()
    except Exception as e:
        if lang == "en":
            return f"## Executive Summary\n\n**Error:** Failed to create overview: `{e!r}`\n"
        return f"## Kurzüberblick\n\n**Fehler:** Konnte Kurzüberblick nicht erzeugen: `{e!r}`\n"


def summarize_item_detail(item: Dict[str, Any], *, language: str = "de", profile: str = "general") -> str:
    """
    Create a single-item deep dive (Markdown). Prompt varies by source.
    """
    lang = _norm_language(language)
    prof = _norm_profile(profile)
    report_key = (os.getenv("REPORT_KEY") or "").strip().lower()
    is_cyberlurch = report_key == "cyberlurch"

    src = (item.get("source") or "youtube").strip().lower()

    if src == "pubmed":
        if is_cyberlurch:
            sys_prompt = _SYS_DETAIL_PUBMED_CYBERLURCH_EN
        else:
            sys_prompt = _SYS_DETAIL_PUBMED_EN if lang == "en" else _SYS_DETAIL_PUBMED_DE
    else:
        if is_cyberlurch:
            sys_prompt = _SYS_DETAIL_YOUTUBE_CYBERLURCH_EN
        else:
            sys_prompt = _SYS_DETAIL_YOUTUBE_EN if lang == "en" else _SYS_DETAIL_YOUTUBE_DE

    # Small additional guidance for medical profile even for YouTube content
    if prof == "medical" and src != "pubmed":
        if lang == "en":
            sys_prompt += "\nMedical focus: emphasize evidence, study design if mentioned, and practical implications; avoid speculation.\n"
        else:
            sys_prompt += "\nMedizinischer Fokus: betone Evidenz, Studiendesign (falls genannt) und praktische Implikationen; keine Spekulation.\n"

    text = (item.get("text") or "").strip()
    max_chars = 30000 if src == "pubmed" else 6000
    if len(text) > max_chars:
        text = text[:max_chars].rstrip()

    abstract_header_seen = False
    fulltext_excerpt = (item.get("full_text_excerpt") or "").strip()
    abstract = (item.get("abstract") or "").strip() if src == "pubmed" else ""

    if src == "pubmed":
        if not abstract:
            abstract, abstract_header_seen = extract_pubmed_abstract(text)
        if not fulltext_excerpt:
            marker = "[PMC Open Access full text]"
            marker_idx = text.find(marker)
            if marker_idx == -1:
                marker = "[Unpaywall OA full text"
                marker_idx = text.find(marker)
            if marker_idx != -1:
                fulltext_excerpt = text[marker_idx:]
                if len(fulltext_excerpt) > 20000:
                    fulltext_excerpt = fulltext_excerpt[:20000].rstrip()
        item["_deep_dive_missing_abstract"] = abstract.strip() == ""
    else:
        item["_deep_dive_missing_abstract"] = False

    published = item.get("published_at")
    if isinstance(published, datetime):
        published_str = published.replace(microsecond=0).isoformat()
    else:
        published_str = str(published) if published else ""

    if src == "pubmed":
        meta = {
            "source": src,
            "title": (item.get("title") or "").strip(),
            "journal": (item.get("journal") or item.get("channel") or "").strip(),
            "year": item.get("year"),
            "published_at": published_str,
            "pmid": (item.get("pmid") or item.get("id") or "").strip(),
            "doi": (item.get("doi") or "").strip(),
            "url": (item.get("url") or "").strip(),
            "abstract": abstract,
        }
        if abstract_header_seen:
            meta["abstract_note"] = "Abstract extracted after initial header lines."
        if fulltext_excerpt:
            meta["full_text_excerpt"] = fulltext_excerpt
        if item.get("fulltext_source"):
            meta["fulltext_source"] = (item.get("fulltext_source") or "").strip()
        if item.get("fulltext_license"):
            meta["fulltext_license"] = (item.get("fulltext_license") or "").strip()
        if item.get("fulltext_host_type"):
            meta["fulltext_host_type"] = (item.get("fulltext_host_type") or "").strip()
    else:
        meta = {
            "source": src,
            "channel": (item.get("channel") or "").strip(),
            "title": (item.get("title") or "").strip(),
            "url": (item.get("url") or "").strip(),
            "published_at": published_str,
            "journal": (item.get("journal") or "").strip(),
            "year": item.get("year"),
            "pmid": (item.get("pmid") or item.get("id") or "").strip(),
            "doi": (item.get("doi") or "").strip(),
            "text": text,
        }
    payload = json.dumps(meta, ensure_ascii=False, indent=2)

    user_prompt = (
        "Item (JSON):\n"
        f"{payload}\n\n"
    )
    if src == "pubmed":
        user_prompt += (
            "Please produce the requested deep dive. Use the structured fields above; the abstract/full text is provided separately from any header text. "
            "If the text contains clues for study type, population, endpoints, results, or limitations, place them in the corresponding fields instead of writing 'Not reported'/'nicht berichtet'. "
            "If a full_text_excerpt is present, treat it as the primary evidence source (abstract is secondary), note that it may be truncated, and prioritize Methods/Results with concrete numbers.\n"
        )
    else:
        user_prompt += "Now write the requested deep dive.\n"

    not_reported_fields = 0
    json_failed = False
    used_markdown_fallback = False
    sparse_after_json = False

    try:
        client = _get_client()
        models = _pubmed_deep_dive_models()
        model_to_use = models.primary if src == "pubmed" else OPENAI_MODEL

        if src == "pubmed":
            fallback_bl = (item.get("bottom_line") or "").strip()
            try:
                json_messages = [
                    {"role": "system", "content": _pubmed_json_system_prompt(lang)},
                    {"role": "user", "content": user_prompt + "Return ONLY the JSON object described."},
                ]
                r_json = client.chat.completions.create(
                    model=model_to_use,
                    messages=json_messages,
                    temperature=0.15,
                    response_format={"type": "json_object"},
                )
                raw_json = (r_json.choices[0].message.content or "").strip()
                parsed = _parse_pubmed_json_output(raw_json)
                if parsed is None:
                    raise ValueError("json parsing failed")
                _validate_pubmed_json_output(parsed)
                content, not_reported_fields = _render_pubmed_deep_dive_from_json(
                    parsed, lang=lang, fallback_bottom_line=fallback_bl
                )
            except Exception as json_err:
                json_failed = True
                print(f"[summarize] WARN: pubmed_deepdive_json_failed pmid={meta.get('pmid', '')} err={json_err}")
                content = ""

            sparse_after_json = _is_sparse_pubmed_deep_dive(not_reported_fields) if content else False

            if json_failed or sparse_after_json:
                reason = "json_failed" if json_failed else "json_sparse"
                try:
                    fb_model = models.fallback or model_to_use
                    fallback_content, fb_missing = _run_pubmed_markdown_deep_dive(
                        client,
                        model=fb_model,
                        sys_prompt=sys_prompt,
                        user_prompt=user_prompt,
                        lang=lang,
                        fallback_bottom_line=fallback_bl,
                    )
                    if fallback_content:
                        content = fallback_content
                        not_reported_fields = fb_missing
                        used_markdown_fallback = True
                    print(
                        f"[summarize] INFO: pubmed_deepdive_markdown_fallback pmid={meta.get('pmid', '')} reason={reason}"
                    )
                except Exception as fb_err:
                    print(
                        f"[summarize] WARN: pubmed_deepdive_markdown_fallback_failed pmid={meta.get('pmid', '')} err={fb_err}"
                    )

            item["_deep_dive_retried"] = json_failed or sparse_after_json or used_markdown_fallback
            item["_deep_dive_json_failed"] = json_failed
            item["_deep_dive_used_markdown_fallback"] = used_markdown_fallback
            item["_deep_dive_sparse_after_json"] = sparse_after_json
        else:
            messages = [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ]
            r = client.chat.completions.create(
                model=model_to_use,
                messages=messages,
                temperature=0.2,
            )
            content = (r.choices[0].message.content or "").strip()
            item["_deep_dive_retried"] = False

        item["_deep_dive_empty_output"] = not bool(content.strip())
        item["_deep_dive_parse_fallback"] = used_markdown_fallback or json_failed or sparse_after_json
        item["_deep_dive_not_reported_fields"] = not_reported_fields
        item["_deep_dive_all_fields_placeholder"] = not_reported_fields >= 7
        return content
    except Exception as e:
        if lang == "en":
            return f"**Error:** Failed to create deep dive: `{e!r}`\n"
        return f"**Fehler:** Konnte Vertiefung nicht erzeugen: `{e!r}`\n"


def summarize_pubmed_bottom_line(item: Dict[str, Any], *, language: str = "en") -> str:
    """
    Produce a short (1–2 sentences) bottom-line summary for a PubMed item.

    This deliberately uses a lightweight prompt and low max tokens to conserve API usage.
    """

    lang = _norm_language(language)
    text = (item.get("text") or "").strip()
    if len(text) > 2000:
        text = text[:2000].rstrip()

    published = item.get("published_at")
    if isinstance(published, datetime):
        published_str = published.replace(microsecond=0).isoformat()
    else:
        published_str = str(published) if published else ""

    meta = {
        "title": (item.get("title") or "").strip(),
        "journal": (item.get("journal") or "").strip(),
        "year": item.get("year"),
        "pmid": (item.get("pmid") or item.get("id") or "").strip(),
        "published_at": published_str,
        "text": text,
    }

    sys_prompt = (
        "You are a concise clinical summarizer. Write strictly in English.\n"
        "You will receive one PubMed abstract (title + journal + PMID/DOI + date + abstract text).\n"
        "Return only a 1–2 sentence BOTTOM LINE capturing what is new, how strong the evidence is (if stated), and why it matters clinically.\n"
        "Do not add section headers or bullets."
    )
    if lang != "en":
        sys_prompt = (
            "You are a concise clinical summarizer. Write strictly in German.\n"
            "You will receive one PubMed abstract (title + journal + PMID/DOI + date + abstract text).\n"
            "Return only a 1–2 sentence BOTTOM LINE capturing what is new, how strong the evidence is (if stated), and why it matters clinically.\n"
            "Keine Überschriften oder Aufzählungen."
        )

    payload = json.dumps(meta, ensure_ascii=False, indent=2)
    user_prompt = (
        "Item (JSON):\n"
        f"{payload}\n\n"
        "Now write the requested bottom line."
    )

    try:
        client = _get_client()
        r = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=90,
        )
        return (r.choices[0].message.content or "").strip()
    except Exception as e:
        fallback = "(Failed to generate bottom line)" if lang == "en" else "(Konnte Bottom Line nicht erzeugen)"
        return f"{fallback} — {e!r}"


def summarize_foamed_bottom_line(item: Dict[str, Any], *, language: str = "en") -> str:
    """
    Create a cautious, single-sentence bottom line for FOAMed/blog content.
    """

    lang = _norm_language(language)
    text = (item.get("text") or "").strip()
    if len(text) > 1500:
        text = text[:1500].rstrip()

    published = item.get("published_at")
    if isinstance(published, datetime):
        published_str = published.replace(microsecond=0).isoformat()
    else:
        published_str = str(published) if published else ""

    meta = {
        "title": (item.get("title") or "").strip(),
        "source": (item.get("foamed_source") or item.get("channel") or "").strip(),
        "url": (item.get("url") or "").strip(),
        "published_at": published_str,
        "text": text,
    }

    sys_prompt = (
        "You are a concise clinical summarizer. Write one sentence, 40–55 words, starting with 'BOTTOM LINE:'. "
        "Base your statement strictly on the provided title/excerpt. If the content is commentary or education without new data, state that plainly. "
        "Avoid speculation and do not invent study results."
    )
    if lang != "en":
        sys_prompt = (
            "Du bist ein prägnanter klinischer Zusammenfasser. Schreibe einen Satz (40–55 Wörter), beginnend mit 'BOTTOM LINE:'. "
            "Nutze nur den bereitgestellten Titel/Excerpt. Wenn es sich um Kommentar oder Lehrinhalt ohne neue Daten handelt, formuliere das klar. "
            "Keine Spekulation, keine erfundenen Studienergebnisse."
        )

    payload = json.dumps(meta, ensure_ascii=False, indent=2)
    user_prompt = (
        "Item (JSON):\n"
        f"{payload}\n\n"
        "Now write the requested bottom line."
    )

    try:
        client = _get_client()
        r = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=120,
        )
        return (r.choices[0].message.content or "").strip()
    except Exception as e:
        fallback = "BOTTOM LINE: Unable to summarize this FOAMed item reliably." if lang == "en" else "BOTTOM LINE: Zusammenfassung für diesen FOAMed-Beitrag nicht möglich."
        return f"{fallback} ({e!r})"
