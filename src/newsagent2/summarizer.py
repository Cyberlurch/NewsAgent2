# src/newsagent2/summarizer.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from openai import OpenAI

# Default model used for all summaries (override via OPENAI_MODEL env var)
OPENAI_MODEL = (os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()


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
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract text when available).\n"
    "Use ONLY the provided text; do not add outside facts. If something is not stated, write 'nicht berichtet'.\n"
    "If the abstract contains enough information to infer the study type/population/endpoints/results, do so; only use 'nicht berichtet' when there is truly no hint in the provided text.\n\n"
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
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract text when available).\n"
    "Use ONLY the provided text; do not add outside facts. If something is not stated, write 'not reported'.\n"
    "If the abstract contains enough information to infer the study type/population/endpoints/results, do so; only use 'Not reported' when there is truly no hint in the provided text.\n\n"
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
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract text when available).\n"
    "Use ONLY the provided text; do not add outside facts. If something is not stated, write 'not reported'.\n\n"
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
    "If the abstract contains enough information to infer the study type/population/endpoints/results, do so; only use 'Not reported' when there is truly no hint in the provided text.\n"
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

    def _clean_value(val: str) -> str:
        cleaned = (val or "").strip()
        if cleaned.lower() in {"not reported", "nicht berichtet"}:
            return placeholder
        return cleaned

    def _extract_bottom_line(text: str) -> str:
        for line in text.splitlines():
            if line.strip().lower().startswith("bottom line"):
                return line.split(":", 1)[-1].strip()
        return ""

    def _extract_field(text: str, label: str) -> str:
        pat = rf"(?im)^\s*(?:[-*•]\s*)?(?:\*\*)?{re.escape(label)}(?:\*\*)?\s*:?\s*(.+)$"
        m = re.search(pat, text)
        if m:
            return _clean_value(m.group(1))
        return ""

    def _extract_limitations(text: str) -> List[str]:
        pat = re.compile(r"(?im)^\s*(?:[-*•]\s*)?(?:\*\*)?limitations(?:\*\*)?\s*:?\s*$")
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

    bottom_line = _clean_value(_extract_bottom_line(md) or fallback_bottom_line or placeholder)
    study_type = _clean_value(_extract_field(md, "Study type"))
    population = _clean_value(_extract_field(md, "Population/setting"))
    intervention = _clean_value(_extract_field(md, "Intervention/exposure & comparator"))
    endpoints = _clean_value(_extract_field(md, "Primary endpoints"))
    key_results = _clean_value(_extract_field(md, "Key results"))
    why_matters = _clean_value(_extract_field(md, "Why this matters"))
    limitations_list = [_clean_value(x) for x in _extract_limitations(md) if _clean_value(x)]

    not_reported_fields = 0
    for val in (study_type, population, intervention, endpoints, key_results, why_matters):
        cleaned = (val or "").strip()
        if not cleaned or cleaned.lower() == lower_placeholder:
            not_reported_fields += 1

    if not limitations_list:
        limitations_list = [placeholder]
        not_reported_fields += 1
    elif all((li or "").strip().lower() == lower_placeholder for li in limitations_list):
        not_reported_fields += 1

    fields = [
        f"- **Study type:** {study_type or placeholder}",
        f"- **Population/setting:** {population or placeholder}",
        f"- **Intervention/exposure & comparator:** {intervention or placeholder}",
        f"- **Primary endpoints:** {endpoints or placeholder}",
        f"- **Key results:** {key_results or placeholder}",
        "- **Limitations:**",
    ]
    fields.extend(f"  - {li or placeholder}" for li in limitations_list)
    fields.append(f"- **Why this matters:** {why_matters or placeholder}")

    normalized = "\n".join([f"BOTTOM LINE: {bottom_line or placeholder}", ""] + fields).strip()
    return normalized, not_reported_fields


def normalize_pubmed_deep_dive(md: str, *, lang: str, fallback_bottom_line: str = "") -> str:
    normalized, _ = _normalize_pubmed_field_values(md, lang=lang, fallback_bottom_line=fallback_bottom_line)
    return normalized


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
    if len(text) > 6000:
        text = text[:6000].rstrip()

    abstract = text
    abstract_header_seen = False
    if src == "pubmed":
        abstract, abstract_header_seen = extract_pubmed_abstract(text)
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
            "Please produce the requested deep dive. Use the structured fields above; the abstract is provided separately from any header text.\n"
        )
    else:
        user_prompt += "Now write the requested deep dive.\n"

    try:
        client = _get_client()
        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ]
        r = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.2,
        )
        content = (r.choices[0].message.content or "").strip()
        retried = False

        if src == "pubmed":
            fallback_bl = (item.get("bottom_line") or "").strip()
            normalized, not_reported_fields = _normalize_pubmed_field_values(
                content, lang=lang, fallback_bottom_line=fallback_bl
            )
            if not_reported_fields >= 4 and len(abstract) >= 400:
                retry_prompt = (
                    user_prompt
                    + "\nRETRY: The abstract is detailed. Extract or infer study type, population/setting, endpoints, results, limitations, and why this matters. "
                    "Only use 'Not reported' or 'nicht berichtet' when there is truly no information in the abstract.\n"
                )
                r_retry = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": retry_prompt},
                    ],
                    temperature=0.15,
                )
                content = (r_retry.choices[0].message.content or "").strip() or content
                retried = True
                normalized, _ = _normalize_pubmed_field_values(
                    content, lang=lang, fallback_bottom_line=fallback_bl
                )
            content = normalized
            item["_deep_dive_retried"] = retried
        else:
            item["_deep_dive_retried"] = False

        item["_deep_dive_empty_output"] = not bool(content.strip())
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
