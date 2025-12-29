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
    "Use ONLY the provided text; do not add outside facts. If something is not stated, write 'nicht berichtet'.\n\n"
    "Return Markdown with EXACTLY these headings (with colons) and bullets under 'Limitations':\n\n"
    "BOTTOM LINE: 1–2 Sätze: Was ist neu, wie belastbar, und mögliche Relevanz für Praxis.\n\n"
    "Study type: (RCT / Kohorte / Fall-Kontrolle / Querschnitt / Systematic Review / Guideline / Sonstiges / nicht berichtet)\n"
    "Population/setting: (kurz)\n"
    "Intervention/exposure & comparator: (wenn vorhanden)\n"
    "Primary endpoints: (wenn vorhanden)\n"
    "Key results: (konkret, Zahlen wenn vorhanden)\n"
    "Limitations:\n"
    "- 1–3 Punkte\n"
    "Why this matters: 1 kurzer Absatz.\n"
)

_SYS_DETAIL_PUBMED_EN = (
    "You are a careful clinical summarizer. Write strictly in English.\n"
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract text when available).\n"
    "Use ONLY the provided text; do not add outside facts. If something is not stated, write 'not reported'.\n\n"
    "Return Markdown with EXACTLY these headings (with colons) and bullets under 'Limitations':\n\n"
    "BOTTOM LINE: 1–2 sentences: what is new, how strong the evidence is, and the possible practice impact.\n\n"
    "Study type: (RCT / cohort / case-control / cross-sectional / systematic review / guideline / other / not reported)\n"
    "Population/setting: (short)\n"
    "Intervention/exposure & comparator: (if stated)\n"
    "Primary endpoints: (if stated)\n"
    "Key results: (concrete; include numbers if present)\n"
    "Limitations:\n"
    "- 1–3 bullets\n"
    "Why this matters: 1 short paragraph.\n"
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
    "Return Markdown with EXACTLY these headings (with colons) and bullets under 'Limitations':\n\n"
    "BOTTOM LINE: 1–2 sentences: what is new, how strong the evidence is, and the possible practice impact.\n\n"
    "Study type: (RCT / cohort / case-control / cross-sectional / systematic review / guideline / other / not reported)\n"
    "Population/setting: (short)\n"
    "Intervention/exposure & comparator: (if stated)\n"
    "Primary endpoints: (if stated)\n"
    "Key results: (concrete; include numbers if present)\n"
    "Limitations:\n"
    "- 1–3 bullets\n"
    "Why this matters: 1 short paragraph.\n"
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


def _ensure_pubmed_deep_dive_template(md: str, *, lang: str, fallback_bottom_line: str = "") -> str:
    """
    Enforce the PubMed deep-dive template and fill missing fields with placeholders.
    """

    placeholder = "not reported" if lang == "en" else "nicht berichtet"

    def _normalize_headings(text: str) -> str:
        normalized = text
        for heading in _PUBMED_REQUIRED_HEADINGS:
            base = heading.rstrip(":")
            patterns = [
                rf"\*\*{re.escape(base)}:\*\*",
                rf"\*\*{re.escape(base)}\*\*",
                rf"{re.escape(base)}\s*:",
            ]
            for pat in patterns:
                normalized = re.sub(pat, heading, normalized, flags=re.IGNORECASE)
        return normalized

    def _extract_bottom_line(text: str) -> str:
        for line in text.splitlines():
            if line.strip().lower().startswith("bottom line:"):
                return line.split(":", 1)[-1].strip()
        return ""

    def _ensure_limitations_bullets(text: str) -> str:
        lower = text.lower()
        idx = lower.find("limitations:")
        if idx == -1:
            return text

        start = idx + len("limitations:")
        next_heading_idx = lower.find("why this matters:", start)
        end = next_heading_idx if next_heading_idx != -1 else len(text)

        before = text[:start]
        section = text[start:end].lstrip("\n")
        after = text[end:]

        lines = [ln for ln in section.splitlines() if ln.strip()]
        if not lines:
            bullets = [f"- {placeholder}"]
        elif any(re.match(r"\\s*[-*•]", ln) for ln in lines):
            bullets = lines
        else:
            bullets = [f"- {ln.strip()}" for ln in lines]
            if not bullets:
                bullets = [f"- {placeholder}"]

        fixed_section = "\n".join(bullets)
        if not fixed_section.endswith("\n"):
            fixed_section += "\n"
        return before + "\n" + fixed_section + after

    md_normalized = _normalize_headings(md)
    lower_md = md_normalized.lower()
    missing = [h for h in _PUBMED_REQUIRED_HEADINGS if h.lower() not in lower_md]

    if missing:
        bottom_line = _extract_bottom_line(md_normalized) or fallback_bottom_line or placeholder
        rebuilt = (
            f"BOTTOM LINE: {bottom_line}\n\n"
            f"Study type: {placeholder}\n"
            f"Population/setting: {placeholder}\n"
            f"Intervention/exposure & comparator: {placeholder}\n"
            f"Primary endpoints: {placeholder}\n"
            f"Key results: {placeholder}\n"
            f"Limitations:\n"
            f"- {placeholder}\n"
            f"Why this matters: {placeholder}\n"
        )
        return rebuilt.strip()

    md_fixed = _ensure_limitations_bullets(md_normalized)
    return md_fixed.strip()


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

    published = item.get("published_at")
    if isinstance(published, datetime):
        published_str = published.replace(microsecond=0).isoformat()
    else:
        published_str = str(published) if published else ""

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
        "Now write the requested deep dive."
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
        content = (r.choices[0].message.content or "").strip()
        if src == "pubmed":
            fallback_bl = (item.get("bottom_line") or "").strip()
            content = _ensure_pubmed_deep_dive_template(content, lang=lang, fallback_bottom_line=fallback_bl)
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
