# src/newsagent2/summarizer.py
from __future__ import annotations

import json
import os
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

_SYS_DETAIL_PUBMED_DE = (
    "You are a careful clinical summarizer. Write strictly in German.\n"
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract text when available).\n"
    "Use ONLY the provided text; do not add outside facts. If something is not stated, write 'nicht berichtet'.\n\n"
    "Return Markdown with this structure:\n"
    "**BOTTOM LINE:** 1–2 Sätze: Was ist neu, wie belastbar, und mögliche Relevanz für Praxis.\n\n"
    "**Studientyp:** (RCT / Kohorte / Fall-Kontrolle / Querschnitt / Systematic Review / Guideline / Sonstiges / nicht berichtet)\n"
    "**Population/Setting:** (kurz)\n"
    "**Intervention/Exposure & Vergleich:** (wenn vorhanden)\n"
    "**Primäre Endpunkte:** (wenn vorhanden)\n"
    "**Wichtigste Ergebnisse:** (konkret, Zahlen wenn vorhanden)\n"
    "**Limitationen:** 1–3 Punkte\n"
    "**Warum das wichtig ist:** 1 kurzer Absatz.\n"
)

_SYS_DETAIL_PUBMED_EN = (
    "You are a careful clinical summarizer. Write strictly in English.\n"
    "You will receive one PubMed item (title + journal + PMID/DOI + date + abstract text when available).\n"
    "Use ONLY the provided text; do not add outside facts. If something is not stated, write 'not reported'.\n\n"
    "Return Markdown with this structure:\n"
    "**BOTTOM LINE:** 1–2 sentences: what is new, how strong the evidence is, and the possible practice impact.\n\n"
    "**Study type:** (RCT / cohort / case-control / cross-sectional / systematic review / guideline / other / not reported)\n"
    "**Population/setting:** (short)\n"
    "**Intervention/exposure & comparator:** (if stated)\n"
    "**Primary endpoints:** (if stated)\n"
    "**Key results:** (concrete; include numbers if present)\n"
    "**Limitations:** 1–3 bullets\n"
    "**Why this matters:** 1 short paragraph.\n"
)


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

    src = (item.get("source") or "youtube").strip().lower()

    if src == "pubmed":
        sys_prompt = _SYS_DETAIL_PUBMED_EN if lang == "en" else _SYS_DETAIL_PUBMED_DE
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
        return (r.choices[0].message.content or "").strip()
    except Exception as e:
        if lang == "en":
            return f"**Error:** Failed to create deep dive: `{e!r}`\n"
        return f"**Fehler:** Konnte Vertiefung nicht erzeugen: `{e!r}`\n"
