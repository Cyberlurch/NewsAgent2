from __future__ import annotations

import os
from typing import List, Dict

from openai import OpenAI

# Standardmodell: kann über OPENAI_MODEL überschrieben werden
MODEL_NAME = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")


SYS_PROMPT_OVERVIEW = (
    "You are a rigorous news editor. The input is a set of short item digests "
    "(title, channel, time) plus snippets from transcripts or descriptions. "
    "Your task is to write ONE concise, cross-channel daily summary.\n"
    "\n"
    "Output structure (Markdown):\n"
    "1) A heading '## Kurzüberblick'.\n"
    "2) Under this heading, 3–7 thematic subsections with '### ' headings, e.g. "
    "'### Russland–Ukraine–Europa', '### USA/NATO', '### Israel und Nahost', etc.\n"
    "3) Under each subsection, use a mix of short paragraphs and bullet points.\n"
    "\n"
    "Content rules:\n"
    "- Include ONLY newsworthy content from the last 24 hours; drop ads, promotions, "
    "merch, coupon codes, self-promotion, greetings, platform meta-talk.\n"
    "- Merge duplicates across channels: if several videos report on the same story, "
    "describe the story once and mention the main channels in parentheses, e.g. "
    "(preppernewsflash, klartextwinkler).\n"
    "- Focus on what is NEW or escalated compared to the usual background situation. "
    "Give only as much context as needed to understand the update.\n"
    "- Keep substance and key reasoning (what happens, why, and what implications are "
    "discussed), but avoid fluff and long digressions.\n"
    "- Aim for roughly 600–900 words total so that the text can be read in about "
    "5 minutes.\n"
    "- Preserve the language of the underlying content: If the evidence is mostly in "
    "German, write German; if mostly English, write English; if mixed, pick the "
    "majority language. Do NOT translate on purpose.\n"
    "- At the very end, add a short section '### Kurz notiert', where you mention "
    "1–5 small but notable points that did not fit into the main themes.\n"
)


SYS_PROMPT_DETAIL = (
    "You are a precise analyst. You receive metadata and text (transcript or "
    "description) of ONE news-related YouTube video.\n"
    "\n"
    "Your task: Write an in-depth but focused summary of THIS ONE video.\n"
    "\n"
    "Output structure (Markdown):\n"
    "- Do NOT repeat the channel name or video title; they will be shown outside.\n"
    "- Start with a bold heading line '**Kernaussagen:**' and then 3–6 bullet points "
    "summarising the main claims and facts.\n"
    "- After that, add a bold heading '**Details & Argumentation:**' and write "
    "2–4 paragraphs that explain:\n"
    "  * what is being claimed,\n"
    "  * what evidence, examples or numbers are mentioned,\n"
    "  * which risks, scenarios or implications are discussed.\n"
    "\n"
    "Content rules:\n"
    "- Focus STRICTLY on the content of the video: facts, claims, argumentation, "
    "risk scenarios. Ignore ads, promotions, merch, coupon codes, channel "
    "self-promotion and technical meta-talk.\n"
    "- Avoid speculation that goes beyond what is actually said in the video.\n"
    "- Total length: roughly 300–600 words so that several such sections together "
    "can still be read within about an hour.\n"
    "- Preserve the language of the underlying content (German/English/Swedish "
    "as in the text). Do NOT translate.\n"
)


def _get_client() -> OpenAI:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY ist nicht gesetzt. "
            "Bitte hinterlege den Schlüssel in deiner .env Datei "
            "oder als GitHub Actions Secret."
        )
    return OpenAI(api_key=api_key)


def summarize(items: List[Dict]) -> str:
    """
    Erzeugt eine einzige, kanalübergreifende Tageszusammenfassung
    als 'Kurzüberblick' mit Themenblöcken.
    """
    if not items:
        return "Keine neuen Inhalte in den letzten 24 Stunden."

    lines: List[str] = []
    for it in items:
        head = f"- {it['title']} — {it['channel']} ({it['url']})"
        raw = (it.get("text") or it.get("description") or "").strip()
        # Mehr Kontext: bis zu ca. 2500 Zeichen pro Item
        snippet = raw[:2500]
        if snippet:
            lines.append(head + "\n  " + snippet)
        else:
            lines.append(head)

    digest = "\n".join(lines)

    messages = [
        {"role": "system", "content": SYS_PROMPT_OVERVIEW},
        {
            "role": "user",
            "content": "Items (titles, channels, links, and snippets):\n" + digest,
        },
    ]

    client = _get_client()

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            max_completion_tokens=900,
        )
    except Exception as e:
        return f"[Fehler beim Aufruf der OpenAI-API (Overview): {e!r}]"

    try:
        content = completion.choices[0].message.content
        return content.strip() if content else ""
    except Exception as e:
        return (
            f"[Fehler beim Auslesen der Overview-Zusammenfassung: {e!r}]\n"
            f"Roh-Response:\n{completion}"
        )


def summarize_item_detail(item: Dict) -> str:
    """
    Erzeugt eine ausführlichere Zusammenfassung für EIN bestimmtes Video.
    """
    raw = (item.get("text") or item.get("description") or "").strip()
    if not raw:
        return "[Keine detaillierte Zusammenfassung möglich – weder Transkript noch Beschreibung verfügbar.]"

    # Ausreichend Kontext für eine tiefere Zusammenfassung
    snippet = raw[:4000]

    meta = (
        f"Title: {item['title']}\n"
        f"Channel: {item['channel']}\n"
        f"URL: {item['url']}\n"
        f"Published: {item.get('published_at')}\n"
    )

    messages = [
        {"role": "system", "content": SYS_PROMPT_DETAIL},
        {
            "role": "user",
            "content": meta + "\n\nVideo text (transcript/description snippet):\n" + snippet,
        },
    ]

    client = _get_client()

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            max_completion_tokens=800,
        )
    except Exception as e:
        return f"[Fehler beim Aufruf der OpenAI-API (Detail): {e!r}]"

    try:
        content = completion.choices[0].message.content
        return content.strip() if content else ""
    except Exception as e:
        return (
            f"[Fehler beim Auslesen der Detail-Zusammenfassung: {e!r}]\n"
            f"Roh-Response:\n{completion}"
        )
