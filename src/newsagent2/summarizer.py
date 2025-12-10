from __future__ import annotations

import os
from typing import List, Dict

from openai import OpenAI

# Standardmodell: kann über OPENAI_MODEL überschrieben werden
MODEL_NAME = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")


SYS_PROMPT_OVERVIEW = (
    "You are a rigorous news editor. Input is a set of short item digests "
    "(title, channel, time) plus snippets from transcripts or descriptions. "
    "Task: Write ONE concise, cross-channel summary of the last 24 hours.\n"
    "Rules:\n"
    "- Include ONLY newsworthy content; drop ads, promotions, merch, coupon "
    "codes, self-promotion, metadata, greetings.\n"
    "- Merge duplicates across channels; mention sources inline like "
    "(Channel1, Channel2) when multiple videos cover the same story.\n"
    "- Keep substance and key reasoning (what/why/implications), but avoid fluff.\n"
    "- Use short paragraphs and/or bullet points per theme.\n"
    "- Aim for roughly 600–900 words total so that it can be read in about "
    "5 minutes.\n"
    "- Preserve the language of the underlying content: If the evidence is "
    "mostly in German, write German; if mostly English, write English; if "
    "mixed, pick the majority language.\n"
    "- At the end add a short Sources section with brief references to the "
    "main stories (no full URLs needed, just titles and channels)."
)


SYS_PROMPT_DETAIL = (
    "You are a precise analyst. You receive metadata and text (transcript "
    "or description) of ONE news-related YouTube video.\n"
    "Task: Write an in-depth but focused summary of THIS ONE video.\n"
    "Rules:\n"
    "- Focus STRICTLY on facts, claims, and arguments presented in the video.\n"
    "- Ignore ads, promotions, merch, coupon codes, channel self-promotion, "
    "and platform meta-talk.\n"
    "- Explain what is being claimed, what evidence is mentioned, and what "
    "implications are discussed.\n"
    "- Use 2–4 paragraphs, together about 300–600 words (roughly 5–10 minutes "
    "reading time if several such sections are read).\n"
    "- Preserve the language of the underlying content (German/English/"
    "Swedish as in the text). Do NOT translate.\n"
    "- Do NOT repeat the full channel name or video title in the body; this "
    "will be shown as a heading outside your answer.\n"
)


def _get_client() -> OpenAI:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY ist nicht gesetzt. "
            "Bitte hinterlege den Schlüssel in deiner .env Datei "
            "oder als GitHub Actions Secret."
        )
    # Der Client ist leichtgewichtig; für Einfachheit erzeugen wir ihn pro Aufruf neu.
    # Für starke Optimierung könnte man ihn global cachen.
    return OpenAI(api_key=api_key)


def summarize(items: List[Dict]) -> str:
    """
    Erzeugt eine einzige, kanalübergreifende Tageszusammenfassung.

    items: Liste von Dicts mit mindestens:
      - title
      - channel
      - url
      - published_at
      - text (optional, Transkript)
      - description (optional, Videobeschreibung)
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

    item: Dict mit mindestens:
      - title
      - channel
      - url
      - published_at
      - text (optional, Transkript)
      - description (optional)
    """
    raw = (item.get("text") or item.get("description") or "").strip()
    if not raw:
        return "[Keine detaillierte Zusammenfassung möglich – weder Transkript noch Beschreibung verfügbar.]"

    # Genug Kontext für eine tiefere Zusammenfassung, ohne das Kontextlimit zu sprengen
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
