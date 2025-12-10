from __future__ import annotations

import os
from typing import List, Dict

from openai import OpenAI

# Standardmodell: kann über OPENAI_MODEL überschrieben werden
MODEL_NAME = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")

SYS_PROMPT = (
    "You are a rigorous news editor. Input is a set of short item digests "
    "(title, channel, time) plus snippets from transcripts or descriptions. "
    "Task: Write ONE concise, cross-channel summary of the last 24 hours.\n"
    "Rules:\n"
    "- Include ONLY newsworthy content; drop ads, promotions, merch, coupon codes,\n"
    "  self-promotion, metadata, greetings.\n"
    "- Merge duplicates across channels; mention sources inline like (Channel1, Channel2).\n"
    "- Keep substance and key reasoning (what/why/implications), but avoid fluff.\n"
    "- Use short paragraphs + bullet points per theme. Aim for roughly 600–900 words total,\n"
    "  so that important details are retained but the text stays readable.\n"
    "- Preserve the language of the underlying content: If the evidence is mostly in German, "
    "write German; if mostly English, write English; if mixed, pick the majority language.\n"
    "- At the end add a short Sources list with video titles and links."
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
    items: Liste von Dicts mit mindestens:
      - title
      - channel
      - url
      - published_at
      - text (optional, Transkript)
      - description (optional, Videobeschreibung)
    Liefert eine einzige, kanalübergreifende Zusammenfassung.
    """
    if not items:
        return "Keine neuen Inhalte in den letzten 24 Stunden."

    # Digest bauen: pro Item ein Kopf + Textauszug
    lines: List[str] = []
    for it in items:
        head = f"- {it['title']} — {it['channel']} ({it['url']})"
        raw = (it.get("text") or it.get("description") or "").strip()
        # Mehr Kontext als zuvor, aber trotzdem begrenzt, um das Kontextlimit nicht zu sprengen
        snippet = raw[:2500]
        if snippet:
            lines.append(head + "\n  " + snippet)
        else:
            lines.append(head)

    digest = "\n".join(lines)

    messages = [
        {"role": "system", "content": SYS_PROMPT},
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
        return f"[Fehler beim Aufruf der OpenAI-API: {e!r}]"

    try:
        content = completion.choices[0].message.content
        return content.strip() if content else ""
    except Exception as e:
        return (
            f"[Fehler beim Auslesen der Zusammenfassung: {e!r}]\n"
            f"Roh-Response:\n{completion}"
        )
