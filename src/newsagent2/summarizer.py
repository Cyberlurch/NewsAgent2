# src/newsagent2/summarizer.py
from __future__ import annotations

import os
import time
import json
from typing import List, Dict

import requests

GROQ_API = "https://api.groq.com/openai/v1/chat/completions"

SYS_PROMPT = (
    "You are a rigorous news editor. Input is a set of short item digests "
    "(title, channel, time) plus snippets from transcripts. "
    "Task: Write ONE concise, cross-channel summary of the last 24 hours.\n"
    "Rules:\n"
    "- Include ONLY newsworthy content; drop ads, promotions, merch, coupon codes,\n"
    "  self-promotion, metadata, greetings.\n"
    "- Merge duplicates across channels; mention sources inline like (Channel1, Channel2).\n"
    "- Keep substance and key reasoning (what/why/implications), but avoid fluff.\n"
    "- Use short paragraphs + bullet points per theme. Max ~450–700 words total.\n"
    "- Preserve the language of the underlying content: If the evidence is mostly in German, "
    "write German; if mostly English, write English; if mixed, pick the majority language.\n"
    "- At the end add a short Sources list with video titles and links."
)

# Mapping alter Groq-Modellnamen auf aktuelle Bezeichnungen
MODEL_ALIASES: Dict[str, str] = {
    "llama3-70b-8192": "llama-3.3-70b-versatile",
    "llama3-8b-8192": "llama-3.1-8b-instant",
}


def _resolve_model(env_model: str | None) -> str:
    """
    Übersetzt alte GROQ_MODEL-Namen in neue und liefert einen sinnvollen Default.
    """
    model = (env_model or "").strip()
    if not model:
        return "llama-3.3-70b-versatile"
    return MODEL_ALIASES.get(model, model)


def _post_groq(
    messages: List[Dict],
    model: str | None = None,
    temperature: float = 0.2,
) -> str:
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")

    pause = float(os.getenv("GROQ_PAUSE_SEC", "8"))

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resolved_model = _resolve_model(model)
    payload = {
        "model": resolved_model,
        "messages": messages,
        "temperature": temperature,
    }

    # Einfache Retry-Logik mit Exponential-Backoff
    for attempt in range(6):
        r = requests.post(
            GROQ_API,
            headers=headers,
            data=json.dumps(payload),
            timeout=120,
        )

        if r.status_code == 200:
            data = r.json()
            return data["choices"][0]["message"]["content"].strip()

        if r.status_code in (429, 500, 502, 503, 504):
            # Ratelimit/Serverfehler: warten und erneut
            time.sleep(pause * (1.5 ** attempt))
            continue

        if r.status_code == 413:
            # Zu viele Tokens – wird außerhalb behandelt
            raise RuntimeError("413")

        raise RuntimeError(f"Groq error {r.status_code}: {r.text[:200]}")

    raise RuntimeError("ratelimit")


def summarize(items: List[Dict]) -> str:
    """
    items: [{title, channel, url, published_at, text?, description?}]
    Liefert eine einzige, kanalübergreifende Zusammenfassung.
    """
    if not items:
        return "Keine neuen Inhalte in den letzten 24 Stunden."

    # Kompaktes Digest pro Item bauen
    lines: List[str] = []
    for it in items:
        head = f"- {it['title']} — {it['channel']} ({it['url']})"
        snippet = (it.get("text") or it.get("description") or "")[:1500]
        if snippet:
            lines.append(head + "\n  " + snippet)
        else:
            lines.append(head)

    digest = "\n".join(lines)

    sys_msg = {"role": "system", "content": SYS_PROMPT}
    user_msg = {"role": "user", "content": f"Items:\n{digest}"}

    env_model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    model = _resolve_model(env_model)

    try:
        return _post_groq([sys_msg, user_msg], model=model)
    except RuntimeError as e:
        if str(e) == "413":
            # Zu groß – in zwei Hälften splitten und danach mergen
            mid = len(items) // 2 or 1
            a = summarize(items[:mid])
            b = summarize(items[mid:])

            merge_user = {
                "role": "user",
                "content": (
                    "Merge the two partial summaries into ONE de-duplicated summary:\n\n"
                    f"A)\n{a}\n\nB)\n{b}"
                ),
            }
            return _post_groq(
                [{"role": "system", "content": SYS_PROMPT}, merge_user],
                model=model,
            )
        raise
