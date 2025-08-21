from __future__ import annotations
import os, time, json
from typing import List, Dict
import requests

GROQ_API = "https://api.groq.com/openai/v1/chat/completions"

SYS_PROMPT = (
    "You are a rigorous news editor. Input is a set of short item digests "
    "(title, channel, time) plus snippets from transcripts. "
    "Task: Write ONE concise, cross-channel summary of the last 24 hours. "
    "Rules:\n"
    "- Include ONLY newsworthy content; drop ads, promotions, merch, coupon codes,\n"
    "  self-promotion, metadata, greetings.\n"
    "- Merge duplicates across channels; mention sources inline like (Channel1, Channel2).\n"
    "- Keep substance and key reasoning (what/why/implications), but avoid fluff.\n"
    "- Use short paragraphs + bullet points per theme. Max ~450–700 words total.\n"
    "- Preserve the language of the underlying content: If the evidence is mostly in German, write German; "
    "  if mostly English, write English; if mixed, pick the majority language.\n"
    "- At the end add a short Sources list with video titles and links."
)

def _post_groq(messages: List[Dict], model: str, temperature: float=0.2) -> str:
    api_key = os.getenv("GROQ_API_KEY", "")
    pause = float(os.getenv("GROQ_PAUSE_SEC", "8"))
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model or "llama-3.3-70b-versatile", "messages": messages, "temperature": temperature}
    for attempt in range(6):
        r = requests.post(GROQ_API, headers=headers, data=json.dumps(payload), timeout=120)
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"].strip()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(pause * (1.5 ** attempt))
            continue
        if r.status_code == 413:
            raise RuntimeError("413")
        raise RuntimeError(f"Groq error {r.status_code}: {r.text[:200]}")
    raise RuntimeError("ratelimit")

def summarize(items: List[Dict]) -> str:
    """items: [{title, channel, url, published_at, text?}]"""
    if not items:
        return "Keine neuen Inhalte in den letzten 24 Stunden."
    # Baue kompaktes Digest pro Item
    lines = []
    for it in items:
        head = f"- {it['title']} — {it['channel']} ({it['url']})"
        snippet = (it.get("text") or it.get("description") or "")[:1500]
        lines.append(head + ("\n  " + snippet if snippet else ""))
    digest = "\n".join(lines)

    sys = {"role": "system", "content": SYS_PROMPT}
    user = {"role": "user", "content": f"Items:\n{digest}"}
    model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

    try:
        return _post_groq([sys, user], model=model)
    except RuntimeError as e:
        if str(e) == "413":
            # Teile in 2 Hälften
            mid = len(items) // 2 or 1
            a = summarize(items[:mid])
            b = summarize(items[mid:])
            # Merge
            merge_user = {
                "role": "user",
                "content": f"Merge the two partial summaries into ONE de-duplicated summary:\n\nA)\n{a}\n\nB)\n{b}"
            }
            return _post_groq([{"role": "system", "content": SYS_PROMPT}, merge_user], model=model)
        raise

