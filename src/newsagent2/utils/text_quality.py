from __future__ import annotations

import re

PROMO_KEYWORDS = [
    "patreon",
    "donate",
    "merch",
    "merchandise",
    "subscribe",
    "sponsor",
    "sponsors",
    "sponsored",
    "affiliate",
    "affiliates",
    "links below",
    "follow us",
    "join the discord",
    "support the channel",
]


def _count_urls(text: str) -> int:
    return len(re.findall(r"https?://\S+|www\.\S+", text, flags=re.IGNORECASE))


def is_low_signal_youtube_text(text: str) -> bool:
    """
    Heuristic to detect promo-only/low-signal YouTube descriptions or transcripts.

    Signals:
      - very short text (<400 chars)
      - URL-heavy relative to word count
      - multiple promo keywords with few remaining content words
    """
    normalized = (text or "").strip()
    if len(normalized) < 400:
        return True

    lowered = normalized.lower()
    url_count = _count_urls(lowered)
    words = re.findall(r"[a-zA-Z0-9']+", lowered)
    word_count = len(words)
    if word_count == 0:
        return True

    # Heavily link-dense descriptions are usually promo/link dumps.
    if url_count >= 3 and url_count * 5 >= word_count:
        return True

    promo_hits = sum(1 for kw in PROMO_KEYWORDS if kw in lowered)
    if promo_hits >= 2:
        non_promo_words = [
            w
            for w in words
            if w
            not in {
                "patreon",
                "donate",
                "subscribe",
                "merch",
                "sponsor",
                "affiliate",
                "links",
            }
        ]
        if len(non_promo_words) < 80 or promo_hits >= 4:
            return True

    link_density = url_count / max(word_count, 1)
    if promo_hits and link_density > 0.05 and word_count < 250:
        return True

    return False


def parse_vtt_to_text(vtt_content: str, *, max_chars: int | None = 7000) -> str:
    """
    Strip timestamps/cue numbers from a VTT payload and collapse into plain text.
    """
    lines = (vtt_content or "").splitlines()
    text_lines = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        upper = line.upper()
        if upper.startswith("WEBVTT") or upper.startswith("NOTE"):
            continue
        if re.match(r"^(KIND|LANGUAGE):", upper):
            continue
        if "-->" in line:
            continue
        if re.match(r"^\d+$", line):
            continue
        if re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}", line):
            continue
        cleaned = re.sub(r"<[^>]+>", "", line)
        if cleaned:
            text_lines.append(cleaned)

    text = " ".join(text_lines)
    text = re.sub(r"\s+", " ", text).strip()
    if max_chars is not None and len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text
