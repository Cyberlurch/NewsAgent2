"""Extractive reader context from saved Cyberlurch prose; no generation or I/O."""
from __future__ import annotations

import re

from .cybermed_quality import contains_generation_error_text
from .youtube_source import stored_basis


_PLACEHOLDER = re.compile(
    r"^(?:unable to summarize\b|no concise item summary\b|summary unavailable\b|"
    r"highlights derived\b|failed to generate\b|"
    r"keine zuverlässige zusammenfassung\b|ingen tillförlitlig sammanfattning\b)", re.I)


def substantive_prose(value) -> str:
    if not isinstance(value, str):
        return ""
    text = re.sub(r"\s+", " ", value).strip()
    text = re.sub(r"^(?:\*\*)?BOTTOM LINE:(?:\*\*)?\s*", "", text, flags=re.I)
    if not text or _PLACEHOLDER.search(text.lstrip("[\"' ")) or contains_generation_error_text(text):
        return ""
    return text


def reader_text_allowed(item: dict) -> bool:
    return (item.get("content_status") != "metadata_only" and item.get("text_source") != "metadata_only"
            and not (isinstance(item.get("source_basis"), dict) and stored_basis(item)["coverage"] == "none"))


def saved_reader_intro(item: dict, *, max_sentences: int = 2, max_words: int = 90) -> str:
    """Prefer context over a thin takeaway, keeping whole source sentences.

    A title or editorial relevance assertion cannot supply missing facts.
    Explicitly unprocessed text never becomes a reader narrative.
    """
    if not reader_text_allowed(item):
        return ""
    title = str(item.get("title") or "").strip().casefold().rstrip(".!?")
    for key in ("transcript_full_summary", "bottom_line", "summary"):
        text = substantive_prose(item.get(key))
        if not text or text.casefold().rstrip(".!?") == title:
            continue
        kept = []
        # Decimal numbers stay intact. Keep the supplied words, negation and
        # attribution; never clip a long sentence into a misleading fragment.
        for sentence in re.split(r"(?<!\bDr\.)(?<!\bMr\.)(?<!\bMs\.)(?<!\bProf\.)(?<!\b[A-Z]\.)(?<=[.!?])\s+(?=[A-ZÄÖÜÅÆØ])", text):
            if (len(" ".join([*kept, sentence]).split()) > max_words
                    or not re.search(r"[.!?][\"’”']?$", sentence)):
                break
            kept.append(sentence)
            if len(kept) >= max_sentences:
                break
        if kept:
            return " ".join(kept)
    return ""
