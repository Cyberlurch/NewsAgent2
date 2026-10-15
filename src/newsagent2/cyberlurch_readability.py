"""Extractive reader context from saved Cyberlurch prose; no generation or I/O."""
from __future__ import annotations

import re

from .cybermed_quality import contains_generation_error_text
from .youtube_source import stored_basis


SOURCE_VOICE_RULE = (
    "Write fluent, concise editorial prose that conveys the source's actual content and distinctive perspective. "
    "The channel is identified by its heading or source reference; do not start each sentence with 'the speaker reports', "
    "'the speaker asserts', 'the speaker identifies', 'der Sprecher', 'der Beitrag', or another reporting formula. "
    "Lead with the concrete subject and action. Preserve the source's emphasis, argument, political or religious viewpoint, "
    "and level of certainty without endorsing, rebutting, sanitizing or balancing it with outside views. "
    "Do not add a truthfulness assessment, skeptical disclaimer or 'allegedly' unless that qualification comes from the source. "
    "Keep original negation, conditional language and forecasts; a prediction must not become an event that already happened. "
    "Keep attribution when it distinguishes a quoted guest, another person's denial, or conflicting channel positions; "
    "identify each position once in context rather than repeating attribution in every sentence. "
    "In the body, name the consequences, mechanisms and affected people or institutions actually described by the source. "
    "'Discusses political and economic implications' is not a substitute for those details. "
    "If the source supplies no concrete consequence, omit the generic phrase instead of inventing one. "
    "Keep the snapshot brief; explain supported details below it."
)
_SENTENCE_BREAK = re.compile(r"(?<!\bDr\.)(?<!\bMr\.)(?<!\bMs\.)(?<!\bProf\.)(?<!\b[A-Z]\.)(?<=[.!?])\s+(?=[A-ZÄÖÜÅÆØ])")
_REPORT_FRAME = re.compile(r"^The (?:speaker|presenter|narrator|video|report) "
    r"(?:says|reports|states|asserts|claims|argues|explains|notes|emphasizes|teaches) that\s+", re.I)


def reader_prose(value, *, channel: str = "") -> str:
    """Remove narrow English reporting wrappers inside an identified source block.

    No verb reordering, translations or guesses about a sentence's meaning:
    German/Swedish subordinate clauses, named guests, denials, forecasts and
    coordinated reporting acts keep their wording. New generation handles the
    full editorial rewrite in the existing extraction call.
    """
    text = substantive_prose(value)
    parts = []
    for sentence in _SENTENCE_BREAK.split(text):
        if channel:
            sentence = re.sub(r"^According to " + re.escape(channel) + r",\s*", "", sentence, flags=re.I)
        match = _REPORT_FRAME.match(sentence)
        if match:
            rest = sentence[match.end():]
            coordinated = re.search(r"\b(?:and|but|or)\s+(?:also\s+)?(?:reports|says|states|asserts|argues|"
                r"explains|notes|identifies|warns|predicts|links|urges|describes|teaches)\b", rest, re.I)
            if len(rest.split()) >= 3 and not coordinated:
                sentence = rest
        if sentence:
            parts.append(sentence[0].upper() + sentence[1:])
    return substantive_prose(" ".join(parts))


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
        text = reader_prose(item.get(key), channel=str(item.get("channel") or ""))
        if not text or text.casefold().rstrip(".!?") == title:
            continue
        kept = []
        # Decimal numbers stay intact. Keep the supplied words, negation and
        # attribution; never clip a long sentence into a misleading fragment.
        for sentence in _SENTENCE_BREAK.split(text):
            if (len(" ".join([*kept, sentence]).split()) > max_words
                    or not re.search(r"[.!?][\"’”']?$", sentence)):
                break
            kept.append(sentence)
            if len(kept) >= max_sentences:
                break
        if kept:
            return " ".join(kept)
    return ""
