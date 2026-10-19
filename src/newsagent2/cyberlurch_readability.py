"""Extractive reader context from saved Cyberlurch prose; no generation or I/O."""
from __future__ import annotations

import re

from .cybermed_quality import contains_generation_error_text
from .youtube_source import stored_basis


SOURCE_VOICE_RULE = (
    "Write fluent, concise editorial prose that conveys the source's actual content and distinctive perspective. "
    "Give the content itself, not a description of the video, transcript, contribution or discussion. "
    "This applies to the summary and EVERY factual list entry, not just notable_claims. "
    "An explicit subject means the actual person, institution, event or proposal, not a reporting device such as 'the speaker'. "
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
    "Keep the snapshot brief; explain supported details below it. "
    "Style examples (use only facts actually present in this source): "
    "'The transcript discusses concerns that the proposal may raise fuel costs' becomes 'The proposal may raise fuel costs'. "
    "'The speaker says that the council has not approved the plan' becomes 'The council has not approved the plan'. "
    "'Der Beitrag berichtet über die Absage des Umzugs wegen zu hoher Sicherheitskosten' becomes "
    "'Der Umzug wurde wegen zu hoher Sicherheitskosten abgesagt'. "
    "Never copy example names or claims into the extraction."
)
_SENTENCE_BREAK = re.compile(r"(?<!\bDr\.)(?<!\bMr\.)(?<!\bMs\.)(?<!\bProf\.)(?<!\b[A-Z]\.)(?<=[.!?])\s+(?=[A-ZÄÖÜÅÆØ])")
_REPORT_FRAME = re.compile(r"^The (?:speaker|presenter|narrator|video|report|transcript|channel) "
    r"(?P<verb>says|reports|states|asserts|claims|argues|explains|notes|emphasizes|teaches|specifies|suggests|adds) that\s+", re.I)
_META_INTRO = re.compile(
    r"^(?:the (?:speaker|presenter|narrator|video|report|transcript|channel) "
    r"(?:says|reports|discusses|describes|asserts|identifies|covers)|"
    r"(?:der (?:beitrag|bericht|sprecher)|das (?:video|transkript)) "
    r"(?:berichtet|bespricht|diskutiert|beschreibt|behandelt|thematisiert)|"
    r"(?:videon|transkriptet|inslaget) (?:handlar om|diskuterar|beskriver))\b", re.I)


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
                r"explains|notes|identifies|warns|predicts|links|urges|describes|teaches|claims|emphasizes|specifies|suggests|adds)\b", rest, re.I)
            coordinated = coordinated or re.search(r'\b(?:and|but|or)\s+that\b', rest, re.I)
            tentative = (match.group('verb').lower() == 'suggests'
                and not re.search(r'\b(?:may|might|could|would|should|seems|appears)\b', rest, re.I))
            if len(rest.split()) >= 3 and not coordinated and not tentative:
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
    candidates = [item.get(key) for key in ("transcript_full_summary", "bottom_line", "summary")]
    # An old generic metadescription must not crowd out already saved, concrete
    # source facts. Do not reorder German subordinate clauses or invent a new
    # assertion from a nominal phrase such as 'discusses the economic impact'.
    if any(_META_INTRO.match(reader_prose(value, channel=str(item.get("channel") or ""))) for value in candidates):
        for key in ("transcript_key_points", "important_details"):
            values = item.get(key)
            if isinstance(values, list): candidates.extend(values)
    for value in candidates:
        text = reader_prose(value, channel=str(item.get("channel") or ""))
        if not text or text.casefold().rstrip(".!?") == title:
            continue
        if _META_INTRO.match(text):
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
