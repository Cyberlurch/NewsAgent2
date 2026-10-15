"""Saved context must remain useful without extra generation or State writes."""
import copy

import pytest

from newsagent2 import reporter
from newsagent2.cyberlurch_monthly import build_source_registry, select_evidence_pack
from newsagent2.cyberlurch_readability import saved_reader_intro


SUMMARY = ("River City approved a new flood barrier for the north district after repeated winter flooding. "
           "The council expects construction next spring, but funding has not been finalized.")


def row(**extra):
    return {"id": "barrier", "title": "Council meeting update", "channel": "Local Briefing",
        "url": "https://example.test/barrier", "published_at": "2026-10-15T08:00:00+00:00",
        "content_status": "full_text", "text_source": "managed_transcript",
        "transcript_full_summary": SUMMARY,
        "transcript_key_points": ["The expected cost is 20 million euros."], **extra}


@pytest.mark.parametrize("deep", [False, True])
def test_daily_keeps_subject_action_and_uncertainty_before_isolated_facts(deep):
    item = row(cyberlurch_daily_deep_dive=deep)
    before = copy.deepcopy(item)
    md = reporter.render_cyberlurch_daily_report([item], title="Cyberlurch Daily", generated_at="now")
    snapshot, body = md.split("## Selected items", 1)
    assert SUMMARY.split(". ", 1)[0] in snapshot
    assert f"**In brief:** Local Briefing: {SUMMARY}" in body
    assert "funding has not been finalized" in body and "20 million euros" in body
    assert item == before


def test_exact_summary_fact_is_not_repeated_as_a_bullet_but_negation_survives():
    fact = SUMMARY.split(". ", 1)[0] + "."
    md = reporter.render_cyberlurch_daily_report([row(transcript_key_points=[fact, "River City did not approve a separate sea wall."])], title="Cyberlurch", generated_at="now")
    body = md.split("## Selected items", 1)[1]
    assert body.count(fact) == 1
    assert "did not approve a separate sea wall" in body


@pytest.mark.parametrize("field", ["bottom_line", "transcript_full_summary"])
def test_generic_text_does_not_override_contextual_saved_prose(field):
    item = row(bottom_line="Unable to summarize reliably from the available excerpt.", summary=SUMMARY)
    item[field] = "Unable to summarize reliably from the available excerpt."
    assert saved_reader_intro(item) == SUMMARY
    registry = build_source_registry([item])
    assert registry[0]["factual_summary"] == SUMMARY
    assert "Unable to summarize" not in str(registry)


@pytest.mark.parametrize("deep", [False, True])
def test_placeholder_claim_is_removed_before_named_attribution(deep):
    md = reporter.render_cyberlurch_daily_report([row(cyberlurch_daily_deep_dive=deep,
        transcript_notable_claims=["Unable to summarize reliably from the available excerpt.", "The barrier could reduce future flood losses."])], title="Cyberlurch", generated_at="now")
    assert "Unable to summarize" not in md
    assert "According to Local Briefing, The barrier could reduce future flood losses." in md


@pytest.mark.parametrize("extra", [
    {"content_status": "metadata_only"}, {"text_source": "metadata_only"},
    {"source_basis": {"version": 1, "kind": "transcript", "provider": "test", "available_chars": 900,
                      "processed_unique_chars": 0, "coverage": "none", "video_completeness": "unknown"}},
])
def test_unprocessed_or_metadata_only_source_cannot_supply_context_or_fact_bullets(extra):
    item = row(**extra)
    assert not saved_reader_intro(item)
    md = reporter.render_cyberlurch_daily_report([item], title="Cyberlurch", generated_at="now")
    assert SUMMARY not in md and "20 million euros" not in md


def test_important_details_are_not_lost_if_no_summary_or_key_points_exist():
    item = row(transcript_full_summary="", transcript_key_points=[],
        important_details=["River City postponed the flood barrier pending an environmental review."])
    md = reporter.render_cyberlurch_daily_report([item], title="Cyberlurch", generated_at="now")
    assert "River City postponed the flood barrier pending an environmental review." in md


def test_known_placeholder_only_record_is_not_replaced_by_a_factual_title():
    item = row(transcript_full_summary="", transcript_key_points=[],
        bottom_line="Unable to summarize reliably from the available excerpt.")
    registry = build_source_registry([item])
    assert registry[0]["factual_summary"] == "" and select_evidence_pack(registry) == []


def test_actual_key_points_can_supply_context_when_only_the_summary_is_unavailable():
    item = row(transcript_full_summary="", bottom_line="Summary unavailable.",
        transcript_key_points=["River City postponed construction pending an environmental review."])
    registry = build_source_registry([item])
    assert registry[0]["factual_summary"] == item["transcript_key_points"][0]
    assert len(select_evidence_pack(registry)) == 1


@pytest.mark.parametrize("text", [
    "Die Stadt plant einen neuen Hochwasserschutz. Die Finanzierung ist noch nicht gesichert.",
    "Staden planerar en ny översvämningsbarriär. Finansieringen är ännu inte säkrad.",
    "Dr. Baker says the U.S. plan may cost 2.5 million euros. Funding has not been approved.",
])
def test_saved_sentences_retain_language_decimals_negation_and_attribution(text):
    assert saved_reader_intro(row(transcript_full_summary=text)) == text


def test_never_clips_a_long_sentence_or_uses_editorial_relevance_as_a_fact():
    item = row(transcript_full_summary="A " + "very " * 100 + "long sentence.", bottom_line="",
        editorial_relevance="This topic is important to every reader.")
    assert saved_reader_intro(item) == ""


def test_monthly_skips_placeholder_and_technical_details_before_model_input():
    item = row(transcript_full_summary="", bottom_line="Summary unavailable.", transcript_key_points=[],
        transcript_notable_claims=["Unable to summarize reliably.", "River City postponed construction."],
        important_details=["Provider error: insufficient_quota."])
    registry = build_source_registry([item])
    assert registry[0]["factual_summary"] == "River City postponed construction."
    assert "insufficient_quota" not in str(registry) and "Unable to summarize" not in str(registry)
