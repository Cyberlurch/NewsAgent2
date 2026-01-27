import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2.utils.text_quality import (
    is_low_signal,
    is_low_signal_youtube_text,
    parse_vtt_to_text,
    vtt_to_text,
)


def test_is_low_signal_youtube_text_flags_promos_with_links():
    promo_text = (
        "Support the channel on Patreon and check the links below for affiliate deals. "
        "Subscribe for more updates and share the merch drop. "
        "https://example.com/deal1 https://example.com/deal2 https://example.com/deal3 "
        "Join the Discord to support the channel and see sponsors."
    ) * 3

    assert is_low_signal_youtube_text(promo_text) is True


def test_is_low_signal_youtube_text_accepts_normal_transcript():
    transcript_like = (
        "Today we examine the latest security updates across multiple platforms. "
        "The report outlines how the patch cycle impacts enterprise deployments and "
        "what administrators should prioritize over the next week. "
        "We also discuss lessons learned from recent incidents and explore mitigation strategies. "
        "Finally, we close with a preview of upcoming research findings relevant to the community."
    ) * 4

    assert is_low_signal_youtube_text(transcript_like) is False


def test_is_low_signal_flags_promo_urls():
    promo_text = (
        "Support our sponsors and subscribe for more. "
        "https://example.com/a https://example.com/b https://example.com/c "
        "Join the Discord for exclusive merch updates."
    ) * 3

    assert is_low_signal(promo_text) is True


def test_is_low_signal_accepts_paragraph():
    paragraph = (
        "This briefing reviews recent security incidents across multiple industries. "
        "We cover the primary causes, response timelines, and actionable mitigations. "
        "The discussion closes with policy recommendations for long-term resilience."
    ) * 4

    assert is_low_signal(paragraph) is False


def test_parse_vtt_to_text_strips_cues_and_timestamps():
    vtt = """WEBVTT
Kind: captions

1
00:00:00.000 --> 00:00:02.000
Hello world!

2
00:00:02.500 --> 00:00:04.000
Visit our site <c.colorE5E5E5>News</c> Agent.
"""

    assert parse_vtt_to_text(vtt) == "Hello world! Visit our site News Agent."


def test_vtt_to_text_extracts_words():
    vtt = """WEBVTT

00:00:00.000 --> 00:00:01.000
Welcome to the show.

00:00:01.500 --> 00:00:03.000
We discuss latest updates.
"""

    text = vtt_to_text(vtt)
    assert "Welcome to the show." in text
    assert "We discuss latest updates." in text
