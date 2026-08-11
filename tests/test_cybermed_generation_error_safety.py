from __future__ import annotations

import pytest

from newsagent2 import emailer, reporter, summarizer
from newsagent2.cybermed_quality import (
    CybermedGenerationError,
    classify_generation_error,
    contains_generation_error_text,
    sanitize_cybermed_payload,
)


RAW_PROVIDER_ERROR = (
    "RateLimitError: Error code: 429 - insufficient_quota "
    "request_id=req_stage4_secret api_key=sk-stage4-secret"
)


class _ProviderFailure(RuntimeError):
    status_code = 429


class _FailingClient:
    class Chat:
        class Completions:
            @staticmethod
            def create(**_kwargs):
                raise _ProviderFailure(RAW_PROVIDER_ERROR)

        completions = Completions()

    chat = Chat()


def test_generation_error_detection_and_sanitization_are_content_safe():
    assert contains_generation_error_text(RAW_PROVIDER_ERROR) is True
    assert contains_generation_error_text("The medication error rate fell after the intervention.") is False
    assert classify_generation_error(_ProviderFailure(RAW_PROVIDER_ERROR)) == "quota"

    cleaned, removed = sanitize_cybermed_payload(
        {"bottom_line": RAW_PROVIDER_ERROR, "nested": ["Clinical summary"]}
    )
    assert cleaned == {"bottom_line": "", "nested": ["Clinical summary"]}
    assert removed == 1


@pytest.mark.parametrize(
    ("call", "expected_stage"),
    [
        (
            lambda: summarizer.summarize(
                [{"title": "Study", "text": "Abstract"}],
                language="en",
                profile="medical",
            ),
            "overview",
        ),
        (
            lambda: summarizer.summarize_pubmed_bottom_line(
                {"title": "Study", "text": "Abstract", "pmid": "1"},
                language="en",
            ),
            "pubmed_bottom_line",
        ),
        (
            lambda: summarizer.summarize_foamed_bottom_line(
                {"title": "Commentary", "text": "Article", "url": "https://example.test"},
                language="en",
            ),
            "foamed_bottom_line",
        ),
    ],
)
def test_cybermed_summarizers_raise_only_safe_categorized_errors(
    monkeypatch, capsys, call, expected_stage
):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setattr(summarizer, "_get_client", lambda: _FailingClient())

    with pytest.raises(CybermedGenerationError) as exc_info:
        call()

    assert exc_info.value.stage == expected_stage
    assert exc_info.value.category == "quota"
    assert RAW_PROVIDER_ERROR not in str(exc_info.value)
    assert "sk-stage4-secret" not in capsys.readouterr().out


def test_cyberlurch_overview_error_fallback_is_unchanged(monkeypatch):
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    monkeypatch.setattr(summarizer, "_get_client", lambda: _FailingClient())

    rendered = summarizer.summarize([], language="en", profile="general")

    assert "**Error:** Failed to create overview" in rendered
    assert "RateLimitError" in rendered


def test_cybermed_reporter_blocks_provider_error_text(monkeypatch):
    monkeypatch.setenv("REPORT_KEY", "cybermed")

    with pytest.raises(RuntimeError, match="cybermed_generation_error_leak_blocked:reporter_input"):
        reporter.to_markdown(
            [],
            f"## Executive Summary\n\n{RAW_PROVIDER_ERROR}",
            {},
            report_title="Custom medical report",
            report_language="en",
            report_mode="daily",
        )


def test_cybermed_emailer_blocks_before_smtp(monkeypatch):
    smtp_started = False

    def _unexpected_smtp(*_args, **_kwargs):
        nonlocal smtp_started
        smtp_started = True
        raise AssertionError("SMTP must not be initialized")

    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("SEND_EMAIL", "1")
    monkeypatch.setattr(emailer.smtplib, "SMTP", _unexpected_smtp)

    with pytest.raises(RuntimeError, match="cybermed_generation_error_leak_blocked:email_body"):
        emailer.send_markdown("Cybermed", RAW_PROVIDER_ERROR)

    assert smtp_started is False
