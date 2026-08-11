from __future__ import annotations

import re
from typing import Any


_ERROR_PATTERNS = (
    re.compile(r"\b(?:RateLimitError|APIConnectionError|AuthenticationError|BadRequestError|InternalServerError)\b", re.I),
    re.compile(r"\bopenai\.[A-Za-z_][A-Za-z0-9_.]*", re.I),
    re.compile(r"\b(?:insufficient_quota|invalid_api_key|model_not_found)\b", re.I),
    re.compile(r"\berror\s*(?:code)?\s*[:=]?\s*(?:401|403|429|5\d\d)\b", re.I),
    re.compile(r"\b(?:http|status)(?:\s+code)?\s*[:=]?\s*(?:401|403|429|5\d\d)\b", re.I),
    re.compile(r"\b(?:request|response)[-_ ]id\s*[:=]", re.I),
    re.compile(r"\btraceback\s*\(most recent call last\)", re.I),
    re.compile(r"\*\*(?:error|fehler):\*\*", re.I),
    re.compile(r"failed to (?:create|generate) (?:overview|deep dive|bottom line)", re.I),
    re.compile(r"(?:konnte|fehler beim) .*?(?:kurzüberblick|vertiefung|bottom line).*?(?:erzeugen|erzeugt)", re.I),
)


class CybermedGenerationError(RuntimeError):
    """A content-safe generation failure carrying no provider message."""

    def __init__(self, *, stage: str, category: str):
        self.stage = str(stage or "unknown")
        self.category = str(category or "other")
        super().__init__(f"cybermed_generation_failed:{self.stage}:{self.category}")


def classify_generation_error(exc: BaseException) -> str:
    if isinstance(exc, CybermedGenerationError):
        return exc.category
    status = getattr(exc, "status_code", None)
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    if status in {401, 403} or "authentication" in name or "permission" in name:
        return "authentication"
    if "quota" in message or "insufficient_quota" in message:
        return "quota"
    if status == 429 or "ratelimit" in name or "rate limit" in message:
        return "rate_limit"
    if "timeout" in name or "timeout" in message:
        return "timeout"
    if status is not None:
        try:
            if int(status) >= 500:
                return "provider_server_error"
        except (TypeError, ValueError):
            pass
    if "json" in name or "parse" in name or "json" in message:
        return "invalid_response"
    if isinstance(exc, ValueError) and str(exc) in {"empty_output", "unsafe_output"}:
        return str(exc)
    return "other"


def contains_generation_error_text(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    return bool(text and any(pattern.search(text) for pattern in _ERROR_PATTERNS))


def safe_generated_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text or contains_generation_error_text(text):
        return ""
    return text


def validate_generated_text(value: Any, *, stage: str) -> str:
    text = safe_generated_text(value)
    if not text:
        category = "unsafe_output" if str(value or "").strip() else "empty_output"
        raise CybermedGenerationError(stage=stage, category=category)
    return text


def sanitize_cybermed_payload(value: Any) -> tuple[Any, int]:
    """Recursively remove provider-error strings from persisted Cybermed data."""

    if isinstance(value, str):
        if contains_generation_error_text(value):
            return "", 1
        return value, 0
    if isinstance(value, list):
        cleaned = []
        removed = 0
        for item in value:
            safe, count = sanitize_cybermed_payload(item)
            cleaned.append(safe)
            removed += count
        return cleaned, removed
    if isinstance(value, tuple):
        cleaned, removed = sanitize_cybermed_payload(list(value))
        return tuple(cleaned), removed
    if isinstance(value, dict):
        cleaned: dict[Any, Any] = {}
        removed = 0
        for key, item in value.items():
            safe, count = sanitize_cybermed_payload(item)
            cleaned[key] = safe
            removed += count
        return cleaned, removed
    return value, 0


def assert_no_generation_error_text(value: Any, *, boundary: str) -> None:
    _, removed = sanitize_cybermed_payload(value)
    if removed:
        raise RuntimeError(f"cybermed_generation_error_leak_blocked:{boundary}:{removed}")


def degraded_overview(*, language: str) -> str:
    if str(language or "").strip().lower().startswith("en"):
        return (
            "## Executive Summary\n\n"
            "Selected papers and commentary are listed below; this edition contains no cross-item synthesis.\n"
        )
    return (
        "## Kurzüberblick\n\n"
        "Die ausgewählten Papers und Kommentare stehen unten; diese Ausgabe enthält keine übergreifende Synthese.\n"
    )
