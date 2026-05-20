from __future__ import annotations

import os
from statistics import median
from typing import Any

import requests

_ALLOWED = {"none", "transcriptapi", "supadata", "generic"}


def _inc(d: dict[str, Any] | None, key: str, n: int = 1) -> None:
    if d is None:
        return
    d[key] = int(d.get(key, 0)) + n


def _parse_langs() -> list[str]:
    raw = (os.getenv("MANAGED_TRANSCRIPT_LANGS") or "de,en,sv").strip()
    langs = [x.strip() for x in raw.split(",") if x.strip()]
    return langs or ["de", "en", "sv"]


def _norm_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _extract_text(body: Any) -> str:
    if isinstance(body, dict):
        for key in ("text", "content", "transcript"):
            val = body.get(key)
            if isinstance(val, str):
                return _norm_text(val)
            if isinstance(val, list):
                return _norm_text(" ".join(str(x.get("text") or "") for x in val if isinstance(x, dict)))
        seg = body.get("segments")
        if isinstance(seg, list):
            return _norm_text(" ".join(str(x.get("text") or "") for x in seg if isinstance(x, dict)))
    elif isinstance(body, list):
        return _norm_text(" ".join(str(x.get("text") or "") for x in body if isinstance(x, dict)))
    return ""


def _base_result(provider: str, status: str = "empty", *, error_kind: str = "", text: str = "") -> dict[str, Any]:
    return {"status": status, "text": text, "source": "managed_transcript", "provider": provider, "error_kind": error_kind, "chars": len(text)}


def fetch_managed_transcript(video_id: str, *, diagnostics: dict | None = None) -> dict:
    provider = (os.getenv("YOUTUBE_TRANSCRIPT_PROVIDER") or "none").strip().lower()
    if provider not in _ALLOWED:
        provider = "none"
    key = (os.getenv("YOUTUBE_TRANSCRIPT_API_KEY") or "").strip()
    base_url = (os.getenv("YOUTUBE_TRANSCRIPT_API_BASE_URL") or "").strip()
    min_chars = max(1, int((os.getenv("MANAGED_TRANSCRIPT_MIN_CHARS") or "300").strip()))
    max_videos = max(0, int((os.getenv("MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN") or "10").strip()))
    langs = _parse_langs()

    if diagnostics is not None:
        diagnostics["youtube_transcript_provider"] = provider
        diagnostics["managed_transcript_configured"] = provider in {"transcriptapi", "supadata", "generic"} and bool(key)
        diagnostics["managed_transcript_api_key_present"] = bool(key)
        diagnostics["managed_transcript_base_url_present"] = bool(base_url)

    if provider == "none":
        return _base_result(provider, "disabled")

    if int((diagnostics or {}).get("managed_transcript_attempted_total", 0)) >= max_videos:
        _inc(diagnostics, "managed_transcript_skipped_budget_total")
        return _base_result(provider, "budget_exhausted")

    _inc(diagnostics, "managed_transcript_attempted_total")

    if not key:
        _inc(diagnostics, "managed_transcript_misconfigured_total")
        return _base_result(provider, "misconfigured", error_kind="missing_api_key")

    try:
        if provider == "transcriptapi":
            url = (base_url or "https://transcriptapi.com/api/v2").rstrip("/") + "/youtube/transcript"
            resp = requests.get(url, headers={"Authorization": f"Bearer {key}"}, params={"video_url": video_id, "format": "json"}, timeout=20)
        elif provider == "supadata":
            root = (base_url or "https://api.supadata.ai/v1").rstrip("/")
            params = {"videoId": video_id, "text": "true", "lang": langs[0]}
            headers = {"x-api-key": key}
            resp = requests.get(root + "/youtube/transcript", headers=headers, params=params, timeout=20)
            if resp.status_code == 404:
                resp = requests.get(root + "/transcript", headers=headers, params=params, timeout=20)
        else:
            if not base_url:
                _inc(diagnostics, "managed_transcript_misconfigured_total")
                return _base_result(provider, "misconfigured", error_kind="missing_base_url")
            resp = requests.post(base_url, headers={"Authorization": f"Bearer {key}"}, json={"video_id": video_id, "languages": langs, "text": True}, timeout=20)

        if resp.status_code in {401, 403}:
            _inc(diagnostics, "managed_transcript_auth_error_total")
            _inc(diagnostics, "managed_transcript_error_total")
            return _base_result(provider, "error", error_kind="auth_error")
        if resp.status_code == 429:
            _inc(diagnostics, "managed_transcript_rate_limited_total")
            _inc(diagnostics, "managed_transcript_error_total")
            return _base_result(provider, "error", error_kind="rate_limited")
        if resp.status_code >= 400:
            _inc(diagnostics, "managed_transcript_error_total")
            return _base_result(provider, "error", error_kind=f"http_{resp.status_code}")

        body = resp.json() if resp.content else {}
        text = _extract_text(body)
        if len(text) >= min_chars:
            _inc(diagnostics, "managed_transcript_success_total")
            hist = diagnostics.setdefault("managed_transcript_chars_values", []) if diagnostics is not None else []
            if isinstance(hist, list):
                hist.append(len(text))
                diagnostics["managed_transcript_chars_min"] = min(hist)
                diagnostics["managed_transcript_chars_median"] = int(median(hist))
                diagnostics["managed_transcript_chars_max"] = max(hist)
            return _base_result(provider, "success", text=text)
        _inc(diagnostics, "managed_transcript_empty_total")
        return _base_result(provider, "empty")
    except Exception:
        _inc(diagnostics, "managed_transcript_error_total")
        return _base_result(provider, "error", error_kind="request_failed")
