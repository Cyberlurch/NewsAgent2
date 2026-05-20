from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .collectors_youtube import fetch_captions_text, fetch_transcript
from .collectors_youtube_timedtext import fetch_captions_via_timedtext
from .utils.text_quality import classify_low_signal_youtube_text

CACHE_PATH = Path("state/youtube_content_cache.json")
DEFAULT_PROVIDER_ORDER = ["youtube_transcript_api", "description", "timedtext", "yt_dlp_captions", "metadata_only"]


@dataclass
class ProviderResult:
    status: str
    text: str
    source: str
    error_kind: str = ""
    duration_s: float = 0.0


class BaseProvider:
    name = "base"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        raise NotImplementedError


class YouTubeTranscriptApiProvider(BaseProvider):
    name = "youtube_transcript_api"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        t0 = time.monotonic()
        text = fetch_transcript(video_id, diagnostics=diagnostics) or ""
        return ProviderResult("success" if text else "empty", text.strip(), self.name, duration_s=time.monotonic() - t0)


class DescriptionProvider(BaseProvider):
    name = "description"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        t0 = time.monotonic()
        text = (description or "").strip()
        min_chars = max(1, int((os.getenv("DESCRIPTION_PROVIDER_MIN_CHARS") or "300").strip()))
        if len(text) < min_chars:
            return ProviderResult("empty", "", self.name, duration_s=time.monotonic() - t0)
        low_signal, _reason = classify_low_signal_youtube_text(text)
        if low_signal:
            return ProviderResult("empty", "", self.name, duration_s=time.monotonic() - t0)
        return ProviderResult("success", text, self.name, duration_s=time.monotonic() - t0)


class TimedTextProvider(BaseProvider):
    name = "timedtext"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        t0 = time.monotonic()
        text, status = fetch_captions_via_timedtext(video_id, ("de", "en", "sv"))
        ek = "" if status != "error_parse" else "error_parse"
        normalized = "error" if status.startswith("error") else status
        return ProviderResult(normalized, (text or "").strip(), self.name, error_kind=ek, duration_s=time.monotonic() - t0)


class YtDlpCaptionsProvider(BaseProvider):
    name = "yt_dlp_captions"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        t0 = time.monotonic()
        text, status, error_kind = fetch_captions_text(video_url, ["de.*", "en.*", "sv.*", "-live_chat"], retries=1)
        return ProviderResult(status, (text or "").strip(), self.name, error_kind=error_kind or "", duration_s=time.monotonic() - t0)


class ManagedTranscriptProvider(BaseProvider):
    name = "managed_transcript"

    def __init__(self) -> None:
        self.profile = (os.getenv("YOUTUBE_TRANSCRIPT_PROVIDER") or "none").strip().lower()
        if self.profile not in {"none", "transcriptapi", "supadata", "generic"}:
            self.profile = "none"
        self.api_key = (os.getenv("YOUTUBE_TRANSCRIPT_API_KEY") or "").strip()
        self.base_url = (os.getenv("YOUTUBE_TRANSCRIPT_API_BASE_URL") or "").strip()
        self.min_chars = max(1, int((os.getenv("MANAGED_TRANSCRIPT_MIN_CHARS") or "300").strip()))
        raw_langs = (os.getenv("MANAGED_TRANSCRIPT_LANGS") or "de,en,sv").strip()
        self.languages = [x.strip() for x in raw_langs.split(",") if x.strip()] or ["de", "en", "sv"]

    def _enabled(self) -> bool:
        return self.profile != "none"

    def _extract_text(self, body: Any) -> str:
        if isinstance(body, dict):
            for key in ("text", "content", "transcript"):
                val = body.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
            segments = body.get("segments")
            if isinstance(segments, list):
                return " ".join(str(s.get("text") or "").strip() for s in segments if isinstance(s, dict)).strip()
        if isinstance(body, list):
            return " ".join(str(s.get("text") or "").strip() for s in body if isinstance(s, dict)).strip()
        return ""

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        t0 = time.monotonic()
        diagnostics["youtube_transcript_provider"] = self.profile
        diagnostics["managed_transcript_configured"] = self._enabled() and bool(self.api_key)
        diagnostics["managed_transcript_api_key_present"] = bool(self.api_key)
        diagnostics["managed_transcript_base_url_present"] = bool(self.base_url)
        if not self._enabled():
            return ProviderResult("empty", "", self.name, error_kind="disabled", duration_s=time.monotonic() - t0)
        diagnostics["managed_transcript_attempted_total"] = int(diagnostics.get("managed_transcript_attempted_total", 0)) + 1
        if not self.api_key:
            diagnostics["managed_transcript_error_total"] = int(diagnostics.get("managed_transcript_error_total", 0)) + 1
            diagnostics["managed_transcript_misconfigured_total"] = int(diagnostics.get("managed_transcript_misconfigured_total", 0)) + 1
            return ProviderResult("error", "", self.name, error_kind="misconfigured", duration_s=time.monotonic() - t0)

        method = (os.getenv("YOUTUBE_TRANSCRIPT_API_METHOD") or ("POST" if self.profile == "generic" else "GET")).strip().upper()
        auth_header = (os.getenv("YOUTUBE_TRANSCRIPT_API_AUTH_HEADER") or ("Authorization" if self.profile == "generic" else "x-api-key")).strip()
        video_param = (os.getenv("YOUTUBE_TRANSCRIPT_API_VIDEO_PARAM") or ("video_id" if self.profile == "generic" else "videoId")).strip()
        url = self.base_url
        if not url:
            if self.profile == "supadata":
                url = "https://api.supadata.ai/v1/youtube/transcript"
            elif self.profile == "transcriptapi":
                url = "https://api.transcriptapi.com/v1/transcript"
        if not url:
            diagnostics["managed_transcript_error_total"] = int(diagnostics.get("managed_transcript_error_total", 0)) + 1
            diagnostics["managed_transcript_misconfigured_total"] = int(diagnostics.get("managed_transcript_misconfigured_total", 0)) + 1
            return ProviderResult("error", "", self.name, error_kind="misconfigured", duration_s=time.monotonic() - t0)

        headers = {auth_header: f"Bearer {self.api_key}" if auth_header.lower() == "authorization" else self.api_key}
        payload = {video_param: video_id if video_param.lower() != "url" else video_url, "url": video_url, "languages": self.languages, "text": True}
        try:
            if method == "POST":
                resp = requests.post(url, headers=headers, json=payload, timeout=20)
            else:
                resp = requests.get(url, headers=headers, params=payload, timeout=20)
            if resp.status_code == 429:
                diagnostics["managed_transcript_rate_limited_total"] = int(diagnostics.get("managed_transcript_rate_limited_total", 0)) + 1
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    time.sleep(min(2.0, max(0.0, float(retry_after))))
                return ProviderResult("error", "", self.name, error_kind="rate_limited", duration_s=time.monotonic() - t0)
            if resp.status_code >= 400:
                diagnostics["managed_transcript_error_total"] = int(diagnostics.get("managed_transcript_error_total", 0)) + 1
                if resp.status_code in {401, 403}:
                    diagnostics["managed_transcript_auth_error_total"] = int(diagnostics.get("managed_transcript_auth_error_total", 0)) + 1
                return ProviderResult("error", "", self.name, error_kind=f"http_{resp.status_code}", duration_s=time.monotonic() - t0)
            body = resp.json() if resp.content else {}
            text = self._extract_text(body)
            if len(text) >= self.min_chars:
                diagnostics["managed_transcript_success_total"] = int(diagnostics.get("managed_transcript_success_total", 0)) + 1
                return ProviderResult("success", text, self.name, duration_s=time.monotonic() - t0)
            diagnostics["managed_transcript_empty_total"] = int(diagnostics.get("managed_transcript_empty_total", 0)) + 1
            return ProviderResult("empty", "", self.name, duration_s=time.monotonic() - t0)
        except Exception:
            diagnostics["managed_transcript_error_total"] = int(diagnostics.get("managed_transcript_error_total", 0)) + 1
            return ProviderResult("error", "", self.name, error_kind="request_failed", duration_s=time.monotonic() - t0)


class MetadataOnlyProvider(BaseProvider):
    name = "metadata_only"
    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        return ProviderResult("empty", "", self.name)


def _load_cache() -> dict[str, Any]:
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache: dict[str, Any]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _cache_ttl_days() -> int:
    try:
        return max(1, int((os.getenv("YOUTUBE_CONTENT_CACHE_DAYS") or "30").strip()))
    except ValueError:
        return 30


def _provider_order() -> list[str]:
    raw = (os.getenv("CYBERLURCH_CONTENT_PROVIDERS") or "").strip()
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
    if (os.getenv("YOUTUBE_TRANSCRIPT_PROVIDER") or "none").strip().lower() != "none":
        return ["youtube_transcript_api", "managed_transcript", "description", "timedtext", "yt_dlp_captions", "metadata_only"]
    return DEFAULT_PROVIDER_ORDER.copy()


def fetch_video_content(*, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
    diagnostics.setdefault("provider_attempted_by_name", {})
    diagnostics.setdefault("provider_success_by_name", {})
    diagnostics.setdefault("provider_empty_by_name", {})
    diagnostics.setdefault("provider_error_by_name", {})
    diagnostics.setdefault("provider_error_kind_by_name", {})
    cache = _load_cache()
    now = dt.datetime.now(dt.timezone.utc)
    ttl = dt.timedelta(days=_cache_ttl_days())

    cache_key = video_id
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        fetched = cached.get("fetched_at_utc")
        try:
            fetched_at = dt.datetime.fromisoformat(str(fetched))
        except Exception:
            fetched_at = None
        if fetched_at and fetched_at.tzinfo and (now - fetched_at) <= ttl and cached.get("status") == "success":
            diagnostics["cache_hit_total"] = int(diagnostics.get("cache_hit_total", 0)) + 1
            return ProviderResult("success", str(cached.get("text") or ""), str(cached.get("source") or "description"))
    diagnostics["cache_miss_total"] = int(diagnostics.get("cache_miss_total", 0)) + 1

    providers = {
        "youtube_transcript_api": YouTubeTranscriptApiProvider(),
        "managed_transcript": ManagedTranscriptProvider(),
        "description": DescriptionProvider(),
        "timedtext": TimedTextProvider(),
        "yt_dlp_captions": YtDlpCaptionsProvider(),
        "metadata_only": MetadataOnlyProvider(),
    }
    order = _provider_order()
    managed_budget = max(0, int((os.getenv("MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN") or "10").strip()))

    for name in order:
        if name == "managed_transcript":
            if int(diagnostics.get("managed_transcript_attempted_total", 0)) >= managed_budget:
                diagnostics["managed_transcript_skipped_budget_total"] = int(diagnostics.get("managed_transcript_skipped_budget_total", 0)) + 1
                continue
        provider = providers.get(name)
        if provider is None:
            continue
        diagnostics["provider_attempted_by_name"][name] = diagnostics["provider_attempted_by_name"].get(name, 0) + 1
        result = provider.fetch(video_id=video_id, video_url=video_url, description=description, diagnostics=diagnostics)
        if result.status == "success" and result.text.strip():
            diagnostics["provider_success_by_name"][name] = diagnostics["provider_success_by_name"].get(name, 0) + 1
            text = result.text.strip()
            cache_text = (os.getenv("YOUTUBE_CONTENT_CACHE_TEXT") or "1").strip() == "1"
            cache_key = video_id if name != "managed_transcript" else f"{video_id}::managed_transcript"
            cache[cache_key] = {"status": "success", "source": result.source, "fetched_at_utc": now.isoformat(), "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(), "text": text if cache_text else ""}
            _save_cache(cache)
            diagnostics["cache_write_total"] = int(diagnostics.get("cache_write_total", 0)) + 1
            return result
        if result.status == "empty":
            diagnostics["provider_empty_by_name"][name] = diagnostics["provider_empty_by_name"].get(name, 0) + 1
        else:
            diagnostics["provider_error_by_name"][name] = diagnostics["provider_error_by_name"].get(name, 0) + 1

    return ProviderResult("empty", "", "metadata_only")
