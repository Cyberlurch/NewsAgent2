from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .collectors_youtube import fetch_captions_text, fetch_transcript
from .collectors_youtube_timedtext import fetch_captions_via_timedtext
from .utils.text_quality import classify_low_signal_youtube_text
from .managed_transcripts import fetch_managed_transcript

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

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        result = fetch_managed_transcript(video_id, diagnostics=diagnostics)
        return ProviderResult(
            result.get("status", "empty"),
            str(result.get("text") or ""),
            self.name,
            error_kind=str(result.get("error_kind") or ""),
        )


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


def fetch_video_content(*, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any], providers_override: str | None = None) -> ProviderResult:
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
    order = [x.strip() for x in providers_override.split(",") if x.strip()] if providers_override else _provider_order()
    for name in order:
        provider = providers.get(name)
        if provider is None:
            continue
        diagnostics["provider_attempted_by_name"][name] = diagnostics["provider_attempted_by_name"].get(name, 0) + 1
        result = provider.fetch(video_id=video_id, video_url=video_url, description=description, diagnostics=diagnostics)
        if result.status == "success" and result.text.strip():
            diagnostics["provider_success_by_name"][name] = diagnostics["provider_success_by_name"].get(name, 0) + 1
            text = result.text.strip()
            cache_text = (os.getenv("YOUTUBE_CONTENT_CACHE_TEXT") or "0").strip() == "1"
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
