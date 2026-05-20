from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import requests
import yt_dlp

from .collectors_youtube import build_ytdlp_common_opts, fetch_captions_text, fetch_transcript
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "text": self.text,
            "source": self.source,
            "error_kind": self.error_kind,
            "duration_s": self.duration_s,
        }


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


class ExternalTranscriptApiProvider(BaseProvider):
    name = "external_api"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        t0 = time.monotonic()
        diagnostics["external_api_attempted_total"] = int(diagnostics.get("external_api_attempted_total", 0)) + 1
        url = (os.getenv("EXTERNAL_YOUTUBE_TRANSCRIPT_API_URL") or "").strip()
        key = (os.getenv("EXTERNAL_YOUTUBE_TRANSCRIPT_API_KEY") or "").strip()
        if not url or not key:
            diagnostics["external_api_error_total"] = int(diagnostics.get("external_api_error_total", 0)) + 1
            return ProviderResult("error", "", self.name, error_kind="misconfigured", duration_s=time.monotonic() - t0)
        try:
            resp = requests.post(url, headers={"Authorization": f"Bearer {key}"}, json={"video_id": video_id, "languages": ["de", "en", "sv"]}, timeout=20)
            if resp.status_code >= 400:
                diagnostics["external_api_error_total"] = int(diagnostics.get("external_api_error_total", 0)) + 1
                return ProviderResult("error", "", self.name, error_kind=f"http_{resp.status_code}", duration_s=time.monotonic() - t0)
            body = resp.json() if resp.content else {}
            text = str(body.get("text") or "").strip()
            if not text and isinstance(body.get("segments"), list):
                text = " ".join(str(s.get("text") or "").strip() for s in body["segments"] if isinstance(s, dict)).strip()
            if text:
                diagnostics["external_api_success_total"] = int(diagnostics.get("external_api_success_total", 0)) + 1
                return ProviderResult("success", text, self.name, duration_s=time.monotonic() - t0)
            diagnostics["external_api_empty_total"] = int(diagnostics.get("external_api_empty_total", 0)) + 1
            return ProviderResult("empty", "", self.name, duration_s=time.monotonic() - t0)
        except Exception:
            diagnostics["external_api_error_total"] = int(diagnostics.get("external_api_error_total", 0)) + 1
            return ProviderResult("error", "", self.name, error_kind="request_failed", duration_s=time.monotonic() - t0)


class MetadataOnlyProvider(BaseProvider):
    name = "metadata_only"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        return ProviderResult("empty", "", self.name)


class ASRProvider(BaseProvider):
    name = "asr"

    def fetch(self, *, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
        if (os.getenv("CYBERLURCH_ENABLE_ASR") or "0").strip() != "1":
            return ProviderResult("empty", "", self.name, error_kind="disabled")
        return ProviderResult("error", "", self.name, error_kind="not_implemented")


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
    if not raw:
        return DEFAULT_PROVIDER_ORDER.copy()
    return [x.strip() for x in raw.split(",") if x.strip()]


def fetch_video_content(*, video_id: str, video_url: str, description: str, diagnostics: dict[str, Any]) -> ProviderResult:
    diagnostics.setdefault("provider_attempted_by_name", {})
    diagnostics.setdefault("provider_success_by_name", {})
    diagnostics.setdefault("provider_empty_by_name", {})
    diagnostics.setdefault("provider_error_by_name", {})
    diagnostics.setdefault("provider_error_kind_by_name", {})
    cache = _load_cache()
    key = video_id
    now = dt.datetime.now(dt.timezone.utc)
    ttl = dt.timedelta(days=_cache_ttl_days())
    cached = cache.get(key)
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
        "description": DescriptionProvider(),
        "timedtext": TimedTextProvider(),
        "yt_dlp_captions": YtDlpCaptionsProvider(),
        "metadata_only": MetadataOnlyProvider(),
        "external_api": ExternalTranscriptApiProvider(),
        "asr": ASRProvider(),
    }
    order = _provider_order()
    if (os.getenv("YOUTUBE_TRANSCRIPT_API_PROVIDER") or "").strip().lower() == "external" and "external_api" not in order:
        order = ["external_api"] + order

    for name in order:
        provider = providers.get(name)
        if provider is None:
            continue
        diagnostics["provider_attempted_by_name"][name] = diagnostics["provider_attempted_by_name"].get(name, 0) + 1
        result = provider.fetch(video_id=video_id, video_url=video_url, description=description, diagnostics=diagnostics)
        if result.status == "success" and result.text.strip():
            diagnostics["provider_success_by_name"][name] = diagnostics["provider_success_by_name"].get(name, 0) + 1
            cache_text = (os.getenv("YOUTUBE_CONTENT_CACHE_TEXT") or "1").strip() == "1"
            text = result.text.strip()
            cache[key] = {
                "status": "success",
                "source": result.source,
                "fetched_at_utc": now.isoformat(),
                "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                "text": text if cache_text else "",
            }
            _save_cache(cache)
            diagnostics["cache_write_total"] = int(diagnostics.get("cache_write_total", 0)) + 1
            return result
        if result.status == "empty":
            diagnostics["provider_empty_by_name"][name] = diagnostics["provider_empty_by_name"].get(name, 0) + 1
        else:
            diagnostics["provider_error_by_name"][name] = diagnostics["provider_error_by_name"].get(name, 0) + 1
            if result.error_kind:
                bucket = diagnostics["provider_error_kind_by_name"].setdefault(name, {})
                bucket[result.error_kind] = bucket.get(result.error_kind, 0) + 1

    return ProviderResult("empty", "", "metadata_only")
