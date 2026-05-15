from __future__ import annotations

import datetime as dt
import os
import re
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import feedparser
import requests


_CHANNEL_ID_RE = re.compile(r"/channel/(UC[0-9A-Za-z_-]{20,})")
_UC_RE = re.compile(r"\b(UC[0-9A-Za-z_-]{20,})\b")
_HANDLE_RE = re.compile(r"/(?:@)([^/?#]+)")


def _diag_inc(diagnostics: dict[str, Any] | None, key: str, amount: int = 1) -> None:
    if diagnostics is not None:
        diagnostics[key] = int(diagnostics.get(key, 0) or 0) + amount


def _utc(value: dt.datetime | None) -> dt.datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _parse_feed_dt(value: Any) -> dt.datetime | None:
    if not value:
        return None
    if isinstance(value, str):
        try:
            text = value.strip().replace("Z", "+00:00")
            return _utc(dt.datetime.fromisoformat(text))
        except Exception:
            pass
    try:
        parsed = feedparser._parse_date(value)  # type: ignore[attr-defined]
        if parsed:
            return dt.datetime(*parsed[:6], tzinfo=dt.timezone.utc)
    except Exception:
        return None
    return None


def _video_id_from_entry(entry: Any) -> str:
    for key in ("yt_videoid", "yt_videoId", "videoid", "id"):
        val = str(getattr(entry, key, "") or "").strip()
        if val.startswith("yt:video:"):
            val = val.rsplit(":", 1)[-1]
        if re.fullmatch(r"[0-9A-Za-z_-]{8,}", val):
            return val
    link = str(getattr(entry, "link", "") or "").strip()
    if link:
        qs = parse_qs(urlparse(link).query)
        if qs.get("v"):
            return qs["v"][0]
    return ""


def parse_youtube_rss(xml_text: str, *, hours: int = 24, max_items: int = 10, now_utc: dt.datetime | None = None) -> List[Dict[str, Any]]:
    if now_utc is None:
        now_utc = dt.datetime.now(dt.timezone.utc)
    else:
        now_utc = _utc(now_utc) or dt.datetime.now(dt.timezone.utc)
    cutoff = now_utc - dt.timedelta(hours=hours)
    feed = feedparser.parse(xml_text or "")
    channel_name = str(getattr(feed.feed, "title", "") or "").strip()
    out: List[Dict[str, Any]] = []
    for entry in getattr(feed, "entries", []) or []:
        video_id = _video_id_from_entry(entry)
        if not video_id:
            continue
        published = _parse_feed_dt(getattr(entry, "published", "") or getattr(entry, "published_parsed", None))
        updated = _parse_feed_dt(getattr(entry, "updated", "") or getattr(entry, "updated_parsed", None))
        effective_dt = published or updated
        if effective_dt is None:
            continue
        if not (cutoff <= effective_dt <= now_utc + dt.timedelta(hours=1)):
            continue
        title = str(getattr(entry, "title", "") or "").strip()
        url = str(getattr(entry, "link", "") or "").strip() or f"https://www.youtube.com/watch?v={video_id}"
        summary = str(getattr(entry, "summary", "") or getattr(entry, "description", "") or "").strip()
        entry_channel = str(getattr(entry, "author", "") or channel_name).strip()
        out.append(
            {
                "id": video_id,
                "title": title,
                "channel": entry_channel,
                "published_at": effective_dt,
                "updated_at": updated,
                "url": url,
                "description": summary,
                "_metadata_source": "youtube_rss",
            }
        )
        if len(out) >= max_items:
            break
    return out


def resolve_channel_id(channel: Dict[str, Any], *, timeout_s: float = 10.0, diagnostics: dict[str, Any] | None = None) -> str:
    configured = str(channel.get("channel_id") or "").strip()
    if configured.startswith("UC"):
        return configured
    url = str(channel.get("url") or "").strip()
    m = _CHANNEL_ID_RE.search(url)
    if m:
        return m.group(1)

    handle = ""
    hm = _HANDLE_RE.search(url)
    if hm:
        handle = hm.group(1).strip()

    api_key = (os.getenv("YOUTUBE_API_KEY") or "").strip()
    if api_key and handle:
        try:
            resp = requests.get(
                "https://www.googleapis.com/youtube/v3/channels",
                params={"part": "id", "forHandle": handle, "key": api_key},
                timeout=timeout_s,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items") or []
            if items and str(items[0].get("id") or "").startswith("UC"):
                return str(items[0].get("id"))
        except Exception:
            _diag_inc(diagnostics, "rss_fallback_error_total")

    if url:
        try:
            resp = requests.get(url, timeout=timeout_s, headers={"User-Agent": "NewsAgent2/1.0"})
            resp.raise_for_status()
            text = resp.text or ""
            for pattern in (
                r'"channelId"\s*:\s*"(UC[0-9A-Za-z_-]{20,})"',
                r'"externalId"\s*:\s*"(UC[0-9A-Za-z_-]{20,})"',
                r'"browseId"\s*:\s*"(UC[0-9A-Za-z_-]{20,})"',
            ):
                match = re.search(pattern, text)
                if match:
                    return match.group(1)
            match = _UC_RE.search(text)
            if match:
                return match.group(1)
        except Exception:
            _diag_inc(diagnostics, "rss_fallback_error_total")

    return ""


def list_recent_videos_rss(
    channel: Dict[str, Any],
    *,
    hours: int = 24,
    max_items: int = 10,
    diagnostics: dict[str, Any] | None = None,
    now_utc: dt.datetime | None = None,
) -> List[Dict[str, Any]]:
    _diag_inc(diagnostics, "rss_fallback_attempted_total")
    channel_id = resolve_channel_id(channel, diagnostics=diagnostics)
    if not channel_id:
        _diag_inc(diagnostics, "rss_fallback_resolution_failed_total")
        return []
    try:
        resp = requests.get(
            "https://www.youtube.com/feeds/videos.xml",
            params={"channel_id": channel_id},
            timeout=10,
            headers={"User-Agent": "NewsAgent2/1.0"},
        )
        resp.raise_for_status()
        items = parse_youtube_rss(resp.text, hours=hours, max_items=max_items, now_utc=now_utc)
        if items:
            _diag_inc(diagnostics, "rss_fallback_success_total")
            _diag_inc(diagnostics, "videos_listed_total", len(items))
            _diag_inc(diagnostics, "videos_kept_after_date_total", len(items))
        return items
    except Exception:
        _diag_inc(diagnostics, "rss_fallback_error_total")
        return []
