from __future__ import annotations

import datetime as dt
import os
import re
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import feedparser
import requests




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


def resolve_identity(channel, *, timeout_s=10.0, diagnostics=None, client=None, allow_api=True):
    from .youtube_source import configured_identity, owner_identity, Identity, CHANNEL_ID
    identity = configured_identity(channel)
    if identity.status != "unresolved":
        return identity
    client = client or requests
    url = identity.source_url
    handle = url.rsplit("/@", 1)[-1] if "/@" in url else ""
    api_key = (os.getenv("YOUTUBE_API_KEY") or "").strip() if allow_api else ""
    # At most one forHandle request and one owner-page fallback. A failed API
    # request is counted separately from the video enrichment ID budget.
    if api_key and handle:
        _diag_inc(diagnostics, "youtube_identity_requests_total")
        try:
            response = client.get("https://www.googleapis.com/youtube/v3/channels",
                params={"part": "id", "forHandle": handle, "key": api_key}, timeout=timeout_s)
            response.raise_for_status()
            items = response.json().get("items", [])
            if len(items) == 1 and CHANNEL_ID.fullmatch(items[0].get("id", "")):
                return Identity("resolved", items[0]["id"], "for_handle", url)
            if len(items) > 1:
                return Identity("conflict", evidence="ambiguous_for_handle", source_url=url)
        except Exception:
            _diag_inc(diagnostics, "youtube_identity_errors_total")
    if url:
        _diag_inc(diagnostics, "youtube_identity_requests_total")
        try:
            response = client.get(url, timeout=timeout_s, headers={"User-Agent": "NewsAgent2/1.0"})
            response.raise_for_status()
            actual_url = getattr(response, "url", url)
            if isinstance(actual_url, str) and "consent." in actual_url:
                return Identity(evidence="consent", source_url=url)
            result = owner_identity(response.text or "", url)
            if result.status == "unresolved" and any(host in (response.text or "") for host in ("consent.youtube.com", "consent.google.com")):
                result.evidence = "consent"
            return result
        except requests.exceptions.SSLError:
            return Identity(evidence="tls_error", source_url=url)
        except Exception:
            _diag_inc(diagnostics, "youtube_identity_errors_total")
    return identity


def resolve_channel_id(channel: Dict[str, Any], *, timeout_s: float = 10.0, diagnostics=None) -> str:
    """Compatibility adapter; a legacy name cache is never verification."""
    result = resolve_identity(channel, timeout_s=timeout_s, diagnostics=diagnostics)
    return result.channel_id if result.status in {"configured", "resolved"} else ""


def list_recent_videos_rss(channel, *, hours=24, max_items=10, diagnostics=None, now_utc=None,
                           client=None, allow_api=True):
    from .youtube_source import Listing, Identity
    import xml.etree.ElementTree as ET
    _diag_inc(diagnostics, "rss_fallback_attempted_total")
    identity = resolve_identity(channel, diagnostics=diagnostics, client=client, allow_api=allow_api)
    if identity.status not in {"configured", "resolved"}:
        _diag_inc(diagnostics, "rss_fallback_resolution_failed_total")
        return Listing(status="identity_conflict" if identity.status == "conflict" else "identity_unresolved", identity=identity)
    client = client or requests
    http_status = None
    try:
        response = client.get("https://www.youtube.com/feeds/videos.xml", params={"channel_id": identity.channel_id},
                              timeout=10, headers={"User-Agent": "NewsAgent2/1.0"})
        http_status = response.status_code
        response.raise_for_status()
    except requests.exceptions.SSLError:
        _diag_inc(diagnostics, "rss_fallback_error_total")
        return Listing(status="tls_error", identity=identity)
    except Exception:
        _diag_inc(diagnostics, "rss_fallback_error_total")
        return Listing(status="access_blocked" if http_status in {403, 429} else "fetch_error", identity=identity, http_status=http_status)
    text = response.text or ""
    try:
        root = ET.fromstring(text)
        atom = "{http://www.w3.org/2005/Atom}"
        if root.tag != atom + "feed":
            raise ValueError("not_atom_feed")
        feed_id = root.findtext(atom + "id", "")
        if feed_id.startswith("yt:channel:") and feed_id.rsplit(":", 1)[-1] != identity.channel_id:
            return Listing(status="identity_conflict", identity=Identity("conflict", identity.channel_id, "feed_mismatch", identity.source_url), http_status=http_status)
        entries = root.findall(atom + "entry")
        uploaders = [e.findtext("{http://www.youtube.com/xml/schemas/2015}channelId", "") for e in [root] + entries]
        if any(cid and cid != identity.channel_id for cid in uploaders):
            return Listing(status="identity_conflict", identity=Identity("conflict", identity.channel_id, "feed_mismatch", identity.source_url), http_status=http_status)
        items = parse_youtube_rss(text, hours=hours, max_items=max_items, now_utc=now_utc)
        # A malformed entry is not evidence of a valid old/empty channel.
        if entries and not items:
            parsed = feedparser.parse(text)
            if any(not _video_id_from_entry(e) or not _parse_feed_dt(getattr(e, "published", "") or getattr(e, "updated", "")) for e in parsed.entries):
                return Listing(status="parse_error", identity=identity, http_status=http_status)
        _diag_inc(diagnostics, "rss_fallback_success_total")
        _diag_inc(diagnostics, "videos_listed_total", len(entries))
        _diag_inc(diagnostics, "videos_kept_after_date_total", len(items))
        return Listing(items, status="items" if items else ("outside_window" if entries else "empty_valid"),
                       identity=identity, raw_count=len(entries), http_status=http_status)
    except (ValueError, ET.ParseError):
        _diag_inc(diagnostics, "rss_fallback_error_total")
        return Listing(status="consent_or_html" if "<html" in text.lower() else "parse_error",
                       identity=identity, http_status=http_status)
