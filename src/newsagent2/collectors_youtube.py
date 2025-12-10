# src/newsagent2/collectors_youtube.py
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo
from typing import List, Dict, Any
import re

import yt_dlp
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

STO = ZoneInfo("Europe/Stockholm")


def _channel_handle_from_url(url: str) -> str:
    """
    Extrahiert den @handle aus einer YouTube-Kanal-URL.
    Fällt zurück auf die URL selbst, falls kein Handle-Muster gefunden wird.
    """
    m = re.search(r"/@([^/]+)/?", url)
    return m.group(1) if m else url


def list_recent_videos(
    channel_url: str,
    hours: int = 24,
    max_items: int = 10,
) -> List[Dict[str, Any]]:
    """
    Liste die jüngsten Videos (ohne Download) eines Kanals via yt-dlp (flat playlist).

    Filter:
      - nur Videos der letzten `hours` Stunden (basierend auf upload_date),
      - begrenzt auf `max_items` Einträge.

    Rückgabe pro Video:
      {
        "id": str,
        "title": str,
        "channel": str,
        "published_at": datetime (tz-aware, UTC),
        "url": str,
        "description": str,
      }
    """
    handle = _channel_handle_from_url(channel_url)

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "playlistend": max_items,
        "skip_download": True,
        "ignoreerrors": True,
        "nocheckcertificate": True,
    }

    url = f"https://www.youtube.com/@{handle}/videos"

    now_utc = dt.datetime.now(dt.timezone.utc)
    cutoff = now_utc - dt.timedelta(hours=hours)

    out: List[Dict[str, Any]] = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        entries = info.get("entries") or []

        for e in entries:
            vid = e.get("id")
            if not vid:
                continue

            title = e.get("title") or ""

            # yt-dlp liefert upload_date als YYYYMMDD, gelegentlich nicht gesetzt
            up = e.get("upload_date")
            if up:
                try:
                    published = dt.datetime.strptime(up, "%Y%m%d").replace(
                        tzinfo=dt.timezone.utc
                    )
                except ValueError:
                    published = now_utc
            else:
                # Fallback: behandeln als "jetzt" -> gilt als jüngst
                published = now_utc

            if published < cutoff:
                continue

            out.append(
                {
                    "id": vid,
                    "title": title,
                    "channel": e.get("uploader") or handle,
                    "published_at": published,
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "description": e.get("description") or "",
                }
            )

    return out


def fetch_transcript(video_id: str) -> str | None:
    """
    Hole Transkript für ein Video, bevorzugt Originalsprache (Deutsch/Englisch/Schwedisch),
    akzeptiere auch auto-generierte Untertitel.

    Rückgabe:
      - Volltext des Transkripts (stripped) oder
      - None, falls kein Transkript verfügbar ist.
    """
    # Reihenfolge beliebter Sprachen (keine Übersetzung!)
    langs = ["de", "en", "sv", "en-US", "de-DE", "sv-SE"]

    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=langs)
        text = " ".join(seg["text"] for seg in transcript if seg.get("text"))
        return text.strip() or None
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        return None
    except Exception:
        return None
