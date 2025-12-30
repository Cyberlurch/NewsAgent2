# src/newsagent2/collectors_youtube.py
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Sequence
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import yt_dlp
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

from .utils.text_quality import parse_vtt_to_text

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


def _pick_vtt_file(files: List[Path], lang_priority: Sequence[str]) -> Path | None:
    if not files:
        return None

    langs = [l.lower() for l in lang_priority if l]
    for lang in langs:
        for f in files:
            name = f.name.lower()
            if name.endswith(f".{lang}.vtt") or f".{lang}." in name:
                return f
    return files[0]


def fetch_youtube_captions_text(
    url: str, lang_priority: Sequence[str] = ("en", "en-US", "en-GB"), *, timeout_s: float = 25.0
) -> str:
    """
    Fetch captions (preferring auto-generated) without downloading the video.
    Returns plain text (VTT stripped) truncated to ~7000 chars.
    """
    target_url = (url or "").strip()
    if not target_url:
        return ""

    with tempfile.TemporaryDirectory() as tmpdir:
        out_tmpl = str(Path(tmpdir) / "%(id)s.%(ext)s")
        cmd = [
            sys.executable,
            "-m",
            "yt_dlp",
            "--skip-download",
            "--write-auto-subs",
            "--write-subs",
            "--sub-lang",
            "en.*",
            "--sub-format",
            "vtt",
            "-o",
            out_tmpl,
            target_url,
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        except Exception:
            return ""

        vtt_files = sorted(Path(tmpdir).glob("*.vtt"))
        if res.returncode != 0 and not vtt_files:
            return ""
        if not vtt_files:
            return ""

        chosen = _pick_vtt_file(list(vtt_files), lang_priority) or vtt_files[0]
        try:
            content = chosen.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""

        text = parse_vtt_to_text(content, max_chars=7000)
        return text.strip()
