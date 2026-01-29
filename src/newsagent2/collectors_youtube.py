# src/newsagent2/collectors_youtube.py
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Sequence
import re
import tempfile
from pathlib import Path

import yt_dlp
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

from .utils.text_quality import vtt_to_text

STO = ZoneInfo("Europe/Stockholm")
CAPTIONS_MIN_CHARS = 200
DESCRIPTION_MIN_CHARS = 100


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

            description = (e.get("description") or "").strip()
            if len(description) < DESCRIPTION_MIN_CHARS:
                description = _fetch_full_video_description(vid) or description

            out.append(
                {
                    "id": vid,
                    "title": title,
                    "channel": e.get("uploader") or handle,
                    "published_at": published,
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "description": description,
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
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        return None
    except Exception:
        return None

    for lang in langs:
        try:
            transcript = transcript_list.find_manually_created_transcript([lang])
        except Exception:
            transcript = None
        if transcript:
            text = " ".join(seg["text"] for seg in transcript.fetch() if seg.get("text"))
            return text.strip() or None

    for lang in langs:
        try:
            transcript = transcript_list.find_generated_transcript([lang])
        except Exception:
            transcript = None
        if transcript:
            text = " ".join(seg["text"] for seg in transcript.fetch() if seg.get("text"))
            return text.strip() or None

    for transcript in transcript_list:
        if getattr(transcript, "is_generated", False):
            try:
                text = " ".join(seg["text"] for seg in transcript.fetch() if seg.get("text"))
                return text.strip() or None
            except Exception:
                return None

    return None


def fetch_captions_text(
    video_url_or_id: str,
    preferred_lang_patterns: Sequence[str],
    *,
    timeout_s: float = 60.0,
    retries: int = 0,
) -> tuple[str, str, str]:
    """
    Fetch captions (preferring auto-generated) without downloading the video.
    Returns (text, status) where status is one of: success, empty, error.
    """
    target = (video_url_or_id or "").strip()
    if not target:
        return "", "empty", ""

    attempt = 0
    last_error_kind = "unknown"
    while attempt <= retries:
        attempt += 1
        with tempfile.TemporaryDirectory() as tmpdir:
            out_tmpl = str(Path(tmpdir) / "%(id)s.%(ext)s")
            ydl_opts = {
                "quiet": True,
                "skip_download": True,
                "writesubtitles": True,
                "writeautomaticsub": True,
                "subtitleslangs": list(preferred_lang_patterns),
                "subtitlesformat": "vtt/best",
                "outtmpl": out_tmpl,
                "nocheckcertificate": True,
                "socket_timeout": timeout_s,
                "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
            }

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([target])
            except Exception as exc:
                last_error_kind = classify_captions_error_kind(str(exc))
                if attempt <= retries:
                    continue
                return "", "error", last_error_kind

            vtt_files = [
                f
                for f in Path(tmpdir).glob("*.vtt")
                if "live_chat" not in f.name.lower()
            ]
            if not vtt_files:
                return "", "empty", ""

            best_text = ""
            for vtt_file in vtt_files:
                try:
                    content = vtt_file.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                text = vtt_to_text(content)
                if len(text) >= CAPTIONS_MIN_CHARS and len(text) > len(best_text):
                    best_text = text

            if not best_text:
                return "", "empty", ""

            return best_text.strip(), "success", ""

    return "", "error", last_error_kind


def fetch_youtube_captions_text(
    url: str, lang_priority: Sequence[str] = ("en", "en-US", "en-GB"), *, timeout_s: float = 25.0
) -> str:
    text, status, _ = fetch_captions_text(url, lang_priority, timeout_s=timeout_s)
    return text if status == "success" else ""


def classify_captions_error_kind(message: str) -> str:
    msg = (message or "").lower()
    if not msg:
        return "unknown"
    if "timed out" in msg or "timeout" in msg:
        return "timeout"
    if "no subtitles" in msg or "there are no subtitles for the requested languages" in msg:
        return "no_subtitles"
    if "http error 403" in msg or "403 forbidden" in msg:
        return "http_403"
    if "http error 429" in msg or "too many requests" in msg:
        return "http_429"
    if "not a bot" in msg or "sign in to confirm" in msg:
        return "bot_check"
    if "no such option" in msg:
        return "cli_option_error"
    if "nsig" in msg or "unable to extract" in msg:
        return "extract_failed"
    return "unknown"


def _fetch_full_video_description(video_id: str) -> str:
    if not video_id:
        return ""
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "ignoreerrors": True,
        "nocheckcertificate": True,
    }
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            if isinstance(info, dict):
                return (info.get("description") or "").strip()
    except Exception:
        return ""
    return ""


def get_yt_dlp_version() -> str:
    try:
        return yt_dlp.version.__version__
    except Exception:
        return "unknown"
