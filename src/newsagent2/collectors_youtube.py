# src/newsagent2/collectors_youtube.py
from __future__ import annotations

import datetime as dt
import os
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


def _diag_inc(diagnostics: dict[str, int] | None, key: str, amount: int = 1) -> None:
    if diagnostics is not None:
        diagnostics[key] = int(diagnostics.get(key, 0) or 0) + amount


def _utc_from_epoch(value: Any) -> dt.datetime | None:
    try:
        if value is None or value == "":
            return None
        return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _upload_date_value(value: Any) -> dt.date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return dt.datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def _published_at_from_entry(entry: Dict[str, Any]) -> tuple[dt.datetime | None, bool]:
    """
    Return (published_at_utc, is_date_granular).

    yt-dlp flat playlist entries often only expose upload_date=YYYYMMDD. That is a
    date bucket, not proof that the video was published at 00:00 UTC. For those
    entries, use noon UTC as a stable representative timestamp while keeping the
    date-granular flag so filtering can compare calendar dates instead of falsely
    dropping early-morning runs' previous-day uploads.
    """
    for key in ("timestamp", "release_timestamp"):
        published = _utc_from_epoch(entry.get(key))
        if published is not None:
            return published, False

    upload_date = _upload_date_value(entry.get("upload_date"))
    if upload_date is not None:
        return dt.datetime.combine(upload_date, dt.time(12, 0), tzinfo=dt.timezone.utc), True

    return None, False


def _is_plausibly_recent(
    published: dt.datetime | None,
    *,
    date_granular: bool,
    cutoff: dt.datetime,
    now_utc: dt.datetime,
) -> bool:
    if published is None:
        return True
    if date_granular:
        return published.date() >= cutoff.date()
    return cutoff <= published <= now_utc + dt.timedelta(hours=1)


def _needs_metadata_enrichment(
    published: dt.datetime | None,
    *,
    date_granular: bool,
    cutoff: dt.datetime,
) -> bool:
    if date_granular or published is None:
        return True
    return abs((published - cutoff).total_seconds()) <= 6 * 3600


def _fetch_full_video_metadata(
    video_url: str,
    diagnostics: dict[str, int] | None = None,
) -> Dict[str, Any] | None:
    _diag_inc(diagnostics, "metadata_enrichment_attempted_total")
    try:
        with yt_dlp.YoutubeDL(
            {
                "quiet": True,
                "skip_download": True,
                "ignoreerrors": True,
                "nocheckcertificate": True,
            }
        ) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except Exception:
        _diag_inc(diagnostics, "metadata_enrichment_error_total")
        return None

    if not isinstance(info, dict):
        _diag_inc(diagnostics, "metadata_enrichment_error_total")
        return None

    _diag_inc(diagnostics, "metadata_enrichment_success_total")
    return info


def _scan_limit(max_items: int) -> int:
    raw = (os.getenv("YOUTUBE_LIST_SCAN_LIMIT") or "").strip()
    if raw:
        try:
            configured = int(raw)
            if configured > 0:
                return min(configured, 500)
        except ValueError:
            pass
    return min(max(50, max_items * 5), 100)


def list_recent_videos(
    channel_url: str,
    hours: int = 24,
    max_items: int = 10,
    diagnostics: dict[str, int] | None = None,
    now_utc: dt.datetime | None = None,
) -> List[Dict[str, Any]]:
    """
    Liste die jüngsten Videos (ohne Download) eines Kanals via yt-dlp (flat playlist).

    Filter:
      - nur Videos der letzten `hours` Stunden,
      - date-only upload_date=YYYYMMDD is treated as date-granular metadata,
      - begrenzt auf `max_items` zurückgegebene Einträge.

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
    scan_limit = _scan_limit(max_items)

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "playlistend": scan_limit,
        "skip_download": True,
        "ignoreerrors": True,
        "nocheckcertificate": True,
    }

    url = f"https://www.youtube.com/@{handle}/videos"

    if now_utc is None:
        now_utc = dt.datetime.now(dt.timezone.utc)
    elif now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
    else:
        now_utc = now_utc.astimezone(dt.timezone.utc)
    cutoff = now_utc - dt.timedelta(hours=hours)

    out: List[Dict[str, Any]] = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        entries = info.get("entries") or [] if isinstance(info, dict) else []

        for e in entries:
            if not isinstance(e, dict):
                continue
            _diag_inc(diagnostics, "videos_listed_total")
            vid = e.get("id")
            if not vid:
                continue

            published, date_granular = _published_at_from_entry(e)
            video_url = f"https://www.youtube.com/watch?v={vid}"
            enriched = False
            metadata_attempted = False

            if not _is_plausibly_recent(published, date_granular=date_granular, cutoff=cutoff, now_utc=now_utc):
                # A precise timestamp just outside the cutoff may be stale flat metadata; try
                # one full metadata fetch before deciding. Date-only entries from before
                # cutoff.date() are not plausible and can be skipped without enrichment.
                if (not date_granular) and _needs_metadata_enrichment(published, date_granular=date_granular, cutoff=cutoff):
                    metadata_attempted = True
                    full = _fetch_full_video_metadata(video_url, diagnostics)
                    if full:
                        full_published, full_date_granular = _published_at_from_entry(full)
                        if full_published is not None:
                            published, date_granular = full_published, full_date_granular
                        e = {**e, **full}
                        enriched = True
                if not _is_plausibly_recent(published, date_granular=date_granular, cutoff=cutoff, now_utc=now_utc):
                    _diag_inc(diagnostics, "videos_skipped_by_date_total")
                    continue
            if _needs_metadata_enrichment(published, date_granular=date_granular, cutoff=cutoff):
                metadata_attempted = True
                full = _fetch_full_video_metadata(video_url, diagnostics)
                if full:
                    full_published, full_date_granular = _published_at_from_entry(full)
                    if full_published is not None:
                        published, date_granular = full_published, full_date_granular
                    e = {**e, **full}
                    enriched = True

            if not _is_plausibly_recent(published, date_granular=date_granular, cutoff=cutoff, now_utc=now_utc):
                _diag_inc(diagnostics, "videos_skipped_by_date_total")
                continue

            if published is None:
                published = now_utc

            description = (e.get("description") or "").strip()
            if len(description) < DESCRIPTION_MIN_CHARS and not enriched and not metadata_attempted:
                full = _fetch_full_video_metadata(video_url, diagnostics)
                if full:
                    full_published, full_date_granular = _published_at_from_entry(full)
                    if full_published is not None:
                        published, date_granular = full_published, full_date_granular
                    if _is_plausibly_recent(published, date_granular=date_granular, cutoff=cutoff, now_utc=now_utc):
                        e = {**e, **full}
                        description = (e.get("description") or "").strip()
                    else:
                        _diag_inc(diagnostics, "videos_skipped_by_date_total")
                        continue

            _diag_inc(diagnostics, "videos_kept_after_date_total")
            out.append(
                {
                    "id": vid,
                    "title": e.get("title") or "",
                    "channel": e.get("uploader") or e.get("channel") or handle,
                    "published_at": published,
                    "url": video_url,
                    "description": description,
                }
            )
            if len(out) >= max_items:
                break

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
                "extractor_args": {
                    "youtube": {"player_client": ["android", "web_safari", "web"]}
                },
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
