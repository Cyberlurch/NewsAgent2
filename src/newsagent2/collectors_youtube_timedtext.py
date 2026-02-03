from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from typing import Iterable, Sequence
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import requests


@dataclass(frozen=True)
class TimedTextTrack:
    lang_code: str
    name: str = ""
    track_id: str = ""
    kind: str = ""
    lang_default: str = ""


def _parse_timedtext_list(xml_text: str) -> list[TimedTextTrack]:
    if not xml_text or not xml_text.strip():
        return []
    root = ET.fromstring(xml_text)
    tracks: list[TimedTextTrack] = []
    for track in root.findall(".//track"):
        tracks.append(
            TimedTextTrack(
                lang_code=track.attrib.get("lang_code", ""),
                name=track.attrib.get("name", ""),
                track_id=track.attrib.get("id", ""),
                kind=track.attrib.get("kind", ""),
                lang_default=track.attrib.get("lang_default", ""),
            )
        )
    return tracks


def list_timedtext_tracks(video_id: str, *, timeout_s: float = 15.0) -> list[dict]:
    if not video_id:
        return []
    params = {"type": "list", "v": video_id}
    url = f"https://video.google.com/timedtext?{urlencode(params)}"
    resp = requests.get(url, timeout=timeout_s)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    tracks = _parse_timedtext_list(resp.text)
    if not tracks:
        return []
    return [
        {
            "lang_code": t.lang_code,
            "name": t.name,
            "id": t.track_id,
            "kind": t.kind,
            **({"lang_default": t.lang_default} if t.lang_default else {}),
        }
        for t in tracks
    ]


def _parse_timedtext_track(xml_text: str) -> str:
    if not xml_text or not xml_text.strip():
        return ""
    root = ET.fromstring(xml_text)
    parts: list[str] = []
    for node in root.findall(".//text"):
        if node.text:
            parts.append(unescape(node.text))
    if not parts:
        return ""
    combined = " ".join(parts)
    combined = re.sub(r"\s+", " ", combined).strip()
    return combined


def fetch_timedtext_track(
    video_id: str, track: dict, *, timeout_s: float = 15.0
) -> str:
    if not video_id or not track:
        return ""
    lang_code = (track.get("lang_code") or "").strip()
    if not lang_code:
        return ""
    params = {
        "type": "track",
        "v": video_id,
        "lang": lang_code,
    }
    name = (track.get("name") or "").strip()
    if name:
        params["name"] = name
    if (track.get("kind") or "").strip().lower() == "asr":
        params["kind"] = "asr"
    url = f"https://video.google.com/timedtext?{urlencode(params)}"
    resp = requests.get(url, timeout=timeout_s)
    if resp.status_code == 404:
        return ""
    resp.raise_for_status()
    return _parse_timedtext_track(resp.text)


def _lang_matches(track_lang: str, desired: str) -> bool:
    if not track_lang or not desired:
        return False
    track_lang_lower = track_lang.lower()
    desired_lower = desired.lower()
    return track_lang_lower == desired_lower or track_lang_lower.startswith(
        f"{desired_lower}-"
    )


def _find_best_track(
    tracks: Iterable[dict],
    lang_priority: Sequence[str],
    *,
    prefer_asr: bool,
) -> dict | None:
    filtered = [t for t in tracks if ((t.get("kind") or "") == "asr") == prefer_asr]
    for lang in lang_priority:
        for track in filtered:
            if _lang_matches(track.get("lang_code", ""), lang):
                return track
    return None


def choose_best_track(
    tracks: Sequence[dict], lang_priority: Sequence[str] = ("en", "de")
) -> dict | None:
    if not tracks:
        return None
    manual = _find_best_track(tracks, lang_priority, prefer_asr=False)
    if manual:
        return manual
    asr = _find_best_track(tracks, lang_priority, prefer_asr=True)
    if asr:
        return asr
    for track in tracks:
        if track.get("lang_default"):
            return track
    return tracks[0]


def fetch_captions_via_timedtext(
    video_id: str, lang_priority: Sequence[str] = ("en", "de")
) -> tuple[str, str]:
    try:
        tracks = list_timedtext_tracks(video_id)
    except requests.RequestException:
        return "", "error_http"
    except ET.ParseError:
        return "", "error_parse"

    if not tracks:
        return "", "error_no_tracks"

    track = choose_best_track(tracks, lang_priority)
    if not track:
        return "", "error_no_tracks"

    try:
        text = fetch_timedtext_track(video_id, track)
    except requests.RequestException:
        return "", "error_http"
    except ET.ParseError:
        return "", "error_parse"

    if not text:
        return "", "empty"
    return text, "success"
