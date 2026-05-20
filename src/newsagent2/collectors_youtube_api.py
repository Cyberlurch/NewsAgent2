from __future__ import annotations

from typing import Any

import requests


def _inc(diagnostics: dict[str, Any], key: str, amount: int = 1) -> None:
    diagnostics[key] = int(diagnostics.get(key, 0) or 0) + amount


def fetch_video_snippets(video_ids: list[str], api_key: str, diagnostics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    cleaned_ids = [str(v).strip() for v in video_ids if str(v).strip()]
    _inc(diagnostics, "youtube_api_metadata_attempted_total", len(cleaned_ids))
    if not cleaned_ids:
        return {}

    try:
        resp = requests.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={
                "part": "snippet,contentDetails,status",
                "id": ",".join(cleaned_ids[:50]),
                "key": api_key,
            },
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
    except Exception:
        _inc(diagnostics, "youtube_api_metadata_error_total", len(cleaned_ids))
        return {}

    items = payload.get("items") or []
    _inc(diagnostics, "youtube_api_metadata_items_returned_total", len(items))
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        vid = str(item.get("id") or "").strip()
        if not vid:
            continue
        snippet = item.get("snippet") if isinstance(item.get("snippet"), dict) else {}
        content_details = item.get("contentDetails") if isinstance(item.get("contentDetails"), dict) else {}
        status = item.get("status") if isinstance(item.get("status"), dict) else {}
        out[vid] = {
            "id": vid,
            "title": str(snippet.get("title") or "").strip(),
            "description": str(snippet.get("description") or "").strip(),
            "channel": str(snippet.get("channelTitle") or "").strip(),
            "channel_id": str(snippet.get("channelId") or "").strip(),
            "published_at": str(snippet.get("publishedAt") or "").strip(),
            "caption": status.get("madeForKids") if "caption" not in content_details else content_details.get("caption"),
            "duration": str(content_details.get("duration") or "").strip(),
        }

    _inc(diagnostics, "youtube_api_metadata_success_total", len(out))
    _inc(diagnostics, "youtube_api_metadata_empty_total", max(0, len(cleaned_ids) - len(out)))
    return out
