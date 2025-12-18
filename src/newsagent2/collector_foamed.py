from __future__ import annotations

import calendar
import html
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TRACKING_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "oly_anon_id",
    "oly_enc_id",
    "ref",
}


def _clean_url(url: str) -> str:
    parsed = urlsplit(url)
    q = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        lk = k.lower()
        if lk in TRACKING_KEYS or lk.startswith("utm_"):
            continue
        q.append((k, v))

    cleaned_query = urlencode(q, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, cleaned_query, "")) or url


def _strip_html(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return html.unescape(cleaned).strip()


def _entry_datetime(entry: Dict[str, Any]) -> datetime | None:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        val = entry.get(key)
        if val:
            try:
                return datetime.fromtimestamp(calendar.timegm(val), tz=timezone.utc)
            except Exception:
                continue
    return None


def _session_with_retries() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def collect_foamed_items(sources_config: List[Dict[str, Any]], now_utc: datetime, lookback_hours: int = 24) -> List[Dict[str, Any]]:
    """
    Fetch FOAMed/blog posts from RSS/Atom feeds.

    Returns items with shape:
        {
            "source": "foamed",
            "foamed_source": <source name>,
            "channel": <source name>,
            "id": <normalized url>,
            "title": ..., "url": ..., "published_at": datetime (UTC),
            "text": <excerpt>,
        }
    """

    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    lookback_hours = max(1, int(lookback_hours))
    cutoff = now_utc - timedelta(hours=lookback_hours)

    session = _session_with_retries()
    items: List[Dict[str, Any]] = []

    for src in sources_config or []:
        if not isinstance(src, dict):
            continue

        name = (src.get("name") or "").strip()
        feed_url = (src.get("feed_url") or "").strip()
        homepage = (src.get("homepage") or "").strip()
        feed_type = (src.get("type") or "").strip().lower() or "rss"

        if not name or not feed_url:
            continue

        total = 0
        kept = 0

        try:
            resp = session.get(feed_url, timeout=10)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
        except Exception as e:
            print(f"[foamed] WARN source={name!r}: fetch/parse failed: {e!r}")
            continue

        entries = parsed.entries if hasattr(parsed, "entries") else []

        for entry in entries:
            total += 1
            link = str(entry.get("link") or "").strip()
            title = str(entry.get("title") or "").strip()
            if not link or not title:
                continue

            published_at = _entry_datetime(entry)
            if not published_at:
                continue

            # Skip items outside the lookback window, using UTC consistently.
            if published_at < cutoff or published_at > now_utc + timedelta(minutes=5):
                continue

            url = _clean_url(link)

            content_val = ""
            contents = entry.get("content")
            if isinstance(contents, list) and contents:
                content_val = str(contents[0].get("value") or "")
            if not content_val:
                content_val = str(entry.get("summary") or entry.get("description") or "")

            text = _strip_html(content_val)
            if not text:
                text = "(No excerpt provided)"

            items.append(
                {
                    "source": "foamed",
                    "foamed_source": name,
                    "channel": name,
                    "id": url,
                    "title": title,
                    "url": url,
                    "homepage": homepage,
                    "feed_type": feed_type,
                    "published_at": published_at,
                    "text": text,
                }
            )
            kept += 1

        print(f"[foamed] source={name!r}: fetched {total}, kept {kept}")

    return items


__all__ = ["collect_foamed_items"]
