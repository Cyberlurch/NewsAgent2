from __future__ import annotations

import calendar
import html
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Tuple
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

DEFAULT_USER_AGENT = "NewsAgent2/1.0 (+https://github.com/openai)"


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


def _entry_datetime(entry: Dict[str, Any]) -> Tuple[datetime | None, str | None]:
    """
    Robustly extract a datetime from common feed fields.

    Returns (dt_utc, source_field)
    """
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        val = entry.get(key)
        if val:
            try:
                return datetime.fromtimestamp(calendar.timegm(val), tz=timezone.utc), key
            except Exception:
                continue

    for key in ("published", "updated", "created", "dc:date"):
        raw = entry.get(key) or entry.get(key.replace(":", "_"))
        if not raw:
            continue
        try:
            dt = parsedate_to_datetime(str(raw))
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc), key
        except Exception:
            continue

    return None, None


def _session_with_retries(user_agent: str | None = None) -> requests.Session:
    session = requests.Session()
    if user_agent:
        session.headers.update({"User-Agent": user_agent})
    retry = Retry(total=2, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def collect_foamed_items(
    sources_config: List[Dict[str, Any]],
    now_utc: datetime,
    lookback_hours: int = 24,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch FOAMed/blog posts from RSS/Atom feeds.

    Returns (items, stats). Items have shape:
        {
            "source": "foamed",
            "foamed_source": <source name>,
            "channel": <source name>,
            "id": <normalized url>,
            "title": ..., "url": ..., "published_at": datetime (UTC),
            "text": <excerpt>,
        }

    Stats (non-sensitive) include per-source error counts and date coverage.
    """

    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    lookback_hours = max(1, int(lookback_hours))
    cutoff = now_utc - timedelta(hours=lookback_hours)

    session = _session_with_retries(DEFAULT_USER_AGENT)
    items: List[Dict[str, Any]] = []
    stats: Dict[str, Any] = {
        "sources_total": 0,
        "sources_ok": 0,
        "sources_failed": 0,
        "items_raw": 0,
        "items_with_date": 0,
        "items_date_unknown": 0,
        "kept_last24h": 0,
        "per_source": {},
    }

    for src in sources_config or []:
        if not isinstance(src, dict):
            continue

        name = (src.get("name") or "").strip()
        feed_url = (src.get("feed_url") or "").strip()
        homepage = (src.get("homepage") or "").strip()
        feed_type = (src.get("type") or "").strip().lower() or "rss"

        if not name or not feed_url:
            continue

        stats["sources_total"] += 1
        per_source = {
            "items_raw": 0,
            "items_with_date": 0,
            "items_date_unknown": 0,
            "kept_last24h": 0,
            "errors": 0,
        }

        total = 0
        kept = 0
        unknown_kept = 0

        try:
            resp = session.get(feed_url, timeout=10)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
        except Exception as e:
            per_source["errors"] += 1
            stats["sources_failed"] += 1
            print(f"[foamed] WARN source={name!r}: fetch/parse failed: {e!r}")
            stats["per_source"][name] = per_source
            continue

        entries = parsed.entries if hasattr(parsed, "entries") else []
        stats["sources_ok"] += 1

        for entry in entries:
            total += 1
            stats["items_raw"] += 1
            per_source["items_raw"] += 1

            link = str(entry.get("link") or "").strip()
            title = str(entry.get("title") or "").strip()
            if not link or not title:
                continue

            published_at, date_field = _entry_datetime(entry)
            if not published_at:
                per_source["items_date_unknown"] += 1
                stats["items_date_unknown"] += 1
            else:
                per_source["items_with_date"] += 1
                stats["items_with_date"] += 1

            # Skip items outside the lookback window, using UTC consistently.
            if published_at and (published_at < cutoff or published_at > now_utc + timedelta(minutes=5)):
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

            date_unknown_allowed = per_source["items_date_unknown"] <= 2
            if not published_at and not date_unknown_allowed:
                continue

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
                    "published_field": date_field,
                    "date_unknown": published_at is None,
                    "text": text,
                }
            )
            kept += 1
            if published_at:
                stats["kept_last24h"] += 1
                per_source["kept_last24h"] += 1
            else:
                unknown_kept += 1
                stats["kept_last24h"] += 1

        per_source["kept_last24h"] += unknown_kept
        stats["per_source"][name] = per_source
        print(f"[foamed] source={name!r}: fetched {total}, kept {kept} (unknown_dates={unknown_kept})")

    return items, stats


__all__ = ["collect_foamed_items"]
