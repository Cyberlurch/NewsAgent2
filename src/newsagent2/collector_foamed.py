from __future__ import annotations

import calendar
import html
import json
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
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


def _safe_parse_date(value: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = dateparser.parse(value)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


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


def _find_json_ld_dates(soup: BeautifulSoup) -> Iterable[str]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(script.get_text("\n"))
        except Exception:
            continue

        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            continue

        for obj in data:
            if not isinstance(obj, dict):
                continue
            for key in ("datePublished", "dateModified", "uploadDate"):
                val = obj.get(key)
                if isinstance(val, str) and val.strip():
                    yield val.strip()


def _extract_published_datetime(soup: BeautifulSoup) -> datetime | None:
    # JSON-LD first (richest signal)
    for candidate in _find_json_ld_dates(soup):
        dt = _safe_parse_date(candidate)
        if dt:
            return dt

    # OpenGraph / article meta tags
    meta_props = [
        ("property", "article:published_time"),
        ("property", "article:modified_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "timestamp"),
        ("name", "date"),
        ("name", "dcterms.date"),
    ]
    for attr, key in meta_props:
        tag = soup.find("meta", attrs={attr: key})
        if tag and tag.get("content"):
            dt = _safe_parse_date(tag.get("content", ""))
            if dt:
                return dt

    # <time datetime="...">
    for t in soup.find_all("time"):
        dt = _safe_parse_date(t.get("datetime") or t.get_text(" "))
        if dt:
            return dt

    return None


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
            "feed_ok": False,
            "feed_failed": False,
            "discovered_feed_used": False,
            "html_fallback_used": False,
            "candidates_found": 0,
            "pages_fetched": 0,
            "pages_with_date": 0,
            "blocked": False,
        }

        total = 0
        kept = 0

        def record_item(item: Dict[str, Any]) -> None:
            items.append(item)
            stats["kept_last24h"] += 1
            per_source["kept_last24h"] += 1

        parsed = None
        feed_response = None

        def _try_fetch(url: str, timeout: int = 10) -> requests.Response | None:
            try:
                r = session.get(url, timeout=timeout)
                if r.status_code in (401, 402, 403, 404):
                    return r
                r.raise_for_status()
                return r
            except requests.exceptions.HTTPError as e:
                if hasattr(e, "response") and getattr(e, "response", None):
                    return getattr(e, "response")
                return None
            except Exception:
                return None

        feed_response = _try_fetch(feed_url)
        if feed_response and feed_response.ok:
            parsed = feedparser.parse(feed_response.content)
        else:
            per_source["feed_failed"] = True
            # Attempt autodiscovery from homepage when feed_url fails.
            if homepage:
                discovery_resp = _try_fetch(homepage, timeout=8)
                if discovery_resp and discovery_resp.ok:
                    soup = BeautifulSoup(discovery_resp.content, "html.parser")
                    discovered = None
                    for link in soup.find_all("link", attrs={"rel": "alternate"}):
                        ltype = str(link.get("type") or "").lower()
                        href = link.get("href")
                        if ltype in {"application/rss+xml", "application/atom+xml"} and href:
                            discovered = urljoin(homepage, href)
                            break
                    if discovered and discovered != feed_url:
                        per_source["discovered_feed_used"] = True
                        feed_response = _try_fetch(discovered)
                        if feed_response and feed_response.ok:
                            parsed = feedparser.parse(feed_response.content)

        entries = parsed.entries if parsed and hasattr(parsed, "entries") else []
        if feed_response and feed_response.ok:
            per_source["feed_ok"] = True
            per_source["feed_failed"] = False
            stats["sources_ok"] += 1
        else:
            stats["sources_failed"] += 1

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

            record_item(
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

        if not entries:
            per_source["feed_failed"] = per_source["feed_failed"] or not per_source["feed_ok"]

        # HTML fallback when feeds are unavailable or empty.
        if (not entries or not per_source["feed_ok"]) and homepage:
            per_source["html_fallback_used"] = True
            try:
                home_resp = session.get(homepage, timeout=8)
                home_resp.raise_for_status()
                soup = BeautifulSoup(home_resp.content, "html.parser")
                candidates: List[str] = []
                for a in soup.find_all("a", href=True):
                    href = a.get("href")
                    if not href:
                        continue
                    abs_url = urljoin(homepage, href)
                    parsed_url = urlsplit(abs_url)
                    if parsed_url.scheme not in {"http", "https"}:
                        continue
                    # Keep to same host when possible to limit noise.
                    home_host = urlsplit(homepage).netloc
                    if home_host and parsed_url.netloc and parsed_url.netloc != home_host:
                        continue
                    cleaned = _clean_url(abs_url)
                    if cleaned not in candidates:
                        candidates.append(cleaned)
                    if len(candidates) >= 30:
                        break

                per_source["candidates_found"] = len(candidates)

                for cand in candidates[:15]:
                    per_source["pages_fetched"] += 1
                    try:
                        page_resp = session.get(cand, timeout=8)
                        if page_resp.status_code in (401, 402, 403):
                            per_source["blocked"] = True
                            continue
                        page_resp.raise_for_status()
                    except Exception:
                        continue

                    page_soup = BeautifulSoup(page_resp.content, "html.parser")
                    published_dt = _extract_published_datetime(page_soup)
                    if not published_dt:
                        continue
                    per_source["pages_with_date"] += 1
                    if published_dt < cutoff or published_dt > now_utc + timedelta(minutes=5):
                        continue

                    stats["items_raw"] += 1
                    per_source["items_raw"] += 1
                    stats["items_with_date"] += 1
                    per_source["items_with_date"] += 1

                    title_tag = page_soup.find("meta", attrs={"property": "og:title"}) or page_soup.find(
                        "title"
                    )
                    title = ""
                    if title_tag:
                        title = title_tag.get("content") or title_tag.get_text(" ") or ""
                    title = title.strip() or name

                    desc_tag = page_soup.find("meta", attrs={"name": "description"})
                    snippet = desc_tag.get("content") if desc_tag else ""
                    if not snippet:
                        p_tag = page_soup.find("p")
                        snippet = p_tag.get_text(" ") if p_tag else ""
                    snippet = _strip_html(snippet)[:600]
                    if not snippet:
                        snippet = "(No excerpt provided)"

                    record_item(
                        {
                            "source": "foamed",
                            "foamed_source": name,
                            "channel": name,
                            "id": cand,
                            "title": title,
                            "url": cand,
                            "homepage": homepage,
                            "feed_type": feed_type,
                            "published_at": published_dt,
                            "published_field": "html_fallback",
                            "date_unknown": False,
                            "text": snippet,
                        }
                    )
                    kept += 1

            except Exception as e:
                per_source["errors"] += 1
                print(f"[foamed] WARN source={name!r}: html fallback failed: {e!r}")

        stats["per_source"][name] = per_source
        print(
            "[foamed] source={name!r}: feed_ok={} discovered_feed_used={} html_fallback={} candidates={} pages={} with_date={} kept_last24h={}".format(
                per_source["feed_ok"],
                per_source["discovered_feed_used"],
                per_source["html_fallback_used"],
                per_source["candidates_found"],
                per_source["pages_fetched"],
                per_source["pages_with_date"],
                per_source["kept_last24h"],
            )
        )

    return items, stats


__all__ = ["collect_foamed_items"]
