from __future__ import annotations

import calendar
import html
import json
import re
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
try:
    from readability import Document
except Exception:  # pragma: no cover
    Document = None
from requests.adapters import HTTPAdapter
try:
    import trafilatura
except Exception:  # pragma: no cover
    trafilatura = None
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

# A browser-like UA reduces the chance of 403 blocks for legitimate FOAMed sites.
# Keep it stable (avoid rotating) to make diagnostics reproducible.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 NewsAgent2/1.0"
)

DEFAULT_FOAMED_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass(frozen=True)
class _FetchResult:
    ok: bool
    status_code: Optional[int]
    content: Optional[bytes]
    final_url: Optional[str]
    error: Optional[str]


def _median(vals: List[int]) -> int:
    xs = sorted(int(v) for v in vals if int(v) >= 0)
    if not xs:
        return 0
    mid = len(xs) // 2
    return xs[mid] if len(xs) % 2 else (xs[mid - 1] + xs[mid]) // 2


def _detect_possible_bot_challenge(text: str) -> bool:
    t = (text or "").lower()
    markers = ["cloudflare", "attention required", "captcha", "verify you are human", "challenge"]
    return any(m in t for m in markers)


def _extract_article_text(html_text: str) -> Tuple[str, str]:
    cleaned = ""
    method = "none"
    if trafilatura is not None:
        try:
            cleaned = (trafilatura.extract(html_text, include_comments=False, include_tables=False) or "").strip()
        except Exception:
            cleaned = ""
    if len(cleaned) >= 500:
        return cleaned, "trafilatura"
    if Document is not None:
        try:
            doc = Document(html_text)
            summary = doc.summary() or ""
            cleaned = _strip_html(summary)
        except Exception:
            cleaned = ""
    if len(cleaned) >= 300:
        return cleaned, "readability_lxml"
    soup = BeautifulSoup(html_text, "html.parser")
    paras = [_strip_html(p.get_text(" ")) for p in soup.find_all("p")]
    paras = [p for p in paras if p]
    if paras:
        return "\n".join(paras[:30]).strip(), "beautifulsoup_fallback"
    return "", method


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


def _fetch_url(
    session: requests.Session,
    url: str,
    timeout_s: int = 10,
    headers: Optional[Dict[str, str]] = None,
) -> _FetchResult:
    """Fetch a URL and return a normalized result without raising.

    We deliberately return 401/402/403/404 responses (as ok=False but with status_code)
    so the caller can classify 'blocked' vs 'not_found' vs 'unavailable'.
    """
    try:
        r = session.get(url, timeout=timeout_s, allow_redirects=True, headers=headers)
        status = int(getattr(r, "status_code", 0) or 0) or None
        if r.ok:
            return _FetchResult(ok=True, status_code=status, content=r.content, final_url=str(r.url), error=None)
        # Keep common 'meaningful failures' for diagnostics.
        if status in (401, 402, 403, 404, 410, 429):
            return _FetchResult(ok=False, status_code=status, content=r.content, final_url=str(r.url), error=None)
        return _FetchResult(ok=False, status_code=status, content=None, final_url=str(r.url), error=None)
    except requests.exceptions.RequestException as e:
        return _FetchResult(ok=False, status_code=None, content=None, final_url=None, error=f"request_exception:{type(e).__name__}")
    except Exception as e:  # pragma: no cover - ultra-safety
        return _FetchResult(ok=False, status_code=None, content=None, final_url=None, error=f"exception:{type(e).__name__}")


def _compile_regex_list(patterns: object) -> List[re.Pattern[str]]:
    if not patterns:
        return []
    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, list):
        return []
    out: List[re.Pattern[str]] = []
    for p in patterns:
        try:
            s = str(p).strip()
            if not s:
                continue
            out.append(re.compile(s, flags=re.IGNORECASE))
        except Exception:
            continue
    return out


def _matches_any(regexes: List[re.Pattern[str]], text: str) -> bool:
    for rx in regexes or []:
        try:
            if rx.search(text or ""):
                return True
        except Exception:
            continue
    return False


def _is_likely_post_url(url: str) -> bool:
    """Heuristic to prioritize URLs that look like actual posts."""
    if not url:
        return False
    u = url.lower()
    # Common date-in-path patterns: /2025/12/... or /2025/12/20/...
    if re.search(r"/20\d{2}/\d{1,2}(/\d{1,2})?/?", u):
        return True
    # Common blog-like segments
    if any(seg in u for seg in ("/blog/", "/posts/", "/post/", "/article/", "/news/")):
        return True
    # WordPress / common CMS patterns
    if any(seg in u for seg in ("?p=", "/?p=", "/wp-content/", "/wp-json/")):
        return False
    return False


def _extract_canonical_url(soup: BeautifulSoup, fallback_url: str) -> str:
    link = soup.find("link", attrs={"rel": "canonical"})
    href = link.get("href") if link else ""
    href = str(href or "").strip()
    if href:
        return _clean_url(urljoin(fallback_url, href))
    og = soup.find("meta", attrs={"property": "og:url"})
    href = (og.get("content") if og else "") or ""
    href = str(href).strip()
    if href:
        return _clean_url(urljoin(fallback_url, href))
    return _clean_url(fallback_url)


def _extract_title(soup: BeautifulSoup, default_title: str) -> str:
    # Prefer OpenGraph title
    ogt = soup.find("meta", attrs={"property": "og:title"})
    if ogt and ogt.get("content"):
        t = str(ogt.get("content") or "").strip()
        if t:
            return t
    h1 = soup.find("h1")
    if h1:
        t = (h1.get_text(" ") or "").strip()
        if t:
            return t
    ttag = soup.find("title")
    if ttag:
        t = (ttag.get_text(" ") or "").strip()
        if t:
            return t
    return default_title or "Untitled"


def _extract_excerpt(soup: BeautifulSoup) -> str:
    for meta in (
        ("meta", {"name": "description"}),
        ("meta", {"property": "og:description"}),
        ("meta", {"name": "twitter:description"}),
    ):
        tag = soup.find(meta[0], attrs=meta[1])
        if tag and tag.get("content"):
            val = _strip_html(str(tag.get("content") or ""))
            if val:
                return val
    # Try to locate an article-ish container
    for selector in ("article", "main", "div", "section"):
        container = soup.find(selector)
        if not container:
            continue
        p = container.find("p")
        if p:
            val = _strip_html(p.get_text(" "))
            if val:
                return val
    p = soup.find("p")
    if p:
        val = _strip_html(p.get_text(" "))
        if val:
            return val
    return ""



def _apply_article_fetch(
    *,
    session: requests.Session,
    item_url: str,
    base_text: str,
    timeout_s: int,
    headers: Optional[Dict[str, str]],
    article_fetch_enabled: bool,
    article_fetch_done: int,
    article_fetch_max: int,
    per_source: Dict[str, Any],
    stats: Dict[str, Any],
) -> Tuple[str, str, str, str, int, Optional[int], str, int]:
    text = base_text or ""
    article_text = ""
    content_source = "rss_full_content" if len(text) >= 600 else ("rss_excerpt" if len(text) >= 80 else "rss_title_only")
    extraction_method = "rss"
    article_fetch_status_code = None
    article_fetch_error_class = ""

    if not article_fetch_enabled or article_fetch_done >= article_fetch_max:
        return text, article_text, content_source, extraction_method, len(article_text), article_fetch_status_code, article_fetch_error_class, article_fetch_done

    article_fetch_done += 1
    per_source["article_fetch_attempted"] += 1
    stats["foamed_article_fetch_attempted_total"] += 1
    fr_item = _fetch_url(session, item_url, timeout_s=timeout_s, headers=headers or None)
    article_fetch_status_code = fr_item.status_code
    if fr_item.status_code in (401, 403, 429):
        article_fetch_error_class = "blocked_or_rate_limited"
        per_source["article_fetch_blocked"] += 1
        stats["foamed_article_fetch_blocked_total"] += 1
    if fr_item.error and "ssl" in fr_item.error.lower():
        article_fetch_error_class = "ssl_error"
        per_source["article_fetch_ssl_error"] += 1
        stats["foamed_article_fetch_ssl_error_total"] += 1
    if fr_item.error and "timeout" in fr_item.error.lower():
        article_fetch_error_class = "timeout"
        per_source["article_fetch_timeout"] += 1
        stats["foamed_article_fetch_timeout_total"] += 1
    if fr_item.ok and fr_item.content:
        html_blob = fr_item.content.decode("utf-8", errors="ignore")
        if _detect_possible_bot_challenge(html_blob):
            article_fetch_error_class = "possible_bot_challenge"
        article_text, extraction_method = _extract_article_text(html_blob)
        per_source["article_fetch_success"] += 1
        stats["foamed_article_fetch_success_total"] += 1
    else:
        per_source["article_fetch_failed"] += 1
        stats["foamed_article_fetch_failed_total"] += 1
    if len(article_text) > max(len(text) + 120, int(len(text) * 1.5)):
        text = article_text
        content_source = "article_full_text" if len(article_text) >= 600 else "article_excerpt"
        per_source["article_fetch_improved_text"] += 1
        stats["foamed_article_fetch_improved_text_total"] += 1
    elif content_source.startswith("rss"):
        content_source = "article_excerpt" if len(text) < 600 else "article_full_text"

    return text, article_text, content_source, extraction_method, len(article_text), article_fetch_status_code, article_fetch_error_class, article_fetch_done

def _run_html_pass(
    session: requests.Session,
    *,
    name: str,
    homepage: str,
    seed_urls: List[str],
    allow_rx: List[re.Pattern[str]],
    deny_rx: List[re.Pattern[str]],
    now_utc: datetime,
    cutoff: datetime,
    max_candidates: int,
    max_pages: int,
    timeout_s: int,
    headers: Optional[Dict[str, str]],
    feed_type: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    stats = {
        "candidates_seen": 0,
        "pages_fetched": 0,
        "pages_with_date": 0,
        "blocked": False,
        "last_published": None,
        "items_in_window": 0,
        "homepage_status_code": None,
    }

    if isinstance(seed_urls, str):
        seed_urls = [seed_urls]
    if not isinstance(seed_urls, list) or not seed_urls:
        seed_urls = [homepage]

    home_host = urlsplit(homepage).netloc

    items: List[Dict[str, Any]] = []

    # Collect candidates across all seed pages.
    candidate_scores: Dict[str, int] = {}

    for seed in seed_urls:
        seed = str(seed or "").strip()
        if not seed:
            continue
        sr = _fetch_url(session, seed, timeout_s=timeout_s, headers=headers)
        if sr.status_code == 403:
            stats["blocked"] = True
        if seed == homepage:
            stats["homepage_status_code"] = sr.status_code
        if not sr.ok or not sr.content:
            continue

        soup = BeautifulSoup(sr.content, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if not href:
                continue
            abs_url = urljoin(seed, href)
            parsed_url = urlsplit(abs_url)
            if parsed_url.scheme not in {"http", "https"}:
                continue
            # Keep to same host (or subdomain) to limit noise.
            if home_host and parsed_url.netloc and not parsed_url.netloc.endswith(home_host):
                continue
            if any(abs_url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf")):
                continue

            cleaned = _clean_url(abs_url)
            if deny_rx and _matches_any(deny_rx, cleaned):
                continue
            if allow_rx and not _matches_any(allow_rx, cleaned):
                continue

            score = 2 if _is_likely_post_url(cleaned) else 1
            candidate_scores[cleaned] = max(candidate_scores.get(cleaned, 0), score)

    # Sort: likely-post first, then stable order.
    candidates = sorted(candidate_scores.items(), key=lambda kv: (-kv[1], kv[0]))
    candidates = [u for u, _ in candidates][: max(1, max_candidates)]
    stats["candidates_seen"] = len(candidates)

    for cand in candidates[: max(1, max_pages)]:
        stats["pages_fetched"] += 1
        pr = _fetch_url(session, cand, timeout_s=timeout_s, headers=headers)
        if pr.status_code == 403:
            stats["blocked"] = True
            continue
        if not pr.ok or not pr.content:
            continue

        page_soup = BeautifulSoup(pr.content, "html.parser")
        published_dt = _extract_published_datetime(page_soup)
        if not published_dt:
            continue
        stats["pages_with_date"] += 1

        if published_dt < cutoff or published_dt > now_utc + timedelta(minutes=5):
            continue

        stats["items_in_window"] += 1
        if not stats["last_published"] or published_dt > stats["last_published"]:
            stats["last_published"] = published_dt

        canonical = _extract_canonical_url(page_soup, cand)
        title = _extract_title(page_soup, default_title=name)
        snippet = _extract_excerpt(page_soup)[:600]
        if not snippet:
            snippet = "(No excerpt provided)"

        items.append(
            {
                "source": "foamed",
                "foamed_source": name,
                "channel": name,
                "id": canonical,
                "title": title,
                "url": canonical,
                "homepage": homepage,
                "feed_type": feed_type,
                "published_at": published_dt,
                "published_field": "html_fallback",
                "date_unknown": False,
                "text": snippet,
            }
        )

    return items, stats


def _wp_rest_audit(session: requests.Session, url: str, timeout_s: int, headers: Optional[Dict[str, str]], cutoff: datetime, now_utc: datetime) -> Dict[str, int | bool]:
    result: Dict[str, int | bool] = {"wp_rest_available": False, "wp_rest_items_seen": 0, "wp_rest_items_in_window": 0}
    if not url:
        return result
    fr = _fetch_url(session, url, timeout_s=timeout_s, headers=headers)
    if not fr.ok or not fr.content:
        return result
    try:
        payload = json.loads(fr.content.decode("utf-8", errors="ignore"))
    except Exception:
        return result
    if not isinstance(payload, list):
        return result
    result["wp_rest_available"] = True
    result["wp_rest_items_seen"] = len(payload)
    in_window = 0
    for post in payload:
        if not isinstance(post, dict):
            continue
        dt = _safe_parse_date(str(post.get("date_gmt") or post.get("modified_gmt") or post.get("date") or ""))
        if dt and cutoff <= dt <= now_utc + timedelta(minutes=5):
            in_window += 1
    result["wp_rest_items_in_window"] = in_window
    return result


def _sitemap_audit(session: requests.Session, url: str, timeout_s: int, headers: Optional[Dict[str, str]], cutoff: datetime, now_utc: datetime) -> Dict[str, int | bool]:
    result: Dict[str, int | bool] = {"sitemap_available": False, "sitemap_items_seen": 0, "sitemap_items_in_window": 0}
    if not url:
        return result
    fr = _fetch_url(session, url, timeout_s=timeout_s, headers=headers)
    if not fr.ok or not fr.content:
        return result
    try:
        root = ET.fromstring(fr.content)
    except Exception:
        return result
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = root.findall(".//sm:url", ns) or root.findall(".//url")
    result["sitemap_available"] = True
    result["sitemap_items_seen"] = len(urls)
    in_window = 0
    for node in urls:
        lastmod = node.find("sm:lastmod", ns) or node.find("lastmod")
        dt = _safe_parse_date((lastmod.text if lastmod is not None else "") or "")
        if dt and cutoff <= dt <= now_utc + timedelta(minutes=5):
            in_window += 1
    result["sitemap_items_in_window"] = in_window
    return result


def _text_len_stats(source_items: List[Dict[str, Any]]) -> Dict[str, int]:
    lengths = sorted(len((it.get("text") or "").strip()) for it in source_items if (it.get("text") or "").strip())
    if not lengths:
        return {
            "text_len_min": 0,
            "text_len_median": 0,
            "text_len_max": 0,
            "items_with_text_total": 0,
            "items_title_only_total": len(source_items),
            "possible_excerpt_total": 0,
            "possible_full_content_total": 0,
        }
    mid = len(lengths) // 2
    median = lengths[mid] if len(lengths) % 2 else (lengths[mid - 1] + lengths[mid]) // 2
    possible_excerpt_total = len([n for n in lengths if n < 280])
    return {
        "text_len_min": lengths[0],
        "text_len_median": median,
        "text_len_max": lengths[-1],
        "items_with_text_total": len(lengths),
        "items_title_only_total": max(0, len(source_items) - len(lengths)),
        "possible_excerpt_total": possible_excerpt_total,
        "possible_full_content_total": max(0, len(lengths) - possible_excerpt_total),
    }


def _classify_content_mode(
    *,
    health: str,
    rss_items_in_window: int,
    html_items_in_window: int,
    rss_text_len_median: int,
    html_text_len_median: int,
) -> str:
    if health in {"blocked_403", "not_found_404", "parse_failed", "other"} and rss_items_in_window == 0 and html_items_in_window == 0:
        return "unavailable"
    if rss_items_in_window == 0 and html_items_in_window == 0:
        return "no_recent_content"
    if rss_items_in_window > 0:
        if rss_text_len_median >= 600:
            return "rss_full_content"
        if rss_text_len_median >= 80:
            return "rss_excerpt"
        return "rss_title_only"
    if html_items_in_window > 0:
        if html_text_len_median >= 600:
            return "html_content"
        return "html_excerpt"
    return "unknown"


def _source_status_from(per_source: Dict[str, Any], *, has_recent_items: bool, strategy: str, audit_only: bool) -> str:
    err = str(per_source.get("error") or "").lower()
    if "timeout" in err or "ssl" in err:
        return "tls_or_timeout_problem"
    feed_status = per_source.get("feed_status_code")
    home_status = per_source.get("homepage_status_code")
    candidates = int(per_source.get("candidates_found") or 0)
    mode = str(per_source.get("content_mode") or "")
    html_viable = candidates > 0 or mode.startswith("html")
    if audit_only or strategy in {"audit_only", "disabled"}:
        if feed_status == 403 and home_status == 403:
            return "blocked"
        if feed_status in (404, 410) and home_status in (404, 410):
            return "stale_or_broken_url"
        if feed_status in (401, 403, 429) and home_status == 200 and html_viable:
            return "audit_only" if not has_recent_items else "usable_html_only"
        if feed_status in (404, 410) and home_status == 200 and html_viable:
            return "audit_only" if not has_recent_items else "usable_html_only"
        return "audit_only"

    if feed_status in (401, 403, 429) and home_status == 200 and html_viable:
        if has_recent_items and mode in {"html_content", "rss_full_content"}:
            return "usable_html_only"
        if has_recent_items and mode in {"html_excerpt", "rss_excerpt", "rss_title_only"}:
            return "usable_discovery_only"
        return "no_recent_content"
    if feed_status in (404, 410) and home_status == 200 and html_viable:
        if has_recent_items and mode in {"html_content", "rss_full_content"}:
            return "usable_html_only"
        if has_recent_items and mode in {"html_excerpt", "rss_excerpt", "rss_title_only"}:
            return "usable_discovery_only"
        return "no_recent_content"
    if feed_status == 403 and home_status == 403:
        return "blocked"
    if feed_status in (404, 410) and home_status in (404, 410):
        return "stale_or_broken_url"
    if per_source.get("blocked"):
        return "blocked"
    if strategy == "html_only":
        return "usable_html_only" if has_recent_items else "no_recent_content"
    if has_recent_items:
        if mode == "rss_excerpt":
            return "usable_excerpt_only"
        if mode in {"rss_title_only", "html_excerpt"}:
            return "usable_discovery_only"
        return "usable_fulltext"
    return "no_recent_content"


def _best_content_mode(per_source: Dict[str, Any], fallback: str = "unknown") -> str:
    counts = per_source.get("content_source_counts") or {}
    if isinstance(counts, dict) and counts:
        return str(max(counts, key=lambda k: counts.get(k, 0)))
    return str(per_source.get("content_mode") or fallback or "unknown")


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
    audit_enabled = (os.getenv("FOAMED_AUDIT", "0") or "0").strip() == "1"
    forced_fallback_sources = {
        s.strip().lower()
        for s in (os.getenv("FOAMED_FORCE_FALLBACK_SOURCES", "") or "").split(",")
        if s.strip()
    }
    article_fetch_enabled = (os.getenv("FOAMED_ARTICLE_FETCH", "0") or "0").strip() == "1"
    article_fetch_max = max(0, int((os.getenv("FOAMED_ARTICLE_FETCH_MAX_PER_RUN", "25") or "25").strip() or 25))
    article_fetch_done = 0
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
        "source_health": {
            "ok_rss": 0,
            "ok_html": 0,
            "blocked_403": 0,
            "not_found_404": 0,
            "parse_failed": 0,
            "other": 0,
        },
        "audit": {
            "enabled": audit_enabled,
            "sources": {},
        },
        "forced_html_fallback_sources": [],
        "foamed_article_fetch_enabled": article_fetch_enabled,
        "foamed_html_only_article_fetch_attempted_total": 0,
        "foamed_html_only_article_fetch_success_total": 0,
        "foamed_html_only_article_fetch_failed_total": 0,
        "foamed_html_only_article_fetch_improved_text_total": 0,
        "foamed_article_fetch_attempted_total": 0,
        "foamed_article_fetch_success_total": 0,
        "foamed_article_fetch_failed_total": 0,
        "foamed_article_fetch_improved_text_total": 0,
        "foamed_article_fetch_blocked_total": 0,
        "foamed_article_fetch_timeout_total": 0,
        "foamed_article_fetch_ssl_error_total": 0,
        "foamed_article_extraction_method_counts": {},
        "foamed_content_source_counts": {},
        "foamed_discovery_content_mode_counts": {},
        "foamed_final_content_source_counts": {},
        "foamed_source_strategy_summary": [],
        "foamed_strategy_override_disabled_total": 0,
        "foamed_html_only_sources_total": 0,
        "foamed_audit_only_sources_total": 0,
        "foamed_strategy_items_returned_total": 0,
        "foamed_strategy_items_audit_only_total": 0,
    }

    for src in sources_config or []:
        if not isinstance(src, dict):
            continue

        name = (src.get("name") or "").strip()
        feed_url = (src.get("feed_url") or "").strip()
        homepage = (src.get("homepage") or "").strip()
        feed_type = (src.get("type") or "").strip().lower() or "rss"

        if not name or (not feed_url and not homepage):
            continue

        stats["sources_total"] += 1
        allow_rx = _compile_regex_list(
            src.get("candidate_url_allow_regex")
            or src.get("candidate_allow_regex")
            or src.get("allow_regex")
        )
        deny_rx = _compile_regex_list(
            src.get("candidate_url_deny_regex")
            or src.get("candidate_deny_regex")
            or src.get("deny_regex")
        )

        max_candidates = int(src.get("max_candidates") or 40)
        max_pages = int(src.get("max_pages") or 12)
        timeout_s = int(src.get("timeout_s") or 10)
        timeout_feed_s = int(src.get("feed_timeout_s") or timeout_s)
        timeout_html_s = int(src.get("html_timeout_s") or timeout_s)

        # Optional per-source headers (non-secret) to reduce 403 blocks.
        headers: Dict[str, str] = {}
        extra_headers = src.get("headers")
        if isinstance(extra_headers, dict):
            for k, v in extra_headers.items():
                ks = str(k).strip()
                vs = str(v).strip()
                if ks and vs:
                    headers[ks] = vs
        ua = str(src.get("user_agent") or "").strip()
        if ua:
            headers["User-Agent"] = ua
        if not headers:
            headers = dict(DEFAULT_FOAMED_HEADERS)

        strategy = str(src.get("extraction_strategy") or "rss_then_article").strip().lower() or "rss_then_article"
        if strategy not in {"rss_then_article", "html_only", "rss_only", "audit_only", "disabled"}:
            strategy = "rss_then_article"
        if strategy == "html_only":
            stats["foamed_html_only_sources_total"] += 1
        if strategy == "audit_only":
            stats["foamed_audit_only_sources_total"] += 1
        if bool(src.get("strategy_override_disabled")):
            stats["foamed_strategy_override_disabled_total"] += 1

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
            "method": None,
            "why": None,
            "health": None,
            "entries_total": 0,
            "entries_with_date": 0,
            "newest_entry_datetime": None,
            "error": None,
            "candidates_found": 0,
            "pages_fetched": 0,
            "pages_with_date": 0,
            "blocked": False,
            "feed_status_code": None,
            "homepage_status_code": None,
            "discovered_feed_url": None,
            "forced_html_fallback": False,
            "audit": None,
            "content_mode": "unknown",
            "discovery_content_mode": "unknown",
            "final_content_source": "unknown",
            "source_status": "no_recent_content",
            "source_strategy": strategy,
            "strategy_override_disabled": bool(src.get("strategy_override_disabled")),
            "disabled_state_present": bool(src.get("disabled_state_present")),
            "ignore_auto_disable_if_strategy_viable": bool(src.get("ignore_auto_disable_if_strategy_viable")),
            "notes_diagnostic": str(src.get("notes_diagnostic") or ""),
            "article_fetch_attempted": 0,
            "article_fetch_success": 0,
            "article_fetch_failed": 0,
            "article_fetch_improved_text": 0,
            "article_fetch_blocked": 0,
            "article_fetch_timeout": 0,
            "article_fetch_ssl_error": 0,
            "extraction_method_counts": {},
            "content_source_counts": {},
            "article_text_lengths": [],
            "final_text_lengths": [],
        }
        rss_urls_in_window: List[str] = []

        def record_item(item: Dict[str, Any]) -> None:
            items.append(item)
            stats["kept_last24h"] += 1
            per_source["kept_last24h"] += 1
            if (item.get("published_field") or "").startswith("html_fallback"):
                return
            url = (item.get("url") or item.get("id") or "").strip()
            if url:
                rss_urls_in_window.append(url)

        # --- RSS stage -----------------------------------------------------
        entries: List[Dict[str, Any]] = []
        parse_failed = False

        forced_fallback = name.lower() in forced_fallback_sources
        if forced_fallback:
            per_source["forced_html_fallback"] = True
            per_source["method"] = "html_fallback"
            per_source["why"] = "forced_html_fallback"
            stats["forced_html_fallback_sources"].append(name)

        run_rss_stage = strategy in {"rss_then_article", "rss_only"}
        run_html_stage = strategy in {"rss_then_article", "html_only"}
        audit_only_strategy = strategy == "audit_only"
        disabled_strategy = strategy == "disabled"

        if feed_url and not forced_fallback and run_rss_stage:
            fr = _fetch_url(session, feed_url, timeout_s=timeout_feed_s, headers=headers or None)
            per_source["feed_status_code"] = fr.status_code
            if fr.status_code == 403:
                per_source["blocked"] = True
            if fr.ok and fr.content:
                parsed = feedparser.parse(fr.content)
                entries = list(getattr(parsed, "entries", []) or [])
                per_source["feed_ok"] = True
                per_source["feed_failed"] = False
                per_source["method"] = "rss"
                per_source["why"] = "feed_ok"
                if getattr(parsed, "bozo", 0) and not entries:
                    parse_failed = True
                    be = getattr(parsed, "bozo_exception", None)
                    per_source["error"] = f"feed_parse_failed:{type(be).__name__}" if be else "feed_parse_failed"
            else:
                per_source["feed_failed"] = True
                if fr.status_code is not None:
                    per_source["error"] = f"feed_http_{fr.status_code}"
                elif fr.error:
                    per_source["error"] = fr.error
                else:
                    per_source["error"] = "feed_unavailable"

        # --- Feed autodiscovery (homepage) ---------------------------------
        if (not entries or not per_source["feed_ok"]) and homepage and not forced_fallback and run_rss_stage:
            hr = _fetch_url(session, homepage, timeout_s=timeout_html_s, headers=headers or None)
            per_source["homepage_status_code"] = hr.status_code
            if hr.status_code == 403:
                per_source["blocked"] = True
            if hr.ok and hr.content:
                soup = BeautifulSoup(hr.content, "html.parser")
                discovered = None
                for link in soup.find_all("link", attrs={"rel": "alternate"}):
                    ltype = str(link.get("type") or "").lower()
                    href = link.get("href")
                    if ltype in {"application/rss+xml", "application/atom+xml"} and href:
                        discovered = urljoin(homepage, href)
                        break

                if discovered and discovered != feed_url:
                    per_source["discovered_feed_used"] = True
                    per_source["discovered_feed_url"] = str(discovered)
                    fr2 = _fetch_url(session, discovered, timeout_s=timeout_feed_s, headers=headers or None)
                    if fr2.status_code == 403:
                        per_source["blocked"] = True
                    if fr2.ok and fr2.content:
                        parsed2 = feedparser.parse(fr2.content)
                        entries2 = list(getattr(parsed2, "entries", []) or [])
                        if entries2:
                            entries = entries2
                            per_source["feed_ok"] = True
                            per_source["feed_failed"] = False
                            per_source["method"] = "discovered_feed"
                            per_source["why"] = "autodiscovered_feed_ok"
                        elif getattr(parsed2, "bozo", 0):
                            parse_failed = True
                            be = getattr(parsed2, "bozo_exception", None)
                            per_source["error"] = f"feed_parse_failed:{type(be).__name__}" if be else "feed_parse_failed"

        # --- Parse RSS entries --------------------------------------------
        newest_entry_dt = None
        entries_with_date = 0
        per_source["entries_total"] = len(entries)

        for entry in entries:
            stats["items_raw"] += 1
            per_source["items_raw"] += 1

            link = str(entry.get("link") or entry.get("id") or "").strip()
            if not link and isinstance(entry.get("links"), list) and entry.get("links"):
                try:
                    link = str(entry.get("links")[0].get("href") or "").strip()
                except Exception:
                    link = ""

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
                if not newest_entry_dt or published_at > newest_entry_dt:
                    newest_entry_dt = published_at
                entries_with_date += 1

            if not published_at:
                # Strict time window: no date -> cannot include.
                continue
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
            rss_text = text
            article_text = ""
            content_source = "rss_full_content" if len(rss_text) >= 600 else ("rss_excerpt" if len(rss_text) >= 80 else "rss_title_only")
            discovery_content_mode = content_source
            extraction_method = "rss"
            text, article_text, content_source, extraction_method, _, article_fetch_status_code, article_fetch_error_class, article_fetch_done = _apply_article_fetch(
                session=session,
                item_url=url,
                base_text=text,
                timeout_s=timeout_html_s,
                headers=headers or None,
                article_fetch_enabled=article_fetch_enabled,
                article_fetch_done=article_fetch_done,
                article_fetch_max=article_fetch_max,
                per_source=per_source,
                stats=stats,
            )

            candidate_item = {
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
                    "date_unknown": False,
                    "text": text,
                    "content_source": content_source,
                    "discovery_content_mode": discovery_content_mode,
                    "final_content_source": content_source,
                    "extraction_method": extraction_method,
                    "text_length": len(text),
                    "rss_text_length": len(rss_text),
                    "article_text_length": len(article_text),
                    "article_fetch_status_code": article_fetch_status_code,
                    "article_fetch_error_class": article_fetch_error_class,
                }
            if not audit_only_strategy:
                record_item(candidate_item)
                stats["foamed_strategy_items_returned_total"] += 1
            else:
                stats["foamed_strategy_items_audit_only_total"] += 1
            per_source["extraction_method_counts"][extraction_method] = int(per_source["extraction_method_counts"].get(extraction_method, 0)) + 1
            per_source["content_source_counts"][content_source] = int(per_source["content_source_counts"].get(content_source, 0)) + 1
            stats["foamed_article_extraction_method_counts"][extraction_method] = int(stats["foamed_article_extraction_method_counts"].get(extraction_method, 0)) + 1
            stats["foamed_content_source_counts"][content_source] = int(stats["foamed_content_source_counts"].get(content_source, 0)) + 1
            stats["foamed_discovery_content_mode_counts"][discovery_content_mode] = int(stats["foamed_discovery_content_mode_counts"].get(discovery_content_mode, 0)) + 1
            stats["foamed_final_content_source_counts"][content_source] = int(stats["foamed_final_content_source_counts"].get(content_source, 0)) + 1
            per_source["article_text_lengths"].append(len(article_text))
            per_source["final_text_lengths"].append(len(text))

        per_source["entries_with_date"] = entries_with_date

        # --- HTML fallback --------------------------------------------------
        needs_html = False
        if homepage and run_html_stage:
            if not entries or not per_source["feed_ok"]:
                needs_html = True
            elif entries and entries_with_date == 0:
                needs_html = True

        fallback_newest_dt = None
        ok_html = False
        if needs_html and homepage:
            per_source["html_fallback_used"] = True
            if not per_source.get("method"):
                per_source["method"] = "html_fallback"
            if not per_source.get("why"):
                if not feed_url:
                    per_source["why"] = "feed_missing"
                elif not per_source.get("feed_ok"):
                    per_source["why"] = per_source.get("error") or "feed_failed"
                elif entries and entries_with_date == 0:
                    per_source["why"] = "feed_no_dates"
                elif not entries:
                    per_source["why"] = "feed_empty"
                else:
                    per_source["why"] = "html_fallback_needed"

            seed_urls = src.get("fallback_urls")
            if isinstance(seed_urls, str) and seed_urls.strip():
                seed_urls = [seed_urls.strip()]
            if not isinstance(seed_urls, list) or not seed_urls:
                seed_urls = [homepage]

            try:
                html_items, html_stats = _run_html_pass(
                    session,
                    name=name,
                    homepage=homepage,
                    seed_urls=seed_urls,
                    allow_rx=allow_rx,
                    deny_rx=deny_rx,
                    now_utc=now_utc,
                    cutoff=cutoff,
                    max_candidates=max_candidates,
                    max_pages=max_pages,
                    timeout_s=timeout_html_s,
                    headers=headers or None,
                    feed_type=feed_type,
                )
                per_source["candidates_found"] = html_stats.get("candidates_seen", 0)
                per_source["pages_fetched"] = html_stats.get("pages_fetched", 0)
                per_source["pages_with_date"] = html_stats.get("pages_with_date", 0)
                if html_stats.get("homepage_status_code") is not None:
                    per_source["homepage_status_code"] = html_stats.get("homepage_status_code")
                if html_stats.get("blocked"):
                    per_source["blocked"] = True
                if html_stats.get("last_published"):
                    fallback_newest_dt = html_stats["last_published"]
                if html_items:
                    ok_html = True
                for item in html_items:
                    stats["items_raw"] += 1
                    per_source["items_raw"] += 1
                    stats["items_with_date"] += 1
                    per_source["items_with_date"] += 1
                    item_url = str(item.get("url") or item.get("id") or "").strip()
                    base_text = str(item.get("text") or "")
                    should_fetch_html_article = bool(item_url) and (strategy == "html_only" or bool(src.get("force_html_fallback")))
                    extraction_method = "html_fallback"
                    article_text = ""
                    content_source = "html_content" if len(base_text) >= 600 else "html_excerpt"
                    article_fetch_status_code = None
                    article_fetch_error_class = ""
                    text = base_text
                    if should_fetch_html_article:
                        text, article_text, content_source, extraction_method, _, article_fetch_status_code, article_fetch_error_class, article_fetch_done = _apply_article_fetch(
                            session=session,
                            item_url=item_url,
                            base_text=base_text,
                            timeout_s=timeout_html_s,
                            headers=headers or None,
                            article_fetch_enabled=article_fetch_enabled,
                            article_fetch_done=article_fetch_done,
                            article_fetch_max=article_fetch_max,
                            per_source=per_source,
                            stats=stats,
                        )
                    item.update(
                        {
                            "text": text,
                            "content_source": content_source,
                            "discovery_content_mode": "html_excerpt" if len(base_text) < 600 else "html_content",
                            "final_content_source": content_source,
                            "extraction_method": extraction_method,
                            "text_length": len(text),
                            "rss_text_length": 0,
                            "article_text_length": len(article_text),
                            "article_fetch_status_code": article_fetch_status_code,
                            "article_fetch_error_class": article_fetch_error_class,
                        }
                    )
                    per_source["extraction_method_counts"][extraction_method] = int(per_source["extraction_method_counts"].get(extraction_method, 0)) + 1
                    per_source["content_source_counts"][content_source] = int(per_source["content_source_counts"].get(content_source, 0)) + 1
                    stats["foamed_article_extraction_method_counts"][extraction_method] = int(stats["foamed_article_extraction_method_counts"].get(extraction_method, 0)) + 1
                    stats["foamed_content_source_counts"][content_source] = int(stats["foamed_content_source_counts"].get(content_source, 0)) + 1
                    stats["foamed_discovery_content_mode_counts"]["html_excerpt" if len(base_text) < 600 else "html_content"] = int(stats["foamed_discovery_content_mode_counts"].get("html_excerpt" if len(base_text) < 600 else "html_content", 0)) + 1
                    stats["foamed_final_content_source_counts"][content_source] = int(stats["foamed_final_content_source_counts"].get(content_source, 0)) + 1
                    per_source["article_text_lengths"].append(len(article_text))
                    per_source["final_text_lengths"].append(len(text))
                    if not audit_only_strategy:
                        record_item(item)
                        stats["foamed_strategy_items_returned_total"] += 1
                    else:
                        stats["foamed_strategy_items_audit_only_total"] += 1

            except Exception as e:
                per_source["errors"] += 1
                per_source["error"] = f"html_fallback_failed:{type(e).__name__}"[:200]
                print(f"[foamed] WARN source={name!r}: html fallback failed: {e!r}")

        if per_source["html_fallback_used"] and not per_source.get("method"):
            per_source["method"] = "html_fallback"

        if fallback_newest_dt and (not newest_entry_dt or fallback_newest_dt > newest_entry_dt):
            newest_entry_dt = fallback_newest_dt

        if newest_entry_dt:
            per_source["newest_entry_datetime"] = newest_entry_dt.astimezone(timezone.utc).isoformat()

        # --- Audit mode (RSS completeness check) --------------------------
        if audit_enabled and per_source.get("feed_ok") and homepage and not forced_fallback and run_html_stage:
            audit_seed_urls = src.get("fallback_urls") or [homepage]
            if isinstance(audit_seed_urls, str) and audit_seed_urls.strip():
                audit_seed_urls = [audit_seed_urls.strip()]
            if not isinstance(audit_seed_urls, list) or not audit_seed_urls:
                audit_seed_urls = [homepage]
            audit_max_candidates = min(max_candidates, 12)
            audit_max_pages = min(max_pages, 4)

            html_audit_items, html_audit_stats = _run_html_pass(
                session,
                name=name,
                homepage=homepage,
                seed_urls=audit_seed_urls,
                allow_rx=allow_rx,
                deny_rx=deny_rx,
                now_utc=now_utc,
                cutoff=cutoff,
                max_candidates=audit_max_candidates,
                max_pages=audit_max_pages,
                timeout_s=timeout_html_s,
                headers=headers or None,
                feed_type=feed_type,
            )

            html_urls_in_window = [it.get("url") or it.get("id") for it in html_audit_items if (it.get("url") or it.get("id"))]
            rss_set = {u for u in rss_urls_in_window if u}
            html_set = {u for u in html_urls_in_window if u}
            html_not_in_rss = sorted(html_set - rss_set)
            rss_not_in_html = sorted(rss_set - html_set)

            per_source["audit"] = {
                "rss_items_seen": per_source.get("entries_total", 0),
                "rss_items_in_window": len(rss_urls_in_window),
                "html_candidates_seen": int(html_audit_stats.get("candidates_seen") or 0),
                "html_items_in_window": int(html_audit_stats.get("items_in_window") or 0),
                "items_found_in_html_not_in_rss": {
                    "count": len(html_not_in_rss),
                    "examples": html_not_in_rss[:5],
                },
                "items_found_in_rss_not_in_html": {
                    "count": len(rss_not_in_html),
                    "examples": rss_not_in_html[:5],
                },
                "audit_pages_fetched": int(html_audit_stats.get("pages_fetched") or 0),
            }
            stats["audit"]["sources"][name] = per_source["audit"]

        # --- Health classification & counters ------------------------------
        html_parse_failed = bool(per_source.get("html_fallback_used") and not ok_html and (per_source.get("pages_fetched") or 0) > 0)
        health = "other"
        if per_source.get("blocked"):
            health = "blocked_403"
        elif per_source.get("feed_status_code") in (404, 410) or per_source.get("homepage_status_code") in (404, 410):
            health = "not_found_404"
        elif per_source.get("feed_ok"):
            health = "ok_rss"
        elif ok_html:
            health = "ok_html"
        elif parse_failed or html_parse_failed:
            health = "parse_failed"

        per_source["health"] = health
        source_items = [it for it in items if (it.get("foamed_source") or "").strip() == name]
        tstats = _text_len_stats(source_items)
        per_source.update(tstats)

        rss_in_window = len(rss_urls_in_window)
        html_in_window = len([it for it in source_items if str(it.get("published_field") or "").startswith("html_fallback")])
        rss_lengths = [len((it.get("text") or "").strip()) for it in source_items if not str(it.get("published_field") or "").startswith("html_fallback")]
        html_lengths = [len((it.get("text") or "").strip()) for it in source_items if str(it.get("published_field") or "").startswith("html_fallback")]
        rss_med = sorted(rss_lengths)[len(rss_lengths)//2] if rss_lengths else 0
        html_med = sorted(html_lengths)[len(html_lengths)//2] if html_lengths else 0
        per_source["content_mode"] = _classify_content_mode(
            health=health,
            rss_items_in_window=rss_in_window,
            html_items_in_window=html_in_window,
            rss_text_len_median=rss_med,
            html_text_len_median=html_med,
        )
        per_source["median_article_text_length"] = _median(per_source.pop("article_text_lengths", []))
        per_source["median_final_text_length"] = _median(per_source.pop("final_text_lengths", []))
        per_source["discovery_content_mode"] = per_source.get("content_mode", "unknown")
        per_source["final_content_source"] = _best_content_mode(per_source, per_source.get("content_mode", "unknown"))
        feed_status = per_source.get("feed_status_code")
        home_status = per_source.get("homepage_status_code")
        candidates = int(per_source.get("candidates_found") or 0)
        alt_path = "none"
        if "timeout" in str(per_source.get("error") or "").lower() or "ssl" in str(per_source.get("error") or "").lower():
            alt_path = "tls_or_timeout_problem"
        elif feed_status in (401, 403, 429) and home_status == 200 and (candidates > 0 or str(per_source.get("content_mode") or "").startswith("html")):
            alt_path = "feed_blocked_but_html_ok"
        elif feed_status in (404, 410) and home_status == 200 and (candidates > 0 or str(per_source.get("content_mode") or "").startswith("html")):
            alt_path = "feed_broken_but_html_ok"
        elif feed_status == 403 and home_status == 403:
            alt_path = "none"
        elif feed_status in (404, 410) and home_status in (404, 410):
            alt_path = "none"
        per_source["alternative_path"] = alt_path
        per_source["source_status"] = _source_status_from(
            per_source,
            has_recent_items=bool(per_source.get("kept_last24h", 0)),
            strategy=str(per_source.get("source_strategy") or "rss_then_article"),
            audit_only=bool(audit_only_strategy or disabled_strategy),
        )
        if isinstance(stats.get("source_health"), dict):
            stats["source_health"][health] = int(stats["source_health"].get(health, 0) or 0) + 1

        if health in ("ok_rss", "ok_html"):
            stats["sources_ok"] += 1
        else:
            stats["sources_failed"] += 1
        if strategy == "html_only":
            stats["foamed_html_only_article_fetch_attempted_total"] += int(per_source.get("article_fetch_attempted", 0) or 0)
            stats["foamed_html_only_article_fetch_success_total"] += int(per_source.get("article_fetch_success", 0) or 0)
            stats["foamed_html_only_article_fetch_improved_text_total"] += int(per_source.get("article_fetch_improved_text", 0) or 0)
            stats["foamed_html_only_article_fetch_failed_total"] += int(per_source.get("article_fetch_failed", 0) or 0)

        stats["per_source"][name] = per_source
        wp_stats = {"wp_rest_available": False, "wp_rest_items_seen": 0, "wp_rest_items_in_window": 0}
        sm_stats = {"sitemap_available": False, "sitemap_items_seen": 0, "sitemap_items_in_window": 0}
        if homepage and (audit_enabled or bool(src.get("disabled", False))):
            wp_stats = _wp_rest_audit(session, homepage, timeout_html_s, headers or None, cutoff, now_utc)
            sitemap_url = str(src.get("sitemap_url") or homepage).strip()
            sm_stats = _sitemap_audit(session, sitemap_url, timeout_html_s, headers or None, cutoff, now_utc)
        per_source.update(wp_stats)
        per_source.update(sm_stats)
        stats["foamed_source_strategy_summary"].append(
            {
                "name": name,
                "extraction_strategy": per_source.get("source_strategy"),
                "source_status": per_source.get("source_status"),
                "health": per_source.get("health"),
                "method": per_source.get("method"),
                "ignore_auto_disable_if_strategy_viable": per_source.get("ignore_auto_disable_if_strategy_viable"),
                "disabled_state_present": per_source.get("disabled_state_present"),
                "strategy_override_disabled": per_source.get("strategy_override_disabled"),
                "discovery_content_mode": per_source.get("discovery_content_mode", per_source.get("content_mode", "unknown")),
                "final_content_source": per_source.get("final_content_source", per_source.get("content_mode", "unknown")),
                "content_mode": per_source.get("content_mode"),
                "feed_status_code": per_source.get("feed_status_code"),
                "homepage_status_code": per_source.get("homepage_status_code"),
                "candidates_found": int(per_source.get("candidates_found", 0) or 0),
                "kept_in_window_count": int(per_source.get("kept_last24h", 0) or 0),
                "article_fetch_attempted": int(per_source.get("article_fetch_attempted", 0) or 0),
                "article_fetch_success": int(per_source.get("article_fetch_success", 0) or 0),
                "article_fetch_improved_text": int(per_source.get("article_fetch_improved_text", 0) or 0),
                "extraction_method_counts": dict(per_source.get("extraction_method_counts") or {}),
                "content_source_counts": dict(per_source.get("content_source_counts") or {}),
                **wp_stats,
                **sm_stats,
                "alternative_path": per_source.get("alternative_path"),
                "notes_diagnostic": per_source.get("notes_diagnostic"),
            }
        )
        if audit_enabled:
            a = per_source.get("audit") or {}
            if not isinstance(a, dict):
                a = {}
            warnings: List[str] = []
            mode = per_source.get("content_mode") or "unknown"
            if mode == "unavailable":
                warnings.append("source_unavailable")
            if mode == "rss_title_only":
                warnings.append("rss_title_only")
            if mode == "rss_excerpt":
                warnings.append("rss_excerpt_only")
            if mode == "no_recent_content":
                warnings.append("no_recent_content")
            if mode == "unknown":
                warnings.append("content_mode_unknown")
            if per_source.get("html_fallback_used") and not ok_html:
                warnings.append("html_failed")
            if (a.get("items_found_in_html_not_in_rss") or {}).get("count", 0) > 0:
                warnings.append("html_found_items_not_in_rss")
            if (a.get("items_found_in_rss_not_in_html") or {}).get("count", 0) > 0:
                warnings.append("rss_recent_items_not_found_in_html")
            a.update({
                "health": health,
                "method": per_source.get("method"),
                "content_mode": mode,
                "html_not_in_rss_count": int(((a.get("items_found_in_html_not_in_rss") or {}).get("count", 0) or 0)),
                "rss_not_in_html_count": int(((a.get("items_found_in_rss_not_in_html") or {}).get("count", 0) or 0)),
                "text_len_median": int(tstats.get("text_len_median", 0) or 0),
                "completeness_warning": sorted(set(warnings)),
            })
            per_source["audit"] = a
            stats["audit"]["sources"][name] = a
        print(
            f"[foamed] source={name!r}: method={per_source.get('method') or 'n/a'} "
            f"why={per_source.get('why') or 'n/a'} health={per_source.get('health') or 'n/a'} "
            f"feed_status={per_source.get('feed_status_code') or 'n/a'} home_status={per_source.get('homepage_status_code') or 'n/a'} "
            f"entries={per_source.get('entries_total', 0)} entries_with_date={per_source.get('entries_with_date', 0)} "
            f"html={per_source.get('html_fallback_used')} candidates={per_source.get('candidates_found', 0)} pages={per_source.get('pages_fetched', 0)} "
            f"kept_last{lookback_hours}h={per_source.get('kept_last24h', 0)}"
        )

    return items, stats


__all__ = ["collect_foamed_items"]
