from __future__ import annotations

import json
import os
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# NCBI rate limits (rough guidance):
# - Without api_key: ~3 requests/second.
# - With api_key: higher, but still subject to throttling on bursts.
#
# We enforce a minimum interval between *all* PubMed requests in this module.
_DEFAULT_MIN_INTERVAL_NO_KEY_S = 0.40
_DEFAULT_MIN_INTERVAL_WITH_KEY_S = 0.12

# Global session to reuse TCP connections
_SESSION = requests.Session()
_LAST_REQUEST_TS = 0.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _date_yyyymmdd(dt: datetime) -> str:
    # PubMed accepts YYYY/MM/DD
    return dt.astimezone(timezone.utc).strftime("%Y/%m/%d")


def _parse_month(mon: str) -> Optional[int]:
    mon = (mon or "").strip()
    if not mon:
        return None
    if mon.isdigit():
        try:
            v = int(mon)
            return v if 1 <= v <= 12 else None
        except Exception:
            return None
    mon3 = mon[:3].lower()
    months = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    return months.get(mon3)


def _get_ncbi_params() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Optional NCBI identifiers.
    - NCBI_API_KEY: increases rate limit and reduces 429 probability.
    - NCBI_TOOL: name of your application (recommended by NCBI).
    - NCBI_EMAIL: contact email (recommended by NCBI).
    """
    api_key = (os.getenv("NCBI_API_KEY", "") or "").strip() or None
    tool = (os.getenv("NCBI_TOOL", "") or "").strip() or None
    email = (os.getenv("NCBI_EMAIL", "") or "").strip() or None
    return api_key, tool, email


def _min_interval_s(api_key: Optional[str]) -> float:
    raw = (os.getenv("PUBMED_MIN_INTERVAL_S", "") or "").strip()
    if raw:
        try:
            v = float(raw)
            return max(0.0, v)
        except Exception:
            print(f"[pubmed] WARN: invalid PUBMED_MIN_INTERVAL_S={raw!r} -> ignoring")
    return _DEFAULT_MIN_INTERVAL_WITH_KEY_S if api_key else _DEFAULT_MIN_INTERVAL_NO_KEY_S


def _date_type() -> str:
    """
    PubMed date field used for filtering.
    - pdat: publication date
    - edat: Entrez date (when record was added/updated in PubMed)
    """
    dt = (os.getenv("PUBMED_DATE_TYPE", "pdat") or "pdat").strip().lower()
    return dt if dt in ("pdat", "edat") else "pdat"


def _rate_limit_sleep(api_key: Optional[str]) -> None:
    """
    Enforce a minimum interval between requests to avoid 429s.
    """
    global _LAST_REQUEST_TS

    interval = _min_interval_s(api_key)
    if interval <= 0:
        return

    now = time.monotonic()
    delta = now - _LAST_REQUEST_TS
    if delta < interval:
        time.sleep(interval - delta)

    _LAST_REQUEST_TS = time.monotonic()


def _build_headers(email: Optional[str]) -> Dict[str, str]:
    # A clear UA string helps upstream debugging/throttling.
    ua = "NewsAgent2/1.0"
    if email:
        ua += f" (mailto:{email})"
    return {"User-Agent": ua}


def _request_with_retries(
    *,
    url: str,
    params: Dict[str, Any],
    timeout_s: int,
    expect: str,
) -> Any:
    """
    expect: "json" or "text"
    """
    api_key, tool, email = _get_ncbi_params()

    # Inject optional NCBI parameters.
    if api_key and "api_key" not in params:
        params["api_key"] = api_key
    if tool and "tool" not in params:
        params["tool"] = tool
    if email and "email" not in params:
        params["email"] = email

    headers = _build_headers(email)

    raw_retries = (os.getenv("PUBMED_MAX_RETRIES", "") or "").strip()
    max_retries = 4
    if raw_retries:
        try:
            max_retries = int(raw_retries)
        except Exception:
            print(f"[pubmed] WARN: invalid PUBMED_MAX_RETRIES={raw_retries!r} -> using default {max_retries}")
    max_retries = max(1, min(max_retries, 10))

    raw_backoff = (os.getenv("PUBMED_BACKOFF_BASE_S", "") or "").strip()
    backoff_base = 1.0
    if raw_backoff:
        try:
            backoff_base = float(raw_backoff)
        except Exception:
            print(f"[pubmed] WARN: invalid PUBMED_BACKOFF_BASE_S={raw_backoff!r} -> using default {backoff_base}")

    last_err: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        _rate_limit_sleep(api_key)

        try:
            r = _SESSION.get(url, params=params, headers=headers, timeout=timeout_s)
        except requests.RequestException as e:
            last_err = f"request error: {e!r}"
            print(f"[pubmed] WARN: {last_err} (attempt {attempt}/{max_retries}, url={url})")
            if attempt < max_retries:
                time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue

        if r.status_code == 429:
            retry_after = (r.headers.get("Retry-After") or "").strip()
            sleep_s = None
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except Exception:
                    sleep_s = None

            if sleep_s is None:
                sleep_s = backoff_base * (2 ** (attempt - 1))

            print(f"[pubmed] WARN: 429 Too Many Requests -> sleeping {sleep_s:.1f}s (attempt {attempt}/{max_retries})")
            if attempt < max_retries:
                time.sleep(max(0.5, sleep_s))
                continue

            last_err = "429 Too Many Requests (exhausted retries)"
            print(f"[pubmed] ERROR: {last_err} (url={url})")
            return {} if expect == "json" else ""

        if 500 <= r.status_code <= 599:
            last_err = f"server error {r.status_code}"
            print(f"[pubmed] WARN: {last_err} (attempt {attempt}/{max_retries})")
            if attempt < max_retries:
                time.sleep(backoff_base * (2 ** (attempt - 1)))
                continue
            print(f"[pubmed] ERROR: {last_err} (url={url})")
            return {} if expect == "json" else ""

        try:
            r.raise_for_status()
        except requests.RequestException as e:
            last_err = f"http error: {e} (status={r.status_code})"
            print(f"[pubmed] ERROR: {last_err} (url={url})")
            return {} if expect == "json" else ""

        if expect == "json":
            try:
                return r.json()
            except json.JSONDecodeError as e:
                last_err = f"invalid JSON: {e!r}"
                print(f"[pubmed] ERROR: {last_err} (url={url})")
                return {}
        else:
            return r.text

    print(f"[pubmed] ERROR: request failed after retries: {last_err or 'unknown error'} (url={url})")
    return {} if expect == "json" else ""


def _request_json(url: str, params: Dict[str, Any], timeout_s: int = 25) -> Dict[str, Any]:
    data = _request_with_retries(url=url, params=dict(params), timeout_s=timeout_s, expect="json")
    return data if isinstance(data, dict) else {}


def _request_text(url: str, params: Dict[str, Any], timeout_s: int = 35) -> str:
    text = _request_with_retries(url=url, params=dict(params), timeout_s=timeout_s, expect="text")
    return text if isinstance(text, str) else ""


def _pubmed_search_url(term: str) -> str:
    return "https://pubmed.ncbi.nlm.nih.gov/?" + urllib.parse.urlencode({"term": term})


def _extract_text(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()


def _normalize_itertext(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    return " ".join("".join(elem.itertext()).split()).strip()


def _extract_title(article: ET.Element) -> str:
    title_elem = article.find(".//ArticleTitle")
    return _normalize_itertext(title_elem)


def _extract_abstract(article: ET.Element) -> str:
    parts: List[str] = []
    for ab in article.findall(".//Abstract/AbstractText"):
        text = _normalize_itertext(ab)
        if not text:
            continue
        label = (ab.attrib.get("Label") or ab.attrib.get("NlmCategory") or "").strip()
        if label and label.lower() not in {"abstract"}:
            if text.startswith(f"{label}:"):
                parts.append(text)
            else:
                parts.append(f"{label}: {text}")
        else:
            parts.append(text)
    return "\n".join(parts).strip()


def _parse_pub_date(article: ET.Element) -> datetime:
    now = _utc_now()

    def _try_date(parent_xpath: str) -> Optional[datetime]:
        node = article.find(parent_xpath)
        if node is None:
            return None
        year = _extract_text(node.find("Year"))
        month = _extract_text(node.find("Month"))
        day = _extract_text(node.find("Day"))

        try:
            y = int(year) if year else None
        except Exception:
            y = None
        m = _parse_month(month) if month else None
        try:
            d = int(day) if day else None
        except Exception:
            d = None

        if not y:
            return None
        if not m:
            m = 1
        if not d:
            d = 1
        try:
            return datetime(y, m, d, 12, 0, 0, tzinfo=timezone.utc)
        except Exception:
            return None

    candidates = [
        ".//ArticleDate[@DateType='Electronic']",
        ".//ArticleDate",
        ".//JournalIssue/PubDate",
        ".//PubDate",
    ]
    for xp in candidates:
        dt = _try_date(xp)
        if dt:
            return dt

    return now


def _parse_pubmed_xml(xml_text: str, max_items: int) -> List[Dict[str, Any]]:
    if not xml_text.strip():
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"[pubmed] ERROR: could not parse XML: {e}")
        return []

    articles: List[Dict[str, Any]] = []
    for art in root.findall(".//PubmedArticle"):
        pmid = _extract_text(art.find(".//PMID"))
        if not pmid:
            continue

        title = _extract_title(art)
        if not title:
            title = f"PubMed Article {pmid}"

        journal = _extract_text(art.find(".//Journal/Title"))
        journal_iso_abbrev = _extract_text(art.find(".//Journal/ISOAbbreviation"))
        journal_medline_ta = _extract_text(art.find(".//MedlineJournalInfo/MedlineTA"))
        abstract = _extract_abstract(art)

        pub_dt = _parse_pub_date(art)

        doi = ""
        for aid in art.findall(".//ArticleIdList/ArticleId"):
            if (aid.attrib.get("IdType") or "").lower() == "doi":
                doi = (aid.text or "").strip()
                break

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        text_chunks = []
        if journal:
            text_chunks.append(f"Journal: {journal}")
        text_chunks.append(f"PMID: {pmid}")
        if doi:
            text_chunks.append(f"DOI: {doi}")
        text_chunks.append(f"Published: {pub_dt.date().isoformat()}")
        text_chunks.append("")
        text_chunks.append(title)
        if abstract:
            text_chunks.append("")
            text_chunks.append(abstract)

        articles.append(
            {
                "id": pmid,
                "title": title.strip(),
                "url": url,
                "published_at": pub_dt,
                "journal": journal.strip() if journal else "",
                "journal_iso_abbrev": journal_iso_abbrev.strip() if journal_iso_abbrev else "",
                "journal_medline_ta": journal_medline_ta.strip() if journal_medline_ta else "",
                "doi": doi,
                "text": "\n".join(text_chunks).strip(),
                "abstract": abstract,
            }
        )

        if len(articles) >= max_items:
            break

    articles.sort(key=lambda x: x.get("published_at") or _utc_now(), reverse=True)
    return articles


def search_recent_pubmed(
    *,
    term: str,
    hours: int = 24,
    max_items: int = 5,
    timeout_s: int = 25,
) -> List[Dict[str, Any]]:
    term = (term or "").strip()
    if not term:
        print("[pubmed] WARN: empty term -> returning 0 items")
        return []

    now = _utc_now()
    since = now - timedelta(hours=hours)

    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retmax": str(max_items),
        "sort": "date",
        "datetype": _date_type(),
        "mindate": _date_yyyymmdd(since),
        "maxdate": _date_yyyymmdd(now),
    }

    print(
        f"[pubmed] Searching term={term!r} datetype={params['datetype']} "
        f"mindate={params['mindate']} maxdate={params['maxdate']} retmax={max_items}"
    )

    data = _request_json(PUBMED_ESEARCH_URL, params, timeout_s=timeout_s)
    idlist = data.get("esearchresult", {}).get("idlist", []) if isinstance(data, dict) else []
    if not idlist:
        return []

    ids = ",".join(idlist)
    fetch_params = {"db": "pubmed", "id": ids, "retmode": "xml"}
    xml_text = _request_text(PUBMED_EFETCH_URL, fetch_params, timeout_s=max(35, timeout_s))
    return _parse_pubmed_xml(xml_text, max_items=max_items)


def fetch_pubmed_abstracts(pmids: List[str], timeout_s: int = 25) -> Dict[str, str]:
    """
    Fetch abstracts for a list of PMIDs. Returns {pmid: abstract}.
    """
    pmids_clean = [str(p).strip() for p in pmids if p and str(p).strip()]
    if not pmids_clean:
        return {}

    unique_pmids = list(dict.fromkeys(pmids_clean))
    params = {"db": "pubmed", "id": ",".join(unique_pmids), "retmode": "xml"}

    xml_text = _request_text(PUBMED_EFETCH_URL, params, timeout_s=max(35, timeout_s))
    if not xml_text.strip():
        return {}

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"[pubmed] ERROR: could not parse XML in fetch_pubmed_abstracts: {e}")
        return {}

    abstracts: Dict[str, str] = {}
    for art in root.findall(".//PubmedArticle"):
        pmid = _extract_text(art.find(".//PMID"))
        if not pmid:
            continue
        abstract = _extract_abstract(art)
        if abstract:
            abstracts[pmid] = abstract

    return abstracts
