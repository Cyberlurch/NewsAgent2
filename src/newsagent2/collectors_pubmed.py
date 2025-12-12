from __future__ import annotations

import json
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests


PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


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


def _request_json(url: str, params: Dict[str, Any], timeout_s: int = 25) -> Dict[str, Any]:
    try:
        r = requests.get(url, params=params, timeout=timeout_s)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"[pubmed] ERROR: request failed: {e} (url={url})")
        return {}
    except json.JSONDecodeError as e:
        print(f"[pubmed] ERROR: invalid JSON response: {e} (url={url})")
        return {}


def _request_text(url: str, params: Dict[str, Any], timeout_s: int = 35) -> str:
    try:
        r = requests.get(url, params=params, timeout=timeout_s)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"[pubmed] ERROR: request failed: {e} (url={url})")
        return ""


def _pubmed_search_url(term: str) -> str:
    return "https://pubmed.ncbi.nlm.nih.gov/?" + urllib.parse.urlencode({"term": term})


def _extract_text(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()


def _parse_pub_date(article: ET.Element) -> datetime:
    # Try ArticleDate (electronic) first, then Journal PubDate, then fallback now.
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

        title = _extract_text(art.find(".//ArticleTitle"))
        if not title:
            title = f"PubMed Article {pmid}"

        journal = _extract_text(art.find(".//Journal/Title"))
        abstract_parts = []
        for ab in art.findall(".//Abstract/AbstractText"):
            part = _extract_text(ab)
            if part:
                label = ab.attrib.get("Label") or ab.attrib.get("NlmCategory") or ""
                label = label.strip()
                if label and label.lower() not in ("abstract",):
                    abstract_parts.append(f"{label}: {part}")
                else:
                    abstract_parts.append(part)
        abstract = "\n\n".join([p for p in abstract_parts if p]).strip()

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
                "doi": doi,
                "text": "\n".join(text_chunks).strip(),
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
    """
    Search PubMed for recent items by publication date.

    - term: PubMed search term (e.g. '"Intensive Care Med"[jour]' or a boolean query).
    - hours: look-back window; applied as date-range filter (day-granularity).
    - max_items: maximum number of results to return.
    """
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
        "datetype": "pdat",
        "mindate": _date_yyyymmdd(since),
        "maxdate": _date_yyyymmdd(now),
    }

    print(f"[pubmed] Searching term={term!r} mindate={params['mindate']} maxdate={params['maxdate']} retmax={max_items}")

    data = _request_json(PUBMED_ESEARCH_URL, params, timeout_s=timeout_s)
    idlist = data.get("esearchresult", {}).get("idlist", []) if isinstance(data, dict) else []
    if not idlist:
        return []

    ids = ",".join(idlist)
    fetch_params = {"db": "pubmed", "id": ids, "retmode": "xml"}
    xml_text = _request_text(PUBMED_EFETCH_URL, fetch_params, timeout_s=max(35, timeout_s))
    return _parse_pubmed_xml(xml_text, max_items=max_items)
