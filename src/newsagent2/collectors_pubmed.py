from __future__ import annotations

import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
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


class PubMedError(RuntimeError):
    """Only a safe category crosses the collector boundary."""
    def __init__(self, category: str):
        self.category = category
        super().__init__("pubmed:" + category)


@dataclass
class RetrievalBudget:
    seconds: float = 120.0
    max_requests: int = 60
    max_records: int = 650
    requests: int = 0
    records: int = 0

    def __post_init__(self):
        self.deadline = time.monotonic() + self.seconds

    def remaining(self):
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise PubMedError("time_budget")
        return left

    def pause(self, seconds):
        if seconds >= self.remaining():
            raise PubMedError("time_budget")
        time.sleep(max(0.0, seconds))
        self.remaining()

    def request(self, timeout):
        if self.requests >= self.max_requests:
            raise PubMedError("request_budget")
        left = self.remaining()
        self.requests += 1
        return min(timeout, left)


_BUDGET = ContextVar("pubmed_retrieval_budget", default=None)


@contextmanager
def retrieval_budget(budget):
    token = _BUDGET.set(budget)
    try:
        yield
    finally:
        _BUDGET.reset(token)


def _request_with_retries(*, url, params, timeout_s, expect):
    api_key, tool, email = _get_ncbi_params()
    params = dict(params)
    for name, value in (("api_key", api_key), ("tool", tool), ("email", email)):
        if value and name not in params:
            params[name] = value
    headers = _build_headers(email)
    try:
        retries = max(1, min(int(os.getenv("PUBMED_MAX_RETRIES") or 4), 10))
        backoff = max(0.0, min(float(os.getenv("PUBMED_BACKOFF_BASE_S") or 1), 30.0))
    except ValueError:
        raise PubMedError("invalid_retry_configuration") from None
    budget = _BUDGET.get()
    pause = budget.pause if budget else time.sleep
    category = "transport_failed"
    for attempt in range(retries):
        if budget:
            global _LAST_REQUEST_TS
            try:
                pause(max(0.0, _min_interval_s(api_key) - (time.monotonic() - _LAST_REQUEST_TS)))
                timeout = budget.request(timeout_s)
            except PubMedError:
                if attempt:
                    raise PubMedError(category) from None
                raise
            _LAST_REQUEST_TS = time.monotonic()
        else:
            _rate_limit_sleep(api_key)
            timeout = timeout_s
        delay = min(30.0, backoff * 2 ** attempt)
        try:
            response = _SESSION.get(url, params=params, headers=headers, timeout=timeout)
        except requests.Timeout:
            category = "timeout"
        except requests.RequestException:
            category = "transport_failed"
        else:
            if budget:
                budget.remaining()
            if response.status_code == 429 or 500 <= response.status_code <= 599:
                category = "rate_limited" if response.status_code == 429 else "server_error"
                try:
                    delay = max(delay, float(response.headers.get("Retry-After") or 0))
                except ValueError:
                    pass
                delay = min(30.0, delay)
            else:
                if response.status_code != 200:
                    raise PubMedError("http_error")
                if expect == "text":
                    if not isinstance(response.text, str) or not response.text.strip():
                        raise PubMedError("empty_response")
                    return response.text
                try:
                    data = response.json()
                except ValueError:
                    raise PubMedError("invalid_json") from None
                if not isinstance(data, dict):
                    raise PubMedError("invalid_json_structure")
                return data
        if attempt + 1 < retries:
            try:
                pause(delay)
            except PubMedError:
                # Budget exhaustion cannot relabel an observed source failure
                # as an ordinary bounded successful partial report.
                raise PubMedError(category) from None
    raise PubMedError(category) from None


def _request_json(url, params, timeout_s=25):
    return _request_with_retries(url=url, params=params, timeout_s=timeout_s, expect="json")


def _request_text(url, params, timeout_s=35):
    return _request_with_retries(url=url, params=params, timeout_s=timeout_s, expect="text")


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


def _extract_abstract_sections(article: ET.Element, max_section_chars: int = 1200) -> List[Dict[str, str]]:
    sections: List[Dict[str, str]] = []
    for ab in article.findall(".//Abstract/AbstractText"):
        text = _normalize_itertext(ab)
        if not text:
            continue
        label = (ab.attrib.get("Label") or ab.attrib.get("NlmCategory") or "").strip()
        if max_section_chars > 0 and len(text) > max_section_chars:
            text = text[:max_section_chars].rstrip() + "…"
        sections.append({"label": label, "text": text})
    return sections


def _extract_unique_values(xpath_values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in xpath_values:
        norm = " ".join((raw or "").split()).strip()
        if not norm:
            continue
        key = norm.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out


def _derive_evidence_tags(
    *,
    publication_types: List[str],
    mesh_headings: List[str],
    keywords: List[str],
    title: str,
    abstract: str,
) -> List[str]:
    haystack = " ".join(
        [title or "", abstract or "", " ".join(mesh_headings), " ".join(keywords), " ".join(publication_types)]
    ).lower()
    pub_types_cf = {p.casefold() for p in publication_types}
    tags: List[str] = []

    def _add(tag: str) -> None:
        if tag not in tags:
            tags.append(tag)

    if "randomized controlled trial" in pub_types_cf or " randomiz" in f" {haystack} ":
        _add("randomized_trial")
    if "clinical trial" in pub_types_cf or " clinical trial" in haystack:
        _add("clinical_trial")
    if "systematic review" in pub_types_cf or "systematic review" in haystack:
        _add("systematic_review")
    if "meta-analysis" in pub_types_cf or "meta analysis" in haystack or "meta-analysis" in haystack:
        _add("meta_analysis")
    if "practice guideline" in pub_types_cf or "practice guideline" in haystack:
        _add("practice_guideline")
    if "guideline" in pub_types_cf or "guideline" in haystack:
        _add("guideline")
    if "observational study" in pub_types_cf or "observational" in haystack:
        _add("observational")
    if " cohort " in f" {haystack} ":
        _add("cohort")
    if "case reports" in pub_types_cf or " case report" in haystack:
        _add("case_report")
    if {"editorial", "comment", "letter"} & pub_types_cf:
        _add("editorial_or_comment")
    if "animals" in haystack or "animal model" in haystack or "preclinical" in haystack:
        _add("animal_or_preclinical")
    if ("machine learning" in haystack or "artificial intelligence" in haystack or "prediction model" in haystack):
        _add("ai_prediction_model")
    if ("diagnostic accuracy" in haystack or "sensitivity" in haystack and "specificity" in haystack):
        _add("diagnostic_accuracy")
    if "resuscitation" in haystack or "cardiopulmonary resuscitation" in haystack:
        _add("resuscitation")
    if "intensive care" in haystack or "icu" in f" {haystack} ":
        _add("intensive_care")
    if "anesthesia" in haystack or "anaesthesia" in haystack:
        _add("anesthesia")
    if "emergency medicine" in haystack or "emergency department" in haystack:
        _add("emergency_medicine")
    if "pain" in haystack and ("regional" in haystack or "nerve block" in haystack):
        _add("pain_regional")
    return tags


def _parse_pub_date(article: ET.Element) -> Optional[datetime]:
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

    return None


def _parse_pubmed_xml(xml_text: str, max_items: int, *, preserve_unknown_dates: bool = False) -> List[Dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        raise PubMedError("invalid_xml") from None
    if root.tag != "PubmedArticleSet":
        raise PubMedError("invalid_xml_structure")

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
        if pub_dt is None and not preserve_unknown_dates:
            pub_dt = _utc_now()  # Compatibility for the legacy publication-date collector.

        article_ids: Dict[str, str] = {}
        for aid in art.findall(".//ArticleIdList/ArticleId"):
            id_type = (aid.attrib.get("IdType") or "").lower().strip()
            value = (aid.text or "").strip()
            if id_type and value and id_type not in article_ids:
                article_ids[id_type] = value
        doi = article_ids.get("doi", "")
        pmcid = article_ids.get("pmc", "") or article_ids.get("pmcid", "")
        pii = article_ids.get("pii", "")
        publication_types = _extract_unique_values(
            [_extract_text(pt) for pt in art.findall(".//PublicationTypeList/PublicationType")]
        )
        mesh_headings = _extract_unique_values(
            [_extract_text(mh) for mh in art.findall(".//MeshHeadingList/MeshHeading/DescriptorName")]
        )
        keywords = _extract_unique_values([_extract_text(kw) for kw in art.findall(".//KeywordList/Keyword")])
        comments_corrections = [{"ref_type": str(node.attrib.get("RefType") or ""),
                                 "pmid": _extract_text(node.find("PMID"))}
                                for node in art.findall(".//CommentsCorrectionsList/CommentsCorrections")]
        abstract_sections = _extract_abstract_sections(art)
        evidence_tags = _derive_evidence_tags(
            publication_types=publication_types,
            mesh_headings=mesh_headings,
            keywords=keywords,
            title=title,
            abstract=abstract,
        )

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        text_chunks = []
        if journal:
            text_chunks.append(f"Journal: {journal}")
        text_chunks.append(f"PMID: {pmid}")
        if doi:
            text_chunks.append(f"DOI: {doi}")
        if pub_dt is not None:
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
                "pmcid": pmcid,
                "pii": pii,
                "publication_types": publication_types,
                "comments_corrections": comments_corrections,
                "mesh_headings": mesh_headings,
                "keywords": keywords,
                "abstract_sections": abstract_sections,
                "evidence_tags": evidence_tags,
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
    return_metadata: bool = False,
    reference_now_utc: datetime | None = None,
    ingestion_session=None,
    query_name: str = "",
) -> Any:
    term = (term or "").strip()
    if ingestion_session is not None:
        return ingestion_session.collect(query_name, term, max_items)
    if not term:
        print("[pubmed] WARN: empty term -> returning 0 items")
        empty_meta = {"query_term": "", "retmax": int(max_items), "esearch_count_total": 0, "idlist_count": 0, "fetched_xml_count": 0, "parsed_article_count": 0, "possibly_truncated": False}
        return ([], empty_meta) if return_metadata else []

    now = reference_now_utc or _utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
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
    esearch_count_total, idlist = validate_esearch(data)
    if not idlist:
        meta = {"query_term": term, "retmax": int(max_items), "esearch_count_total": esearch_count_total, "idlist_count": 0, "fetched_xml_count": 0, "parsed_article_count": 0, "possibly_truncated": esearch_count_total > 0}
        return ([], meta) if return_metadata else []

    ids = ",".join(idlist)
    fetch_params = {"db": "pubmed", "id": ids, "retmode": "xml"}
    xml_text = _request_text(PUBMED_EFETCH_URL, fetch_params, timeout_s=max(35, timeout_s))
    parsed = _parse_pubmed_xml(xml_text, max_items=max_items)
    if {row["id"] for row in parsed} != set(idlist):
        raise PubMedError("missing_fetch_ids")
    meta = {"query_term": term, "retmax": int(max_items), "esearch_count_total": esearch_count_total, "idlist_count": len(idlist), "fetched_xml_count": len(parsed), "parsed_article_count": len(parsed), "possibly_truncated": esearch_count_total > len(idlist)}
    return (parsed, meta) if return_metadata else parsed


def fetch_pubmed_abstracts(pmids: List[str], timeout_s: int = 25) -> Dict[str, str]:
    """
    Fetch abstracts for a list of PMIDs. Returns {pmid: abstract}.
    """
    pmids_clean = [str(p).strip() for p in pmids if p and str(p).strip()]
    if not pmids_clean:
        return {}

    unique_pmids = list(dict.fromkeys(pmids_clean))
    params = {"db": "pubmed", "id": ",".join(unique_pmids), "retmode": "xml"}

    records = fetch_pubmed_records(unique_pmids, timeout_s=timeout_s)
    return {row["id"]: row["abstract"] for row in records if row.get("abstract")}


def validate_esearch(data):
    result = data.get("esearchresult") if isinstance(data, dict) else None
    if not isinstance(result, dict) or "error" in result or result.get("errorlist") or data.get("error"):
        raise PubMedError("invalid_esearch")
    count = result.get("count")
    ids = result.get("idlist")
    if not isinstance(count, (str, int)) or isinstance(count, bool) or not str(count).isdigit():
        raise PubMedError("invalid_esearch_count")
    count = int(count)
    if (not isinstance(ids, list) or any(not isinstance(i, str) or not i.isdigit() for i in ids)
            or len(set(ids)) != len(ids) or len(ids) > count or (count == 0 and ids)):
        raise PubMedError("invalid_esearch_ids")
    if count and not ids:
        raise PubMedError("missing_search_ids")
    return count, ids


def discover_pubmed_ids(term, start, end, *, page_size=500):
    """Snapshot at most the documented 10,000 PubMed ESearch IDs.

    Never persist a retstart offset into a mutable result set. A truncated or
    changing snapshot is retried from zero; fetched work remains in the store.
    """
    query = f'({term}) AND ("{start:%Y/%m/%d}"[crdt] : "{end:%Y/%m/%d}"[crdt])'
    found = []
    count = None
    pages = 0
    history = None
    budget = _BUDGET.get()
    # A very small remaining request budget must not repeatedly abandon the
    # same prefix. PubMed's entire supported ID range fits one bounded request.
    if budget and budget.max_requests - budget.requests < (10000 + page_size - 1) // page_size + 1:
        page_size = 10000
    while count is None or len(found) < count:
        params = {"db": "pubmed", "term": query if history is None else "#" + history[1],
                  "retmode": "json", "retstart": str(len(found)), "retmax": str(page_size),
                  "sort": "pub date", "usehistory": "y"}
        if history:
            params["WebEnv"] = history[0]
        data = _request_json(PUBMED_ESEARCH_URL, params)
        total, ids = validate_esearch(data)
        pages += 1
        if total > 10000:
            raise PubMedError("api_result_limit")
        if count is not None and total != count:
            raise PubMedError("search_snapshot_changed")
        count = total
        if pages == 1 and count > len(ids):
            result = data["esearchresult"]
            webenv, querykey = result.get("webenv"), result.get("querykey")
            if not isinstance(webenv, str) or not webenv or not isinstance(querykey, str) or not querykey.isdigit():
                raise PubMedError("invalid_esearch_history")
            history = (webenv, querykey)
        if set(ids) & set(found):
            raise PubMedError("search_snapshot_changed")
        found.extend(ids)
        if len(found) > count:
            raise PubMedError("invalid_esearch_count")
    return found, {"esearch_count_total": count, "pages": pages,
                   "window_start": start.isoformat(), "window_end": end.isoformat(), "date_basis": "crdt"}


def fetch_pubmed_records(ids, timeout_s=25):
    if not ids:
        return []
    xml = _request_text(PUBMED_EFETCH_URL, {"db": "pubmed", "id": ",".join(ids), "retmode": "xml"}, timeout_s)
    records = _parse_pubmed_xml(xml, max_items=len(ids) + 1, preserve_unknown_dates=True)
    actual = [row["id"] for row in records]
    if len(actual) != len(set(actual)) or set(actual) != set(ids):
        raise PubMedError("missing_fetch_ids")
    return records
