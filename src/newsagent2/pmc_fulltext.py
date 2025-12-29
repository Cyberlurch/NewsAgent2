from __future__ import annotations

import io
import os
import tarfile
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

_PMC_ID_CONVERTER_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
_PMC_OA_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"
_DEFAULT_HEADERS = {"User-Agent": os.getenv("NCBI_TOOL") or "NewsAgent2/1.0"}


def _bounded_get(url: str, *, timeout: float, max_bytes: int) -> Tuple[bytes, bool]:
    resp = requests.get(url, timeout=timeout, stream=True, headers=_DEFAULT_HEADERS)
    resp.raise_for_status()
    data = io.BytesIO()
    downloaded = 0
    for chunk in resp.iter_content(chunk_size=8192):
        if not chunk:
            continue
        downloaded += len(chunk)
        if downloaded > max_bytes:
            return b"", True
        data.write(chunk)
    return data.getvalue(), False


def _extract_text_from_xml_bytes(xml_bytes: bytes, max_chars: int) -> str:
    soup = BeautifulSoup(xml_bytes, "xml")
    body = soup.find("body") or soup
    text = body.get_text(" ", strip=True)
    if len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text


def get_pmcids_for_pmids(pmids: List[str], *, timeout: float = 10.0) -> Dict[str, str]:
    ids = [pid.strip() for pid in pmids if pid and pid.strip()]
    if not ids:
        return {}

    params = {
        "ids": ",".join(ids),
        "format": "json",
        "tool": os.getenv("NCBI_TOOL") or "NewsAgent2",
        "email": os.getenv("NCBI_EMAIL", ""),
    }
    try:
        r = requests.get(_PMC_ID_CONVERTER_URL, params=params, timeout=timeout, headers=_DEFAULT_HEADERS)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return {}

    records = data.get("records") if isinstance(data, dict) else None
    if not isinstance(records, list):
        return {}

    pmcid_map: Dict[str, str] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        pmid = str(rec.get("pmid") or "").strip()
        pmcid = str(rec.get("pmcid") or "").strip()
        if pmid and pmcid:
            pmcid_map[pmid] = pmcid
    return pmcid_map


def get_oa_links(pmcid: str, *, timeout: float = 10.0) -> List[Dict[str, str]]:
    pmcid_norm = pmcid.strip()
    if not pmcid_norm:
        return []
    if not pmcid_norm.lower().startswith("pmc"):
        pmcid_norm = f"PMC{pmcid_norm}"

    try:
        r = requests.get(
            _PMC_OA_URL,
            params={"id": pmcid_norm, "format": "xml"},
            timeout=timeout,
            headers=_DEFAULT_HEADERS,
        )
        r.raise_for_status()
    except Exception:
        return []

    try:
        soup = BeautifulSoup(r.text, "xml")
        links = []
        for link in soup.find_all("link"):
            href = (link.get("href") or "").strip()
            fmt = (link.get("format") or "").strip().lower()
            if href:
                links.append({"href": href, "format": fmt})
        return links
    except Exception:
        return []


def fetch_and_extract_fulltext(
    links: List[Dict[str, str]],
    *,
    timeout_s: float,
    max_bytes: int,
    max_chars: int,
) -> Tuple[str, bool]:
    """
    Returns (text, skipped_for_size).
    """

    if not links:
        return "", False

    preferred_order = ["tgz", "xml"]
    chosen = None
    for fmt in preferred_order:
        for link in links:
            if (link.get("format") or "").lower() == fmt and (link.get("href") or "").strip():
                chosen = link
                break
        if chosen:
            break

    if not chosen:
        return "", False

    href = (chosen.get("href") or "").strip()
    fmt = (chosen.get("format") or "").lower()
    if not href:
        return "", False

    try:
        raw_bytes, size_exceeded = _bounded_get(href, timeout=timeout_s, max_bytes=max_bytes)
        if size_exceeded:
            return "", True
        if fmt == "tgz":
            with tarfile.open(mode="r:gz", fileobj=io.BytesIO(raw_bytes)) as tf:
                members = [m for m in tf.getmembers() if m.isfile() and (m.name.endswith(".nxml") or m.name.endswith(".xml"))]
                if not members:
                    return "", False
                candidate = members[0]
                if candidate.size > max_bytes:
                    return "", True
                extracted = tf.extractfile(candidate)
                if not extracted:
                    return "", False
                xml_bytes = extracted.read()
                text = _extract_text_from_xml_bytes(xml_bytes, max_chars)
                return text, False
        if fmt == "xml":
            text = _extract_text_from_xml_bytes(raw_bytes, max_chars)
            return text, False
    except Exception:
        return "", False

    return "", False
