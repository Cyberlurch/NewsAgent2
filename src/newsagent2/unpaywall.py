from __future__ import annotations

import io
import os
import re
import time
from typing import Dict, Optional, Tuple
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

_UNPAYWALL_BASE = "https://api.unpaywall.org/v2/"
_DEFAULT_HEADERS = {"User-Agent": os.getenv("NCBI_TOOL") or "NewsAgent2/1.0"}
_MIN_TEXT_CHARS = 300


def lookup_unpaywall(doi: str, email: str, timeout: int = 20) -> Optional[Dict]:
    doi_norm = (doi or "").strip()
    if not doi_norm or not email:
        return None

    url = f"{_UNPAYWALL_BASE}{quote_plus(doi_norm)}"
    params = {"email": email}

    backoff_s = 1.0
    attempts = 3
    for attempt in range(attempts):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=timeout,
                headers=_DEFAULT_HEADERS,
            )
            if resp.status_code == 404:
                return None
            if resp.status_code == 429 and attempt < attempts - 1:
                time.sleep(backoff_s)
                backoff_s *= 2
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                return data
            return None
        except Exception:
            if attempt < attempts - 1:
                time.sleep(backoff_s)
                backoff_s *= 2
            else:
                return None
    return None


def pick_best_oa_url(data: Optional[Dict]) -> Optional[Dict[str, str]]:
    if not isinstance(data, dict):
        return None

    def _is_usable(loc: Dict) -> bool:
        if not isinstance(loc, dict):
            return False
        if not loc.get("is_oa") and not loc.get("license"):
            return False
        url_for_pdf = (loc.get("url_for_pdf") or "").strip()
        url = (loc.get("url") or "").strip()
        return bool(url_for_pdf or url)

    best = data.get("best_oa_location") if isinstance(data.get("best_oa_location"), dict) else None
    if not _is_usable(best):
        oa_locations = data.get("oa_locations") if isinstance(data.get("oa_locations"), list) else []
        publisher_first = sorted(
            [loc for loc in oa_locations if _is_usable(loc)],
            key=lambda x: (0 if (x.get("host_type") or "") == "publisher" else 1),
        )
        best = publisher_first[0] if publisher_first else None

    if not _is_usable(best):
        return None

    return {
        "url_for_pdf": (best.get("url_for_pdf") or "").strip(),
        "url": (best.get("url") or "").strip(),
        "host_type": (best.get("host_type") or "").strip(),
        "license": (best.get("license") or "").strip(),
    }


def _bounded_get(url: str, *, max_bytes: int, timeout: int) -> Tuple[bytes, bool]:
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


def extract_text_from_pdf_bytes(pdf_bytes: bytes, *, max_chars: int = 20000, max_pages: int = 12) -> str:
    text_parts = []
    reader = PdfReader(io.BytesIO(pdf_bytes))
    for idx, page in enumerate(reader.pages):
        if idx >= max_pages:
            break
        try:
            extracted = page.extract_text() or ""
        except Exception:
            extracted = ""
        cleaned = re.sub(r"\s+", " ", extracted).strip()
        if cleaned:
            text_parts.append(cleaned)
        if sum(len(p) for p in text_parts) >= max_chars:
            break
    combined = " ".join(text_parts).strip()
    if len(combined) > max_chars:
        combined = combined[:max_chars].rstrip()
    return combined


def _extract_html_text(html_bytes: bytes, *, max_chars: int) -> str:
    soup = BeautifulSoup(html_bytes, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text


def fetch_best_oa_fulltext(
    oa_choice: Dict[str, str],
    *,
    timeout: int,
    max_bytes: int,
    max_chars: int,
) -> Tuple[str, str, bool]:
    """
    Returns (text, source_type, size_exceeded_flag).
    source_type is 'pdf' or 'html' when text is returned.
    """

    if not isinstance(oa_choice, dict):
        return "", "", False

    pdf_url = (oa_choice.get("url_for_pdf") or "").strip()
    html_url = (oa_choice.get("url") or "").strip()

    if pdf_url:
        try:
            pdf_bytes, size_exceeded = _bounded_get(pdf_url, timeout=timeout, max_bytes=max_bytes)
            if size_exceeded:
                return "", "", True
            if pdf_bytes:
                text = extract_text_from_pdf_bytes(pdf_bytes, max_chars=max_chars)
                if len(text) >= _MIN_TEXT_CHARS:
                    return text, "pdf", False
        except Exception:
            pass

    if html_url:
        try:
            html_bytes, size_exceeded = _bounded_get(html_url, timeout=timeout, max_bytes=max_bytes)
            if size_exceeded:
                return "", "", True
            text = _extract_html_text(html_bytes, max_chars=max_chars)
            if len(text) >= _MIN_TEXT_CHARS:
                return text, "html", False
        except Exception:
            pass

    return "", "", False
