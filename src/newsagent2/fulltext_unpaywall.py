from __future__ import annotations

import os
import re
import tempfile
from typing import Dict, Optional, Tuple
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

_UNPAYWALL_BASE = "https://api.unpaywall.org/v2/"
_DEFAULT_HEADERS = {"User-Agent": os.getenv("NCBI_TOOL") or "NewsAgent2/1.0"}


def lookup_unpaywall(
    doi: str,
    email: str,
    *,
    connect_timeout: float = 10.0,
    read_timeout: float = 30.0,
    retries: int = 2,
) -> Dict[str, str]:
    doi_norm = (doi or "").strip()
    if not doi_norm or not email:
        return {}

    url = f"{_UNPAYWALL_BASE}{quote_plus(doi_norm)}"
    params = {"email": email}
    attempts = max(1, retries + 1)
    for _ in range(attempts):
        try:
            r = requests.get(
                url,
                params=params,
                timeout=(connect_timeout, read_timeout),
                headers=_DEFAULT_HEADERS,
            )
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                continue
            best = data.get("best_oa_location")
            if not isinstance(best, dict) or not best.get("is_oa"):
                for loc in data.get("oa_locations", []) or []:
                    if isinstance(loc, dict) and loc.get("is_oa"):
                        best = loc
                        break
            if not isinstance(best, dict) or not best.get("is_oa"):
                return {}
            return {
                "is_oa": bool(best.get("is_oa")),
                "url_for_pdf": (best.get("url_for_pdf") or "").strip(),
                "url": (best.get("url") or "").strip(),
                "host_type": (best.get("host_type") or "").strip(),
                "license": (best.get("license") or "").strip(),
            }
        except Exception:
            continue
    return {}


def _download_to_tempfile(
    url: str, *, max_bytes: int, connect_timeout: float, read_timeout: float
) -> Tuple[str, str, bool]:
    """
    Download URL to a temporary file. Returns (path, content_type, size_exceeded).
    """

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        downloaded = 0
        content_type = ""
        try:
            with requests.get(
                url,
                stream=True,
                timeout=(connect_timeout, read_timeout),
                headers=_DEFAULT_HEADERS,
            ) as resp:
                resp.raise_for_status()
                content_type = (resp.headers.get("Content-Type") or "").lower()
                for chunk in resp.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    downloaded += len(chunk)
                    if downloaded > max_bytes:
                        return tmp.name, content_type, True
                    tmp.write(chunk)
        except Exception:
            return "", "", False
    return tmp.name, content_type, False


def _extract_pdf_text(path: str, *, max_pages: int, max_chars: int) -> str:
    text_parts = []
    with open(path, "rb") as f:
        reader = PdfReader(f)
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


def _extract_html_text(path: str, *, max_chars: int) -> str:
    with open(path, "rb") as f:
        raw = f.read()
    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text


def download_best_oa_fulltext(
    best_oa: Dict[str, str],
    *,
    max_bytes: int = 25_000_000,
    max_chars: int = 30_000,
    max_pages: int = 8,
    connect_timeout: float = 10.0,
    read_timeout: float = 30.0,
) -> Tuple[str, bool]:
    """
    Attempt to download and extract text from the best OA location.

    Returns (text, size_exceeded_flag).
    """

    if not isinstance(best_oa, dict) or not best_oa.get("is_oa"):
        return "", False

    url = (best_oa.get("url_for_pdf") or best_oa.get("url") or "").strip()
    if not url:
        return "", False

    path, content_type, size_exceeded = _download_to_tempfile(
        url, max_bytes=max_bytes, connect_timeout=connect_timeout, read_timeout=read_timeout
    )
    if not path:
        return "", size_exceeded
    if size_exceeded:
        try:
            os.remove(path)
        except Exception:
            pass
        return "", True

    try:
        text = ""
        content_type = content_type or ""
        is_pdf = "pdf" in content_type or url.lower().endswith(".pdf")
        if is_pdf:
            text = _extract_pdf_text(path, max_pages=max_pages, max_chars=max_chars)
        elif "html" in content_type or "text" in content_type:
            text = _extract_html_text(path, max_chars=max_chars)
        if len(text) > max_chars:
            text = text[:max_chars].rstrip()
        return text, size_exceeded
    finally:
        try:
            os.remove(path)
        except Exception:
            pass
