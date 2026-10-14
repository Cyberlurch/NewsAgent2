"""Conservative source diagnostics and dated commentary extraction; no I/O."""
from __future__ import annotations

import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit

import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from .store_io import StoreError, _pairs


def load_sources(path) -> list[dict]:
    """Missing optional configuration is distinct from invalid existing JSON."""
    def nonfinite(_):
        raise ValueError("nonfinite_json_number")
    try:
        with open(path, encoding="utf-8") as stream:
            sources = json.load(stream, object_pairs_hook=_pairs, parse_constant=nonfinite)
    except FileNotFoundError:
        return []
    except (ValueError, UnicodeError):
        raise StoreError("foamed_configuration", "corrupted_existing_store") from None
    except OSError:
        raise StoreError("foamed_configuration", "unreadable_existing_store") from None
    strategies = {"rss_then_article", "rss_only", "html_only", "audit_only", "disabled", "reference"}
    if (not isinstance(sources, list) or any(not isinstance(s, dict) or not isinstance(s.get("name"), str)
            or not s["name"].strip() or not isinstance(s.get("extraction_strategy", "rss_then_article"), str)
            or s.get("extraction_strategy", "rss_then_article") not in strategies
            or any(key in s and not isinstance(s[key], str) for key in ("feed_url", "homepage")) for s in sources)
            or len({s["name"] for s in sources}) != len(sources)):
        raise StoreError("foamed_configuration", "invalid_existing_store")
    return sources


def parse_feed(content: bytes):
    """Accept RSS/Atom structure, including empty feeds and wrong MIME types."""
    parsed = feedparser.parse(content)
    try:
        root = ET.fromstring(content)
        tag = root.tag.rsplit("}", 1)[-1].lower()
        valid = (tag == "rss" and root.find("channel") is not None) or (
            root.tag == "{http://www.w3.org/2005/Atom}feed"
        ) or (root.tag == "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF"
              and root.find("{http://purl.org/rss/1.0/}channel") is not None)
    except ET.ParseError:
        valid = False
    entries = list(getattr(parsed, "entries", []) or [])
    if valid:
        return entries, "entries" if entries else "empty_feed"
    if BeautifulSoup(content, "html.parser").find("html"):
        return [], "html_instead_of_feed"
    return [], "parse_failed"


def commentary_identity(item: dict) -> tuple[str, str] | None:
    """Scoped adapter identity; legacy rows keep their existing dedupe rules."""
    source = str(item.get("source_type") or item.get("source") or "").lower()
    iid = str(item.get("item_id") or item.get("id") or "")
    if source == "foamed" and item.get("source_role") == "commentary" and re.fullmatch(r"foamed:ccr:[0-9a-f]{64}", iid):
        return "commentary_id", iid
    return None


def freshness(newest: str | None, now: datetime, expected_days) -> str:
    if not newest or type(expected_days) not in (int, float) or not math.isfinite(expected_days) or expected_days <= 0:
        return "unknown"
    try:
        dt = datetime.fromisoformat(newest)
        if dt.tzinfo is None or dt > now:
            return "unknown"
        return "stale" if (now - dt).total_seconds() > expected_days * 86400 else "current"
    except (ValueError, TypeError):
        return "unknown"


def primary_references(block, page_url: str) -> list[dict]:
    refs = {}
    for a in block.find_all("a", href=True):
        url = urljoin(page_url, str(a["href"]))
        p = urlsplit(url)
        if p.scheme != "https" or p.username or p.password:
            continue
        host = (p.hostname or "").lower()
        if host in {"pubmed.ncbi.nlm.nih.gov", "www.pubmed.ncbi.nlm.nih.gov"}:
            m = re.fullmatch(r"/(\d+)/?", p.path)
            if m:
                value = m[1]
                refs[("pmid", value)] = {"kind": "pmid", "value": value,
                    "url": f"https://pubmed.ncbi.nlm.nih.gov/{value}/", "role": "primary_reference"}
        elif host in {"doi.org", "dx.doi.org"} and re.fullmatch(r"/10\.\d{4,9}/\S+", p.path, re.I):
            value = p.path[1:].lower()
            refs[("doi", value)] = {"kind": "doi", "value": value,
                "url": "https://doi.org/" + value, "role": "primary_reference"}
    return [refs[key] for key in sorted(refs)]


def ccr_items(content: bytes, page_url: str, name: str, now: datetime, cutoff: datetime):
    """Only explicit article blocks: never assign page-wide dates to papers.

    Unsupported live layouts are deliberately withheld pending a reviewed
    adapter fixture. Nested containers cannot assign one date to several papers.
    """
    soup = BeautifulSoup(content, "html.parser")
    items, seen = [], set()
    diagnostics = {"adapter": "ccr_dated_articles_v1", "rejected_entries": 0,
                   "dated_entries": 0, "newest_entry_datetime": None}
    blocks = soup.select("article, [itemprop='articleBody']")
    blocks = [b for b in blocks if not b.select("article, [itemprop='articleBody']")]
    diagnostics["entries_truncated"] = len(blocks) > 40
    if not blocks:
        diagnostics["adapter_result"] = "unsupported_structure"
    else:
        diagnostics["adapter_result"] = "recognized_structure"
    for block in blocks[:40]:
        dates = set()
        for tag in block.select("time[datetime]"):
            raw = tag.get("datetime", "")
            # Explicit year and calendar date; relative strings are not evidence.
            if not re.match(r"^\d{4}-\d{2}-\d{2}(?:$|T| )", raw):
                continue
            try:
                dt = dateparser.isoparse(raw)
                dates.add(dt.date().isoformat())
            except ValueError:
                pass
        refs = primary_references(block, page_url)
        heading = block.find(["h1", "h2", "h3"])
        # One paper per block. A DOI and PMID may both identify that paper; two
        # IDs of either kind are ambiguous and require a narrower source block.
        ambiguous = any(sum(r["kind"] == k for r in refs) > 1 for k in ("pmid", "doi"))
        if len(dates) != 1 or not refs or not heading or ambiguous:
            diagnostics["rejected_entries"] += 1
            continue
        day = next(iter(dates))
        if day > now.date().isoformat():
            diagnostics["rejected_entries"] += 1
            continue
        dt = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        diagnostics["dated_entries"] += 1
        newest = diagnostics["newest_entry_datetime"]
        diagnostics["newest_entry_datetime"] = max(newest or "", dt.isoformat())
        if dt.date() < cutoff.date():
            continue
        title = heading.get_text(" ", strip=True)
        # Prefer PMID when a source supplies both DOI and PMID.
        primary = next((r for r in refs if r["kind"] == "pmid"), refs[0])
        identity = hashlib.sha256((name + "\0" + day + "\0" + primary["kind"] + ":" + primary["value"]).encode()).hexdigest()
        iid = "foamed:ccr:" + identity
        if iid in seen:
            continue
        seen.add(iid)
        text = block.get_text(" ", strip=True)[:12000]
        items.append({"source": "foamed", "foamed_source": name, "source_name": name, "channel": name,
            "source_role": "commentary", "id": iid, "title": title,
            "url": page_url, "homepage": page_url, "legacy_listing_id": page_url,
            "published_at": dt, "published_field": "html_listing_entry_date",
            "published_precision": "date", "date_unknown": False, "text": text,
            "content_source": "html_excerpt", "extraction_method": "ccr_dated_articles_v1",
            "primary_references": refs})
    return items, diagnostics
