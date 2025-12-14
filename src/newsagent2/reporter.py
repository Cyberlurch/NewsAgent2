from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

STO = ZoneInfo("Europe/Stockholm")


def _norm_language(lang: str) -> str:
    l = (lang or "").strip().lower()
    if l.startswith("en"):
        return "en"
    return "de"


def _md_escape_label(text: str) -> str:
    # Escape only what commonly breaks markdown link labels.
    return (text or "").replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def _is_cybermed_report(report_title: str, report_language: str) -> bool:
    # Primary gate: REPORT_KEY=cybermed (recommended)
    rk = (os.getenv("REPORT_KEY") or "").strip().lower()
    if rk == "cybermed":
        return True

    # Secondary gate: title contains "cybermed"
    if "cybermed" in (report_title or "").strip().lower():
        return True

    # Tertiary gate: explicitly set REPORT_PROFILE=medical
    rp = (os.getenv("REPORT_PROFILE") or "").strip().lower()
    if rp == "medical":
        return True

    # Do NOT gate purely by language; Cyberlurch could include English sources.
    return False


def _extract_bottom_line(detail_md: str) -> str:
    """
    Try to extract the BOTTOM LINE from a PubMed deep dive markdown.
    Supports formats like:
      **BOTTOM LINE:** ...
    """
    if not detail_md:
        return ""
    m = re.search(r"\*\*BOTTOM LINE:\*\*\s*(.+)", detail_md, flags=re.IGNORECASE)
    if not m:
        return ""
    line = (m.group(1) or "").strip()
    # Stop at end-of-line; deep dives usually place it on one line.
    line = line.splitlines()[0].strip()
    return line


def _infer_med_category(item: Dict[str, Any]) -> str:
    """
    Lightweight heuristic categorization based on title/journal keywords.
    If you later add item['category'] in the collector, this will respect it.
    """
    cat = (item.get("category") or "").strip()
    if cat:
        # Normalize a few expected variants
        c = cat.lower()
        if "anest" in c:
            return "Anesthesia"
        if "intensive" in c or "icu" in c or "critical" in c:
            return "Intensive Care"
        if "pain" in c or "analges" in c:
            return "Pain"
        if "ai" in c or "machine" in c or "deep learning" in c or "llm" in c:
            return "AI"
        return "Other"

    hay = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("journal") or ""),
            str(item.get("channel") or ""),
            str(item.get("text") or "")[:500],
        ]
    ).lower()

    if any(k in hay for k in ["anesth", "anaesth", "perioper", "surgery", "intraoper", "sedation"]):
        return "Anesthesia"
    if any(k in hay for k in ["icu", "intensive care", "critical care", "sepsis", "ventilat", "ards", "shock", "crrt"]):
        return "Intensive Care"
    if any(k in hay for k in ["pain", "analges", "opioid", "ketamine", "neuropath", "regional anesthesia"]):
        return "Pain"
    if any(k in hay for k in ["artificial intelligence", "machine learning", "deep learning", "llm", "chatgpt", "model", "algorithm"]):
        return "AI"
    return "Other"


def _build_source_label(item: Dict[str, Any]) -> str:
    """
    Source label format (best effort):
      YEAR · JOURNAL · FIRSTAUTHOR · SHORTTITLE
    Falls back to whatever is available.
    """
    year = str(item.get("year") or "").strip()
    journal = str(item.get("journal") or item.get("channel") or "").strip()
    first_author = str(item.get("first_author") or item.get("author_first") or "").strip()
    short_title = str(item.get("short_title") or "").strip()

    parts: List[str] = []
    if year:
        parts.append(year)
    if journal:
        parts.append(journal)
    if first_author:
        parts.append(first_author)
    if short_title:
        parts.append(short_title)

    if not parts:
        # Final fallback: title itself (keeps label from being empty)
        t = str(item.get("title") or "").strip()
        return t[:140] if t else "Source"
    return " · ".join(parts)


def to_markdown(
    items: List[Dict[str, Any]],
    overview_markdown: str,
    details_by_id: Dict[str, str],
    *,
    report_title: str = "Daily Report",
    report_language: str = "de",
) -> str:
    """
    Build the final Markdown report.

    - items: list of collected items (already filtered/sorted in main)
    - overview_markdown: the overview section produced by summarizer
    - details_by_id: mapping item.id -> detail markdown (deep dives)
    """
    lang = _norm_language(report_language)

    title = (report_title or "Daily Report").strip()
    now_str = datetime.now(tz=STO).strftime("%Y-%m-%d %H:%M")
    if lang == "de":
        now_str += " Uhr"

    md_lines: List[str] = []
    md_lines.append(title)
    md_lines.append(now_str)
    md_lines.append("")  # blank line

    overview_markdown = (overview_markdown or "").strip()
    if overview_markdown:
        md_lines.append(overview_markdown)
        md_lines.append("")

    # -------------------------
    # Cybermed paper-first block
    # -------------------------
    if _is_cybermed_report(title, report_language):
        pubmed_items = [it for it in items if (str(it.get("source") or "").strip().lower() == "pubmed")]
        if pubmed_items:
            md_lines.append("## Papers")
            md_lines.append("")

            # Group by category
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for it in pubmed_items:
                c = _infer_med_category(it)
                grouped.setdefault(c, []).append(it)

            order = ["Anesthesia", "Intensive Care", "Pain", "AI", "Other"]
            for cat in order:
                cat_items = grouped.get(cat, [])
                if not cat_items:
                    continue

                md_lines.append(f"### {cat}")
                md_lines.append("")

                for it in cat_items:
                    iid = str(it.get("id") or "").strip()
                    url = str(it.get("url") or "").strip()
                    title_lbl = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                    label = _md_escape_label(_build_source_label(it))

                    detail = (details_by_id.get(iid) or "").strip()
                    bottom = _extract_bottom_line(detail)

                    # Paper line: clickable title + source label
                    if url:
                        md_lines.append(f"- [{title_lbl}]({url}) — *{label}*")
                    else:
                        md_lines.append(f"- {title_lbl} — *{label}*")

                    # Bottom line (best effort)
                    if bottom:
                        md_lines.append(f"  - **BOTTOM LINE:** {bottom}")
                    else:
                        md_lines.append("  - **BOTTOM LINE:** (not available)")

                md_lines.append("")

    deep_dives_heading = "## Vertiefungen" if lang == "de" else "## Deep Dives"
    sources_heading = "## Quellen" if lang == "de" else "## Sources"

    # Deep dives: only for items that have a detail summary
    detail_items = [it for it in items if str(it.get("id") or "") in details_by_id]
    if detail_items:
        md_lines.append(deep_dives_heading)
        md_lines.append("")

        for it in detail_items:
            iid = str(it.get("id") or "").strip()
            ch = _md_escape_label(str(it.get("channel") or "").strip())
            title_lbl = _md_escape_label(str(it.get("title") or "").strip())
            url = str(it.get("url") or "").strip()

            if ch:
                md_lines.append(f"### {ch}: [{title_lbl}]({url})")
            else:
                md_lines.append(f"### [{title_lbl}]({url})")
            md_lines.append("")
            md_lines.append(details_by_id.get(iid, "").strip())
            md_lines.append("")

    # Sources: dedup by URL
    seen_urls: set[str] = set()
    sources: List[str] = []
    for it in items:
        url = str(it.get("url") or "").strip()
        title_lbl = str(it.get("title") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        sources.append(f"- {title_lbl}: {url}")

    md_lines.append(sources_heading)
    md_lines.append("")
    if sources:
        md_lines.extend(sources)
    else:
        md_lines.append("- (keine)" if lang == "de" else "- (none)")

    md_lines.append("")
    return "\n".join(md_lines)
