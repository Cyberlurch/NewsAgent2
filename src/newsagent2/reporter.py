from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
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
