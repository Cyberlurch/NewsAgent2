from __future__ import annotations

from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo


def to_markdown(
    items: List[Dict],
    overview: str,
    details_by_id: Dict[str, str],
    report_title: str = "The Cyberlurch Report",
) -> str:
    """
    Baut den finalen Markdown-Report:

    - Kopf: Titel + Zeitstempel
    - Overview: kompletter Text aus dem Summarizer (inkl. eigener Überschriften)
    - Vertiefungen: Detailblöcke pro ausgewähltem Video
    - Quellen: Liste aller verarbeiteten Videos
    """
    sto = ZoneInfo("Europe/Stockholm")
    now_sto = datetime.now(tz=sto)
    date_str = now_sto.strftime("%Y-%m-%d %H:%M Uhr")

    title = (report_title or "").strip() or "The Cyberlurch Report"

    md: List[str] = []

    # Kopf
    md.append(f"# {title}")
    md.append("")
    md.append(f"#### {date_str}")
    md.append("")

    # Overview – bereits vom Summarizer mit Überschrift(en) versehen
    md.append((overview or "").strip())
    md.append("")

    # Vertiefungen (Detail-Summaries)
    if details_by_id:
        md.append("---")
        md.append("## Vertiefungen")
        md.append("")

        # Nur Items mit Detailtext, sortiert nach Veröffentlichungszeit (neueste zuerst)
        ordered_items = sorted(
            [it for it in items if it.get("id") in details_by_id],
            key=lambda it: it["published_at"],
            reverse=True,
        )
        for it in ordered_items:
            detail = (details_by_id.get(it["id"]) or "").strip()
            if not detail:
                continue

            # Kanal: [Titel](URL) – Titel klickbar
            md.append(f"### {it['channel']}: [{it['title']}]({it['url']})")
            md.append("")
            md.append(detail)
            md.append("")

    # Quellenliste
    md.append("---")
    md.append("## Quellen")
    for it in items:
        md.append(f"- {it['channel']}: [{it['title']}]({it['url']})")
    md.append("")

    return "\n".join(md)
