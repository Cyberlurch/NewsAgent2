from __future__ import annotations

from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo


def to_markdown(
    items: List[Dict],
    overview: str,
    details_by_id: Dict[str, str],
) -> str:
    """
    Erzeugt den finalen Markdown-Report mit:
      - Header + Zeitstempel,
      - Kurzüberblick (Overview),
      - optional: Vertiefungen pro Video,
      - Quellenliste.
    """
    sto = ZoneInfo("Europe/Stockholm")
    now_sto = datetime.now(tz=sto)
    date_str = now_sto.strftime("%Y-%m-%d %H:%M")

    md: List[str] = []
    md.append(f"# Daily Summary – {date_str} (Stockholm)")
    md.append("")

    # Kurzüberblick
    md.append("## Kurzüberblick")
    md.append("")
    md.append((overview or "").strip())
    md.append("")

    # Vertiefungen (falls vorhanden)
    if details_by_id:
        md.append("---")
        md.append("## Vertiefungen")
        md.append("")

        # Reihenfolge: neueste Videos zuerst, aber nur die mit Detailtext
        ordered_items = sorted(
            [it for it in items if it.get("id") in details_by_id],
            key=lambda it: it["published_at"],
            reverse=True,
        )

        for it in ordered_items:
            detail = (details_by_id.get(it["id"]) or "").strip()
            if not detail:
                continue
            md.append(f"### {it['channel']}: {it['title']}")
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
