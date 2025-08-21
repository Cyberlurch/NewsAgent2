from __future__ import annotations
from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo

def to_markdown(items: List[Dict], summary: str) -> str:
    sto = ZoneInfo("Europe/Stockholm")
    now_sto = datetime.now(tz=sto)
    date_str = now_sto.strftime("%Y-%m-%d %H:%M")

    md = []
    md.append(f"# Daily Summary – {date_str} (Stockholm)")
    md.append("")
    md.append(summary.strip())
    md.append("")
    md.append("---")
    md.append("## Quellen")
    for it in items:
        md.append(f"- {it['channel']}: [{it['title']}]({it['url']})")
    md.append("")
    return "\n".join(md)

