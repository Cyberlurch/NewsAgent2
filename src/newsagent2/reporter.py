from __future__ import annotations

import os, re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

STO = ZoneInfo("Europe/Stockholm")

def _norm_language(lang: str) -> str:
    l = (lang or "").strip().lower()
    return "en" if l.startswith("en") else "de"

def _md_escape_label(text: str) -> str:
    return (text or "").replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")

def _is_cybermed_report(report_title: str, report_language: str) -> bool:
    rk = (os.getenv("REPORT_KEY") or "").strip().lower()
    if rk == "cybermed":
        return True
    if "cybermed" in (report_title or "").strip().lower():
        return True
    rp = (os.getenv("REPORT_PROFILE") or "").strip().lower()
    return rp == "medical"

def _extract_bottom_line(detail_md: str) -> str:
    if not detail_md:
        return ""
    m = re.search(r"\*\*BOTTOM LINE:\*\*\s*(.+)", detail_md, flags=re.IGNORECASE)
    if not m:
        return ""
    return (m.group(1) or "").strip().splitlines()[0].strip()

def _extract_cybermed_meta_block(overview_markdown: str) -> str:
    text = (overview_markdown or "").strip()
    if not text:
        return ""
    lines = text.splitlines()
    start: Optional[int] = None
    for i, ln in enumerate(lines):
        if ln.strip().lower() == "**cybermed report metadata**":
            start = i
            break
    if start is None:
        return ""
    kept: List[str] = []
    for ln in lines[start:]:
        s = ln.strip().lower()
        if s.startswith("## "):
            break
        if s.startswith("executive summary") or s.startswith("kurzüberblick"):
            break
        if s.startswith("in brief"):
            break
        if s.startswith("papers"):
            break
        kept.append(ln.rstrip())
    return "\n".join(kept).strip()

def _build_source_label(item: Dict[str, Any]) -> str:
    year = str(item.get("year") or "").strip()
    journal = str(item.get("journal") or "").strip()
    if not journal:
        journal = str(item.get("channel") or "").strip()
        if journal.lower().startswith("pubmed:"):
            journal = journal.split(":", 1)[1].strip()
    first_author = str(item.get("first_author") or item.get("author_first") or "").strip()
    parts = [p for p in (year, journal, first_author) if p]
    return " · ".join(parts) if parts else "PubMed"

def _study_strength_from_text(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ("meta-analysis", "systematic review", "network meta", "umbrella review")):
        return "Higher (review/meta-analysis)"
    if any(k in t for k in ("randomized", "randomised", "randomized controlled", "rct", "trial")):
        return "Moderate–higher (trial)"
    if any(k in t for k in ("cohort", "case-control", "observational", "registry", "retrospective", "prospective")):
        return "Moderate (observational)"
    if any(k in t for k in ("case series", "case report")):
        return "Low (case-based)"
    if any(k in t for k in ("commentary", "editorial", "letter", "reply", "correspondence")):
        return "Very low (commentary/correspondence)"
    return "Unclear (methods not classified)"

def _fallback_bottom_line(item: Dict[str, Any]) -> str:
    title = (item.get("title") or "").strip()
    strength = _study_strength_from_text((item.get("text") or "").strip())
    anchor = "This paper" if not title else f"This paper ({title[:90].rstrip()}...)"
    return (
        f"{anchor} was listed, but no deep-dive summary was generated for it in this run; treat any clinical implication as preliminary. "
        f"Evidence strength (best-effort from abstract keywords): {strength}."
    )

def _infer_track_and_subcategory(item: Dict[str, Any]) -> Tuple[str, str]:
    hay = " ".join((str(item.get(k) or "") for k in ("title", "journal", "channel"))).lower()

    if any(k in hay for k in ("acta anaesthesiol scand", "anaesthesia", "br j anaesth", "anesth analg", "reg anesth pain med", "pain")):
        track = "Anaesthesiology"
    elif any(k in hay for k in ("intensive care med", "crit care", "resuscitation")):
        track = "Critical Care"
    else:
        ana_hits = any(k in hay for k in ("anaesth", "anesth", "anesthesia", "perioper", "postoperative", "regional", "neuraxial", "epidural", "spinal", "nerve block", "pain", "analges"))
        cc_hits = any(k in hay for k in ("icu", "intensive care", "critical care", "sepsis", "shock", "ventilat", "ards", "ecmo", "resuscitation", "cardiac arrest", "crrt", "dialysis", "vasopressor", "norepinephrine"))
        if ana_hits and not cc_hits:
            track = "Anaesthesiology"
        elif cc_hits and not ana_hits:
            track = "Critical Care"
        else:
            track = "Anaesthesiology" if any(k in hay for k in ("perioper", "postoperative", "regional", "pain")) else "Critical Care"

    if track == "Critical Care":
        if any(k in hay for k in ("shock", "vasopressor", "norepinephrine", "hemodynamic", "haemodynamic", "circulat", "cardiac", "arrest", "ecpr")):
            sub = "Circulation"
        elif any(k in hay for k in ("ventilat", "ards", "oxygen", "respir", "intubat", "airway", "ecmo", "pneumonia")):
            sub = "Respiration"
        elif any(k in hay for k in ("sepsis", "septic", "infection", "bacter", "antibiotic", "fungal", "pneumonia")):
            sub = "Infection/Sepsis"
        elif any(k in hay for k in ("renal", "kidney", "crrt", "dialysis", "hemofiltration", "haemofiltration")):
            sub = "Renal/CRRT"
        elif any(k in hay for k in ("neuro", "brain", "stroke", "delirium", "seizure", "intracran", "cerebral")):
            sub = "Neuro"
        else:
            sub = "Other Critical Care"
    else:
        if any(k in hay for k in ("regional", "nerve block", "block", "neuraxial", "epidural", "spinal", "analges", "pain")):
            sub = "Pain/Regional Anesthesia"
        elif any(k in hay for k in ("perioper", "postoperative", "post-operative", "surgery", "surgical", "anemia", "anaemia")):
            sub = "Perioperative Medicine"
        elif any(k in hay for k in ("induction", "maintenance", "airway", "volatile", "propofol", "ketamine", "anesthetic", "anaesthetic")):
            sub = "General Anesthesia"
        else:
            sub = "Other Anaesthesiology"

    return track, sub

def to_markdown(items: List[Dict[str, Any]], overview_markdown: str, details_by_id: Dict[str, str], *, report_title: str = "Daily Report", report_language: str = "de") -> str:
    lang = _norm_language(report_language)
    title = report_title.strip()
    now_str = datetime.now(tz=STO).strftime("%Y-%m-%d %H:%M") + (" Uhr" if lang == "de" else "")
    md: List[str] = [title, now_str, ""]
    is_cybermed = _is_cybermed_report(title, report_language)

    overview_markdown = (overview_markdown or "").strip()
    if overview_markdown:
        if is_cybermed:
            meta_only = _extract_cybermed_meta_block(overview_markdown)
            if meta_only:
                md.extend([meta_only, ""])
        else:
            md.extend([overview_markdown, ""])

    if is_cybermed:
        pubmed_items = [it for it in items if str(it.get("source") or "").strip().lower() == "pubmed"]
        if pubmed_items:
            md.extend(["## Papers", ""])
            grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
            for it in pubmed_items:
                track, sub = _infer_track_and_subcategory(it)
                grouped.setdefault(track, {}).setdefault(sub, []).append(it)

            track_order = ["Critical Care", "Anaesthesiology"]
            cc_order = ["Circulation", "Respiration", "Infection/Sepsis", "Renal/CRRT", "Neuro", "Other Critical Care"]
            an_order = ["General Anesthesia", "Perioperative Medicine", "Pain/Regional Anesthesia", "Other Anaesthesiology"]

            for track in track_order:
                subs = grouped.get(track, {})
                if not subs:
                    continue
                md.extend([f"### {track}", ""])
                base = cc_order if track == "Critical Care" else an_order
                extra = sorted([s for s in subs.keys() if s not in base])
                for sub in [s for s in base if s in subs] + extra:
                    sub_items = subs.get(sub, [])
                    if not sub_items:
                        continue
                    md.extend([f"#### {sub}", ""])
                    for it in sub_items:
                        iid = str(it.get("id") or "").strip()
                        url = str(it.get("url") or "").strip()
                        title_lbl = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                        label = _md_escape_label(_build_source_label(it))
                        detail = (details_by_id.get(iid) or "").strip()
                        bottom = _extract_bottom_line(detail)
                        md.append(f"- [{title_lbl}]({url}) — *{label}*" if url else f"- {title_lbl} — *{label}*")
                        md.append(f"  - **BOTTOM LINE:** {bottom}" if bottom else f"  - **BOTTOM LINE:** {_fallback_bottom_line(it)}")
                    md.append("")

    deep_dives_heading = "## Vertiefungen" if lang == "de" else "## Deep Dives"
    sources_heading = "## Quellen" if lang == "de" else "## Sources"

    detail_items = [it for it in items if str(it.get("id") or "") in details_by_id]
    if detail_items:
        md.extend([deep_dives_heading, ""])
        for it in detail_items:
            iid = str(it.get("id") or "").strip()
            ch = _md_escape_label(str(it.get("channel") or "").strip())
            title_lbl = _md_escape_label(str(it.get("title") or "").strip())
            url = str(it.get("url") or "").strip()
            md.append(f"### {ch}: [{title_lbl}]({url})" if url else f"### {ch}: {title_lbl}")
            md.append("")
            md.append((details_by_id.get(iid) or "").rstrip())
            md.append("")

    if not is_cybermed:
        seen = set()
        src_lines: List[str] = []
        for it in items:
            url = str(it.get("url") or "").strip()
            title_lbl = _md_escape_label(str(it.get("title") or "").strip())
            if not url or url in seen:
                continue
            seen.add(url)
            src_lines.append(f"- {title_lbl}: {url}")
        md.extend([sources_heading, ""])
        md.extend(src_lines if src_lines else ["- (keine)" if lang == "de" else "- (none)"])
        md.append("")

    return "\n".join(md)
