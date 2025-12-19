from __future__ import annotations

import os, re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:  # Optional import; keep reporter usable without selector
    from .selector_medical import load_cybermed_selection_config
except Exception:  # pragma: no cover - fallback for non-cybermed runs
    load_cybermed_selection_config = None

CYBERMED_JOURNAL_CATEGORY_MAP = {
    # Anesthesia / Perioperative
    "anesthesiology": "anesthesia",
    "br j anaesth": "anesthesia",
    "anaesthesia": "anesthesia",
    "anesth analg": "anesthesia",
    "acta anaesthesiol scand": "anesthesia",
    "eur j anaesthesiol": "anesthesia",
    "anaesth crit care pain med": "anesthesia",
    "curr opin anaesthesiol": "anesthesia",

    # Intensive Care
    "intensive care med": "intensive",
    "crit care": "intensive",
    "crit care med": "intensive",
    "ann intensive care": "intensive",
    "j intensive care": "intensive",
    "j crit care": "intensive",
    "crit care nurse": "intensive",
    "intensive crit care nurs": "intensive",
    "am j respir crit care med": "intensive",
    "chest": "intensive",
    "lancet respir med": "intensive",
    "ann am thorac soc": "intensive",

    # Emergency / Resuscitation
    "ann emerg med": "emergency",
    "resuscitation": "emergency",

    # Pain / Regional
    "reg anesth pain med": "pain",
    "pain": "pain",

    # General / High-impact
    "lancet": "general",
    "n engl j med": "general",
    "jama": "general",
    "bmj": "general",
}

STO = ZoneInfo("Europe/Stockholm")

def _normalize_journal_name(name: str) -> str:
    normalized = (name or "").strip().lower()
    normalized = normalized.replace(".", " ").replace("-", " ").rstrip(":")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.rstrip(". ")

def _journal_category(item: Dict[str, Any]) -> Optional[str]:
    for key in ("journal_iso_abbrev", "journal_medline_ta", "journal"):
        jn = _normalize_journal_name(str(item.get(key) or ""))
        if not jn:
            continue
        cat = CYBERMED_JOURNAL_CATEGORY_MAP.get(jn)
        if cat:
            return cat
    return None

def _norm_language(lang: str) -> str:
    l = (lang or "").strip().lower()
    return "en" if l.startswith("en") else "de"

def _md_escape_label(text: str) -> str:
    return (text or "").replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")

def _prefix_star(text: str) -> str:
    stripped = (text or "").lstrip()
    if stripped.startswith("⭐"):
        return text
    return f"⭐ {text}" if text else "⭐"

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


def _cybermed_deep_dive_limit() -> int:
    if not load_cybermed_selection_config:
        return 8

    try:
        cfg = load_cybermed_selection_config()
        sel = cfg.get("selection", {}) if isinstance(cfg.get("selection"), dict) else {}
        return int(sel.get("max_deep_dives", 8) or 8)
    except Exception:
        return 8

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


def _parse_cybermed_counts(meta_block: str) -> Tuple[Optional[int], Optional[int]]:
    screened = None
    after_state = None

    try:
        m_screened = re.search(r"-\s*(\d+)\s+papers\s+screened", meta_block)
        if m_screened:
            screened = int(m_screened.group(1))

        m_after_state = re.search(r"New \(not previously processed\):\s*(\d+)", meta_block)
        if m_after_state:
            after_state = int(m_after_state.group(1))
    except Exception:
        pass

    return screened, after_state


def _format_cybermed_metadata(
    items: List[Dict[str, Any]],
    meta_block: str,
    foamed_stats: Optional[Dict[str, Any]] = None,
    cybermed_stats: Optional[Dict[str, Any]] = None,
) -> str:
    screened, after_state = _parse_cybermed_counts(meta_block or "")
    included_overview = sum(1 for it in items if it.get("cybermed_included"))
    selected_deep_dives = sum(1 for it in items if it.get("cybermed_deep_dive"))

    domain_counts: Counter[str] = Counter()
    tier_counts: Counter[str] = Counter()
    deep_dive_reason_counts: Counter[str] = Counter()

    for it in items:
        if it.get("cybermed_included"):
            flags = it.get("cybermed_domain_flags") or {}
            for name, flag in flags.items():
                if flag:
                    domain_counts[name] += 1

            tier = str(it.get("cybermed_tier") or "").strip().lower()
            if tier.startswith("tier1"):
                tier_counts["tier1"] += 1
            elif tier.startswith("tier2"):
                tier_counts["tier2"] += 1
            elif tier.startswith("tier3"):
                tier_counts["tier3"] += 1

        if it.get("cybermed_deep_dive"):
            reasons = it.get("cybermed_deep_dive_reasons") or []
            for r in reasons:
                if r:
                    deep_dive_reason_counts[str(r)] += 1

    lines: List[str] = []
    lines.append("**Run diagnostics**")
    lines.append(
        f"- stepcounts: screened={screened if screened is not None else 'n/a'}, "
        f"after_state={after_state if after_state is not None else 'n/a'}, "
        f"included_overview={included_overview}, selected_deep_dives={selected_deep_dives}"
    )

    if isinstance(foamed_stats, dict) and foamed_stats:
        lines.append(
            "- foamed: "
            f"screened={foamed_stats.get('screened', 0)}, "
            f"after_state={foamed_stats.get('after_state', 0)}, "
            f"included_overview={foamed_stats.get('included_overview', 0)}, "
            f"top_picks={foamed_stats.get('top_picks', 0)}, "
            f"sources_total={foamed_stats.get('sources_total', 0)}, "
            f"sources_ok={foamed_stats.get('sources_ok', 0)}, "
            f"sources_failed={foamed_stats.get('sources_failed', 0)}, "
            f"items_raw={foamed_stats.get('items_raw', 0)}, "
            f"items_with_date={foamed_stats.get('items_with_date', 0)}, "
            f"items_date_unknown={foamed_stats.get('items_date_unknown', 0)}, "
            f"kept_last24h={foamed_stats.get('kept_last24h', 0)}"
        )
        per_source = foamed_stats.get("per_source") or {}
        if isinstance(per_source, dict) and per_source:
            lines.append("  - per_source_errors: " + ", ".join(f"{k}:{v.get('errors', 0)}" for k, v in per_source.items()))

    if isinstance(cybermed_stats, dict) and cybermed_stats.get("pubmed"):
        pub = cybermed_stats.get("pubmed", {})
        sel = pub.get("selection", {}) if isinstance(pub.get("selection"), dict) else {}
        lines.append(
            "- pubmed_selection: "
            f"candidates={pub.get('candidates_total', 'n/a')}, "
            f"new_unique={pub.get('new_unique', 'n/a')}, "
            f"included_overview={pub.get('selected_overview', 'n/a')}, "
            f"deep_dives={pub.get('selected_deep_dives', 'n/a')}, "
            f"excluded_offtopic={sel.get('excluded_overview_offtopic', 'n/a')}, "
            f"below_threshold={sel.get('below_threshold_overview', sel.get('below_threshold', 'n/a'))}, "
            f"excluded_by_allowlist={sel.get('excluded_by_allowlist', 'n/a')}, "
            f"deep_dive_low_score={sel.get('excluded_deep_dive_low_score', 'n/a')}, "
            f"deep_dive_hard_excluded={sel.get('deep_dive_hard_excluded', 'n/a')}"
        )

    if domain_counts:
        lines.append("- domain_counts:")
        for name, count in sorted(domain_counts.items()):
            lines.append(f"  - {name}: {count}")

    lines.append("- journal_tiers:")
    for tier_key in ("tier1", "tier2", "tier3"):
        lines.append(f"  - {tier_key}: {tier_counts.get(tier_key, 0)}")

    if deep_dive_reason_counts:
        lines.append("- deep_dive_top_reason_codes:")
        for reason, count in deep_dive_reason_counts.most_common():
            lines.append(f"  - {reason}: {count}")

    return "\n".join(lines).strip()

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


def _best_bottom_line(item: Dict[str, Any], detail_md: str) -> str:
    explicit = (item.get("bottom_line") or "").strip()
    if explicit:
        return explicit

    extracted = _extract_bottom_line(detail_md)
    if extracted:
        return extracted

    return _fallback_bottom_line(item)

def _infer_track_and_subcategory(item: Dict[str, Any]) -> Tuple[str, str]:
    hay = " ".join((str(item.get(k) or "") for k in ("title", "journal", "channel"))).lower()

    journal_category = _journal_category(item)
    track: Optional[str] = None
    sub: Optional[str] = None

    if journal_category == "emergency":
        track, sub = "Critical Care", "Other Critical Care"
    elif journal_category == "intensive":
        track = "Critical Care"
    elif journal_category == "anesthesia":
        track = "Anaesthesiology"
    elif journal_category == "pain":
        track, sub = "Anaesthesiology", "Pain/Regional Anesthesia"

    ana_hits = any(
        k in hay
        for k in (
            "anaesth",
            "anesth",
            "anesthesia",
            "perioper",
            "postoperative",
            "regional",
            "neuraxial",
            "epidural",
            "spinal",
            "nerve block",
            "pain",
            "analges",
        )
    )
    cc_hits = any(
        k in hay
        for k in (
            "icu",
            "intensive care",
            "critical care",
            "sepsis",
            "shock",
            "ventilat",
            "ards",
            "ecmo",
            "resuscitation",
            "cardiac arrest",
            "crrt",
            "dialysis",
            "vasopressor",
            "norepinephrine",
        )
    )

    if journal_category == "general" and track is None:
        if cc_hits and not ana_hits:
            track = "Critical Care"
        elif ana_hits and not cc_hits:
            track = "Anaesthesiology"
        elif cc_hits and ana_hits:
            track = "Critical Care"
        else:
            track, sub = "Critical Care", "Other Critical Care"

    if track is None:
        if any(k in hay for k in ("acta anaesthesiol scand", "anaesthesia", "br j anaesth", "anesth analg", "reg anesth pain med", "pain")):
            track = "Anaesthesiology"
        elif any(k in hay for k in ("intensive care med", "crit care", "resuscitation")):
            track = "Critical Care"
        else:
            if ana_hits and not cc_hits:
                track = "Anaesthesiology"
            elif cc_hits and not ana_hits:
                track = "Critical Care"
            else:
                track = "Critical Care" if cc_hits else "Anaesthesiology"

    if sub is None:
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

def to_markdown(
    items: List[Dict[str, Any]],
    overview_markdown: str,
    details_by_id: Dict[str, str],
    *,
    report_title: str = "Daily Report",
    report_language: str = "de",
    foamed_stats: Optional[Dict[str, Any]] = None,
    cybermed_stats: Optional[Dict[str, Any]] = None,
) -> str:
    lang = _norm_language(report_language)
    title = report_title.strip()
    now_str = datetime.now(tz=STO).strftime("%Y-%m-%d %H:%M") + (" Uhr" if lang == "de" else "")
    md: List[str] = [title, now_str, ""]
    is_cybermed = _is_cybermed_report(title, report_language)

    overview_markdown = (overview_markdown or "").strip()
    meta_only = ""
    if overview_markdown:
        if is_cybermed:
            meta_only = _extract_cybermed_meta_block(overview_markdown)
        else:
            md.extend([overview_markdown, ""])

    if is_cybermed:
        pubmed_items = [it for it in items if str(it.get("source") or "").strip().lower() == "pubmed"]
        md.extend(["## Papers", ""])
        if pubmed_items:
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
                        display_title = _prefix_star(title_lbl) if it.get("top_pick") else title_lbl
                        label = _md_escape_label(_build_source_label(it))
                        detail = (details_by_id.get(iid) or "").strip()
                        bottom = _best_bottom_line(it, detail)
                        md.append(
                            f"- [{display_title}]({url}) — *{label}*" if url else f"- {display_title} — *{label}*"
                        )
                        md.append(f"  - **BOTTOM LINE:** {bottom}" if bottom else f"  - **BOTTOM LINE:** {_fallback_bottom_line(it)}")
                    md.append("")
        else:
            _, after_state = _parse_cybermed_counts(meta_only or "")
            if after_state is not None:
                if after_state <= 0:
                    md.append("No new papers selected (all screened items were already processed).")
                else:
                    md.append("No new papers selected (new papers were filtered by policy).")
            else:
                md.append("No new papers selected (all screened items were already processed or filtered by policy).")

        # FOAMed overview sits between Papers and Deep Dives.
        foamed_items = [it for it in items if str(it.get("source") or "").strip().lower() == "foamed"]
        md.extend(["", "## FOAMed & Commentary", ""])
        if foamed_items:
            foamed_sorted = sorted(
                foamed_items,
                key=lambda it: it.get("published_at") or datetime.min.replace(tz=STO),
                reverse=True,
            )
            for it in foamed_sorted:
                title_lbl = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                url = str(it.get("url") or "").strip()
                source_name = _md_escape_label(str(it.get("foamed_source") or it.get("channel") or "FOAMed"))
                display_title = _prefix_star(title_lbl) if it.get("top_pick") else title_lbl
                line = f"- [{display_title}]({url}) — {source_name}" if url else f"- {display_title} — {source_name}"
                md.append(line)
                bottom_line = (it.get("bottom_line") or "").strip()
                if bottom_line and not bottom_line.lower().startswith("bottom line"):
                    bottom_line = f"BOTTOM LINE: {bottom_line}"
                if not bottom_line:
                    bottom_line = "BOTTOM LINE: No summary available."
                md.append(f"  - {bottom_line}")
            md.append("")
        else:
            md.append("- No new FOAMed posts in the last 24 hours.")
            md.append("")

    deep_dives_heading = "## Vertiefungen" if lang == "de" else "## Deep Dives"
    sources_heading = "## Quellen" if lang == "de" else "## Sources"

    def _deep_dive_sort_key(it: Dict[str, Any]) -> Tuple[int, float, datetime]:
        rank = int(it.get("cybermed_rank") or 10**9)
        score = float(it.get("cybermed_score") or 0.0)
        ts = it.get("published_at") or datetime.min.replace(tzinfo=STO)
        return (rank, -score, ts)

    detail_items: List[Dict[str, Any]]
    if is_cybermed:
        detail_items = [it for it in items if it.get("cybermed_deep_dive")]
        if not detail_items:
            detail_items = [it for it in items if str(it.get("id") or "") in details_by_id]
        detail_items = sorted(detail_items, key=_deep_dive_sort_key)
        deep_dive_cap = max(0, _cybermed_deep_dive_limit())
        if deep_dive_cap:
            detail_items = detail_items[:deep_dive_cap]
    else:
        detail_items = [it for it in items if str(it.get("id") or "") in details_by_id]

    if detail_items:
        md.extend([deep_dives_heading, ""])
        for it in detail_items:
            iid = str(it.get("id") or "").strip()
            ch = _md_escape_label(str(it.get("channel") or "").strip())
            title_lbl = _md_escape_label(str(it.get("title") or "").strip())
            url = str(it.get("url") or "").strip()
            heading_body = f"{ch}: [{title_lbl}]({url})" if url else f"{ch}: {title_lbl}"
            if it.get("top_pick"):
                heading_body = _prefix_star(heading_body)
            md.append(f"### {heading_body}")
            md.append("")
            detail_block = (details_by_id.get(iid) or "").rstrip()
            if not detail_block and is_cybermed:
                detail_block = f"**BOTTOM LINE:** {_best_bottom_line(it, detail_block)}"
            md.append(detail_block)
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

    if is_cybermed:
        extra_meta = _format_cybermed_metadata(items, meta_only, foamed_stats, cybermed_stats)
        meta_blocks = [block.strip() for block in (meta_only, extra_meta) if block.strip()]
        if meta_blocks:
            if md and md[-1] != "":
                md.append("")
            meta_content = "\n\n".join(meta_blocks)
            md.extend(
                [
                    "<details>",
                    "  <summary>Run Metadata (click to expand)</summary>",
                    "  <pre>",
                    meta_content,
                    "  </pre>",
                    "</details>",
                ]
            )

    return "\n".join(md)
