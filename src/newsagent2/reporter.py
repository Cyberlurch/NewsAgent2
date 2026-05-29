from __future__ import annotations

import ast
import os, re
import html as html_module
import os
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .summarizer import normalize_pubmed_deep_dive, render_pubmed_deep_dive_from_abstract

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

def _label_text(value: Any) -> str:
    txt = str(value or "").strip()
    return txt.capitalize() if txt else ""


def _join_compact_segments(segments: List[str]) -> str:
    clean = [seg.strip() for seg in segments if str(seg or "").strip()]
    return " · ".join(clean)


def _pubmed_compact_line(item: Dict[str, Any]) -> str:
    segs: List[str] = []
    ev = _label_text(item.get("evidence_strength_label"))
    if ev:
        segs.append(f"Evidence {ev}")
    rel = item.get("clinical_relevance_1_5")
    if rel not in {None, ""}:
        segs.append(f"Relevance {int(rel)}/5")
    impact = item.get("practice_change_potential_1_5")
    if impact not in {None, ""}:
        segs.append(f"Practice impact {int(impact)}/5")
    conf = _label_text(item.get("text_confidence_label"))
    if conf:
        segs.append(f"Confidence {conf}")
    return _join_compact_segments(segs)


def _foamed_compact_line(item: Dict[str, Any]) -> str:
    segs: List[str] = []
    quality = _label_text(item.get("source_quality_label"))
    if quality:
        segs.append(f"Source quality {quality}")
    useful = item.get("clinical_usefulness_1_5")
    if useful not in {None, ""}:
        segs.append(f"Usefulness {int(useful)}/5")
    pr = item.get("practice_relevance_1_5")
    if pr not in {None, ""}:
        segs.append(f"Practice relevance {int(pr)}/5")
    conf = _label_text(item.get("text_confidence_label"))
    if conf:
        segs.append(f"Confidence {conf}")
    return _join_compact_segments(segs)


def _is_cybermed_report(report_title: str, report_language: str) -> bool:
    rk = (os.getenv("REPORT_KEY") or "").strip().lower()
    if rk == "cybermed":
        return True
    if "cybermed" in (report_title or "").strip().lower():
        return True
    rp = (os.getenv("REPORT_PROFILE") or "").strip().lower()
    return rp == "medical"


def _is_cyberlurch_report(report_title: str) -> bool:
    rk = (os.getenv("REPORT_KEY") or "").strip().lower()
    if rk == "cyberlurch":
        return True
    return "cyberlurch" in (report_title or "").strip().lower()

def _extract_bottom_line(detail_md: str) -> str:
    if not detail_md:
        return ""
    m = re.search(r"(?:\*\*)?BOTTOM LINE:(?:\*\*)?\s*(.+)", detail_md, flags=re.IGNORECASE)
    if not m:
        return ""
    return (m.group(1) or "").strip().splitlines()[0].strip()


def _bold_bottom_line_label(text: str) -> str:
    if not text:
        return text

    def _repl(match: re.Match[str]) -> str:
        leading = match.group(1) or ""
        return f"{leading}**BOTTOM LINE:**"

    return re.sub(r"(?im)^(\s*)(?:\*\*)?BOTTOM LINE:(?:\*\*)?", _repl, text)




CYBERMED_STORED_DEEP_DIVE_STRUCTURED_FIELDS = (
    "study_type",
    "population_setting",
    "intervention_or_exposure",
    "comparator",
    "primary_endpoint",
    "primary_result_direction",
    "primary_result_significance",
    "key_secondary_results",
    "clinical_interpretation",
    "limitations",
    "deep_dive_reasons",
)


def _has_stored_cybermed_deep_dive_content(item: Dict[str, Any]) -> bool:
    if item.get("cybermed_stored_deep_dive_has_structured_content") is True:
        return True
    source = item.get("cybermed_stored_deep_dive") if isinstance(item.get("cybermed_stored_deep_dive"), dict) else item
    for field in CYBERMED_STORED_DEEP_DIVE_STRUCTURED_FIELDS:
        value = (source or {}).get(field)
        if isinstance(value, list):
            if any(str(v).strip() for v in value):
                return True
        elif isinstance(value, dict):
            if any(str(v).strip() for v in value.values()):
                return True
        elif str(value or "").strip():
            return True
    return False


def _stored_cybermed_deep_dive_block(item: Dict[str, Any]) -> str:
    source = item.get("cybermed_stored_deep_dive") if isinstance(item.get("cybermed_stored_deep_dive"), dict) else item

    def val(*names: str) -> str:
        for name in names:
            raw = (source or {}).get(name)
            if raw is None or raw == "":
                raw = item.get(name)
            text = _flatten_text_field(raw)
            if text:
                return text
        return ""

    lines: List[str] = []
    rows = [
        ("Study type", val("study_type")),
        ("Population/setting", val("population_setting")),
        ("Intervention/exposure & comparator", _join_compact_segments([val("intervention_or_exposure"), val("comparator")]) or val("intervention_exposure_comparator")),
        ("Primary endpoint", val("primary_endpoint")),
        ("Key results", _join_compact_segments([val("primary_result_direction"), val("primary_result_significance"), val("key_secondary_results")])),
        ("Limitations", val("limitations")),
        ("Why this matters / clinical interpretation", _join_compact_segments([val("clinical_interpretation"), val("deep_dive_reasons")])),
    ]
    for label, text in rows:
        if text:
            lines.append(f"- **{label}:** {text}")
    bottom = val("bottom_line") or str(item.get("bottom_line") or "").strip()
    if bottom:
        lines.extend(["", f"**BOTTOM LINE:** {bottom}"])
    return "\n".join(lines).strip()

def _flatten_text_field(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(str(v).strip() for v in value if str(v).strip())
    if isinstance(value, dict):
        return "; ".join(f"{k}: {v}" for k, v in value.items() if str(v).strip())
    return str(value or "").strip()


def _normalize_detail_block_headings(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"(?im)^\s*#{1,2}\s*Key takeaways\s*$", "#### Key takeaways", text)
    text = re.sub(r"(?im)^\s*#{1,2}\s*Details\s*&\s*reasoning\s*$", "#### Details & reasoning", text)
    return text


def _topic_from_item(item: Dict[str, Any]) -> str:
    primary = str(item.get("topic_primary") or "").strip()
    if primary:
        return primary
    topic = str(item.get("topic") or "").strip()
    if topic:
        return topic
    topics = item.get("topics")
    if isinstance(topics, list):
        for t in topics:
            ts = str(t).strip()
            if ts:
                return ts
    return "Other"


def _to_clean_text(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(str(v).strip() for v in value[:3] if str(v).strip())
    if isinstance(value, dict):
        pairs = [f"{k}: {v}" for k, v in value.items() if str(v).strip()]
        return "; ".join(pairs[:3])
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return "; ".join(str(v).strip() for v in parsed[:3] if str(v).strip())
        except Exception:
            text = text.strip("[]").replace("'", "").replace('"', "")
            return re.sub(r"\s*,\s*", "; ", text).strip()
    return text


def _extract_first_useful_paragraph(detail_block: str) -> str:
    for para in re.split(r"\n\s*\n", detail_block or ""):
        p = para.strip()
        low = p.lower()
        if not p:
            continue
        if low.startswith("#") or low.startswith("title:") or low.startswith("channel:") or low.startswith("published:") or "watch on youtube" in low:
            continue
        if low.startswith("bottom line:") or low.startswith("**bottom line:**"):
            continue
        return p
    return ""

def _trim_sentence_aware(text: str, max_chars: int) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if not normalized or max_chars <= 0:
        return ""
    if len(normalized) <= max_chars:
        return normalized
    window = normalized[:max_chars]
    sentence_endings = [m.end() for m in re.finditer(r"[.!?](?:['\")\]]+)?(?:\s+|$)", window)]
    if sentence_endings:
        trimmed = window[:sentence_endings[-1]].rstrip()
        if trimmed:
            return re.sub(r"[.!?…]+$", "", trimmed).rstrip() + "…"
        fallback = normalized[:max_chars].rsplit(" ", 1)[0].rstrip()
        return re.sub(r"[.!?…]+$", "", fallback).rstrip() + "…"
    cut = window.rsplit(" ", 1)[0].rstrip()
    if not cut:
        cut = window.rstrip()
    cut = re.sub(r"[.!?…]+$", "", cut).rstrip()
    return f"{cut}…"


def _ensure_sentence_end(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return cleaned
    if re.search(r"[.!?…](?:['\")\]]+)?$", cleaned):
        return cleaned
    return f"{cleaned}."


def _rewrite_report_prose_openers(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if not cleaned:
        return cleaned
    rules = (
        (r"^the transcript is (?:highly )?relevant(?: for)?\s*", ""),
        (r"^this transcript is (?:highly )?relevant(?: for)?\s*", ""),
        (r"^the transcript provides (?:insight|insights)(?: into)?\s*", ""),
        (r"^this transcript provides (?:insight|insights)(?: into)?\s*", ""),
        (r"^the transcript (?:shows|covers|describes|highlights)\s*", ""),
        (r"^this transcript (?:shows|covers|describes|highlights)\s*", ""),
        (r"^the video is about\s*", ""),
        (r"^this video is about\s*", ""),
        (r"^video is about\s*", ""),
        (r"^the video is on\s*", ""),
        (r"^video is on\s*", ""),
        (r"^the transcript is a discussion(?: between [^,.;:]+)?(?: about| of| on)?\s*", ""),
        (r"^transcript is a discussion(?: between [^,.;:]+)?(?: about| of| on)?\s*", ""),
    )
    for pattern, replacement in rules:
        updated = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE).strip(" ,;:-")
        if updated != cleaned:
            cleaned = updated
            break
    if cleaned:
        cleaned = cleaned[:1].upper() + cleaned[1:]
    return cleaned

def _strip_generic_summary_openers(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    replacements = (
        "the transcript is a discussion",
        "transcript is a discussion",
        "the video is about",
        "video is about",
        "the video is on",
        "video is on",
    )
    lowered = cleaned.lower()
    for prefix in replacements:
        if lowered.startswith(prefix):
            nxt = cleaned[len(prefix):].strip(" ,.-:")
            if nxt:
                cleaned = nxt[:1].upper() + nxt[1:]
                lowered = cleaned.lower()
    return cleaned


def _cyberlurch_topic_bullet(item: Dict[str, Any], detail_block: str) -> str:
    content = (
        _to_clean_text(item.get("transcript_full_summary"))
        or _to_clean_text(item.get("important_details"))
        or _to_clean_text(item.get("editorial_relevance"))
        or _to_clean_text(item.get("summary") or item.get("bottom_line"))
        or _to_clean_text(_extract_first_useful_paragraph(detail_block))
        or _to_clean_text(item.get("title"))
        or "No summary available"
    )
    relevance = (
        _to_clean_text(item.get("editorial_relevance"))
        or _to_clean_text(item.get("important_details"))
        or _to_clean_text(item.get("bottom_line"))
        or "high relevance for current channel discourse"
    )
    content = _rewrite_report_prose_openers(_strip_generic_summary_openers(content)).strip("-• ")
    relevance = _rewrite_report_prose_openers(_strip_generic_summary_openers(relevance)).strip("-• ")
    content = _trim_sentence_aware(content, 280)
    relevance = _trim_sentence_aware(relevance, 160)
    content = _ensure_sentence_end(content)
    relevance = _ensure_sentence_end(relevance)
    channel_text = _md_escape_label(str(item.get("channel") or "").strip() or "Unknown channel")
    return f"- **{channel_text}:** {content} _Why it matters:_ {relevance}"


def _strip_cyberlurch_detail_metadata_block(detail_block: str, item_title: str) -> str:
    lines = (detail_block or "").splitlines()
    kept_start = 0
    saw_metadata = False
    for idx, raw in enumerate(lines):
        line = raw.strip()
        low = line.lower()
        if not line:
            if saw_metadata:
                kept_start = idx + 1
            continue
        if re.match(r"^#{1,4}\s*(key takeaways|details\s*&\s*reasoning|uncertainties)\b", line, flags=re.IGNORECASE):
            break
        is_generated_title = bool(item_title and re.match(r"^####\s+.+$", line) and re.sub(r"^####\s+", "", line).strip().lower() == item_title.strip().lower())
        is_metadata = bool(
            is_generated_title
            or re.match(r"^\*?\*?channel:\*?\*?\s*", line, flags=re.IGNORECASE)
            or re.match(r"^\*?\*?published:\*?\*?\s*", line, flags=re.IGNORECASE)
            or re.match(r"^\*?\*?url:\*?\*?\s*", line, flags=re.IGNORECASE)
            or re.match(r"^\[(watch here|watch on youtube)\]\([^)]+\)\s*$", line, flags=re.IGNORECASE)
        )
        if is_metadata:
            saw_metadata = True
            kept_start = idx + 1
            continue
        if saw_metadata and line == "---":
            kept_start = idx + 1
            continue
        break
    return "\n".join(lines[kept_start:]).lstrip()


def _normalize_deep_dive_headings(detail_block: str, *, item_title: str) -> str:
    if not detail_block:
        return detail_block
    detail_block = _strip_cyberlurch_detail_metadata_block(detail_block, item_title)
    lines = []
    for raw in detail_block.splitlines():
        line = raw.strip()
        low = line.lower()
        if re.match(r"^#\s+title\s*$", line, flags=re.IGNORECASE):
            continue
        if item_title and re.match(r"^#\s+.+$", line) and re.sub(r"^#\s+", "", line).strip().lower() == item_title.strip().lower():
            continue
        if re.match(r"^#\s*(key takeaways|details\s*&\s*reasoning|uncertainties)\s*$", line, flags=re.IGNORECASE):
            label = re.sub(r"^#\s*", "", line, flags=re.IGNORECASE).strip()
            lines.append(f"#### {label}")
            continue
        if re.match(r"^##\s*(key takeaways|details\s*&\s*reasoning|uncertainties)\s*$", line, flags=re.IGNORECASE):
            label = re.sub(r"^##\s*", "", line, flags=re.IGNORECASE).strip()
            lines.append(f"#### {label}")
            continue
        if re.match(r"^#{1,2}\s+", line):
            label = re.sub(r"^#{1,2}\s*", "", line).strip()
            lines.append(f"#### {label}")
            continue
        lines.append(raw)
    return "\n".join(lines).strip()

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


def _cybermed_deep_dive_limit(report_mode: Optional[str] = None) -> int:
    if not load_cybermed_selection_config:
        return 8

    try:
        cfg = load_cybermed_selection_config()
        sel = cfg.get("selection", {}) if isinstance(cfg.get("selection"), dict) else {}
        config_max = int(sel.get("max_deep_dives", 8) or 8)
    except Exception:
        config_max = 8

    mode = (report_mode or "").strip().lower()
    if mode == "weekly":
        mode_cap = 3
    elif mode == "monthly":
        mode_cap = 2
    elif mode == "yearly":
        mode_cap = 0
    else:
        mode_cap = config_max

    return min(config_max, mode_cap)


def _detail_lookup(details_by_id: Dict[str, str], item: Dict[str, Any]) -> str:
    iid = str(item.get("id") or "").strip()
    url = str(item.get("url") or "").strip()
    title = str(item.get("title") or "").strip()
    src = str(item.get("source") or "youtube").strip().lower()
    candidates = []
    if iid:
        candidates.append(iid)
    if src and iid:
        candidates.append(f"{src}:{iid}")
    for key in (url, title):
        if key:
            candidates.append(key)
    for key in candidates:
        if key in details_by_id:
            return (details_by_id.get(key) or "").strip()
    return ""


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

        sh = foamed_stats.get("source_health") or {}
        if isinstance(sh, dict) and sh:
            lines.append(
                "  - source_health: "
                f"ok_rss={sh.get('ok_rss', 0)}, "
                f"ok_html={sh.get('ok_html', 0)}, "
                f"blocked_403={sh.get('blocked_403', 0)}, "
                f"not_found_404={sh.get('not_found_404', 0)}, "
                f"parse_failed={sh.get('parse_failed', 0)}, "
                f"other={sh.get('other', 0)}"
            )

        per_source = foamed_stats.get("per_source") or {}
        if isinstance(per_source, dict) and per_source:
            lines.append("  - per_source_errors: " + ", ".join(f"{k}:{v.get('errors', 0)}" for k, v in per_source.items()))
            lines.append("  - per_source_diagnostics:")
            for name, st in sorted(per_source.items()):
                if not isinstance(st, dict):
                    continue
                method = st.get("method") or "rss"
                why = st.get("why") or "n/a"
                health = st.get("health") or "n/a"
                feed_status = st.get("feed_status_code") or "n/a"
                home_status = st.get("homepage_status_code") or "n/a"
                newest = st.get("newest_entry_datetime") or "n/a"
                entries_total = st.get("entries_total", st.get("items_raw", 0))
                entries_with_date = st.get("entries_with_date", st.get("items_with_date", 0))
                kept = st.get("kept_last24h", 0)
                err = st.get("error")
                diag = (
                    f"{name}: method={method}, why={why}, health={health}, "
                    f"feed_status={feed_status}, home_status={home_status}, "
                    f"entries_total={entries_total}, entries_with_date={entries_with_date}, "
                    f"newest={newest}, kept_last24h={kept}"
                )
                if err:
                    diag += f", error={err}"
                lines.append(f"    - {diag}")

        forced = foamed_stats.get("forced_html_fallback_sources") or []
        if forced:
            lines.append(f"  - forced_html_fallback_sources: {', '.join(forced)}")

        audit_stats = foamed_stats.get("audit") or {}
        if isinstance(audit_stats, dict) and audit_stats.get("enabled") and audit_stats.get("sources"):
            lines.append("  - audit:")
            for name, audit in sorted((audit_stats.get("sources") or {}).items()):
                if not isinstance(audit, dict):
                    continue
                html_extra = audit.get("items_found_in_html_not_in_rss") or {}
                rss_extra = audit.get("items_found_in_rss_not_in_html") or {}
                html_list = html_extra.get("examples") or []
                rss_list = rss_extra.get("examples") or []
                audit_line = (
                    f"    - {name}: "
                    f"rss_items_seen={audit.get('rss_items_seen', 0)}, "
                    f"rss_items_in_window={audit.get('rss_items_in_window', 0)}, "
                    f"html_candidates_seen={audit.get('html_candidates_seen', 0)}, "
                    f"html_items_in_window={audit.get('html_items_in_window', 0)}, "
                    f"html_not_in_rss={html_extra.get('count', 0)}, "
                    f"rss_not_in_html={rss_extra.get('count', 0)}, "
                    f"audit_pages_fetched={audit.get('audit_pages_fetched', 0)}"
                )
                if html_list:
                    audit_line += f", html_examples={'; '.join(html_list[:5])}"
                if rss_list:
                    audit_line += f", rss_examples={'; '.join(rss_list[:5])}"
                lines.append(audit_line)

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

    if isinstance(cybermed_stats, dict):
        deep = cybermed_stats.get("deep_dives") or {}
        if isinstance(deep, dict) and deep:
            lines.append(
                "- deep_dive_stats: "
                f"candidates={deep.get('candidates', 'n/a')}, "
                f"requested={deep.get('requested_deep_dives', 'n/a')}, "
                f"generated={deep.get('generated_deep_dives', 'n/a')}, "
                f"retried={deep.get('retried_deep_dives', 'n/a')}, "
                f"empty_outputs={deep.get('empty_deep_dive_outputs', 'n/a')}, "
                f"missing_abstracts={deep.get('missing_abstract_count', 'n/a')}, "
                f"fulltext_enriched={deep.get('enriched_fulltext_count', 'n/a')}, "
                f"unpaywall_oa_found={deep.get('unpaywall_oa_found_count', 'n/a')}, "
                f"download_successes={deep.get('download_success_count', 'n/a')}, "
                f"parse_fallback_used={deep.get('parse_fallback_used_count', 'n/a')}, "
                f"all_fields_not_reported={deep.get('not_reported_all_fields_count', 'n/a')}"
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


def _ensure_pubmed_deep_dive_template(detail_md: str, fallback_bottom_line: str, *, lang: str = "en") -> str:
    normalized = normalize_pubmed_deep_dive(detail_md or "", lang=lang, fallback_bottom_line=fallback_bottom_line)
    return normalized or f"BOTTOM LINE: {fallback_bottom_line or 'Not reported'}"

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



def render_cyberlurch_monthly_trend_report(items, *, title, generated_at, diagnostics=None) -> str:
    items = items or []
    diagnostics = diagnostics or {}
    by_topic: dict[str, list[dict]] = {}
    for it in items:
        by_topic.setdefault(_topic_from_item(it), []).append(it)
    top_channels = Counter(str(it.get("channel") or "Unknown") for it in items)
    full_text_count = sum(1 for it in items if str(it.get("content_status") or "") != "metadata_only")
    metadata_only_count = max(0, len(items) - full_text_count)
    cap = max(1, int((os.getenv("CYBERLURCH_MONTHLY_REPRESENTATIVE_LINKS_PER_TOPIC", "3") or "3").strip() or "3"))
    lines = [f"# {title}", "", "## Executive Summary", ""]
    lines += [f"- {len(items)} curated items across {len(by_topic)} active topics.", f"- Top channels were {', '.join([c for c,_ in top_channels.most_common(3)]) or 'limited coverage'}.", "- Coverage blended current affairs, trend analysis, and evergreen material.", "- Repeated themes clustered around major topic streams rather than isolated clips.", "- Representative links are compacted by topic for readability."]
    lines += ["", "## Monthly trend map", ""]
    for t, grouped in sorted(by_topic.items(), key=lambda kv: len(kv[1]), reverse=True):
        count = len(grouped)
        ch = ", ".join([c for c,_ in Counter(str(i.get('channel') or 'Unknown') for i in grouped).most_common(3)])
        trend_status = "single representative item" if count == 1 else "repeated topic stream"
        summary_seed = next((str(i.get("transcript_full_summary") or i.get("editorial_relevance") or "").strip() for i in grouped if str(i.get("transcript_full_summary") or i.get("editorial_relevance") or "").strip()), "")
        summary_note = _trim_sentence_aware(summary_seed, 180) if summary_seed else "No concise item summary available."
        item_word = "item" if count == 1 else "items"
        lines.append(f"- **{t}**: {count} {item_word}; main channels: {ch}; trend status: {trend_status}; summary: {summary_note}")
    lines += ["", "## Topic streams", ""]
    for t, grouped in sorted(by_topic.items(), key=lambda kv: len(kv[1]), reverse=True):
        count = len(grouped)
        item_word = "item" if count == 1 else "items"
        lines.append(f"### {t}")
        if count == 1:
            lines.append("- Discussed: single representative item this month.")
            lines.append("- Pattern status: not yet a repeated monthly pattern; included as a representative item rather than a trend.")
        else:
            lines.append(f"- Discussed: recurring analysis and updates across {count} {item_word}.")
            lines.append("- Changed/repeated: narratives were iterative rather than one-off.")
        lines.append(f"- Representative channels: {', '.join([c for c,_ in Counter(str(i.get('channel') or 'Unknown') for i in grouped).most_common(3)])}.")
        for it in grouped[:cap]:
            if it.get('url'): lines.append(f"- [{it.get('title') or 'Untitled'}]({it.get('url')})")
        lines.append("")
    lines += ["## Crisis and development trajectories", ""]
    trajectory_topics: list[tuple[str, list[dict]]] = []
    for t, grouped in sorted(by_topic.items(), key=lambda kv: len(kv[1]), reverse=True):
        temps = {str(i.get("temporality") or "").strip() for i in grouped}
        if len(grouped) >= 2 and temps.intersection({"current_affairs", "trend_analysis", "mixed"}):
            trajectory_topics.append((t, grouped))
    if trajectory_topics:
        for t, grouped in trajectory_topics[:5]:
            channels = ", ".join([c for c, _ in Counter(str(i.get("channel") or "Unknown") for i in grouped).most_common(3)])
            concrete_sentence = next((str(i.get("transcript_full_summary") or i.get("editorial_relevance") or "").strip() for i in grouped if str(i.get("transcript_full_summary") or i.get("editorial_relevance") or "").strip()), "")
            concrete_sentence = _trim_sentence_aware(concrete_sentence, 180) if concrete_sentence else "No concise item summary available."
            lines.append(f"- **{t}**: {len(grouped)} related items across {channels}. {concrete_sentence}")
    else:
        lines.append("- No multi-item crisis trajectory was detected in this period; current-affairs items are represented under Topic streams.")

    lines += ["", "## Evergreen / long-shelf-life items", ""]
    evergreen_items = [it for it in items if str(it.get("temporality") or "").strip() == "evergreen"]
    if evergreen_items:
        for it in evergreen_items[:5]:
            summary = _trim_sentence_aware(str(it.get("transcript_full_summary") or it.get("editorial_relevance") or "").strip(), 180) or "No concise item summary available."
            lines.append(f"- **{it.get('channel') or 'Unknown'}** — {it.get('title') or 'Untitled'}: {summary}")
    else:
        lines.append("- No clear evergreen items were detected in this period.")

    lines += ["", "## Representative links", ""]
    for t, grouped in sorted(by_topic.items(), key=lambda kv: len(kv[1]), reverse=True):
        lines.append(f"### {t}")
        for it in grouped[:cap]:
            if it.get('url'): lines.append(f"- [{it.get('title') or 'Untitled'}]({it.get('url')})")
    lines += ["", "## Source/channel summary", "", f"- Full text items: {full_text_count}", f"- Metadata-only items: {metadata_only_count}"]
    return "\n".join(lines).rstrip() + "\n"


def render_cyberlurch_yearly_analysis(rollups, *, target_year, generated_at) -> str:
    cap = max(1, int((os.getenv("CYBERLURCH_YEARLY_REPRESENTATIVE_LINKS_PER_THEME", "3") or "3").strip() or "3"))
    rollups = [r for r in (rollups or []) if isinstance(r, dict)]
    by_month = sorted(rollups, key=lambda r: str(r.get('month') or ''))
    limited = any(not r.get('topic_summaries') for r in by_month)
    lines=[f"# The Cyberlurch Year in Review — {target_year}","", "## Executive Summary", "", f"- {len(by_month)} monthly rollups analyzed."]
    if limited:
        lines.append("- Some earlier months contain thinner rollup detail; summaries below use the available monthly titles, channels and derived summaries.")
    lines += ["", "## Key themes across the year", ""]
    themes = Counter()
    channels = Counter()
    trajectories: list[str] = []
    evergreen: list[str] = []
    for r in by_month:
        for th in (r.get("top_themes") or []):
            if isinstance(th, dict) and th.get("theme"):
                themes[str(th.get("theme"))] += int(th.get("count") or 1)
        for ch in (r.get("top_channels") or []):
            if isinstance(ch, dict) and ch.get("channel"):
                channels[str(ch.get("channel"))] += int(ch.get("count") or 1)
        trajectories.extend([str(x) for x in (r.get("topic_trajectories") or []) if str(x).strip()])
        evergreen.extend([str(x) for x in (r.get("evergreen_highlights") or []) if str(x).strip()])
    lines.append(f"- Leading themes: {', '.join([t for t, _ in themes.most_common(6)]) or 'No enriched theme data available.'}")
    lines += ["", "## Crisis trajectories", ""]
    if trajectories:
        for tr in trajectories[:8]:
            lines.append(f"- {tr}")
    else:
        lines.append("- No multi-month crisis trajectory was strongly repeated in available rollups.")
    lines += ["", "## Recurring narratives", ""]
    topic_summaries = [str(s) for r in by_month for s in (r.get("topic_summaries") or []) if str(s).strip()]
    if topic_summaries:
        for s in topic_summaries[:8]:
            lines.append(f"- {s}")
    else:
        lines.append("- Recurring narratives were sparse in stored monthly summaries.")
    lines += ["", "## Topic and channel weights", "", f"- Top channels: {', '.join([c for c, _ in channels.most_common(8)]) or 'No enriched channel counts available.'}", "", "## Evergreen highlights", ""]
    if evergreen:
        for ev in evergreen[:8]:
            lines.append(f"- {ev}")
    else:
        lines.append("- No clear evergreen highlights were captured in monthly rollups.")
    lines += ["", "## By month", ""]
    for r in by_month:
        m = str(r.get('month') or 'Unknown month')
        try:
            m = datetime.strptime(f"{m}-01", "%Y-%m-%d").strftime("%B %Y")
        except Exception:
            pass
        lines.append(f"### {m}")
        lines.append(f"- {(r.get('executive_summary') or ['No summary captured'])[0] if isinstance(r.get('executive_summary'), list) and r.get('executive_summary') else 'No summary captured'}")
    lines += ["", "## Representative links", ""]
    for r in by_month:
        m = str(r.get('month') or 'Unknown month')
        try:
            m = datetime.strptime(f"{m}-01", "%Y-%m-%d").strftime("%B %Y")
        except Exception:
            pass
        lines.append(f"### {m}")
        items = r.get('representative_items') or r.get('top_items') or []
        for it in items[:cap]:
            if isinstance(it, dict) and it.get('url'):
                lines.append(f"- [{it.get('title') or 'Untitled'}]({it.get('url')})")
    return "\n".join(lines).rstrip()+"\n"



def _cybermed_monthly_theme_label(it: Dict[str, Any]) -> str:
    text = " ".join([
        str(it.get("title") or ""),
        str(it.get("journal") or it.get("source_journal") or ""),
        str(it.get("evidence_strength_label") or ""),
        str(it.get("bottom_line") or ""),
    ]).lower()
    mapping = [
        ("Respiratory / Ventilation", ["ventilat", "respirat", "ards", "oxygen", "airway"]),
        ("Infectious Diseases / Sepsis", ["sepsis", "infect", "antibi", "bacter", "viral"]),
        ("Emergency Medicine / Resuscitation", ["resusc", "cardiac arrest", "emergency", "trauma"]),
        ("Anaesthesia / Perioperative Medicine", ["anest", "anaest", "perioper", "surgery", "sedat"]),
        ("Intensive Care / Critical Care", ["icu", "critical care", "intensive", "shock"]),
        ("Methods / AI / Systems", ["ai", "machine learning", "model", "system", "workflow"]),
    ]
    for label, needles in mapping:
        if any(n in text for n in needles):
            return label
    return "Methods / AI / Systems"


def _cybermed_practice_bucket(it: Dict[str, Any]) -> str:
    ev = str(it.get("evidence_strength_label") or "").lower()
    srcq = str(it.get("source_quality_label") or "").lower()
    cr = int(it.get("clinical_relevance_1_5") or 0)
    pc = int(it.get("practice_change_potential_1_5") or 0)
    if pc >= 4 and cr >= 4 and ("high" in ev or "guideline" in ev or "high" in srcq):
        return "Potentially practice-changing"
    if pc >= 3 or cr >= 3:
        return "Worth knowing"
    return "Background / commentary"

def to_markdown(
    items: List[Dict[str, Any]],
    overview_markdown: str,
    details_by_id: Dict[str, str],
    *,
    report_title: str = "Daily Report",
    report_language: str = "de",
    foamed_stats: Optional[Dict[str, Any]] = None,
    cybermed_stats: Optional[Dict[str, Any]] = None,
    report_mode: Optional[str] = None,
    run_metadata: Optional[str] = None,
) -> str:
    lang = _norm_language(report_language)
    title = report_title.strip()
    now_str = datetime.now(tz=STO).strftime("%Y-%m-%d %H:%M") + (" Uhr" if lang == "de" else "")
    # Email clients differ in how they render markdown headers.
    # A simple inline-styled <h1> is more consistent while still producing
    # usable plaintext (the emailer strips HTML tags for the text part).
    safe_title = html_module.escape(title or "")
    md: List[str] = [
        f"<h1 style=\"margin:0 0 4px 0; font-size:32px; line-height:1.15;\">{safe_title}</h1>",
        f"*{now_str}*",
        "",
    ]
    is_cybermed = _is_cybermed_report(title, report_language)
    is_cyberlurch = _is_cyberlurch_report(title)

    overview_markdown = (overview_markdown or "").strip()
    normalized_mode = (report_mode or "").strip().lower()
    if not normalized_mode:
        title_lower = title.lower()
        if "weekly" in title_lower:
            normalized_mode = "weekly"
        elif "monthly" in title_lower:
            normalized_mode = "monthly"
    is_cyberlurch_periodic = is_cyberlurch and normalized_mode in {"weekly", "monthly", "yearly"}
    if is_cyberlurch and normalized_mode == 'monthly':
        return render_cyberlurch_monthly_trend_report(items, title='The Cyberlurch Report — Monthly', generated_at=datetime.now(tz=STO), diagnostics=run_metadata or {})
    meta_only = ""
    if overview_markdown:
        if is_cybermed:
            meta_only = _extract_cybermed_meta_block(overview_markdown)
        else:
            md.extend([overview_markdown, ""])

    if is_cybermed:
        is_cybermed_weekly_digest_only = (
            normalized_mode == "weekly"
            and any((it.get("digest_derived") is True) or (it.get("cybermed_weekly_digest_only") is True) for it in items)
        )
        if normalized_mode == "weekly":
            md[0] = "<h1 style=\"margin:0 0 4px 0; font-size:32px; line-height:1.15;\">Cybermed Weekly Report</h1>"
        elif normalized_mode == "monthly":
            month_key = datetime.now(tz=STO).strftime("%Y-%m")
            if isinstance(cybermed_stats, dict):
                period_start = str(cybermed_stats.get("monthly_digest_period_start") or "").strip()
                if len(period_start) >= 7:
                    month_key = period_start[:7]
            md[0] = f"<h1 style=\"margin:0 0 4px 0; font-size:32px; line-height:1.15;\">Cybermed Monthly Report – {month_key}</h1>"
        greeting_enabled = str(os.getenv("CYBERMED_SEASONAL_GREETING", "1")).strip().lower() not in {"0", "false", "off", "no"}
        greeting = (os.getenv("CYBERMED_SEASONAL_GREETING_TEXT", "") or "").strip()
        if greeting_enabled and greeting:
            md.append(f"*{greeting}*")
            md.append("")
        if normalized_mode in {"weekly", "monthly"}:
            pubmed_count = sum(1 for it in items if str(it.get("source") or "").strip().lower() == "pubmed")
            foamed_count = sum(1 for it in items if str(it.get("source") or "").strip().lower() == "foamed")
            top_pick_count = sum(1 for it in items if it.get("top_pick") is True)
            if normalized_mode in {"weekly", "monthly"} and isinstance(cybermed_stats, dict):
                pubmed_count = int(
                    cybermed_stats.get(
                        f"cybermed_{normalized_mode}_rendered_pubmed_items_total",
                        cybermed_stats.get(f"cybermed_{normalized_mode}_pubmed_items_selected_total", pubmed_count),
                    )
                    or 0
                )
                foamed_count = int(
                    cybermed_stats.get(
                        f"cybermed_{normalized_mode}_rendered_foamed_items_total",
                        cybermed_stats.get(f"cybermed_{normalized_mode}_foamed_items_selected_total", foamed_count),
                    )
                    or 0
                )
                top_pick_count = int(
                    cybermed_stats.get(
                        f"cybermed_{normalized_mode}_rendered_top_picks_total",
                        cybermed_stats.get(
                            f"cybermed_{normalized_mode}_selected_top_picks_total",
                            cybermed_stats.get(f"cybermed_{normalized_mode}_top_picks_selected_total", top_pick_count),
                        ),
                    )
                    or 0
                )
                if normalized_mode in {"weekly", "monthly"}:
                    top_pick_count = min(top_pick_count, 5)
                cybermed_stats[f"cybermed_{normalized_mode}_intro_pubmed_items_total"] = pubmed_count
                cybermed_stats[f"cybermed_{normalized_mode}_intro_foamed_items_total"] = foamed_count
                cybermed_stats[f"cybermed_{normalized_mode}_intro_top_picks_total"] = top_pick_count
                mismatch_fields = []
                if pubmed_count != int(cybermed_stats.get(f"cybermed_{normalized_mode}_rendered_pubmed_items_total", pubmed_count) or 0):
                    mismatch_fields.append("pubmed")
                if foamed_count != int(cybermed_stats.get(f"cybermed_{normalized_mode}_rendered_foamed_items_total", foamed_count) or 0):
                    mismatch_fields.append("foamed")
                if top_pick_count != int(cybermed_stats.get(f"cybermed_{normalized_mode}_rendered_top_picks_total", top_pick_count) or 0):
                    mismatch_fields.append("top_picks")
                cybermed_stats[f"cybermed_{normalized_mode}_intro_count_mismatch_fields"] = mismatch_fields
                cybermed_stats[f"cybermed_{normalized_mode}_intro_count_mismatch_total"] = len(mismatch_fields)
            period_line = ""
            if isinstance(cybermed_stats, dict):
                start = str(cybermed_stats.get("weekly_period_start") or "").strip()
                end = str(cybermed_stats.get("weekly_period_end") or "").strip()
                if start or end:
                    period_line = f"Period: {start or '?'} to {end or '?'}."
            if period_line:
                md.append(period_line)
            md.append(f"Top picks (⭐) first; Daily digests used this period; total included: {pubmed_count} papers, {foamed_count} FOAMed, {top_pick_count} top picks.")
            md.append("")
            top_items = [it for it in items if it.get("top_pick") is True]
            if top_items:
                md.extend(["## Top Picks", ""])
                for it in top_items[:5]:
                    title_lbl = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                    url = str(it.get("url") or "").strip()
                    source_name = str(it.get("source") or "").strip().lower()
                    compact = _pubmed_compact_line(it) if source_name == "pubmed" else _foamed_compact_line(it)
                    md.append(
                        f"**⭐ [{title_lbl}]({url})** — {'PubMed' if source_name == 'pubmed' else 'FOAMed'}"
                        if url
                        else f"**⭐ {title_lbl}** — {'PubMed' if source_name == 'pubmed' else 'FOAMed'}"
                    )
                    if compact:
                        md.append("")
                        md.append(compact)
                    md.extend(["", "---", ""])
            if normalized_mode == "monthly":
                md.extend(["## Executive editorial summary", ""])
                summary_candidates = sorted(
                    items,
                    key=lambda it: (
                        int(it.get("practice_change_potential_1_5") or 0),
                        int(it.get("clinical_relevance_1_5") or 0),
                        1 if str(it.get("evidence_strength_label") or "").lower().find("high") >= 0 else 0,
                    ),
                    reverse=True,
                )
                bullets = []
                for it in summary_candidates:
                    bl = str(it.get("bottom_line") or "").strip()
                    t = str(it.get("title") or "").strip() or "Untitled"
                    if bl:
                        bullets.append(f"- {t}: {bl}")
                    if len(bullets) >= 5:
                        break
                if len(bullets) < 3:
                    md.append("- Insufficient stored digest metadata/bottom lines for a full monthly editorial synopsis.")
                md.extend(bullets[:5])
                md.append("")

                themed = {}
                for it in items:
                    themed.setdefault(_cybermed_monthly_theme_label(it), []).append(it)
                top_themes = sorted(themed.items(), key=lambda kv: len(kv[1]), reverse=True)[:6]
                md.extend(["## This month’s clinical themes", ""])
                for theme, theme_items in top_themes[:6]:
                    md.append(f"### {theme}")
                    for it in theme_items[:3]:
                        t = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                        src = str(it.get("journal") or it.get("foamed_source") or it.get("source") or "").strip()
                        ev = str(it.get("evidence_strength_label") or it.get("source_quality_label") or "").strip()
                        bl = str(it.get("bottom_line") or "").strip()
                        md.append(f"- **{t}** ({src}; {ev}) — {bl or 'No stored bottom line available.'}")
                    md.append("")

                buckets = {"Potentially practice-changing": [], "Worth knowing": [], "Background / commentary": []}
                for it in items:
                    buckets[_cybermed_practice_bucket(it)].append(it)
                md.extend(["## Practice-impact section", ""])
                for label in ["Potentially practice-changing", "Worth knowing", "Background / commentary"]:
                    md.append(f"### {label}")
                    for it in buckets[label][:5]:
                        t = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                        bl = str(it.get("bottom_line") or "").strip()
                        md.append(f"- **{t}** — {bl or 'No stored bottom line available.'}")
                    if not buckets[label]:
                        md.append("- None classified in this bucket from stored monthly digest metadata.")
                    md.append("")
                if isinstance(cybermed_stats, dict):
                    cybermed_stats["cybermed_monthly_editorial_mode"] = True
                    cybermed_stats["cybermed_monthly_theme_count"] = len(top_themes)
                    cybermed_stats["cybermed_monthly_practice_changing_count"] = len(buckets["Potentially practice-changing"])
                    cybermed_stats["cybermed_monthly_worth_knowing_count"] = len(buckets["Worth knowing"])
                    cybermed_stats["cybermed_monthly_commentary_count"] = len(buckets["Background / commentary"])
                    cybermed_stats["cybermed_monthly_editorial_summary_generated_from_digest"] = True
                    cybermed_stats["cybermed_monthly_live_collection_used"] = False

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
                    for idx, it in enumerate(sub_items):
                        iid = str(it.get("id") or "").strip()
                        url = str(it.get("url") or "").strip()
                        title_lbl = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                        display_title = title_lbl
                        label = _md_escape_label(_build_source_label(it))
                        detail = _detail_lookup(details_by_id, it)
                        if is_cybermed_weekly_digest_only:
                            bottom = (it.get("bottom_line") or "").strip()
                        else:
                            bottom = (it.get("bottom_line") or "").strip() if normalized_mode == "weekly" else _best_bottom_line(it, detail)
                        md.append(
                            f"**[{display_title}]({url})** — *{label}*" if url else f"**{display_title}** — *{label}*"
                        )
                        compact = _pubmed_compact_line(it)
                        if it.get("top_pick") is True:
                            compact = _join_compact_segments(["⭐ Top pick", compact])
                        if compact:
                            md.append("")
                            md.append(compact)
                        if bottom:
                            md.append("")
                            md.append(f"**BOTTOM LINE:** {bottom}")
                        else:
                            md.append("")
                            md.append("**BOTTOM LINE:** No stored bottom line available.")
                        if idx < len(sub_items) - 1:
                            md.extend(["", "---", ""])
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
            for idx, it in enumerate(foamed_sorted):
                title_lbl = _md_escape_label(str(it.get("title") or "").strip() or "Untitled")
                url = str(it.get("url") or "").strip()
                source_name = _md_escape_label(str(it.get("foamed_source") or it.get("channel") or "FOAMed"))
                display_title = title_lbl
                line = f"**[{display_title}]({url})** — *{source_name}*" if url else f"**{display_title}** — *{source_name}*"
                md.append(line)
                compact = _foamed_compact_line(it)
                if it.get("top_pick") is True:
                    compact = _join_compact_segments(["⭐ Top pick", compact])
                if compact:
                    md.append("")
                    md.append(compact)
                bottom_line = (it.get("bottom_line") or "").strip()
                if bottom_line and not bottom_line.lower().startswith("bottom line"):
                    bottom_line = f"BOTTOM LINE: {bottom_line}"
                if not bottom_line:
                    bottom_line = "BOTTOM LINE: No stored bottom line available."
                md.append("")
                if bottom_line.upper().startswith("BOTTOM LINE:"):
                    md.append(re.sub(r"(?i)^BOTTOM LINE:\s*", "**BOTTOM LINE:** ", bottom_line, count=1))
                else:
                    md.append(bottom_line)
                if idx < len(foamed_sorted) - 1:
                    md.extend(["", "---", ""])
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
        if normalized_mode == "weekly":
            detail_items = [
                it for it in detail_items
                if not (it.get("digest_derived") is True or it.get("cybermed_weekly_digest_only") is True)
                or _has_stored_cybermed_deep_dive_content(it)
            ]
        if not detail_items:
            detail_items = [it for it in items if _detail_lookup(details_by_id, it)]
        detail_items = sorted(detail_items, key=_deep_dive_sort_key)
        deep_dive_cap = max(0, _cybermed_deep_dive_limit(normalized_mode))
        if deep_dive_cap:
            detail_items = detail_items[:deep_dive_cap]
    else:
        detail_items = [it for it in items if _detail_lookup(details_by_id, it)]

    if detail_items:
        if is_cyberlurch:
            if normalized_mode == "monthly":
                md.extend(["## Monthly trend map", "", "## Topic streams", "", "## Crisis/development trajectories", "", "## Evergreen / long-shelf-life items", "", "## Representative items", "", "## Source/channel summary", ""])
            elif normalized_mode == "yearly":
                md.extend(["## Key themes across the year", "", "## Crisis trajectories", "", "## Recurring narratives", "", "## Topic and channel weights", "", "## Evergreen highlights", "", "## By month", "", "## Representative links", ""])
            topic_candidates: dict[str, list[tuple[int, Dict[str, Any]]]] = {}
            for it in items:
                topic = _topic_from_item(it)
                score = 0
                if str(it.get("transcript_processing") or "").strip() == "direct_full_transcript":
                    score += 100
                if _to_clean_text(it.get("transcript_full_summary")):
                    score += 80
                if it.get("id") in {d.get("id") for d in detail_items}:
                    score += 50
                score += int(float(it.get("cyberlurch_deep_dive_score") or 0.0) * 10)
                if str(it.get("content_status") or "").strip() == "metadata_only":
                    score -= 1000
                topic_candidates.setdefault(topic, []).append((score, it))

            topic_points: dict[str, list[str]] = {}
            for topic, scored_items in topic_candidates.items():
                has_full_text = any(str(si[1].get("content_status") or "").strip() != "metadata_only" for si in scored_items)
                ranked = sorted(scored_items, key=lambda t: t[0], reverse=True)
                for _, it in ranked:
                    if has_full_text and str(it.get("content_status") or "").strip() == "metadata_only":
                        continue
                    bullet = _cyberlurch_topic_bullet(it, _detail_lookup(details_by_id, it))
                    topic_points.setdefault(topic, [])
                    if bullet not in topic_points[topic]:
                        topic_points[topic].append(bullet)
                    if len(topic_points[topic]) >= 5:
                        break
            if topic_points:
                md.extend(["## Themenbereiche / Topic sections", ""])
                for topic, points in sorted(topic_points.items(), key=lambda kv: (-len(kv[1]), kv[0].lower())):
                    md.append(f"### {topic}")
                    for p in points[:5]:
                        md.append(p)
                    md.append("")
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
            detail_block = _detail_lookup(details_by_id, it)
            best_bottom_line = _best_bottom_line(it, detail_block) if is_cybermed else ""
            report_key = str(it.get("report_key") or it.get("cadence") or "").strip().lower()
            is_digest_derived_weekly = (
                is_cybermed
                and normalized_mode == "weekly"
                and (
                    (it.get("digest_derived") is True)
                    or (it.get("cybermed_weekly_digest_only") is True)
                )
                and (not report_key or ("cybermed" in report_key and "weekly" in report_key))
            )
            if is_digest_derived_weekly:
                detail_block = _stored_cybermed_deep_dive_block(it)
                if not detail_block:
                    continue
            elif is_cybermed and str(it.get("source") or "").strip().lower() == "pubmed":
                if not detail_block:
                    abstract = (it.get("abstract") or it.get("text") or "").strip()
                    if len(abstract) >= 200:
                        detail_block = render_pubmed_deep_dive_from_abstract(abstract)
                detail_block = _ensure_pubmed_deep_dive_template(detail_block, best_bottom_line, lang=lang)
            if not detail_block and is_cybermed:
                detail_block = f"**BOTTOM LINE:** {best_bottom_line}"
            if is_cyberlurch:
                detail_block = re.sub(r"(?im)^\s*(Title|Channel|Published|Watch on YouTube)\s*:\s*.*$", "", detail_block)
                detail_block = re.sub(r"(?im)^\s*\[?\s*Watch on YouTube\s*\]?\s*(?:\([^)]*\)|:\s*.+)?\s*$", "", detail_block)
                detail_block = _normalize_deep_dive_headings(detail_block, item_title=str(it.get("title") or "").strip())
            detail_block = _bold_bottom_line_label(detail_block)
            detail_block = _normalize_detail_block_headings(detail_block)
            md.append(detail_block)
            md.append("")

    if not is_cybermed:
        show_top_videos = items and (normalized_mode in {"weekly", "monthly"} or (is_cyberlurch and normalized_mode in {"", "daily"}))
        if show_top_videos:
            heading = "## Top videos (this period)" if normalized_mode in {"weekly", "monthly"} else "## Top videos"
            md.extend([heading, ""])
            if is_cyberlurch and normalized_mode in {"", "daily"} and items and all(
                it.get("content_status") == "metadata_only" for it in items
            ):
                md.append(
                    "This run found recent videos, but YouTube transcript/caption extraction was unavailable; summaries are limited to metadata."
                )
                md.append("")
            seen_urls = set()
            periodic_cap = None
            grouped_links: dict[str, list[dict[str, Any]]] = {}
            for it in items:
                grouped_links.setdefault(str(it.get("topic_primary") or "General").strip() or "General", []).append(it)
            if normalized_mode == "weekly":
                periodic_cap = max(1, int((os.getenv("CYBERLURCH_WEEKLY_TOP_LINKS_MAX", "20") or "20").strip() or "20"))
            per_topic_cap = 999
            if normalized_mode == "monthly":
                per_topic_cap = max(1, int((os.getenv("CYBERLURCH_MONTHLY_REPRESENTATIVE_LINKS_PER_TOPIC", "3") or "3").strip() or "3"))
            elif normalized_mode == "yearly":
                per_topic_cap = max(1, int((os.getenv("CYBERLURCH_YEARLY_REPRESENTATIVE_LINKS_PER_THEME", "3") or "3").strip() or "3"))
            for _, grouped in sorted(grouped_links.items(), key=lambda kv: len(kv[1]), reverse=True):
              for it in grouped[:per_topic_cap]:
                if periodic_cap is not None and len(seen_urls) >= periodic_cap:
                    break
                url = str(it.get("url") or "").strip()
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                title_lbl = _md_escape_label(str(it.get("title") or "").strip()) or "Untitled"
                channel_lbl = _md_escape_label(str(it.get("channel") or "").strip())
                date_val = it.get("published_at")
                date_str = ""
                if isinstance(date_val, datetime):
                    date_str = date_val.strftime("%Y-%m-%d")
                if is_cyberlurch_periodic:
                    star_prefix = "⭐ " if it.get("top_pick") else ""
                    title_part = f"[{title_lbl}]({url})" if url else title_lbl
                    suffix = f" — {channel_lbl}" if channel_lbl else ""
                    date_suffix = f" ({date_str})" if date_str else ""
                    line = f"- {star_prefix}{title_part}{suffix}{date_suffix}"
                else:
                    display_title = _prefix_star(title_lbl) if it.get("top_pick") else title_lbl
                    suffix = f" — {channel_lbl}" if channel_lbl else ""
                    date_suffix = f" ({date_str})" if date_str else ""
                    line = (
                        f"- [{display_title}]({url}){suffix}{date_suffix}"
                        if url
                        else f"- {display_title}{suffix}{date_suffix}"
                    )
                md.append(line)
                src_map = {
                    "youtube_transcript_api": "YouTube transcript API",
                    "description": "YouTube description",
                    "metadata_only": "metadata only",
                }
                text_source = str(it.get("text_source") or "").strip().lower()
                if not text_source and it.get("content_status") == "metadata_only":
                    text_source = "metadata_only"
                if text_source == "managed_transcript":
                    was_direct = str(it.get("transcript_processing") or "").strip() == "direct_full_transcript" and bool(it.get("transcript_direct_success"))
                    was_chunked = str(it.get("transcript_processing") or "").strip() == "chunked_full_transcript" and bool(it.get("transcript_chunking_success"))
                    was_truncated = bool(it.get("transcript_was_truncated"))
                    full_chars = int(it.get("transcript_full_chars_available") or 0)
                    used_chars = int(it.get("transcript_chars_used_for_summary") or 0)
                    if was_direct:
                        label = "TranscriptAPI, full transcript analyzed"
                    elif str(it.get("transcript_processing") or "").strip() == "direct_full_transcript_fallback" and bool(it.get("transcript_direct_success")):
                        label = "TranscriptAPI, full transcript analyzed (fallback digest)"
                    elif was_chunked:
                        label = "TranscriptAPI, full transcript chunked"
                    elif str(it.get("transcript_processing") or "").strip() == "excerpt_fallback":
                        label = "TranscriptAPI, transcript excerpt fallback"
                    elif was_truncated or (full_chars > 0 and used_chars > 0 and used_chars < full_chars):
                        label = "TranscriptAPI, transcript excerpt fallback"
                    else:
                        label = "TranscriptAPI, transcript excerpt fallback"
                    md.append(f"  - Source: {label}")
                elif text_source in src_map:
                    label = src_map[text_source]
                    md.append(f"  - Source: {label}")
                if (it.get("content_status") == "metadata_only") or (text_source == "metadata_only"):
                    md.append("  - Transcript/caption text unavailable; listed from metadata only.")
                if is_cyberlurch_periodic:
                    bottom_line = (it.get("bottom_line") or "").strip()
                    if bottom_line:
                        bl = bottom_line
                        if not bottom_line.lower().startswith("bottom line"):
                            bl = f"BOTTOM LINE: {bottom_line}"
                        md.append(f"  - {bl}")
            md.append("")

        show_sources = not is_cyberlurch_periodic and not (is_cyberlurch and normalized_mode in {"", "daily"} and items)
        if show_sources:
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
