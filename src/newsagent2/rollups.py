from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Sequence

from .reporter import render_cyberlurch_yearly_analysis
from .cybermed_quality import assert_no_generation_error_text, sanitize_cybermed_payload
from zoneinfo import ZoneInfo

STO = ZoneInfo("Europe/Stockholm")
DEFAULT_ROLLUPS_PATH = "state/rollups.json"

_PROVIDER_FAILURE_RE = re.compile(
    r"(?:RateLimitError|APIConnectionError|AuthenticationError|BadRequestError|"
    r"InternalServerError|insufficient_quota|credit_balance_exhausted|no credits remaining|"
    r"provider[ _-]?error|(?:openai|api)[ _/-]?error(?:[ _-]?code)?|"
    r"HTTP\s*(?:429|5\d\d)\b)",
    re.IGNORECASE,
)


def contains_provider_failure_text(value: Any) -> bool:
    """Recognize narrow provider/API failures without matching ordinary 'error'."""
    return bool(_PROVIDER_FAILURE_RE.search(str(value or "")))


def sanitize_cyberlurch_generated_fields(
    executive_summary: Sequence[Any], items: Sequence[Dict[str, Any]]
) -> tuple[List[str], List[Dict[str, Any]], int]:
    """Return a sanitized copy of generated Monthly fields and a removal count."""
    removed = 0
    safe_items: List[Dict[str, Any]] = []
    for original in items:
        item = deepcopy(original)
        for field in ("bottom_line", "transcript_full_summary_short", "summary"):
            if item.get(field) and contains_provider_failure_text(item[field]):
                item[field] = ""
                removed += 1
        safe_items.append(item)
    safe_summary = []
    for line in executive_summary or []:
        if contains_provider_failure_text(line):
            removed += 1
        elif str(line).strip():
            safe_summary.append(str(line).strip())
    if not safe_summary:
        candidates: List[str] = []
        for item in safe_items:
            title = str(item.get("title") or "").strip()
            bottom = str(item.get("bottom_line") or "").strip()
            if title:
                candidates.append(f"{title} — {bottom}" if bottom else title)
        safe_summary = candidates[:3] or ["No safe persisted monthly summary was available."]
    return safe_summary, safe_items, removed


def prepare_yearly_rollups(
    rollups: Sequence[Dict[str, Any]], target_year: int, *, sanitize_cyberlurch: bool = True
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Select and sanitize one persisted Monthly record per calendar month, purely."""
    expected = [f"{target_year}-{month:02d}" for month in range(1, 13)]
    candidates = [r for r in rollups if isinstance(r, dict) and str(r.get("month") or "") in expected]
    selected: Dict[str, Dict[str, Any]] = {}
    for raw in candidates:
        month = str(raw.get("month") or "")
        current = selected.get(month)
        if current is None or _parse_generated_at(str(raw.get("generated_at") or "")) >= _parse_generated_at(str(current.get("generated_at") or "")):
            selected[month] = raw
    prepared: List[Dict[str, Any]] = []
    removed_total = sanitized_months = 0
    for month in expected:
        if month not in selected:
            continue
        entry = deepcopy(selected[month])
        if sanitize_cyberlurch:
            summary, top_items, removed = sanitize_cyberlurch_generated_fields(
                entry.get("executive_summary") or [], entry.get("top_items") or []
            )
            entry["executive_summary"] = summary
            entry["top_items"] = top_items
            representative, rep_items, rep_removed = sanitize_cyberlurch_generated_fields(
                [], entry.get("representative_items") or []
            )
            del representative
            entry["representative_items"] = rep_items
            removed += rep_removed
            for field in ("topic_summaries", "topic_trajectories", "evergreen_highlights"):
                values = entry.get(field) or []
                safe = [deepcopy(v) for v in values if not contains_provider_failure_text(v)]
                removed += len(values) - len(safe)
                entry[field] = safe
            removed_total += removed
            sanitized_months += int(removed > 0)
        prepared.append(entry)
    found = [m for m in expected if m in selected]
    missing = [m for m in expected if m not in selected]
    return prepared, {
        "target_year": target_year, "expected_months": expected, "found_months": found,
        "missing_months": missing, "coverage_complete": found == expected,
        "monthly_rollups_loaded_total": len(candidates),
        "duplicate_monthly_rollups_suppressed_total": len(candidates) - len(selected),
        "generation_error_text_removed_total": removed_total,
        "months_with_sanitized_content_total": sanitized_months,
    }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at_utc": _utc_now_iso(),
        "reports": {},
    }


def _parse_generated_at(value: str) -> datetime:
    raw = (value or "").strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _sanitize_rollups_state(state: Dict[str, Any]) -> bool:
    changed = False
    reports = state.setdefault("reports", {})
    if not isinstance(reports, dict):
        state["reports"] = {}
        reports = state["reports"]
        changed = True

    for report_key, entries in list(reports.items()):
        if not isinstance(entries, list):
            reports[report_key] = []
            changed = True
            continue

        dedup: Dict[str, Dict[str, Any]] = {}
        passthrough_entries: List[Any] = []

        for entry in entries:
            if not isinstance(entry, dict):
                passthrough_entries.append(entry)
                continue

            month_key = str(entry.get("month") or "").strip()
            generated_at = str(entry.get("generated_at") or "").strip()

            top_items_raw = entry.get("top_items")
            if isinstance(top_items_raw, list):
                sanitized_items = [_sanitize_item(it) for it in top_items_raw if isinstance(it, dict)]
                if sanitized_items != top_items_raw:
                    entry["top_items"] = sanitized_items
                    changed = True
            else:
                sanitized_items = []
                entry["top_items"] = sanitized_items
                changed = True

            fallback = _fallback_summary_from_items(sanitized_items)
            sanitized_summary = sanitize_rollup_summary(entry.get("executive_summary") or [], fallback=fallback)
            limited_summary = sanitized_summary[:8]
            if limited_summary != entry.get("executive_summary"):
                entry["executive_summary"] = limited_summary
                changed = True

            entry["month"] = month_key
            entry["generated_at"] = generated_at
            if month_key:
                existing = dedup.get(month_key)
                if existing is None or _parse_generated_at(generated_at) >= _parse_generated_at(
                    existing.get("generated_at", "")
                ):
                    dedup[month_key] = entry
                    changed = changed or existing is not None
            else:
                passthrough_entries.append(entry)

        sanitized_entries = sorted(dedup.values(), key=lambda e: _month_sort_key(str(e.get("month") or "")))
        sanitized_entries.extend(passthrough_entries)
        if sanitized_entries != entries:
            reports[report_key] = sanitized_entries
            changed = True

    return changed


def load_rollups_state(path: str, *, create_if_missing: bool = True) -> Dict[str, Any]:
    if not path:
        print("[rollups] WARN: empty path -> starting fresh")
        return _new_state()
    if not os.path.exists(path):
        print(f"[rollups] No state file found at {path!r} -> starting fresh")
        state = _new_state()
        if create_if_missing:
            try:
                save_rollups_state(path, state)
            except Exception as e:
                print(f"[rollups] WARN: failed to initialize rollups state at {path!r}: {e!r}")
        return state

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            print(f"[rollups] WARN: state file {path!r} is empty -> starting fresh")
            state = _new_state()
            if create_if_missing:
                try:
                    save_rollups_state(path, state)
                except Exception as e:
                    print(f"[rollups] WARN: failed to reinitialize empty rollups state at {path!r}: {e!r}")
            return state

        data = json.loads(raw)
        if not isinstance(data, dict):
            print(f"[rollups] WARN: invalid JSON root type in {path!r} -> starting fresh")
            return _new_state()

        data.setdefault("version", 1)
        data.setdefault("updated_at_utc", _utc_now_iso())
        data.setdefault("reports", {})
        if not isinstance(data.get("reports"), dict):
            data["reports"] = {}

        changed = _sanitize_rollups_state(data)
        if changed and path:
            try:
                save_rollups_state(path, data)
            except Exception as e:
                print(f"[rollups] WARN: failed to self-heal state at {path!r}: {e!r}")

        return data
    except Exception as e:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        corrupt = f"{path}.corrupt.{ts}"
        try:
            os.replace(path, corrupt)
            print(
                f"[rollups] ERROR: failed to parse {path!r}: {e!r}. "
                f"Renamed to {corrupt!r} and starting fresh."
            )
        except Exception as e2:
            print(
                f"[rollups] ERROR: failed to parse {path!r}: {e!r}. "
                f"Also failed to rename corrupt file: {e2!r}. Starting fresh."
            )
        return _new_state()


def load_rollups_state_readonly(path: str) -> Dict[str, Any]:
    """Load rollups without self-healing, creating, renaming, or writing files."""
    if not path or not os.path.exists(path):
        return _new_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _new_state()
        # Yearly deliberately consumes the persisted representation. Rendering
        # performs its own non-mutating normalization and sanitation.
        return data
    except Exception as exc:
        print(f"[rollups] WARN: read-only load failed for {path!r}: {exc!r}")
        return _new_state()


def save_rollups_state(path: str, state: Dict[str, Any]) -> None:
    if not path:
        print("[rollups] WARN: empty path -> not saving")
        return

    if not isinstance(state, dict):
        raise TypeError(f"save_rollups_state expects dict state, got {type(state)!r}")

    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    except Exception as e:
        print(f"[rollups] ERROR: cannot create state directory for {path!r}: {e!r}")
        raise

    state["updated_at_utc"] = _utc_now_iso()
    payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(payload)
        f.write("\n")
    os.replace(tmp_path, path)
    print(f"[rollups] Saved rollups to {path!r}")


def _sanitize_item(it: Dict[str, Any]) -> Dict[str, Any]:
    title = (it.get("title") or "").strip()
    url = (it.get("url") or "").strip()
    channel = (it.get("channel") or "").strip()
    source = (it.get("source") or "").strip()
    top_pick = bool(it.get("top_pick"))
    bottom_line_raw = (it.get("bottom_line") or "").strip()
    bottom_line_cleaned = re.sub(r"\s+", " ", bottom_line_raw)
    bottom_line = bottom_line_cleaned[:600].strip()
    published = it.get("published_at") or it.get("date") or ""
    date_val = ""
    if isinstance(published, datetime):
        date_val = published.astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_val = str(published).strip()
        if date_val:
            try:
                dt = datetime.fromisoformat(date_val.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                date_val = dt.strftime("%Y-%m-%d")
            except Exception:
                pass

    return {
        "title": title,
        "url": url,
        "channel": channel,
        "source": source,
        "top_pick": top_pick,
        "date": date_val,
        "bottom_line": bottom_line,
        "topic_primary": (it.get("topic_primary") or "").strip(),
        "topics": it.get("topics") or [],
        "text_source": (it.get("text_source") or "").strip(),
        "content_status": (it.get("content_status") or "").strip(),
        "transcript_processing": (it.get("transcript_processing") or "").strip(),
        "editorial_relevance": (it.get("editorial_relevance") or "").strip(),
        "transcript_full_summary_short": re.sub(r"\s+", " ", str(it.get("transcript_full_summary_short") or "").strip())[:600],
    }


def _month_sort_key(month_str: str) -> tuple[int, str]:
    raw = (month_str or "").strip()
    try:
        dt = datetime.strptime(f"{raw}-01", "%Y-%m-%d")
        return (0, dt.strftime("%Y-%m"))
    except Exception:
        return (1, raw)


def upsert_monthly_rollup(
    state: Dict[str, Any],
    *,
    report_key: str,
    month: str,
    generated_at: str,
    executive_summary: Sequence[str],
    top_items: Sequence[Dict[str, Any]],
    extra_fields: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    if not isinstance(state, dict):
        raise TypeError(f"upsert_monthly_rollup expects dict state, got {type(state)!r}")

    rk = (report_key or "").strip() or "default"
    month_key = (month or "").strip()
    if not month_key:
        raise ValueError("month is required for monthly rollup")

    reports = state.setdefault("reports", {})
    if not isinstance(reports, dict):
        state["reports"] = {}
        reports = state["reports"]

    rollups = reports.setdefault(rk, [])
    if not isinstance(rollups, list):
        reports[rk] = []
        rollups = reports[rk]

    sanitized_items = [_sanitize_item(it) for it in top_items if it]
    sanitized_exec = sanitize_rollup_summary(
        executive_summary,
        fallback=_fallback_summary_from_items(sanitized_items),
    )
    if not sanitized_exec:
        sanitized_exec = _fallback_summary_from_items(sanitized_items)

    payload = {
        "month": month_key,
        "generated_at": generated_at,
        "executive_summary": sanitized_exec[:8],
        "top_items": sanitized_items,
    }
    if isinstance(extra_fields, dict):
        payload.update(extra_fields)

    replaced = False
    for idx, entry in enumerate(rollups):
        if isinstance(entry, dict) and entry.get("month") == month_key:
            rollups[idx] = payload
            replaced = True
            break
    if not replaced:
        rollups.append(payload)

    rollups.sort(key=lambda e: _month_sort_key(str(e.get("month") or "")))
    state["updated_at_utc"] = _utc_now_iso()
    return state


def prune_rollups(
    state: Dict[str, Any],
    *,
    report_key: str,
    max_months: int,
    keep_month: str | None = None,
) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return _new_state()

    if max_months is None or max_months <= 0:
        return state

    rk = (report_key or "").strip() or "default"
    reports = state.setdefault("reports", {})
    if not isinstance(reports, dict):
        state["reports"] = {}
        reports = state["reports"]

    rollups = reports.get(rk, [])
    if not isinstance(rollups, list):
        reports[rk] = []
        return state

    if not rollups:
        return state

    keep_month_key = (keep_month or "").strip()

    valid_entries: List[Dict[str, Any]] = []
    passthrough_entries: List[Any] = []
    for entry in rollups:
        if isinstance(entry, dict) and entry.get("month"):
            valid_entries.append(entry)
        else:
            passthrough_entries.append(entry)

    if not valid_entries:
        reports[rk] = passthrough_entries
        return state

    sorted_entries = sorted(valid_entries, key=lambda e: _month_sort_key(str(e.get("month") or "")))
    month_order: List[str] = []
    for entry in sorted_entries:
        m = str(entry.get("month") or "").strip()
        if m and m not in month_order:
            month_order.append(m)

    keep_count = max(1, int(max_months))
    keep_months = set(month_order[-keep_count:])
    if keep_month_key:
        keep_months.add(keep_month_key)

    pruned_rollups = [entry for entry in sorted_entries if str(entry.get("month") or "").strip() in keep_months]
    pruned_rollups.extend(passthrough_entries)
    reports[rk] = pruned_rollups
    return state


def extract_summary_bullets(markdown_text: str, max_bullets: int = 8, *, require_exec_section: bool = False) -> List[str]:
    if max_bullets <= 0:
        return []

    text = (markdown_text or "").strip()
    if not text:
        return []

    lines = [ln.rstrip() for ln in text.splitlines()]
    bullets: List[str] = []
    exec_section_found = False

    def _flush_sentence_pool(pool: List[str]) -> None:
        nonlocal bullets
        if bullets or not pool:
            return
        sentence = " ".join(pool).strip()
        if sentence:
            bullets.append(sentence)

    in_exec_section = False
    sentence_pool: List[str] = []

    for ln in lines:
        stripped = ln.strip()
        if not stripped:
            continue
        if stripped.lower().startswith("## "):
            header = stripped[3:].strip().lower()
            is_exec_header = header.startswith("executive summary") or header.startswith("kurzüberblick")
            if is_exec_header:
                exec_section_found = True
                in_exec_section = True
                bullets = []
                sentence_pool = []
            else:
                if in_exec_section:
                    _flush_sentence_pool(sentence_pool)
                    break
                _flush_sentence_pool(sentence_pool)
                sentence_pool = []
                in_exec_section = False
            continue
        if stripped.lower().startswith("### "):
            if in_exec_section:
                _flush_sentence_pool(sentence_pool)
                break
            continue
        if in_exec_section or (not require_exec_section and not bullets):
            if stripped.startswith(("-", "*")):
                bullet = stripped.lstrip("-* ").strip()
                if bullet:
                    bullets.append(bullet)
                    if len(bullets) >= max_bullets:
                        break
                continue
            sentence_pool.append(stripped)
            if len(" ".join(sentence_pool)) > 240 or stripped.endswith("."):
                _flush_sentence_pool(sentence_pool)
                sentence_pool = []
                if len(bullets) >= max_bullets:
                    break

    _flush_sentence_pool(sentence_pool)
    if require_exec_section and not exec_section_found:
        return []
    return bullets[:max_bullets]


def sanitize_rollup_summary(lines: Sequence[str] | str, *, fallback: Sequence[str] | None = None) -> List[str]:
    forbidden = [
        "metadata",
        "run metadata",
        "attached",
        "lookback window",
        "foamed source health",
        "pubmed items",
        "foamed items",
    ]

    def _clean(raw_lines: Sequence[str] | str) -> List[str]:
        cleaned: List[str] = []
        if isinstance(raw_lines, str):
            iterable: Sequence[str] = [raw_lines]
        else:
            iterable = raw_lines or []

        for raw in iterable:
            text = str(raw or "").strip()
            if not text:
                continue
            if text.startswith(("- ", "* ")):
                text = text[2:].lstrip()
            while text.startswith(("*", "_")):
                text = text[1:].lstrip()
            while text.endswith(("*", "_")):
                text = text[:-1].rstrip()
            text = text.replace("**", "").strip()
            lowered = text.lower()
            if any(term in lowered for term in forbidden) or re.search(r"\bmetadata\b", lowered):
                continue
            if text:
                cleaned.append(text)
        return cleaned

    cleaned_primary = _clean(lines)
    if cleaned_primary:
        return cleaned_primary

    fb_lines: Sequence[str] = fallback if fallback is not None else ["Highlights derived from top items."]
    cleaned_fallback = _clean(fb_lines)
    return cleaned_fallback or ["Highlights derived from top items."]


def _fallback_summary_from_items(top_items: Sequence[Dict[str, Any]]) -> List[str]:
    ranked: List[Dict[str, Any]] = []
    for idx, item in enumerate(top_items or []):
        bottom_line = (item.get("bottom_line") or "").strip()
        snippet = re.sub(r"\s+", " ", bottom_line)[:120].strip()
        ranked.append(
            {
                "title": (item.get("title") or "").strip(),
                "top_pick": bool(item.get("top_pick")),
                "bottom_line": snippet,
                "_idx": idx,
            }
        )
    ranked.sort(key=lambda it: (0 if it.get("top_pick") else 1, it.get("_idx", 0)))
    entries = [it for it in ranked if it.get("title")]
    if not entries:
        return ["(no summary captured)"]
    bullets = ["Highlights derived from top items."]
    for it in entries[:2]:
        prefix = "⭐ " if it.get("top_pick") else ""
        line = f"{prefix}{it['title']}"
        if it.get("bottom_line"):
            line = f"{line} — {it['bottom_line']}"
        bullets.append(line)
    return bullets


def derive_monthly_summary(
    overview_markdown: str,
    *,
    top_items: Sequence[Dict[str, Any]],
    max_bullets: int = 8,
) -> List[str]:
    cleaned_overview = _strip_metadata_sections(overview_markdown)
    exec_bullets = extract_summary_bullets(cleaned_overview, max_bullets=max_bullets, require_exec_section=True)
    fallback = _fallback_summary_from_items(top_items)
    sanitized_exec = sanitize_rollup_summary(exec_bullets, fallback=fallback)
    sanitized_exec = sanitized_exec[:max_bullets]
    return sanitized_exec or ["(no summary captured)"]


def normalize_rollup_summary(entry: Dict[str, Any]) -> List[str]:
    raw_summary = entry.get("executive_summary") or []
    summary = sanitize_rollup_summary(raw_summary, fallback=_fallback_summary_from_items(entry.get("top_items") or []))
    return summary or ["(no summary captured)"]


def _strip_metadata_sections(md_text: str) -> str:
    text = (md_text or "").strip()
    if not text:
        return ""

    lines = text.splitlines()
    cleaned: List[str] = []
    skip_block = False
    in_cybermed_meta = False

    for ln in lines:
        stripped = ln.strip()
        lowered = stripped.lower()

        if re.match(r"^##\s*run metadata", lowered):
            skip_block = True
            continue

        if stripped.startswith("## ") and skip_block:
            skip_block = False

        if lowered == "**cybermed report metadata**":
            in_cybermed_meta = True
            continue

        if in_cybermed_meta:
            if stripped.startswith("## "):
                in_cybermed_meta = False
            else:
                continue

        if skip_block:
            continue

        if "run metadata" in lowered or "metadata" in lowered:
            continue

        cleaned.append(ln)

    return "\n".join(cleaned)


def rollups_for_year(state: Dict[str, Any], report_key: str, year: int) -> List[Dict[str, Any]]:
    if not isinstance(state, dict):
        return []

    rollups = state.get("reports", {}).get(report_key, [])
    if not isinstance(rollups, list):
        return []

    prefix = f"{year:04d}-"
    filtered = [r for r in rollups if isinstance(r, dict) and str(r.get("month") or "").startswith(prefix)]
    return sorted(filtered, key=lambda r: r.get("month") or "")


def _short_bottom_line(text: str, *, max_len: int = 160) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    if len(cleaned) > max_len:
        return cleaned[: max_len - 1].rstrip() + "…"
    return cleaned


def _derive_themes(items: Sequence[Dict[str, Any]]) -> List[str]:
    themes: List[str] = []
    theme_keywords = [
        ("Resuscitation & cardiac arrest", ["arrest", "resuscitation", "ecpr"]),
        ("Airway management", ["airway", "intubation"]),
        ("Analgesia & regional anesthesia", ["analgesia", "pain", "block"]),
        ("Perioperative hemodynamics", ["hemodynamic", "blood pressure", "fluid"]),
        ("Guidelines & methodology", ["guideline", "standards", "grade", "methodology"]),
    ]
    for label, keywords in theme_keywords:
        combined_texts = " ".join(
            f"{(it.get('title') or '').lower()} {(it.get('bottom_line') or '').lower()}" for it in items
        )
        if any(kw in combined_texts for kw in keywords):
            themes.append(label)
        if len(themes) >= 4:
            break

    if not themes and items:
        return ["Mixed clinical topics"]
    return themes[:4]


def render_yearly_markdown(
    *,
    report_title: str,
    report_language: str,
    year: int,
    rollups: Sequence[Dict[str, Any]],
    daily_digests: Sequence[Dict[str, Any]] | None = None,
    diagnostics: Dict[str, Any] | None = None,
) -> str:
    lang = (report_language or "en").strip().lower()
    is_de = lang.startswith("de")
    is_cyberlurch = "cyberlurch" in (report_title or "").strip().lower()
    is_cybermed = "cybermed" in (report_title or "").strip().lower()
    if is_cyberlurch:
        return render_cyberlurch_yearly_analysis(rollups, target_year=year, generated_at=datetime.now(tz=STO), diagnostics=diagnostics)
    now_str = datetime.now(tz=STO).strftime("%Y-%m-%d %H:%M") + (" Uhr" if is_de else "")

    def _month_label(month_value: str) -> str:
        try:
            return datetime.strptime(f"{month_value}-01", "%Y-%m-%d").strftime("%B %Y")
        except Exception:
            return month_value or str(year)

    sorted_rollups = sorted(
        [entry for entry in rollups if isinstance(entry, dict)],
        key=lambda entry: _month_sort_key(str(entry.get("month") or "")),
    )
    rollup_count = len(sorted_rollups)
    normalized_rollups: List[Dict[str, Any]] = []
    starred_items: List[Dict[str, Any]] = []
    other_items: List[Dict[str, Any]] = []

    for entry in sorted_rollups:
        month = str(entry.get("month") or "")
        label = _month_label(month)
        summary = normalize_rollup_summary(entry)
        items: List[Dict[str, Any]] = []
        for raw_item in entry.get("top_items") or []:
            if not isinstance(raw_item, dict):
                continue
            normalized_item = _sanitize_item(raw_item)
            normalized_item["month_label"] = label
            items.append(normalized_item)
            (starred_items if normalized_item.get("top_pick") else other_items).append(normalized_item)
        normalized_rollups.append({"month": month, "label": label, "summary": summary, "items": items})

    combined_items = starred_items + other_items
    top_ten = combined_items[:10]
    if is_cybermed:
        sorted_rollups, _ = sanitize_cybermed_payload(sorted_rollups)
        assert_no_generation_error_text(
            {"rollups": sorted_rollups, "daily_digests": daily_digests or []},
            boundary="yearly_renderer_input",
        )
        monthly_items: List[Dict[str, Any]] = []
        rich_months = legacy_months = rich_loaded = legacy_loaded = 0
        for entry in sorted_rollups:
            rich = [item for item in (entry.get("cybermed_items") or []) if isinstance(item, dict) and item]
            source_items = rich if rich else [item for item in (entry.get("top_items") or []) if isinstance(item, dict) and item]
            if rich:
                rich_months += 1
                rich_loaded += len(source_items)
            else:
                legacy_months += 1
                legacy_loaded += len(source_items)
            for item in source_items:
                if isinstance(item, dict):
                    monthly_items.append(dict(item))

        def _number(value: Any) -> float | None:
            try:
                return float(value) if value is not None and str(value).strip() else None
            except (TypeError, ValueError):
                return None

        def _bottom_line(value: Any) -> str:
            text = str(value or "").strip()
            prefix = re.compile(r"^\s*(?:\*\*)?bottom\s+line\s*:\s*(?:\*\*)?\s*", re.I)
            while prefix.match(text):
                text = prefix.sub("", text, count=1).strip()
            return text

        def _identity(it: Dict[str, Any]) -> tuple[str, str]:
            for field in ("pmid", "doi", "url"):
                value = str(it.get(field) or "").strip().lower()
                if value:
                    return field, value.removeprefix("https://doi.org/") if field == "doi" else value.rstrip("/")
            title = re.sub(r"[^a-z0-9]+", " ", str(it.get("title") or "").lower()).strip()
            return "title", title

        def _richness(it: Dict[str, Any]) -> tuple[int, int, int]:
            scores = sum(_number(it.get(k)) is not None for k in (
                "evidence_strength_1_5", "practice_relevance_1_5", "practice_change_potential_1_5"
            ))
            return scores, bool(_bottom_line(it.get("bottom_line"))), len([v for v in it.values() if v not in (None, "", [], {})])

        deduped: Dict[tuple[str, str], Dict[str, Any]] = {}
        duplicates = 0
        for item in monthly_items:
            key = _identity(item)
            # An empty title cannot safely identify unrelated records.
            if key == ("title", ""):
                key = ("anonymous", str(len(deduped)))
            if key in deduped:
                duplicates += 1
                if _richness(item) > _richness(deduped[key]):
                    deduped[key] = item
            else:
                deduped[key] = item
        all_items = list(deduped.values())
        coverage = diagnostics if diagnostics is not None else {}
        coverage.update({
            "cybermed_yearly_rich_months_total": rich_months,
            "cybermed_yearly_legacy_months_total": legacy_months,
            "cybermed_yearly_rich_items_loaded_total": rich_loaded,
            "cybermed_yearly_legacy_items_loaded_total": legacy_loaded,
            "cybermed_yearly_duplicates_suppressed_total": duplicates,
            "rich_months_total": rich_months,
            "legacy_months_total": legacy_months,
            "rich_items_loaded_total": rich_loaded,
            "legacy_items_loaded_total": legacy_loaded,
        })
        def _score(it: Dict[str, Any]) -> tuple[float, float, float, int]:
            ev = _number(it.get("evidence_strength_1_5")) or -1
            rel = _number(it.get("practice_relevance_1_5")) or -1
            imp = _number(it.get("practice_change_potential_1_5")) or -1
            ed_penalty = 1 if any(x in str((it.get("publication_types") or "")).lower() for x in ["editorial", "comment", "letter"]) else 0
            return (imp, ev, rel, -ed_penalty)
        ranked = sorted(all_items, key=_score, reverse=True)
        practice_changing = [it for it in ranked if (_number(it.get("practice_change_potential_1_5")) or -1) >= 4][:8]
        interesting = [it for it in ranked if _number(it.get("practice_change_potential_1_5")) is not None and _number(it.get("practice_change_potential_1_5")) < 4][:8]
        unscored = [it for it in ranked if _number(it.get("practice_change_potential_1_5")) is None][:8]
        guidelines = [it for it in ranked if any(k in f"{it.get('title','')} {it.get('bottom_line','')}".lower() for k in ["guideline", "consensus", "systematic review", "meta-analysis"])][:8]
        md = [f"# Cybermed Year in Review – {year}", ""]
        md.extend([
            "## Coverage note",
            f"- Monthly rollups available: {coverage.get('cybermed_yearly_monthly_rollups_loaded_total', len(sorted_rollups))}",
            "- Direct Daily inputs: disabled (Yearly reads Monthly rollups only)",
            f"- Date range covered: {coverage.get('cybermed_yearly_coverage_start','unknown')} to {coverage.get('cybermed_yearly_coverage_end','unknown')}",
            f"- Coverage incomplete: {'YES' if not coverage.get('cybermed_yearly_coverage_complete', not coverage.get('cybermed_yearly_coverage_incomplete', len(sorted_rollups)<12)) else 'NO'}",
            "",
            "## Executive summary",
        ])
        for it in ranked[:8]:
            md.append(f"- {(_bottom_line(it.get('bottom_line')) or it.get('title') or 'Stored item').strip()}")
        md.extend(["", "## Top papers of the year"])
        for it in ranked[:10]:
            t = it.get("title") or "Untitled"
            u = it.get("url") or ""
            bl = _bottom_line(it.get("bottom_line"))
            md.append(f"- [{t}]({u})" if u else f"- {t}")
            if bl:
                md.append(f"  - **BOTTOM LINE:** {bl}")
        md.extend(["", "## Potentially practice-changing items"])
        for it in practice_changing:
            md.append(f"- {(it.get('title') or 'Untitled').strip()} — impact {it.get('practice_change_potential_1_5')}")
        md.extend(["", "## Interesting but not practice-changing"])
        for it in interesting[:8]:
            md.append(f"- {(it.get('title') or 'Untitled').strip()}")
        if unscored:
            md.extend(["", "## Legacy / not scored"])
            for it in unscored:
                md.append(f"- {(it.get('title') or 'Untitled').strip()}")
        if guidelines:
            md.extend(["", "## Guidelines / consensus / systematic reviews"])
            for it in guidelines:
                md.append(f"- {(it.get('title') or 'Untitled').strip()}")
        theme_counts: Dict[str, int] = {}
        for it in ranked:
            structured = [it.get("domain"), it.get("domain_group"), it.get("topic_primary")]
            structured += list(it.get("evidence_tags") or []) if isinstance(it.get("evidence_tags"), list) else []
            for theme in structured:
                clean = str(theme or "").strip()
                if clean:
                    theme_counts[clean] = theme_counts.get(clean, 0) + 1
        if theme_counts:
            md.extend(["", "## Clinical themes of the year"])
            for theme, count in sorted(theme_counts.items(), key=lambda row: (-row[1], row[0].lower()))[:6]:
                md.append(f"- {theme} ({count})")
        else:
            md.extend([
                "",
                "## Clinical themes of the year",
                "- Stored rollups do not contain enough structured theme data.",
            ])
        foamed = [it for it in ranked if str(it.get("source_type") or it.get("source") or "").lower() in {"foamed", "commentary"}]
        if foamed:
            md.extend(["", "## FOAMed & commentary"])
            for it in foamed[:5]:
                md.append(f"- {(it.get('title') or 'Untitled').strip()}")
        md.extend(["", "## What to watch next year"])
        for it in ranked[:5]:
            md.append(f"- Recurring signal: {(it.get('topic_primary') or it.get('title') or 'General').strip()}")
        return "\n".join(md).strip() + "\n"

    md: List[str] = [
        f"<h1 style=\"margin:0 0 4px 0; font-size:32px; line-height:1.15;\">{report_title}</h1>",
        f"*{now_str}*",
        "",
    ]

    if rollup_count < 6:
        md.append(f"Coverage note: only {rollup_count} monthly editions were available for this year.")
        md.append("")

    md.append("## Executive Summary" if not is_de else "## Kurzüberblick")
    if not normalized_rollups:
        md.append(
            "- No monthly rollups were found for this year."
            if not is_de
            else "- Keine monatlichen Zusammenfassungen für dieses Jahr gefunden."
        )
    else:
        md.append("")
        coverage_label = "Coverage" if not is_de else "Abdeckung"
        themes_label = "Key themes" if not is_de else "Schlüsselthemen"
        revisit_label = "Quick revisit (Top 3)" if not is_de else "Schnellüberblick (Top 3)"
        md.append(f"- {coverage_label}: {rollup_count} month(s) captured.")
        themes = _derive_themes(top_ten)
        if is_cyberlurch and themes == ["Mixed clinical topics"]:
            themes = ["General Cyberlurch items"]
        md.append(f"- {themes_label}: {', '.join(themes) if themes else ('(none detected)' if not is_de else '(keine erkannt)')}")
        md.append(f"- {revisit_label}:")
        if top_ten:
            for item in top_ten[:3]:
                title = item.get("title") or "(untitled)"
                url = item.get("url") or ""
                snippet = _short_bottom_line(item.get("bottom_line") or "", max_len=180)
                link = f"[{title}]({url})" if url else title
                line = f"  - {link}"
                if snippet:
                    line = f"{line} — {snippet}"
                md.append(line)
        else:
            md.append("  - (no monthly highlights captured)")
    md.append("")

    md.append("## Top 10 items" if not is_de else "## Top 10 Artikel")
    md.append("")

    if not top_ten:
        md.append("- No monthly highlights were captured.")
    else:
        for item in top_ten:
            prefix = "⭐ " if item["top_pick"] else ""
            title = item["title"]
            url = item["url"]
            meta_parts = [p for p in (item.get("channel"), item.get("date"), item.get("month_label")) if p]
            meta = f" — {' · '.join(meta_parts)}" if meta_parts else ""
            line = f"- {prefix}[{title}]({url}){meta}" if url else f"- {prefix}{title}{meta}"
            if not url:
                line = line.replace("[]()", "")  # guard against empty markdown links if url missing
            md.append(line)
            bottom_line_text = _short_bottom_line(item.get("bottom_line") or "", max_len=220)
            if bottom_line_text:
                md.append(f"  - **BOTTOM LINE:** {bottom_line_text}")
            else:
                fallback_topic = str(item.get("topic_primary") or "").strip() or "General Cyberlurch items"
                fallback_parts = [p for p in [item.get("title"), item.get("channel"), fallback_topic] if str(p or "").strip()]
                md.append(f"  - **BOTTOM LINE:** {' — '.join(str(p).strip() for p in fallback_parts)}")
    md.append("")

    md.append("## By month" if not is_de else "## Nach Monaten")
    for entry in normalized_rollups:
        heading = entry["label"]
        md.append(f"### {heading}")
        md.append("")

        bullets: List[str] = []
        summary = entry["summary"]
        if summary:
            bullets.append(summary[0])

        items = entry["items"]
        primary_item = None
        if items:
            primary_item = next((it for it in items if it.get("top_pick")), items[0])
        secondary_item = None
        if items and len(items) > 1:
            secondary_candidates = [it for it in items if it is not primary_item]
            if secondary_candidates:
                secondary_item = secondary_candidates[0]

        def _format_month_item(item: Dict[str, Any]) -> str:
            title = item.get("title") or "(untitled)"
            url = item.get("url") or ""
            label_parts = [p for p in ((item.get("channel") or "").strip(), (item.get("date") or "").strip()) if p]
            label = " — ".join(label_parts) if label_parts else ""
            snippet = _short_bottom_line(item.get("bottom_line") or "", max_len=140)
            line = f"{title}" if not url else f"[{title}]({url})"
            if label:
                line = f"{line} — {label}"
            if snippet:
                line = f"{line} — {snippet}"
            return line

        if primary_item and len(bullets) < 3:
            bullets.append(_format_month_item(primary_item))
        if secondary_item and len(bullets) < 3:
            bullets.append(_format_month_item(secondary_item))

        if not bullets:
            bullets.append("(no summary captured)")

        md.extend([f"- {b}" for b in bullets])
        md.append("")

    return "\n".join(md).rstrip() + "\n"
