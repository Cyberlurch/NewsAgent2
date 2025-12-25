from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence
from zoneinfo import ZoneInfo

STO = ZoneInfo("Europe/Stockholm")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at_utc": _utc_now_iso(),
        "reports": {},
    }


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

        for entry in entries:
            if not isinstance(entry, dict):
                continue

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

    return changed


def load_rollups_state(path: str) -> Dict[str, Any]:
    if not path:
        print("[rollups] WARN: empty path -> starting fresh")
        return _new_state()

    if not os.path.exists(path):
        print(f"[rollups] No state file found at {path!r} -> starting fresh")
        return _new_state()

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            print(f"[rollups] WARN: state file {path!r} is empty -> starting fresh")
            return _new_state()

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
            if any(term in lowered for term in forbidden):
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
        ranked.append(
            {
                "title": (item.get("title") or "").strip(),
                "top_pick": bool(item.get("top_pick")),
                "_idx": idx,
            }
        )
    ranked.sort(key=lambda it: (0 if it.get("top_pick") else 1, it.get("_idx", 0)))
    titles = [it["title"] for it in ranked if it["title"]]
    if not titles:
        return ["(no summary captured)"]
    bullets = ["Highlights derived from top items."]
    bullets.extend(titles[:2])
    return bullets


def derive_monthly_summary(
    overview_markdown: str,
    *,
    top_items: Sequence[Dict[str, Any]],
    max_bullets: int = 8,
) -> List[str]:
    exec_bullets = extract_summary_bullets(overview_markdown, max_bullets=max_bullets, require_exec_section=True)
    fallback = _fallback_summary_from_items(top_items)
    sanitized_exec = sanitize_rollup_summary(exec_bullets, fallback=fallback)
    sanitized_exec = sanitized_exec[:max_bullets]
    return sanitized_exec or ["(no summary captured)"]


def normalize_rollup_summary(entry: Dict[str, Any]) -> List[str]:
    raw_summary = entry.get("executive_summary") or []
    summary = sanitize_rollup_summary(raw_summary, fallback=_fallback_summary_from_items(entry.get("top_items") or []))
    return summary or ["(no summary captured)"]


def rollups_for_year(state: Dict[str, Any], report_key: str, year: int) -> List[Dict[str, Any]]:
    if not isinstance(state, dict):
        return []

    rollups = state.get("reports", {}).get(report_key, [])
    if not isinstance(rollups, list):
        return []

    prefix = f"{year:04d}-"
    filtered = [r for r in rollups if isinstance(r, dict) and str(r.get("month") or "").startswith(prefix)]
    return sorted(filtered, key=lambda r: r.get("month") or "")


def render_yearly_markdown(
    *,
    report_title: str,
    report_language: str,
    year: int,
    rollups: Sequence[Dict[str, Any]],
) -> str:
    lang = (report_language or "en").strip().lower()
    is_de = lang.startswith("de")
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

    md: List[str] = [
        f"<h1 style=\"margin:0 0 4px 0; font-size:32px; line-height:1.15;\">{report_title}</h1>",
        f"*{now_str}*",
        "",
    ]

    if rollup_count < 6:
        md.append(f"Coverage note: only {rollup_count} monthly editions were available for this year.")
        md.append("")

    md.append("## Executive Summary" if not is_de else "## Kurzüberblick")
    if not sorted_rollups:
        md.append(
            "- No monthly rollups were found for this year."
            if not is_de
            else "- Keine monatlichen Zusammenfassungen für dieses Jahr gefunden."
        )
    else:
        md.append("")
        for entry in sorted_rollups:
            month = str(entry.get("month") or "")
            summary = normalize_rollup_summary(entry)
            label = _month_label(month)
            md.append(f"- {label}: " + "; ".join(summary))
    md.append("")

    md.append("## Top 10 items" if not is_de else "## Top 10 Artikel")
    md.append("")

    starred_items: List[Dict[str, Any]] = []
    other_items: List[Dict[str, Any]] = []
    for entry in sorted_rollups:
        month = str(entry.get("month") or "")
        label = _month_label(month)
        for raw_item in entry.get("top_items") or []:
            item = raw_item or {}
            normalized = {
                "title": (item.get("title") or "").strip() or "(untitled)",
                "url": (item.get("url") or "").strip(),
                "channel": (item.get("channel") or "").strip(),
                "source": (item.get("source") or "").strip(),
                "date": (item.get("date") or "").strip(),
                "month_label": label,
                "top_pick": bool(item.get("top_pick")),
            }
            (starred_items if normalized["top_pick"] else other_items).append(normalized)

    combined_items = starred_items + other_items
    top_ten = combined_items[:10]
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
    md.append("")

    md.append("## By month" if not is_de else "## Nach Monaten")
    for entry in sorted_rollups:
        month = str(entry.get("month") or "")
        heading = _month_label(month)
        md.append(f"### {heading}")
        md.append("")

        summary = normalize_rollup_summary(entry)
        bullets: List[str] = []
        for bullet in summary:
            if len(bullets) >= 3:
                break
            bullets.append(bullet)

        if len(bullets) < 3:
            for item in entry.get("top_items") or []:
                if len(bullets) >= 3:
                    break
                title = (item.get("title") or "").strip() or "(untitled)"
                url = (item.get("url") or "").strip()
                label_parts = [p for p in ((item.get("channel") or "").strip(), (item.get("date") or "").strip()) if p]
                label = " — ".join(label_parts) if label_parts else ""
                entry_line = f"{title} ({label})" if label else title
                bullets.append(f"[{entry_line}]({url})" if url else entry_line)

        if not bullets:
            bullets.append("(no summary captured)")

        md.extend([f"- {b}" for b in bullets])
        md.append("")

    return "\n".join(md).rstrip() + "\n"
