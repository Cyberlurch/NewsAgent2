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
        "date": date_val,
    }


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
    sanitized_exec = [str(x).strip() for x in executive_summary if str(x).strip()]

    payload = {
        "month": month_key,
        "generated_at": generated_at,
        "executive_summary": sanitized_exec,
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

    rollups.sort(key=lambda e: e.get("month") or "")
    state["updated_at_utc"] = _utc_now_iso()
    return state


def extract_summary_bullets(markdown_text: str, max_bullets: int = 8) -> List[str]:
    if max_bullets <= 0:
        return []

    text = (markdown_text or "").strip()
    if not text:
        return []

    lines = [ln.rstrip() for ln in text.splitlines()]
    bullets: List[str] = []

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
            in_exec_section = header.startswith("executive summary") or header.startswith("kurzüberblick")
            if not in_exec_section:
                _flush_sentence_pool(sentence_pool)
                sentence_pool = []
            continue
        if stripped.lower().startswith("### "):
            if in_exec_section:
                break
            continue
        if in_exec_section or not bullets:
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
    return bullets[:max_bullets]


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

    md: List[str] = [
        f"<h1 style=\"margin:0 0 4px 0; font-size:32px; line-height:1.15;\">{report_title}</h1>",
        f"*{now_str}*",
        "",
    ]

    md.append("## Executive Summary" if not is_de else "## Kurzüberblick")
    if not rollups:
        md.append(
            "- No monthly rollups were found for this year."
            if not is_de
            else "- Keine monatlichen Zusammenfassungen für dieses Jahr gefunden."
        )
    else:
        md.append("")
        for entry in rollups:
            month = str(entry.get("month") or "")
            summary = entry.get("executive_summary") or []
            try:
                label = datetime.strptime(f"{month}-01", "%Y-%m-%d").strftime("%B %Y")
            except Exception:
                label = month or str(year)
            if summary:
                md.append(f"- {label}: " + "; ".join(str(x).strip() for x in summary if str(x).strip()))
            else:
                md.append(f"- {label}: (no summary captured)")
    md.append("")

    md.append("## Monthly highlights" if not is_de else "## Monatliche Highlights")
    for entry in rollups:
        month = str(entry.get("month") or "")
        try:
            heading = datetime.strptime(f"{month}-01", "%Y-%m-%d").strftime("%B %Y")
        except Exception:
            heading = month or str(year)
        md.append(f"### {heading}")
        md.append("")
        summary = entry.get("executive_summary") or []
        if summary:
            for bullet in summary:
                if str(bullet).strip():
                    md.append(f"- {str(bullet).strip()}")
        else:
            md.append("- (no summary captured)")

        items = entry.get("top_items") or []
        if items:
            md.append("")
            md.append("Top items:")
            for it in items:
                title = (it.get("title") or "").strip() or "(untitled)"
                url = (it.get("url") or "").strip()
                channel = (it.get("channel") or "").strip()
                src = (it.get("source") or "").strip()
                date = (it.get("date") or "").strip()
                label_parts = [p for p in (channel, src, date) if p]
                label = " — ".join(label_parts) if label_parts else ""
                line = f"- [{title}]({url})" if url else f"- {title}"
                if label:
                    line += f" ({label})"
                md.append(line)
        md.append("")

    return "\n".join(md).rstrip() + "\n"
