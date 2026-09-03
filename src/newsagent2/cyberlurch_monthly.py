from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List


MONTHLY_EVIDENCE_LIMIT = 80
MONTHLY_EVIDENCE_PROSE_CHARS = 700


class MonthlySynthesisError(RuntimeError):
    """A Monthly edition could not be made safe for delivery."""


_VAGUE_ATTRIBUTION = re.compile(r"\b(?:the speaker|the presenter|the podcast|der sprecher|der bericht)\b", re.I)


def _initial_abbreviation(name: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", name)
    if len(words) > 1:
        return "".join(word[0] for word in words).upper()
    token = words[0] if words else "SOURCE"
    capitals = "".join(ch for ch in token if ch.isupper() or ch.isdigit())
    return (capitals if len(capitals) >= 2 else token[:2]).upper()


def source_abbreviations(names: Iterable[str]) -> Dict[str, str]:
    """Return stable, collision-free abbreviations derived only from source names."""
    unique = sorted({str(name).strip() for name in names if str(name).strip()}, key=str.casefold)
    bases = {name: _initial_abbreviation(name) for name in unique}
    result: Dict[str, str] = {}
    used: set[str] = set()
    for name in unique:
        base = bases[name]
        candidate = base
        width = 2
        compact = re.sub(r"[^A-Za-z0-9]", "", name).upper()
        while candidate in used:
            candidate = base + compact[:width]
            width += 1
        result[name] = candidate
        used.add(candidate)
    return result


def build_source_registry(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in items]
    abbreviations = source_abbreviations(str(row.get("channel") or "") for row in rows)
    prepared = []
    for row in rows:
        source = str(row.get("channel") or "").strip()
        published = row.get("published_at")
        if isinstance(published, datetime):
            date = published
        else:
            try:
                date = datetime.fromisoformat(str(published).replace("Z", "+00:00"))
            except ValueError as exc:
                raise MonthlySynthesisError(f"invalid source date for {source!r}") from exc
        url, title = str(row.get("url") or "").strip(), str(row.get("title") or "").strip()
        if not source or not url or not title:
            raise MonthlySynthesisError("persisted Monthly source lacks channel, title, or URL")
        prepared.append({
            "source": source, "source_date": date.strftime("%Y-%m-%d"),
            "date_label": date.strftime("%d/%m"), "title": title, "url": url,
            "source_identifier": str(row.get("id") or row.get("video_id") or "").strip(),
            "factual_summary": _persisted_fact(row),
            "topic_hints": row.get("topics") or ([row.get("topic_primary")] if row.get("topic_primary") else []),
            "temporality": str(row.get("temporality") or "").strip(),
            "_abbr": abbreviations[source],
        })
    prepared.sort(key=lambda row: (row["source_date"], row["_abbr"], row["source_identifier"], row["url"]))
    groups: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in prepared:
        groups[(row["_abbr"], row["date_label"])].append(row)
    for rows_on_day in groups.values():
        for index, row in enumerate(rows_on_day):
            suffix = f"-{chr(65 + index)}" if len(rows_on_day) > 1 else ""
            row["ref_id"] = f"{row.pop('_abbr')} {row['date_label']}{suffix}"
            row.pop("date_label")
    return prepared


def _quality_key(row: Dict[str, Any]) -> tuple:
    """Rank persisted records without treating their stored topic as ground truth."""
    status = str(row.get("content_status") or "").lower()
    text_source = str(row.get("text_source") or "").lower()
    temporality = str(row.get("temporality") or "").lower()
    score = 0
    score += 8 if status == "full_text" else (3 if status and status != "metadata_only" else 0)
    score += 6 if text_source in {"managed_transcript", "transcript", "captions"} else (2 if text_source != "metadata_only" else 0)
    score += 4 if row.get("transcript_full_summary") else 0
    score += 3 if row.get("bottom_line") or row.get("summary") else 0
    try:
        deep_dive_score = int(float(row.get("cyberlurch_deep_dive_score") or 0))
    except (TypeError, ValueError):
        deep_dive_score = 0
    score += min(5, max(0, deep_dive_score))
    score += 2 if temporality in {"breaking_news", "current_affairs"} else 0
    score += 1 if row.get("topics") or row.get("topic_primary") else 0
    published = row.get("published_at")
    stamp = published.isoformat() if isinstance(published, datetime) else str(published or "")
    identity = str(row.get("id") or row.get("video_id") or row.get("url") or row.get("title") or "")
    return (-score, stamp, str(row.get("channel") or "").casefold(), identity)


def select_monthly_evidence(items: Iterable[Dict[str, Any]], limit: int = MONTHLY_EVIDENCE_LIMIT) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Deterministically select a diverse, month-spanning pack from persisted rows only.

    One best record per channel and per quarter-month is considered first. Remaining
    slots use source round-robin order. A generous per-source soft cap still permits
    repeated same-channel evidence while preventing publication volume dominance.
    """
    rows = [dict(row) for row in items]
    if limit <= 0:
        raise ValueError("Monthly evidence limit must be positive")
    by_source: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[str(row.get("channel") or "").strip()].append(row)
    for source in by_source:
        by_source[source].sort(key=_quality_key)
    cap = max(4, math.ceil(limit * 0.12))
    selected: List[Dict[str, Any]] = []
    selected_ids: set[int] = set()
    source_counts: Dict[str, int] = defaultdict(int)

    def add(row: Dict[str, Any]) -> None:
        marker = id(row)
        if marker not in selected_ids and len(selected) < limit:
            selected.append(row); selected_ids.add(marker)
            source_counts[str(row.get("channel") or "").strip()] += 1

    # Guarantee temporal spread when evidence exists in each part of the month.
    for quarter in range(4):
        candidates = []
        for source_rows in by_source.values():
            for row in source_rows:
                raw = row.get("published_at")
                try:
                    day = raw.day if isinstance(raw, datetime) else datetime.fromisoformat(str(raw).replace("Z", "+00:00")).day
                except (TypeError, ValueError):
                    continue
                if min((day - 1) // 8, 3) == quarter:
                    candidates.append(row); break
        if candidates:
            add(sorted(candidates, key=_quality_key)[0])

    # Give every channel a voice before adding repeated evidence.
    for source in sorted(by_source, key=str.casefold):
        if by_source[source]: add(by_source[source][0])
    while len(selected) < min(limit, len(rows)):
        progress = False
        for source in sorted(by_source, key=str.casefold):
            options = by_source[source]
            candidate = next((row for row in options if id(row) not in selected_ids), None)
            if candidate is not None and source_counts[source] < cap:
                add(candidate); progress = True
            if len(selected) >= limit: break
        if not progress: break
    # The cap is deliberately soft: if all other channels are exhausted, use the
    # best remaining persisted evidence rather than padding or losing useful
    # source-specific repetition.
    if len(selected) < min(limit, len(rows)):
        remaining = [row for row in rows if id(row) not in selected_ids]
        for row in sorted(remaining, key=_quality_key):
            add(row)
            if len(selected) >= limit: break
    selected.sort(key=lambda row: (str(row.get("published_at") or ""), str(row.get("channel") or "").casefold(), str(row.get("id") or row.get("video_id") or row.get("url") or "")))
    diagnostics = {
        "persisted_records_available": len(rows), "evidence_records_retained": len(selected),
        "evidence_records_excluded": len(rows) - len(selected),
        "unique_channels_available": len({s for s in by_source if s}),
        "unique_channels_represented": len({str(row.get('channel') or '').strip() for row in selected if str(row.get('channel') or '').strip()}),
        "evidence_limit": limit, "per_source_soft_cap": cap,
    }
    return selected, diagnostics


def _persisted_fact(row: Dict[str, Any]) -> str:
    for key in ("bottom_line", "transcript_full_summary", "summary", "transcript_notable_claims", "important_details"):
        value = row.get(key)
        if isinstance(value, list):
            value = "; ".join(str(part) for part in value)
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if text:
            return text[:MONTHLY_EVIDENCE_PROSE_CHARS]
    return str(row.get("title") or "").strip()


def monthly_prompt(registry: List[Dict[str, Any]], language: str) -> tuple[str, str]:
    lang = "English" if str(language).lower().startswith("en") else "German"
    system = f"""You produce a neutral monthly briefing from persisted facts only. Write all narrative prose and headings in {lang}; source names and titles may retain their original language. Return one JSON object, not Markdown, with keys executive_summary (4-6 objects with synthesis and source_refs), trends (3-6 objects with heading, synthesis, source_refs), notable_developments (objects with heading, synthesis, source_refs; may be empty), month_in_brief (one string), and source_refs_used (array). Each trend must combine at least two source items. Put isolated items only in notable_developments. Repeated items from one channel may demonstrate that channel's changing emphasis, but must be explicitly described as a source-specific trend, never as a general external-world trend. Topic fields are fallible hints: regroup facts semantically. Attribute commentary to the named channel. Never use generic 'the speaker', 'the presenter', 'the podcast', 'der Sprecher', or 'der Bericht'. Avoid advice, moral judgments, 'this highlights/underscores/reflects', invented facts, URLs, and source IDs. Put citations only in source_refs arrays; do not embed citations or URLs in prose."""
    payload = [{k: v for k, v in row.items() if k != "date_label"} for row in registry]
    return system, "Persisted Monthly sources (JSON):\n" + json.dumps(payload, ensure_ascii=False, indent=2)


def validate_synthesis(value: Any, registry: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise MonthlySynthesisError("Monthly synthesis is not a JSON object")
    known = {row["ref_id"] for row in registry}
    executive = value.get("executive_summary")
    trends = value.get("trends")
    notable = value.get("notable_developments", [])
    brief = value.get("month_in_brief")
    if not isinstance(executive, list) or not executive:
        raise MonthlySynthesisError("invalid executive_summary")
    if not isinstance(trends, list) or not isinstance(notable, list) or not isinstance(brief, str) or not brief.strip():
        raise MonthlySynthesisError("invalid Monthly section shape")
    used: List[str] = []
    prose = [brief]
    for entry in executive:
        if not isinstance(entry, dict) or not str(entry.get("synthesis") or "").strip():
            raise MonthlySynthesisError("invalid executive_summary entry")
        refs = entry.get("source_refs")
        if not isinstance(refs, list) or not refs or any(ref not in known for ref in refs):
            raise MonthlySynthesisError("unknown or missing source reference in executive_summary")
        used.extend(refs); prose.append(str(entry["synthesis"]))
    for section_name, sections in (("trend", trends), ("notable development", notable)):
        for section in sections:
            if not isinstance(section, dict) or not str(section.get("heading") or "").strip() or not str(section.get("synthesis") or "").strip():
                raise MonthlySynthesisError(f"invalid {section_name}")
            refs = section.get("source_refs")
            if not isinstance(refs, list) or not refs or any(ref not in known for ref in refs):
                raise MonthlySynthesisError(f"unknown or missing source reference in {section_name}")
            if section_name == "trend" and len(set(refs)) < 2:
                raise MonthlySynthesisError("a trend must cite at least two persisted items")
            used.extend(refs); prose.extend([str(section["heading"]), str(section["synthesis"])])
    declared = value.get("source_refs_used")
    if not isinstance(declared, list) or set(declared) != set(used) or any(ref not in known for ref in declared):
        raise MonthlySynthesisError("source_refs_used does not match section citations")
    joined = " ".join(prose)
    if _VAGUE_ATTRIBUTION.search(joined):
        raise MonthlySynthesisError("generic unidentified attribution in Monthly prose")
    if any(row["url"] in joined for row in registry) or re.search(r"https?://", joined):
        raise MonthlySynthesisError("model-generated prose contains a URL")
    value["source_refs_used"] = sorted(set(used))
    return value


def synthesize_monthly(registry: List[Dict[str, Any]], language: str, call: Callable[[str, str], str], *, evidence_registry: List[Dict[str, Any]] | None = None, diagnostics: Dict[str, Any] | None = None) -> Dict[str, Any]:
    system, user = monthly_prompt(evidence_registry or registry, language)
    if diagnostics is not None:
        diagnostics["prompt_character_count"] = len(system) + len(user)
        diagnostics["approximate_input_tokens"] = math.ceil((len(system) + len(user)) / 4)
        diagnostics["provider_operation_count"] = 0
    last_error: Exception | None = None
    for attempt in range(2):
        repair = "" if attempt == 0 else f"\nYour prior output was invalid ({last_error}). Return a corrected JSON object using only the supplied sources."
        try:
            if diagnostics is not None: diagnostics["provider_operation_count"] += 1
            raw = call(system, user + repair)
            if diagnostics is not None:
                usage = getattr(call, "last_usage", {}) or {}
                diagnostics["provider_input_tokens"] = usage.get("input_tokens")
                diagnostics["provider_output_tokens"] = usage.get("output_tokens")
            return validate_synthesis(json.loads(raw), registry)
        except (json.JSONDecodeError, MonthlySynthesisError) as exc:
            last_error = exc
    raise MonthlySynthesisError(f"Monthly synthesis validation failed after one retry: {last_error}")


def render_monthly(title: str, synthesis: Dict[str, Any], registry: List[Dict[str, Any]]) -> str:
    lines = [f"# {title}", "", "## Executive Summary", ""]
    lines += [f"- {entry['synthesis']} [{'; '.join(entry['source_refs'])}]" for entry in synthesis["executive_summary"]]
    lines += ["", "## Key Trends", ""]
    for trend in synthesis["trends"]:
        refs = "; ".join(trend["source_refs"])
        lines += [f"### {trend['heading']}", "", f"{trend['synthesis']} [{refs}]", ""]
    if synthesis["notable_developments"]:
        lines += ["## Notable Developments", ""]
        for item in synthesis["notable_developments"]:
            refs = "; ".join(item["source_refs"])
            lines += [f"### {item['heading']}", "", f"{item['synthesis']} [{refs}]", ""]
    lines += ["## Month in Brief", "", synthesis["month_in_brief"], "", "## Sources", ""]
    used = set(synthesis["source_refs_used"])
    for row in registry:
        if row["ref_id"] in used:
            lines.append(f"[{row['ref_id']}] {row['source']} — “{row['title']}” — <{row['url']}>")
    return "\n".join(lines).rstrip() + "\n"
