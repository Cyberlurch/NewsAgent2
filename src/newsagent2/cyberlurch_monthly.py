from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, MutableMapping


MONTHLY_EVIDENCE_MAX = 80
MONTHLY_EVIDENCE_TARGET = 72
MONTHLY_CHANNEL_SOFT_CAP = 8
MONTHLY_SUMMARY_MAX_CHARS = 1200
MONTHLY_DETAILS_MAX_CHARS = 700
MONTHLY_DETAILS_MAX_POINTS = 4


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
            "supporting_details": _supporting_details(row),
            "topic_hints": row.get("topics") or ([row.get("topic_primary")] if row.get("topic_primary") else []),
            "temporality": str(row.get("temporality") or "").strip(),
            "_content_status": str(row.get("content_status") or "").strip(),
            "_deep_dive_score": float(row.get("cyberlurch_deep_dive_score") or 0),
            "_top_pick": bool(row.get("top_pick")),
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


def _evidence_score(row: Dict[str, Any]) -> tuple[float, str, str]:
    """Rank persisted evidence without consulting live sources or a model."""
    summary = str(row.get("factual_summary") or "").strip()
    title = str(row.get("title") or "").strip()
    score = min(len(summary), 900) / 180
    if summary and summary != title:
        score += 5
    if row.get("_content_status") not in {"metadata_only", "unavailable"}:
        score += 2
    if row.get("temporality"):
        score += 1
    if row.get("topic_hints"):
        score += 1
    score += min(max(float(row.get("_deep_dive_score") or 0), 0), 10)
    score += 3 if row.get("_top_pick") else 0
    # The remaining fields make ties stable, independent of input order.
    return score, str(row.get("source_date") or ""), str(row.get("ref_id") or "")


def select_evidence_pack(
    registry: List[Dict[str, Any]], *, target: int = MONTHLY_EVIDENCE_TARGET,
    maximum: int = MONTHLY_EVIDENCE_MAX,
) -> List[Dict[str, Any]]:
    """Select a bounded, diverse evidence pack from the authoritative registry."""
    limit = max(0, min(int(maximum), MONTHLY_EVIDENCE_MAX))
    target = min(max(0, int(target)), limit)
    useful = [
        row for row in registry
        if str(row.get("factual_summary") or "").strip()
        and not (
            row.get("_content_status") in {"metadata_only", "unavailable"}
            and row.get("factual_summary") == row.get("title")
            and not row.get("_top_pick")
        )
    ]
    ranked = sorted(useful, key=lambda row: (-_evidence_score(row)[0], _evidence_score(row)[1], _evidence_score(row)[2]))
    selected: List[Dict[str, Any]] = []
    chosen: set[str] = set()
    channel_counts: Dict[str, int] = defaultdict(int)

    def add(row: Dict[str, Any], cap: int) -> bool:
        ref = str(row["ref_id"])
        source = str(row["source"])
        if ref in chosen or channel_counts[source] >= cap or len(selected) >= target:
            return False
        chosen.add(ref); channel_counts[source] += 1; selected.append(row)
        return True

    # Seed calendar weeks, then channels. This protects temporal and source spread
    # before quality-ranked filling, while the cap still permits source trends.
    week_best: Dict[str, Dict[str, Any]] = {}
    channel_best: Dict[str, Dict[str, Any]] = {}
    for row in ranked:
        week = str(row["source_date"])[:8] + str((int(str(row["source_date"])[8:10]) - 1) // 7)
        week_best.setdefault(week, row)
        channel_best.setdefault(str(row["source"]), row)
    for row in sorted(week_best.values(), key=lambda r: (r["source_date"], r["ref_id"])):
        add(row, MONTHLY_CHANNEL_SOFT_CAP)
    # Diversity is a quality-aware constraint, not a quota for every channel.
    # Seed only the strongest channel representatives; low-value channels do not
    # enter merely because they exist in the month.
    diversity_seed_limit = min(len(channel_best), max(8, target // 3))
    strongest_channels = sorted(
        channel_best.values(),
        key=lambda row: (-_evidence_score(row)[0], row["source"].casefold(), row["ref_id"]),
    )[:diversity_seed_limit]
    for row in strongest_channels:
        add(row, MONTHLY_CHANNEL_SOFT_CAP)
    for row in ranked:
        add(row, MONTHLY_CHANNEL_SOFT_CAP)

    # A soft cap must not strand a sparse month below the target. Relax it only
    # when the available channel mix cannot supply enough records.
    cap = MONTHLY_CHANNEL_SOFT_CAP + 1
    while len(selected) < target and len(selected) < len(ranked):
        changed = False
        for row in ranked:
            changed = add(row, cap) or changed
        cap += 1
        if cap > target:
            break
    return sorted(selected, key=lambda row: (row["source_date"], row["ref_id"]))


def _persisted_fact(row: Dict[str, Any]) -> str:
    for key in ("bottom_line", "transcript_full_summary", "summary", "transcript_notable_claims", "important_details"):
        value = row.get(key)
        if isinstance(value, list):
            value = "; ".join(str(part) for part in value)
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if text:
            return text[:MONTHLY_SUMMARY_MAX_CHARS]
    return str(row.get("title") or "").strip()


def _supporting_details(row: Dict[str, Any]) -> List[str]:
    """Compact useful persisted points without transcript recollection."""
    summary = _persisted_fact(row)
    points: List[str] = []
    seen: set[str] = set()
    total = 0
    for key in ("transcript_key_points", "transcript_notable_claims", "important_details"):
        value = row.get(key)
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            point = re.sub(r"\s+", " ", str(candidate or "")).strip(" -•\t")
            normalized = point.casefold()
            if not point or point == summary or normalized in seen:
                continue
            remaining = MONTHLY_DETAILS_MAX_CHARS - total
            if remaining <= 0 or len(points) >= MONTHLY_DETAILS_MAX_POINTS:
                return points
            point = point[:remaining].rstrip()
            if point:
                points.append(point)
                seen.add(normalized)
                total += len(point)
    return points


def monthly_prompt(registry: List[Dict[str, Any]], language: str) -> tuple[str, str]:
    lang = "English" if str(language).lower().startswith("en") else "German"
    system = f"""You produce a neutral, information-dense monthly trend briefing from persisted facts only. Write all narrative prose and headings in {lang}; source names and titles may retain their original language. Return one JSON object, not Markdown, with keys executive_summary (4-6 objects with heading, synthesis, source_refs), trends (3-6 objects with heading, synthesis, source_refs, scope, and source when scope is source_specific), notable_developments (at most 3 objects with heading, synthesis, source_refs; may be empty), month_in_brief (one concise string), and source_refs_used (array).
Trends have scope cross_source or source_specific. A cross_source trend needs representative evidence from at least two distinct channels (2-5 citations) that genuinely supports the same development; never combine unrelated one-offs under an invented causal theme. A source_specific trend needs 3-5 relevant records from one named channel, must name that channel in its heading and prose, and describes a change in that channel's coverage rather than an external-world trend. Write important trends at roughly 80-140 words when evidence supports that density. Explain what happened, change over the month, concrete examples, and source agreement or differences.
Executive entries use a short descriptive heading and 2-4 citations. Notable developments use normally 1-2 citations and must be material standalone events. Exclude programming or promotional updates, isolated low-significance anecdotes, generic impersonation stories, and vague medical claims whose intervention is unidentified; omit rather than guess. Topic fields are fallible hints: regroup facts semantically. Prefer persisted names, actions, institutions, numbers, and concrete claims over abstractions, while clearly attributing allegations. Attribute commentary to the named channel. Never use generic 'the speaker', 'the presenter', 'the podcast', 'der Sprecher', or 'der Bericht'. Avoid advice, moral judgments, generic filler endings, invented facts, URLs, and source IDs. Put citations only in source_refs arrays; do not embed citations or URLs in prose."""
    payload_keys = ("ref_id", "source", "source_date", "title", "factual_summary", "supporting_details", "topic_hints", "temporality")
    payload = [{key: row.get(key) for key in payload_keys} for row in registry]
    return system, "Persisted Monthly sources (JSON):\n" + json.dumps(payload, ensure_ascii=False, indent=2)


def validate_synthesis(value: Any, registry: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise MonthlySynthesisError("Monthly synthesis is not a JSON object")
    known = {row["ref_id"] for row in registry}
    by_ref = {row["ref_id"]: row for row in registry}
    executive = value.get("executive_summary")
    trends = value.get("trends")
    notable = value.get("notable_developments", [])
    brief = value.get("month_in_brief")
    if not isinstance(executive, list) or not executive:
        raise MonthlySynthesisError("invalid executive_summary")
    if not isinstance(trends, list) or not isinstance(notable, list) or not isinstance(brief, str) or not brief.strip():
        raise MonthlySynthesisError("invalid Monthly section shape")
    if len(notable) > 3:
        raise MonthlySynthesisError("notable_developments exceeds maximum of three")
    used: List[str] = []
    prose = [brief]
    for entry in executive:
        if not isinstance(entry, dict) or not str(entry.get("heading") or "").strip() or not str(entry.get("synthesis") or "").strip():
            raise MonthlySynthesisError("invalid executive_summary entry")
        refs = entry.get("source_refs")
        if not isinstance(refs, list) or not refs or any(ref not in known for ref in refs):
            raise MonthlySynthesisError("unknown or missing source reference in executive_summary")
        if not min(2, len(known)) <= len(refs) <= 4:
            raise MonthlySynthesisError("executive_summary must cite 2-4 records")
        used.extend(refs); prose.append(str(entry["synthesis"]))
    for section_name, sections in (("trend", trends), ("notable development", notable)):
        for section in sections:
            if not isinstance(section, dict) or not str(section.get("heading") or "").strip() or not str(section.get("synthesis") or "").strip():
                raise MonthlySynthesisError(f"invalid {section_name}")
            refs = section.get("source_refs")
            if not isinstance(refs, list) or not refs or any(ref not in known for ref in refs):
                raise MonthlySynthesisError(f"unknown or missing source reference in {section_name}")
            if section_name == "notable development" and len(refs) > 2:
                raise MonthlySynthesisError("notable development must cite at most two records")
            if section_name == "trend":
                scope = section.get("scope")
                cited = [by_ref[ref] for ref in refs]
                channels = {str(row["source"]) for row in cited}
                if scope == "cross_source":
                    if not 2 <= len(refs) <= 5 or len(channels) < 2:
                        raise MonthlySynthesisError("cross-source trend needs 2-5 records from distinct channels")
                    topic_sets = [{str(t).strip().casefold() for t in (row.get("topic_hints") or []) if str(t).strip()} for row in cited]
                    shared = {topic for topics in topic_sets for topic in topics if sum(topic in other for other in topic_sets) >= 2}
                    if topic_sets and all(topic_sets) and not shared:
                        raise MonthlySynthesisError("cross-source trend evidence has no shared persisted topic")
                elif scope == "source_specific":
                    source = str(section.get("source") or "").strip()
                    attributed_prose = f"{section['heading']} {section['synthesis']}".casefold()
                    if not 3 <= len(refs) <= 5 or channels != {source} or source.casefold() not in attributed_prose:
                        raise MonthlySynthesisError("source-specific trend must explicitly name its one source and cite 3-5 records")
                else:
                    raise MonthlySynthesisError("trend scope must be cross_source or source_specific")
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


def synthesize_monthly(registry: List[Dict[str, Any]], language: str, call: Callable[[str, str], str],
                       diagnostics: MutableMapping[str, Any] | None = None) -> Dict[str, Any]:
    evidence = select_evidence_pack(registry)
    system, user = monthly_prompt(evidence, language)
    if diagnostics is not None:
        diagnostics.update({
            "monthly_persisted_records_available": len(registry),
            "monthly_evidence_items_selected": len(evidence),
            "monthly_evidence_items_excluded": len(registry) - len(evidence),
            "monthly_unique_channels_available": len({row["source"] for row in registry}),
            "monthly_unique_channels_represented": len({row["source"] for row in evidence}),
            "monthly_max_evidence_items_one_channel": max((sum(item["source"] == source for item in evidence) for source in {row["source"] for row in evidence}), default=0),
            "monthly_prompt_character_count": len(system) + len(user),
            "monthly_provider_input_tokens": None,
            "monthly_provider_output_tokens": None,
            "monthly_provider_operations": 0,
            "monthly_collection_operations": 0,
        })
    last_error: Exception | None = None
    for attempt in range(2):
        repair = "" if attempt == 0 else f"\nYour prior output was invalid ({last_error}). Return a corrected JSON object using only the supplied sources."
        try:
            if diagnostics is not None:
                diagnostics["monthly_provider_operations"] += 1
            raw = call(system, user + repair)
            return validate_synthesis(json.loads(raw), evidence)
        except (json.JSONDecodeError, MonthlySynthesisError) as exc:
            last_error = exc
    raise MonthlySynthesisError(f"Monthly synthesis validation failed after one retry: {last_error}")


def render_monthly(title: str, synthesis: Dict[str, Any], registry: List[Dict[str, Any]]) -> str:
    lookup = {row["ref_id"]: row for row in registry}
    def links(refs: List[str]) -> str:
        return "(" + " · ".join(f"[{ref}]({lookup[ref]['url']})" for ref in refs) + ")"
    month = datetime.strptime(registry[0]["source_date"], "%Y-%m-%d").strftime("%B %Y") if registry else "Monthly"
    lines = ["# The Cyberlurch Report", "", f"**Monthly — {month}**", "", "---", "", "## Executive Brief", ""]
    for entry in synthesis["executive_summary"]:
        heading = str(entry.get("heading") or "Monthly overview").rstrip(".")
        lines += [f"**{heading}.**", f"{entry['synthesis']} {links(entry['source_refs'])}", ""]
    lines += ["---", "", "## Key Trends", ""]
    for index, trend in enumerate(synthesis["trends"], 1):
        lines += [f"### {index}. {trend['heading']}", "", trend['synthesis'], "", links(trend["source_refs"]), ""]
    if synthesis["notable_developments"]:
        lines += ["---", "", "## Worth Noting", ""]
        for item in synthesis["notable_developments"]:
            lines += [f"**{item['heading'].rstrip('.')}.** {item['synthesis']} {links(item['source_refs'])}", ""]
    lines += ["---", "", "## Month in Brief", "", synthesis["month_in_brief"], "", "---", "", "## Sources", ""]
    used = set(synthesis["source_refs_used"])
    for row in registry:
        if row["ref_id"] in used:
            lines.append(f"[{row['ref_id']}]({row['url']}) — {row['source']} — “{row['title']}”")
    return "\n".join(lines).rstrip() + "\n"
