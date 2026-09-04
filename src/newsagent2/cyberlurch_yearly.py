"""Semantic Cyberlurch Yearly reporting from persisted Monthly rollups only."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Iterable, MutableMapping


SEMANTIC_SCHEMA = "cyberlurch-monthly-semantic-v2"
LEGACY_ITEM_LIMIT = 16
NOTABLE_DEVELOPMENT_LIMIT = 7


_OVERLAP_STOPWORDS = {
    "and", "are", "but", "for", "from", "into", "that", "the", "their", "this",
    "through", "with", "during", "over", "under", "after", "before", "reported",
}


class YearlySynthesisError(RuntimeError):
    """The Yearly edition could not be validated for safe delivery."""


def _text(value: Any, limit: int = 1800) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _topic_terms(item: dict[str, Any]) -> set[str]:
    """Return stable, moderately selective terms for conservative deduplication."""
    words = re.findall(r"[a-z0-9]+", f"{item.get('heading', '')} {item.get('synthesis', '')}".casefold())
    return {word for word in words if len(word) >= 5 and word not in _OVERLAP_STOPWORDS}


def _obviously_overlaps_trend(item: dict[str, Any], trends: list[dict[str, Any]]) -> bool:
    """Identify only high-confidence topic duplication backed by shared evidence."""
    item_refs = set(item["source_refs"])
    item_terms = _topic_terms(item)
    for trend in trends:
        trend_refs = set(trend["source_refs"])
        shared_refs = len(item_refs & trend_refs) / min(len(item_refs), len(trend_refs))
        trend_terms = _topic_terms(trend)
        if not item_terms or not trend_terms:
            continue
        shared_terms = item_terms & trend_terms
        topic_overlap = len(shared_terms) / min(len(item_terms), len(trend_terms))
        if shared_refs >= .5 and len(shared_terms) >= 3 and topic_overlap >= .6:
            return True
    return False


def _legacy_fact(item: dict[str, Any]) -> str:
    for key in ("bottom_line", "summary", "transcript_full_summary_short", "transcript_full_summary"):
        value = _text(item.get(key), 900)
        low_signal = ("provided excerpt" in value.casefold() or "contains only promotional" in value.casefold()
                      or "does not contain enough" in value.casefold())
        if value and not low_signal and value.casefold() != _text(item.get("title"), 900).casefold():
            return value
    return ""


def classify_monthly_rollup(rollup: dict[str, Any]) -> str:
    """Classify evidence by persisted substance, never by a hard-coded month."""
    if rollup.get("schema") == SEMANTIC_SCHEMA:
        return "semantic_v2"
    candidates = rollup.get("representative_items") or rollup.get("top_items") or []
    substantive = [item for item in candidates if isinstance(item, dict) and _legacy_fact(item)]
    provenance_signal = int(rollup.get("full_text_count") or 0) > 0 or any(
        _text(item.get("content_status")).casefold() == "full_text" or bool(_text(item.get("text_source")))
        for item in substantive
    )
    if substantive and provenance_signal:
        return "enriched_legacy"
    return "thin_legacy"


def _valid_month(value: Any, year: int) -> str:
    month = str(value or "")
    return month if re.fullmatch(rf"{year}-(?:0[1-9]|1[0-2])", month) else ""


def _legacy_ref(month: str, index: int) -> str:
    return f"{month}::LEGACY-{index:02d}"


def build_annual_evidence(
    rollups: Iterable[dict[str, Any]], target_year: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Build a bounded evidence payload and authoritative URL registry."""
    expected = [f"{target_year}-{number:02d}" for number in range(1, 13)]
    # Keep the latest persisted copy per month without mutating caller data.
    by_month: dict[str, dict[str, Any]] = {}
    for raw in rollups:
        month = _valid_month(raw.get("month"), target_year)
        if month and (month not in by_month or str(raw.get("generated_at", "")) >= str(by_month[month].get("generated_at", ""))):
            by_month[month] = raw

    payload: list[dict[str, Any]] = []
    registry: list[dict[str, Any]] = []
    classes: dict[str, list[str]] = defaultdict(list)
    enriched_dates: dict[str, dict[str, str]] = {}

    for month in sorted(by_month):
        rollup = by_month[month]
        quality = classify_monthly_rollup(rollup)
        classes[quality].append(month)
        if quality == "thin_legacy":
            continue
        if quality == "semantic_v2":
            ref_map: dict[str, str] = {}
            for source in rollup.get("source_registry") or []:
                if not isinstance(source, dict):
                    continue
                local_ref, url = _text(source.get("ref_id"), 160), _text(source.get("url"), 2000)
                channel, title = _text(source.get("source") or source.get("channel"), 200), _text(source.get("title"), 500)
                if not local_ref or not url or not channel or not title:
                    continue
                annual_ref = f"{month}::{local_ref}"
                ref_map[local_ref] = annual_ref
                registry.append({"ref_id": annual_ref, "month": month, "source": channel,
                                 "source_date": _text(source.get("source_date"), 40), "title": title,
                                 "url": url, "evidence_quality": quality, "local_ref": local_ref})

            def semantic_entries(values: Any, *, scope: bool = False) -> list[dict[str, Any]]:
                result = []
                for value in values if isinstance(values, list) else []:
                    if not isinstance(value, dict):
                        continue
                    refs = [ref_map[ref] for ref in value.get("source_refs", []) if ref in ref_map]
                    row = {"heading": _text(value.get("heading"), 300),
                           "synthesis": _text(value.get("synthesis")), "source_refs": refs}
                    if scope:
                        row["scope"] = _text(value.get("scope"), 80)
                    if row["synthesis"] and refs:
                        result.append(row)
                return result

            summaries = rollup.get("executive_summary") or []
            payload.append({
                "month": month, "evidence_quality": quality,
                "executive_summary": [_text(v.get("synthesis") if isinstance(v, dict) else v) for v in summaries][0:5],
                "themes": semantic_entries(rollup.get("themes"), scope=True),
                "notable_developments": semantic_entries(rollup.get("notable_developments")),
                "month_in_brief": _text(rollup.get("month_in_brief")),
            })
            continue

        items = rollup.get("representative_items") or rollup.get("top_items") or []
        useful: list[dict[str, Any]] = []
        dates: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            fact = _legacy_fact(item)
            channel, title, url = _text(item.get("channel"), 200), _text(item.get("title"), 500), _text(item.get("url"), 2000)
            if not fact or not channel or not title or not url:
                continue
            if len(useful) >= LEGACY_ITEM_LIMIT:
                break
            source_date = _text(item.get("date") or item.get("published_at"), 40)[:10]
            annual_ref = _legacy_ref(month, len(useful) + 1)
            registry.append({"ref_id": annual_ref, "month": month, "source": channel,
                             "source_date": source_date, "title": title, "url": url,
                             "evidence_quality": quality, "local_ref": annual_ref.split("::", 1)[1]})
            useful.append({"title": title, "channel": channel, "date": source_date,
                           "summary": fact, "source_refs": [annual_ref]})
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", source_date):
                dates.append(source_date)
        if dates:
            enriched_dates[month] = {"earliest": min(dates), "latest": max(dates)}
        payload.append({"month": month, "evidence_quality": quality, "items": useful})

    found = sorted(by_month)
    diagnostics = {
        "target_year": target_year, "expected_months": expected, "found_months": found,
        "missing_months": [month for month in expected if month not in found],
        "calendar_coverage_complete": found == expected,
        "monthly_rollups_found": len(found),
        "semantic_v2_months": classes["semantic_v2"],
        "enriched_legacy_months": classes["enriched_legacy"],
        "thin_legacy_months": classes["thin_legacy"],
        "semantic_v2_month_count": len(classes["semantic_v2"]),
        "enriched_legacy_month_count": len(classes["enriched_legacy"]),
        "thin_legacy_month_count": len(classes["thin_legacy"]),
        "enriched_legacy_source_date_coverage": enriched_dates,
        "annual_evidence_units": sum(len(row.get("themes", [])) + len(row.get("notable_developments", [])) + len(row.get("items", [])) for row in payload),
        "authoritative_source_refs_available": len(registry),
        "unique_channels_represented": len({row["source"] for row in registry}),
        "collection_operations": 0, "daily_digest_inputs": 0,
        "transcript_recollection_operations": 0, "rollups_state_mutated": False,
    }
    return payload, registry, diagnostics


def yearly_prompt(payload: list[dict[str, Any]], registry: list[dict[str, Any]], language: str,
                  coverage: MutableMapping[str, Any] | None = None) -> tuple[str, str]:
    lang = "English" if str(language).lower().startswith("en") else "German"
    system = f"""Produce a neutral, precise annual trend analysis in {lang}, using only the persisted Monthly evidence supplied. Return exactly one JSON object with executive_summary (3-4 objects), annual_trends (normally 4-6 objects), turning_points (only supported, non-redundant objects), notable_developments (normally 5-7 objects when the evidence is rich; hard maximum 7), timeline (4-8 chronological objects), year_in_brief (one substantive string), and source_refs_used (array). Every section object has heading (timeline uses period), synthesis, and source_refs. Return source IDs only: never return or write a URL.
Section roles are distinct. Executive Brief is a 45-80-word-per-entry abstract and may orient readers to subjects analyzed later, but must not duplicate full trend explanations. Major Trends are the substantive cross-month analysis: each needs at least two distinct months and two distinct sources/channels and should use 120-220 words only when supported. Turning Points are discrete events or changes that demonstrably altered the covered trajectory; omit one that merely restates a Major Trend without independent event evidence, and prefer at least two sources. Notable Developments preserve significant material that is not sufficiently cross-month for a trend or trajectory-changing for a turning point. Give Monthly notable_developments preferential consideration for annual Notable Developments when they are materially significant, add a genuinely different subject, and are not already adequately represented in Executive Brief, Major Trends, or Turning Points. Do not mechanically include every Monthly notable_development, use predefined categories, or force a seventh item; use a seventh only when it adds materially distinct information. Ask explicitly: “Which materially different developments would otherwise disappear because the dominant trends consume most of the report?” Frequency must not equal importance; never force filler.
Preserve direct provenance for narrow claims. Do not use a source reference merely because it belongs to a broader Monthly theme. For a narrow factual development, cite only evidence that directly supports that development. If the same development exists as a Monthly notable_development, treat its source_refs as the preferred direct provenance. Narrow Turning Points may still arise from themes and need not all originate in Monthly notable_developments.
This is an annual analysis, not twelve monthly reports concatenated. Prioritize material change, cross-month trajectories, escalation/de-escalation, political/economic/technological shifts, turning points, and meaningful differences between sources. Do not count labels, channels, or publication frequency; reproduce Monthly briefs; add advice, moral judgments, filler, or unsupported causal links. A Major Trend may have multiple independent channels overall while a factual subclaim within it is supported by only one persisted source; attribute that narrow claim to the named source rather than presenting it as independently established. Apply this precise attribution to allegations, casualty numbers, settlement amounts, predictions, disputed statistics, financial interpretations, causal claims, and other source-specific claims. Precise attribution is sufficient; do not add repetitive skeptical boilerplate. Thin legacy months have deliberately supplied no factual evidence and must not become claims. The Year in Brief may summarize only topics already represented in a preceding rendered section. Keep the timeline chronological and concise; describe narrow partial-month evidence as late/early month rather than implying full-month coverage. For a partial audit, 1,700-2,300 recipient-facing words is reasonable; reserve 2,500-3,500 guidance for a substantially complete rich year and never pad."""
    # Titles help the model select direct evidence. URLs remain excluded from model
    # input and authoritative only in the renderer registry.
    source_metadata = [{key: row[key] for key in ("ref_id", "month", "source", "source_date", "title", "evidence_quality")} for row in registry]
    user = "Annual Monthly evidence (JSON):\n" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if coverage is not None:
        context = {key: coverage.get(key) for key in (
            "target_year", "calendar_coverage_complete", "semantic_v2_months",
            "enriched_legacy_months", "thin_legacy_months", "missing_months",
            "enriched_legacy_source_date_coverage",
        )}
        user += "\nDeterministic coverage context (JSON):\n" + json.dumps(context, ensure_ascii=False, separators=(",", ":"))
        if not context["calendar_coverage_complete"]:
            user += ("\nPARTIAL-COVERAGE GUARD: thin legacy months contain no factual synthesis evidence. "
                     "Do not say ‘the year began’, ‘throughout the year’, ‘all year’, or ‘over the entire year’ "
                     "unless factual evidence truly spans that interval. Prefer ‘By June’, ‘From late May through "
                     "August’, ‘In the available record’, or ‘During the covered period’, as the supplied dates warrant.")
    # Keep this final for compatibility with controlled providers that parse the
    # authoritative metadata marker through the end of the prompt.
    user += "\nAuthoritative source metadata (JSON):\n" + json.dumps(source_metadata, ensure_ascii=False, separators=(",", ":"))
    return system, user


def validate_yearly_synthesis(value: Any, registry: list[dict[str, Any]], diagnostics: MutableMapping[str, Any] | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise YearlySynthesisError("Yearly synthesis is not a JSON object")
    if "http://" in json.dumps(value).lower() or "https://" in json.dumps(value).lower():
        raise YearlySynthesisError("model output contains a URL")
    by_ref = {row["ref_id"]: row for row in registry}
    used: list[str] = []

    def section(name: str, *, required: bool = True) -> list[dict[str, Any]]:
        values = value.get(name, [] if not required else None)
        if not isinstance(values, list) or (required and not values):
            raise YearlySynthesisError(f"invalid {name}")
        result = []
        label_key = "period" if name == "timeline" else "heading"
        for entry in values:
            if not isinstance(entry, dict) or not _text(entry.get(label_key)) or not _text(entry.get("synthesis")):
                raise YearlySynthesisError(f"invalid entry in {name}")
            refs = entry.get("source_refs")
            if not isinstance(refs, list) or not refs or any(not isinstance(ref, str) or ref not in by_ref for ref in refs):
                raise YearlySynthesisError(f"unknown or missing source reference in {name}")
            refs = list(dict.fromkeys(refs))
            if name in {"executive_summary", "annual_trends"} and all(by_ref[ref]["evidence_quality"] == "thin_legacy" for ref in refs):
                raise YearlySynthesisError(f"thin legacy evidence solely supports {name}")
            result.append({label_key: _text(entry[label_key], 300), "synthesis": _text(entry["synthesis"], 5000), "source_refs": refs})
            used.extend(refs)
        return result

    executive = section("executive_summary")
    trends_returned = section("annual_trends", required=False)
    turning_returned = section("turning_points", required=False)
    notable_returned = section("notable_developments", required=False)
    timeline = section("timeline")
    retained, reclassified = [], []
    for trend in trends_returned:
        rows = [by_ref[ref] for ref in trend["source_refs"]]
        if len({row["month"] for row in rows}) >= 2 and len({row["source"] for row in rows}) >= 2:
            retained.append(trend)
        else:
            reclassified.append(trend)
    # Keep model-returned Notables ahead of later reclassifications when applying
    # the recipient-facing cap.
    notable = list(notable_returned)
    reclassified_notable: list[dict[str, Any]] = list(reclassified)
    dropped_overlap = 0
    turning: list[dict[str, Any]] = []
    reclassified_turning: list[dict[str, Any]] = []
    for item in turning_returned:
        item_refs = set(item["source_refs"])
        if any(item_refs <= set(trend["source_refs"]) or
               (len(item_refs & set(trend["source_refs"])) / len(item_refs) >= .8)
               for trend in retained):
            dropped_overlap += 1
            continue
        channels = {by_ref[ref]["source"] for ref in item["source_refs"]}
        if len(channels) == 1:
            item["source_attribution"] = next(iter(channels))
            reclassified_turning.append(item)
            reclassified_notable.append(item)
        else:
            turning.append(item)
    notable_before_overlap = notable + reclassified_notable
    notable = [item for item in notable_before_overlap if not _obviously_overlaps_trend(item, retained)]
    notable_dropped_overlap = len(notable_before_overlap) - len(notable)
    notable = notable[:NOTABLE_DEVELOPMENT_LIMIT]
    single_source_attributed = 0
    for item in notable:
        channels = {by_ref[ref]["source"] for ref in item["source_refs"]}
        if len(channels) == 1:
            item["source_attribution"] = next(iter(channels))
            single_source_attributed += 1
    brief = _text(value.get("year_in_brief"), 5000)
    if not brief:
        raise YearlySynthesisError("invalid year_in_brief")
    authoritative_union = list(dict.fromkeys(ref for group in (executive, retained, turning, notable, timeline) for item in group for ref in item["source_refs"]))
    out = {"executive_summary": executive, "annual_trends": retained, "turning_points": turning,
           "notable_developments": notable, "timeline": timeline, "year_in_brief": brief, "source_refs_used": authoritative_union}
    if diagnostics is not None:
        diagnostics.update({"annual_trends_returned": len(trends_returned), "annual_trends_retained": len(retained),
                            "annual_trends_reclassified": len(reclassified),
                            "notable_developments_returned": len(notable_returned),
                            "notable_developments_retained": len(notable),
                            "notable_developments_dropped_overlap": notable_dropped_overlap,
                            "notable_developments_dropped_cap": max(
                                0, len(notable_before_overlap) - notable_dropped_overlap - len(notable)),
                            "turning_points_returned": len(turning_returned),
                            "turning_points_retained": len(turning),
                            "turning_points_dropped_overlap": dropped_overlap,
                            "turning_points_reclassified_to_notable": len(reclassified_turning),
                            "single_source_items_attributed": single_source_attributed})
    return out


def synthesize_yearly(payload: list[dict[str, Any]], registry: list[dict[str, Any]], language: str,
                      provider: Callable[[str, str], str], diagnostics: MutableMapping[str, Any]) -> dict[str, Any]:
    system, user = yearly_prompt(payload, registry, language, diagnostics)
    diagnostics["prompt_character_count"] = len(system) + len(user)
    diagnostics["provider_operations"] = 0
    error: Exception | None = None
    for attempt in range(2):
        diagnostics["provider_operations"] += 1
        prompt = user if attempt == 0 else user + "\nYour prior response was structurally invalid. Return corrected JSON only; obey every schema and source-ID constraint."
        try:
            raw = provider(system, prompt)
            return validate_yearly_synthesis(json.loads(raw), registry, diagnostics)
        except (ValueError, TypeError, YearlySynthesisError) as exc:
            error = exc
    raise YearlySynthesisError(f"Yearly synthesis failed after one repair: {error}") from error


def _source_links(refs: Iterable[str], by_ref: dict[str, dict[str, Any]]) -> str:
    links = []
    for ref in refs:
        row = by_ref[ref]
        month = datetime.strptime(row["month"], "%Y-%m").strftime("%b")
        links.append(f"[{month} · {row['local_ref']}]({row['url']})")
    return " · ".join(links)


def render_yearly(target_year: int, synthesis: dict[str, Any], registry: list[dict[str, Any]], diagnostics: dict[str, Any], *, partial_audit: bool) -> str:
    by_ref = {row["ref_id"]: row for row in registry}
    def month_names(months: Iterable[str]) -> str:
        return ", ".join(datetime.strptime(month, "%Y-%m").strftime("%B") for month in months) or "none"

    detailed = month_names(diagnostics["semantic_v2_months"])
    archive = month_names(diagnostics["thin_legacy_months"])
    missing = month_names(diagnostics["missing_months"])
    coverage_parts = [f"Detailed Monthly evidence is available for {detailed}."]
    for month in diagnostics["enriched_legacy_months"]:
        dates = diagnostics.get("enriched_legacy_source_date_coverage", {}).get(month, {})
        if dates.get("earliest") and dates.get("latest"):
            start, end = datetime.fromisoformat(dates["earliest"]), datetime.fromisoformat(dates["latest"])
            coverage_parts.append(f"{start.strftime('%B')} has partial detailed coverage from {start.day}–{end.day} {start.strftime('%B')}.")
        else:
            coverage_parts.append(f"{month_names([month])} has partial detailed coverage.")
    if diagnostics["thin_legacy_months"]:
        coverage_parts.append(f"{archive} are represented only by archive-level records and are therefore not used as factual foundations.")
    if diagnostics["missing_months"]:
        coverage_parts.append(f"{missing} are not yet available.")
    coverage_parts.append("This is a partial-year audit." if partial_audit else "Calendar coverage is complete.")
    diagnostics["partial_coverage_guard_applied"] = bool(partial_audit)
    lines = [f"# The Cyberlurch Year in Review — {target_year}", "", "**Year in Review**", "", "---", "", "## Coverage Note", "",
             " ".join(coverage_parts), ""]

    def add_section(title: str, values: list[dict[str, Any]], label: str, *, optional: bool = False) -> None:
        if optional and not values:
            return
        lines.extend(["---", "", f"## {title}", ""])
        for item in values:
            if item.get("source_attribution"):
                lines.extend([f"### {item[label]}", "", f"*Source-specific reporting: {item['source_attribution']}*", "",
                              item["synthesis"], "", f"_Sources: {_source_links(item['source_refs'], by_ref)}_", ""])
            else:
                lines.extend([f"### {item[label]}", "", item["synthesis"], "", f"_Sources: {_source_links(item['source_refs'], by_ref)}_", ""])

    add_section("Executive Brief", synthesis["executive_summary"], "heading")
    add_section("Major Trends", synthesis["annual_trends"], "heading")
    add_section("Turning Points", synthesis["turning_points"], "heading", optional=True)
    add_section("Notable Developments", synthesis.get("notable_developments", []), "heading", optional=True)
    add_section("The Year in Motion", synthesis["timeline"], "period")
    lines.extend(["---", "", "## Year in Brief", "", synthesis["year_in_brief"], "", "---", "", "## Source Index", ""])
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for ref in synthesis["source_refs_used"]:
        grouped[by_ref[ref]["source"]].append(by_ref[ref])
    for source in sorted(grouped, key=str.casefold):
        rows = sorted(grouped[source], key=lambda row: (row["source_date"], row["ref_id"]))
        lines.extend([f"**{source}**", _source_links([row["ref_id"] for row in rows], by_ref), ""])
    return "\n".join(lines).rstrip() + "\n"
