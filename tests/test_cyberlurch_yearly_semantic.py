from __future__ import annotations

import json

import pytest

from newsagent2.cyberlurch_yearly import (
    YearlySynthesisError,
    build_annual_evidence,
    classify_monthly_rollup,
    render_yearly,
    synthesize_yearly,
    validate_yearly_synthesis,
    yearly_prompt,
)


def _semantic(month: str, source: str, url: str, local_ref: str = "SRC 01/01"):
    return {
        "month": month, "schema": "cyberlurch-monthly-semantic-v2",
        "executive_summary": ["Persisted monthly synthesis"], "month_in_brief": "A substantive month.",
        "themes": [{"heading": "Theme", "synthesis": "Persisted semantic evidence.", "scope": "cross_source", "source_refs": [local_ref]}],
        "notable_developments": [],
        "source_registry": [{"ref_id": local_ref, "source": source, "source_date": month + "-01", "title": "Persisted title", "url": url}],
    }


def _output(refs, trend_refs=None):
    return {
        "executive_summary": [{"heading": "Brief", "synthesis": "A supported claim.", "source_refs": refs[:1]}],
        "annual_trends": [{"heading": "Trend", "synthesis": "A supported trend.", "source_refs": trend_refs or refs}],
        "turning_points": [{"heading": "Turn", "synthesis": "A supported event.", "source_refs": refs[:1]}],
        "notable_developments": [],
        "timeline": [{"period": "Mid-year", "synthesis": "A supported trajectory.", "source_refs": refs[:1]}],
        "year_in_brief": "The evidence records material changes during the year.",
        "source_refs_used": ["ignored"],
    }


def test_quality_payload_namespacing_and_legacy_safeguards():
    thin = {"month": "2026-01", "top_items": [{"title": "Sensational", "url": "https://thin", "channel": "Thin"}]}
    enriched = {"month": "2026-05", "full_text_count": 1, "top_items": [{"title": "Useful", "url": "https://legacy", "channel": "Legacy", "date": "2026-05-29", "bottom_line": "Persisted factual detail."}]}
    months = [thin, enriched, _semantic("2026-06", "Alpha", "https://june"), _semantic("2026-07", "Beta", "https://july")]
    payload, registry, diag = build_annual_evidence(months, 2026)
    assert classify_monthly_rollup(thin) == "thin_legacy"
    assert classify_monthly_rollup(enriched) == "enriched_legacy"
    assert [row["evidence_quality"] for row in payload] == ["enriched_legacy", "semantic_v2", "semantic_v2"]
    assert "2026-01" not in {row["month"] for row in registry}
    semantic_refs = [row["ref_id"] for row in registry if row["evidence_quality"] == "semantic_v2"]
    assert semantic_refs == ["2026-06::SRC 01/01", "2026-07::SRC 01/01"]
    assert diag["enriched_legacy_source_date_coverage"]["2026-05"] == {"earliest": "2026-05-29", "latest": "2026-05-29"}


def test_validation_reclassifies_single_month_and_derives_union_without_retry():
    _, registry, _ = build_annual_evidence([_semantic("2026-06", "Alpha", "https://a"), _semantic("2026-07", "Beta", "https://b")], 2026)
    refs = [row["ref_id"] for row in registry]
    value = _output(refs, trend_refs=refs[:1])
    calls, diagnostics = [], {}
    result = synthesize_yearly([], registry, "en", lambda *_: calls.append(1) or json.dumps(value), diagnostics)
    assert len(calls) == diagnostics["provider_operations"] == 1
    assert not result["annual_trends"] and len(result["notable_developments"]) == 2
    assert result["source_refs_used"] == refs[:1]


def test_partial_coverage_context_and_temporal_guard_reach_prompt():
    diagnostics = {
        "target_year": 2026, "calendar_coverage_complete": False,
        "semantic_v2_months": ["2026-06", "2026-07", "2026-08"],
        "enriched_legacy_months": ["2026-05"],
        "thin_legacy_months": ["2026-01", "2026-02", "2026-03", "2026-04"],
        "missing_months": ["2026-09"],
        "enriched_legacy_source_date_coverage": {
            "2026-05": {"earliest": "2026-05-26", "latest": "2026-05-29"},
        },
    }
    _, prompt = yearly_prompt([], [], "en", diagnostics)
    assert '"target_year":2026' in prompt and '"earliest":"2026-05-26"' in prompt
    assert "thin legacy months contain no factual synthesis evidence" in prompt
    assert "Do not say ‘the year began’" in prompt


def test_overlap_is_dropped_without_retry_and_single_source_moves_to_notable():
    _, registry, _ = build_annual_evidence([
        _semantic("2026-06", "Alpha", "https://a", "A"),
        _semantic("2026-07", "Beta", "https://b", "B"),
        _semantic("2026-08", "Canonical Source", "https://c", "C"),
    ], 2026)
    refs = [row["ref_id"] for row in registry]
    value = _output(refs, trend_refs=refs[:2])
    value["turning_points"] = [
        {"heading": "Duplicate", "synthesis": "Repeats the trend.", "source_refs": refs[:2]},
        {"heading": "El Niño risk", "synthesis": "A material standalone food-system risk.", "source_refs": refs[2:]},
    ]
    calls, diagnostics = [], {}
    result = synthesize_yearly([], registry, "en", lambda *_: calls.append(1) or json.dumps(value), diagnostics)
    assert len(calls) == 1 and not result["turning_points"]
    assert [item["heading"] for item in result["notable_developments"]] == ["El Niño risk"]
    assert result["notable_developments"][0]["source_attribution"] == "Canonical Source"
    assert diagnostics["turning_points_dropped_overlap"] == 1
    assert diagnostics["turning_points_reclassified_to_notable"] == 1


def test_human_coverage_note_and_notable_rendering():
    months = [
        {"month": "2026-01", "top_items": [{"title": "Archive"}]},
        {"month": "2026-05", "full_text_count": 1, "top_items": [
            {"title": "Late May", "url": "https://may", "channel": "May Source",
             "date": "2026-05-26", "bottom_line": "Material detail."},
            {"title": "Later May", "url": "https://may2", "channel": "May Source",
             "date": "2026-05-29", "bottom_line": "More material detail."},
        ]},
        _semantic("2026-06", "Alpha", "https://june"),
    ]
    _, registry, diagnostics = build_annual_evidence(months, 2026)
    ref = registry[-1]["ref_id"]
    synthesis = {
        "executive_summary": [], "annual_trends": [], "turning_points": [],
        "notable_developments": [{"heading": "Standalone", "synthesis": "Material development.",
                                  "source_refs": [ref], "source_attribution": "Alpha"}],
        "timeline": [], "year_in_brief": "Material development.", "source_refs_used": [ref],
    }
    rendered = render_yearly(2026, synthesis, registry, diagnostics, partial_audit=True)
    coverage_note = rendered.split("## Coverage Note", 1)[1].split("---", 1)[0]
    assert "26–29 May" in coverage_note and "archive-level records" in coverage_note
    assert "Semantic-v2" not in coverage_note and "enriched legacy" not in coverage_note
    assert "## Notable Developments" in rendered
    assert "Source-specific reporting: Alpha" in rendered


def test_empty_optional_sections_are_suppressed_but_nonempty_turning_points_render():
    _, registry, diagnostics = build_annual_evidence([
        _semantic("2026-06", "Alpha", "https://a", "A"),
        _semantic("2026-07", "Beta", "https://b", "B"),
    ], 2026)
    refs = [row["ref_id"] for row in registry]
    synthesis = validate_yearly_synthesis(_output(refs), registry)
    synthesis["turning_points"] = [{"heading": "Turn", "synthesis": "A supported event.",
                                     "source_refs": refs[1:]}]
    empty_render = render_yearly(2026, {**synthesis, "turning_points": [], "notable_developments": []},
                                 registry, diagnostics, partial_audit=True)
    assert "## Turning Points" not in empty_render
    assert "## Notable Developments" not in empty_render
    assert "## Major Trends" in empty_render and "## The Year in Motion" in empty_render

    nonempty_render = render_yearly(2026, synthesis, registry, diagnostics, partial_audit=True)
    assert "## Turning Points" in nonempty_render
    assert "### Turn" in nonempty_render


def test_final_notable_cap_prefers_original_items_after_all_reclassification():
    months = [_semantic(f"2026-{month:02d}", f"Source {month}", f"https://{month}", f"R{month}")
              for month in range(1, 10)]
    _, registry, _ = build_annual_evidence(months, 2026)
    refs = [row["ref_id"] for row in registry]
    value = _output(refs, trend_refs=refs[:2])
    value["notable_developments"] = [
        {"heading": f"Original {number}", "synthesis": f"Distinct development number {number}.",
         "source_refs": [refs[number + 1]]}
        for number in range(6)
    ]
    value["annual_trends"].append(
        {"heading": "Narrow candidate", "synthesis": "A separate narrow candidate.", "source_refs": [refs[8]]}
    )
    diagnostics = {}
    result = validate_yearly_synthesis(value, registry, diagnostics)
    assert [item["heading"] for item in result["notable_developments"]] == [f"Original {n}" for n in range(6)]
    assert diagnostics["notable_developments_retained"] == 6
    assert diagnostics["notable_developments_dropped_cap"] == 1


def test_obvious_notable_trend_overlap_drops_but_distinct_subject_survives():
    _, registry, _ = build_annual_evidence([
        _semantic("2026-06", "Alpha", "https://a", "A"),
        _semantic("2026-07", "Beta", "https://b", "B"),
        _semantic("2026-08", "Gamma", "https://c", "C"),
    ], 2026)
    refs = [row["ref_id"] for row in registry]
    value = _output(refs, trend_refs=refs[:2])
    value["annual_trends"][0].update({
        "heading": "Migration and social policy strains",
        "synthesis": "Migration pressures intensified social policy strains across Europe.",
    })
    value["notable_developments"] = [
        {"heading": "Migration social policy strains", "synthesis": "Migration pressures intensified social policy strains.",
         "source_refs": refs[:2]},
        {"heading": "Independent technology launch", "synthesis": "A satellite launch created distinct technical capacity.",
         "source_refs": refs[2:]},
    ]
    diagnostics = {}
    result = validate_yearly_synthesis(value, registry, diagnostics)
    assert [item["heading"] for item in result["notable_developments"]] == ["Independent technology launch"]
    assert diagnostics["notable_developments_dropped_overlap"] == 1


def test_source_attribution_precedes_synthesis_and_uses_registry_only():
    _, registry, diagnostics = build_annual_evidence([
        _semantic("2026-06", "Registry Alpha", "https://a", "A"),
        _semantic("2026-07", "Registry Beta", "https://b", "B"),
    ], 2026)
    refs = [row["ref_id"] for row in registry]
    value = _output(refs)
    value["notable_developments"] = [
        {"heading": "Single", "synthesis": "Single-source factual claim.", "source_refs": refs[:1],
         "source_attribution": "Invented Name"},
        {"heading": "Multiple", "synthesis": "Multi-source factual claim.", "source_refs": refs},
    ]
    result = validate_yearly_synthesis(value, registry)
    rendered = render_yearly(2026, result, registry, diagnostics, partial_audit=True)
    assert rendered.index("*Source-specific reporting: Registry Alpha*") < rendered.index("Single-source factual claim.")
    assert "Invented Name" not in rendered
    multiple = rendered.split("### Multiple", 1)[1].split("## The Year in Motion", 1)[0]
    assert "Source-specific reporting" not in multiple


def test_trend_requires_two_months_and_two_channels():
    months = [_semantic("2026-06", "Same", "https://a", "A"), _semantic("2026-07", "Same", "https://b", "B")]
    _, registry, _ = build_annual_evidence(months, 2026)
    result = validate_yearly_synthesis(_output([row["ref_id"] for row in registry]), registry)
    assert not result["annual_trends"]


def test_urls_are_authoritative_and_model_urls_are_rejected():
    _, registry, diag = build_annual_evidence([_semantic("2026-06", "Alpha", "https://persisted/a"), _semantic("2026-07", "Beta", "https://persisted/b")], 2026)
    refs = [row["ref_id"] for row in registry]
    valid = validate_yearly_synthesis(_output(refs), registry)
    rendered = render_yearly(2026, valid, registry, diag, partial_audit=True)
    assert "https://persisted/a" in rendered and "https://persisted/b" in rendered
    poisoned = _output(refs); poisoned["year_in_brief"] = "Visit https://invented.invalid"
    with pytest.raises(YearlySynthesisError, match="URL"):
        validate_yearly_synthesis(poisoned, registry)


def test_unrecoverable_output_gets_only_one_repair():
    calls = []
    with pytest.raises(YearlySynthesisError):
        synthesize_yearly([], [], "en", lambda *_: calls.append(1) or "not json", {})
    assert len(calls) == 2
