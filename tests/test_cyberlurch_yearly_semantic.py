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
    assert not result["annual_trends"] and len(result["turning_points"]) == 2
    assert result["source_refs_used"] == refs[:1]


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
