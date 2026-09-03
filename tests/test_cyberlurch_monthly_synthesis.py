from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from newsagent2.cyberlurch_monthly import (
    MonthlySynthesisError, build_source_registry, render_monthly,
    monthly_prompt, select_evidence_pack, source_abbreviations,
    synthesize_monthly, validate_synthesis,
)


def item(channel, day, ident, title="Title", topic="wrong-label"):
    return {"channel":channel, "published_at":datetime(2026, 8, day, tzinfo=timezone.utc),
            "id":ident, "title":title, "url":f"https://www.youtube.com/watch?v={ident}",
            "bottom_line":f"{channel} reported persisted fact {ident}.", "topic_primary":topic}


def valid(registry):
    refs = [row["ref_id"] for row in registry]
    return {"executive_summary":[{"heading":"Shared coverage", "synthesis":"Coverage developed across the month.", "source_refs":[refs[0], refs[1]]}],
            "trends":[{"heading":"Semantically regrouped theme", "scope":"cross_source", "synthesis":"Alpha News and Alpine Network returned to a shared development.", "source_refs":refs[:2]}],
            "notable_developments":[{"heading":"Isolated event", "synthesis":"Canadian Prepper recorded a separate event.", "source_refs":[refs[-1]]}],
            "month_in_brief":"The month combined recurring coverage with one isolated event.", "source_refs_used":refs}


def test_deterministic_ids_collisions_and_same_day_suffixes():
    rows = [item("Alpha News", 8, "b"), item("Alpine Network", 8, "c"), item("Canadian Prepper", 8, "z"), item("Canadian Prepper", 8, "a")]
    first = build_source_registry(rows); second = build_source_registry(reversed(rows))
    assert [r["ref_id"] for r in first] == [r["ref_id"] for r in second]
    assert len(set(source_abbreviations(["Alpha News", "Alpine Network"]).values())) == 2
    cp = [r["ref_id"] for r in first if r["source"] == "Canadian Prepper"]
    assert cp == ["CP 08/08-A", "CP 08/08-B"]


def test_citations_urls_attribution_and_semantic_theme_are_guarded():
    registry = build_source_registry([item("Alpha News", 2, "a"), item("Alpine Network", 3, "b"), item("Canadian Prepper", 4, "c")])
    synthesis = validate_synthesis(valid(registry), registry)
    report = render_monthly("Monthly", synthesis, registry)
    assert "Semantically regrouped theme" in report and "wrong-label" not in report
    assert "the speaker" not in report.lower()
    assert "example.invalid" not in report
    for ref in synthesis["source_refs_used"]: assert f"[{ref}](https://" in report
    assert "— <https://" not in report
    bad = valid(registry); bad["trends"][0]["source_refs"] = ["FAKE 01/01"]; bad["source_refs_used"] = ["FAKE 01/01", registry[-1]["ref_id"]]
    with pytest.raises(MonthlySynthesisError): validate_synthesis(bad, registry)
    bad = valid(registry); bad["month_in_brief"] = "See https://invented.invalid"
    with pytest.raises(MonthlySynthesisError): validate_synthesis(bad, registry)


def test_one_call_normal_path_retry_only_on_invalid_and_no_false_single_item_trend():
    registry = build_source_registry([item("Alpha News", 2, "a"), item("Alpine Network", 3, "b"), item("Canadian Prepper", 4, "c")])
    calls=[]
    result=synthesize_monthly(registry,"en",lambda *_: calls.append(1) or json.dumps(valid(registry)))
    assert len(calls)==1 and len(result["trends"][0]["source_refs"]) >= 2
    invalid=valid(registry); invalid["trends"][0]["source_refs"]=[registry[0]["ref_id"]]; invalid["source_refs_used"]=[registry[0]["ref_id"],registry[-1]["ref_id"]]
    with pytest.raises(MonthlySynthesisError): validate_synthesis(invalid,registry)


def test_prompt_enforces_one_language_and_identified_sources():
    registry=build_source_registry([item("Alpha News",2,"a"),item("Alpine Network",3,"b")])
    seen={}
    def call(system,user): seen.update(system=system,user=user); return json.dumps({**valid(registry),"notable_developments":[],"source_refs_used":[r["ref_id"] for r in registry]})
    synthesize_monthly(registry,"en",call)
    assert "all narrative prose and headings in English" in seen["system"]
    assert "Topic fields are fallible hints" in seen["system"]
    assert "generic 'the speaker'" in seen["system"]
    assert "Executive entries use 1-4 representative citations." in seen["system"]


def test_citation_presentation_is_normalized_without_retry():
    registry = build_source_registry([
        item("Alpha News", 2, "a"), item("Alpine Network", 3, "b"),
        item("Canadian Prepper", 4, "c"), item("Delta Report", 5, "d"),
        item("Echo Report", 6, "e"), item("Foxtrot Report", 7, "f"),
    ])
    refs = [row["ref_id"] for row in registry]
    synthesis = valid(registry)
    synthesis["executive_summary"][0]["source_refs"] = [refs[0], refs[0], *refs[1:]]
    synthesis["notable_developments"][0]["source_refs"] = [refs[-1], refs[-1], refs[2], refs[3]]
    synthesis["source_refs_used"] = ["a redundant model field is ignored"]
    calls = []

    result = synthesize_monthly(
        registry, "en", lambda *_: calls.append(1) or json.dumps(synthesis)
    )

    assert calls == [1]
    assert result["executive_summary"][0]["source_refs"] == refs[:4]
    assert result["notable_developments"][0]["source_refs"] == [refs[-1], refs[2]]
    assert result["source_refs_used"] == list(dict.fromkeys(
        ref for section in result["executive_summary"] + result["trends"] + result["notable_developments"]
        for ref in section["source_refs"]
    ))


def test_single_source_executive_entry_is_valid():
    registry = build_source_registry([
        item("Alpha News", 2, "a"), item("Alpine Network", 3, "b"),
        item("Canadian Prepper", 4, "c"),
    ])
    synthesis = valid(registry)
    synthesis["executive_summary"][0]["source_refs"] = [registry[0]["ref_id"]]
    assert validate_synthesis(synthesis, registry)["executive_summary"][0]["source_refs"] == [registry[0]["ref_id"]]


def test_evidence_pack_bounds_diversifies_and_is_deterministic():
    rows = []
    for index in range(430):
        channel = "Flood Channel" if index < 360 else f"Channel {index % 12}"
        rows.append(item(channel, index % 28 + 1, f"id-{index}"))
    registry = build_source_registry(rows)
    first = select_evidence_pack(registry)
    second = select_evidence_pack(list(reversed(registry)))
    assert len(first) <= 80
    assert [row["ref_id"] for row in first] == [row["ref_id"] for row in second]
    counts = {channel: sum(row["source"] == channel for row in first) for channel in {row["source"] for row in first}}
    assert len(counts) > 1
    assert counts["Flood Channel"] < len(first) / 2
    assert {int(row["source_date"][8:10]) // 7 for row in first} >= {0, 1, 2, 3}


def test_useful_same_source_repeats_and_prompt_is_minimal():
    registry = build_source_registry([item("Trend Source", day, f"trend-{day}") for day in range(1, 13)])
    evidence = select_evidence_pack(registry)
    assert sum(row["source"] == "Trend Source" for row in evidence) > 2
    _, prompt = monthly_prompt(evidence, "en")
    assert '"url"' not in prompt
    assert '"_deep_dive_score"' not in prompt
    assert '"factual_summary"' in prompt


def test_compact_persisted_supporting_details_reach_evidence_prompt():
    row = item("Riks", 8, "detail")
    row.update({
        "transcript_key_points": ["Lottery sales on credit were alleged.", "Debt collection against elderly buyers was alleged."],
        "transcript_notable_claims": ["Public bookings were alleged to benefit associated facilities."],
        "important_details": ["Union-member consent was disputed.", "This fifth point must be omitted."],
    })
    registry = build_source_registry([row])
    assert len(registry[0]["supporting_details"]) == 4
    _, prompt = monthly_prompt(registry, "en")
    assert "Lottery sales on credit" in prompt
    assert "fifth point" not in prompt
    assert len(registry[0]["factual_summary"]) <= 1200
    assert sum(map(len, registry[0]["supporting_details"])) <= 700


def test_trend_scope_channel_and_notable_rules_are_validated():
    registry = build_source_registry([
        item("Alpha News", 2, "a"), item("Alpine Network", 3, "b"),
        item("Canadian Prepper", 4, "c"), item("Canadian Prepper", 9, "d"), item("Canadian Prepper", 18, "e"),
    ])
    base = valid(registry)
    base["source_refs_used"] = list(dict.fromkeys(ref for section in base["executive_summary"] + base["trends"] + base["notable_developments"] for ref in section["source_refs"]))
    assert validate_synthesis(base, registry)
    one_channel = json.loads(json.dumps(base))
    one_channel["trends"][0]["source_refs"] = [registry[2]["ref_id"], registry[3]["ref_id"]]
    one_channel["source_refs_used"] = list(dict.fromkeys(ref for section in one_channel["executive_summary"] + one_channel["trends"] + one_channel["notable_developments"] for ref in section["source_refs"]))
    with pytest.raises(MonthlySynthesisError, match="distinct channels"):
        validate_synthesis(one_channel, registry)
    specific = json.loads(json.dumps(base))
    cp_refs = [r["ref_id"] for r in registry if r["source"] == "Canadian Prepper"]
    specific["trends"] = [{"heading":"Canadian Prepper coverage changed", "synthesis":"Canadian Prepper moved toward continuity coverage.", "scope":"source_specific", "source":"Canadian Prepper", "source_refs":cp_refs}]
    specific["notable_developments"] = []
    specific["source_refs_used"] = list(dict.fromkeys(specific["executive_summary"][0]["source_refs"] + cp_refs))
    assert validate_synthesis(specific, registry)
    specific["trends"][0]["heading"] = "Preparedness coverage changed"
    specific["trends"][0]["synthesis"] = "Coverage moved toward continuity measures."
    with pytest.raises(MonthlySynthesisError, match="explicitly name"):
        validate_synthesis(specific, registry)
    too_many = json.loads(json.dumps(base)); too_many["notable_developments"] *= 4
    with pytest.raises(MonthlySynthesisError, match="maximum of three"):
        validate_synthesis(too_many, registry)


def test_unrelated_cross_source_one_offs_fail_topic_coherence():
    rows = [item("Flood News", 2, "flood", topic="flood"), item("Wine News", 3, "wine", topic="harvest")]
    registry = build_source_registry(rows)
    synthesis = valid(registry)
    with pytest.raises(MonthlySynthesisError, match="no shared persisted topic"):
        validate_synthesis(synthesis, registry)


def test_large_synthesis_uses_selected_provenance_and_one_call():
    registry = build_source_registry([item(f"Source {index % 15}", index % 28 + 1, f"large-{index}") for index in range(405)])
    evidence = select_evidence_pack(registry)
    calls = []
    diagnostics = {}
    result = synthesize_monthly(
        registry, "en",
        lambda *_: calls.append(1) or json.dumps(valid(evidence[:3])),
        diagnostics,
    )
    assert len(calls) == diagnostics["monthly_provider_operations"] == 1
    assert diagnostics["monthly_evidence_items_selected"] <= 80
    assert diagnostics["monthly_collection_operations"] == 0
    assert set(result["source_refs_used"]) <= {row["ref_id"] for row in evidence}
    invented = valid(evidence)
    invented["month_in_brief"] = "Invented https://example.invalid"
    with pytest.raises(MonthlySynthesisError):
        validate_synthesis(invented, evidence)
