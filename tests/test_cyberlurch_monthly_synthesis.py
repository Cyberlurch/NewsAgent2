from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from newsagent2.cyberlurch_monthly import (
    MONTHLY_CHANNEL_SOFT_CAP, MONTHLY_EVIDENCE_MAX, MONTHLY_EVIDENCE_TARGET,
    MonthlySynthesisError, build_source_registry, canonical_sources_for_refs, render_monthly,
    monthly_prompt, select_evidence_pack, source_abbreviations,
    synthesize_monthly, validate_synthesis,
)


def item(channel, day, ident, title="Title", topic="wrong-label"):
    return {"channel":channel, "published_at":datetime(2026, 8, day, tzinfo=timezone.utc),
            "id":ident, "title":title, "url":f"https://www.youtube.com/watch?v={ident}",
            "bottom_line":f"{channel} reported persisted fact {ident}.", "topic_primary":topic}


def valid(registry):
    refs = [row["ref_id"] for row in registry]
    return {"executive_summary":[
                {"heading":"Shared coverage", "synthesis":"Coverage developed across the month.", "source_refs":[refs[0], refs[1]]},
                {"heading":"Material shift", "synthesis":"A second material development shaped the month.", "source_refs":[refs[0], refs[1]]},
                {"heading":"Continuing development", "synthesis":"The recurring development remained consequential.", "source_refs":[refs[0], refs[1]]},
            ],
            "trends":[
                {"heading":"Semantically regrouped theme", "scope":"cross_source", "synthesis":"Alpha News and Alpine Network returned to a shared development.", "source_refs":refs[:2]},
                {"heading":"Continuing shared theme", "scope":"cross_source", "synthesis":"Alpha News and Alpine Network also documented a continuing development.", "source_refs":refs[:2]},
            ],
            "notable_developments":[{"heading":"Isolated event", "synthesis":f"{registry[-1]['source']} recorded a separate event.", "source_refs":[refs[-1]]}],
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
    report = render_monthly("Monthly", synthesis, registry, "2026-08")
    assert "Semantically regrouped theme" in report and "wrong-label" not in report
    assert "the speaker" not in report.lower()
    assert "example.invalid" not in report
    for ref in synthesis["source_refs_used"]: assert f"[{ref}](https://" in report
    assert "— <https://" not in report
    assert "**Monthly — August 2026**" in report
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
    assert "3-4 entries" in seen["system"]
    assert "Normally include at most one source_specific trend" in seen["system"]


def test_stockholm_boundary_and_authoritative_period_source_index_rendering():
    boundary = item("Canadian Prepper", 1, "boundary", title="A very long YouTube title")
    boundary["published_at"] = "2026-07-31T22:00:17+00:00"
    other = item("Canadian Prepper", 21, "other", title="Another full title")
    uncited = item("Unused Channel", 9, "unused", title="Must not appear")
    registry = build_source_registry([boundary, other, uncited])
    cp = [row for row in registry if row["source"] == "Canadian Prepper"]
    assert cp[0]["source_date"] == "2026-08-01"
    assert cp[0]["ref_id"] == "CP 01/08"
    synthesis = {
        "executive_summary": [
            {"heading": f"Event {n}", "synthesis": "A material development mattered.", "source_refs": [cp[0]["ref_id"]]}
            for n in range(1, 4)
        ],
        "trends": [{"heading": "Development", "scope": "source_specific", "source": "Canadian Prepper",
                    "synthesis": "Canadian Prepper documented a material change.",
                    "source_refs": [cp[0]["ref_id"], cp[1]["ref_id"], cp[0]["ref_id"]]}],
        "notable_developments": [], "month_in_brief": "Material events shaped the month.",
        "source_refs_used": [cp[0]["ref_id"], cp[1]["ref_id"]],
    }
    report = render_monthly("Monthly", synthesis, registry, "2026-08")
    assert "Monthly — August 2026" in report and "Monthly — July 2026" not in report
    index = report.split("## Source Index", 1)[1]
    assert index.count("**Canadian Prepper (CP)**") == 1
    assert "A very long YouTube title" not in index and "Another full title" not in index
    assert "Unused Channel" not in index and "Must not appear" not in index
    assert f"[01/08]({cp[0]['url']})" in index and f"[21/08]({cp[1]['url']})" in index
    trend = report.split("### 1. Development", 1)[1].split("## Month in Brief", 1)[0]
    assert "documented a material change.\n\n_Sources:" in trend


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
    one_channel["trends"].append(json.loads(json.dumps(one_channel["trends"][1])))
    one_channel["trends"][-1]["heading"] = "Third valid development"
    one_channel["trends"][0]["source_refs"] = [registry[2]["ref_id"], registry[3]["ref_id"]]
    one_channel["source_refs_used"] = list(dict.fromkeys(ref for section in one_channel["executive_summary"] + one_channel["trends"] + one_channel["notable_developments"] for ref in section["source_refs"]))
    normalized = validate_synthesis(one_channel, registry)
    assert len(normalized["trends"]) == 2
    specific = json.loads(json.dumps(base))
    cp_refs = [r["ref_id"] for r in registry if r["source"] == "Canadian Prepper"]
    specific["trends"] = base["trends"][:1] + [{"heading":"Canadian Prepper coverage changed", "synthesis":"Canadian Prepper moved toward continuity coverage.", "scope":"source_specific", "source":"Canadian Prepper", "source_refs":cp_refs}]
    specific["notable_developments"] = []
    specific["source_refs_used"] = list(dict.fromkeys(specific["executive_summary"][0]["source_refs"] + cp_refs))
    assert validate_synthesis(specific, registry)
    specific["trends"].append(json.loads(json.dumps(base["trends"][1])))
    specific["trends"][1]["heading"] = "Preparedness coverage changed"
    specific["trends"][1]["synthesis"] = "Coverage moved toward continuity measures."
    normalized_specific = validate_synthesis(specific, registry)
    assert len(normalized_specific["trends"]) == 3
    assert normalized_specific["trends"][1]["source"] == "Canadian Prepper"
    too_many = json.loads(json.dumps(base)); too_many["notable_developments"] *= 4
    assert len(validate_synthesis(too_many, registry)["notable_developments"]) == 4


def test_persisted_topic_mismatch_is_not_a_hard_failure():
    rows = [item("Flood News", 2, "flood", topic="flood"), item("Wine News", 3, "wine", topic="harvest")]
    registry = build_source_registry(rows)
    synthesis = valid(registry)
    assert len(validate_synthesis(synthesis, registry)["trends"]) == 2


def test_source_specific_trends_are_limited_when_three_cross_source_trends_exist():
    registry = build_source_registry([
        item("Alpha News", day, f"a-{day}", topic="shared") for day in (1, 2, 3)
    ] + [
        item("Beta News", day, f"b-{day}", topic="shared") for day in (4, 5, 6)
    ])
    alpha = [row["ref_id"] for row in registry if row["source"] == "Alpha News"]
    beta = [row["ref_id"] for row in registry if row["source"] == "Beta News"]
    synthesis = valid(registry)
    synthesis["trends"] = [
        {"heading": f"Shared development {n}", "scope": "cross_source",
         "synthesis": "Alpha News and Beta News described the shared development.",
         "source_refs": [alpha[n - 1], beta[n - 1]]}
        for n in range(1, 4)
    ] + [
        {"heading": f"{source} framing changed", "scope": "source_specific", "source": source,
         "synthesis": f"{source} materially changed its framing.", "source_refs": refs}
        for source, refs in (("Alpha News", alpha), ("Beta News", beta))
    ]
    result = validate_synthesis(synthesis, registry)
    assert [trend["scope"] for trend in result["trends"]] == [
        "cross_source", "cross_source", "cross_source", "source_specific"
    ]
    assert result["trends"][-1]["source"] == "Alpha News"


def test_same_channel_cross_source_is_reclassified_or_dropped_without_retry():
    registry = build_source_registry([
        item("Alpha News", day, f"a-{day}") for day in (1, 2, 3)
    ] + [item("Beta News", 4, "b")])
    alpha = [row["ref_id"] for row in registry if row["source"] == "Alpha News"]
    beta = next(row["ref_id"] for row in registry if row["source"] == "Beta News")
    synthesis = valid(registry)
    synthesis["trends"] = [
        {"heading": "Alpha News emphasis", "scope": "cross_source",
         "synthesis": "Alpha News repeatedly covered the development.", "source_refs": alpha},
        {"heading": "Insufficient pattern", "scope": "cross_source",
         "synthesis": "Two records described a possible pattern.", "source_refs": alpha[:2]},
        {"heading": "Shared pattern", "scope": "cross_source",
         "synthesis": "Alpha News and Beta News covered the development.", "source_refs": [alpha[0], beta]},
    ]
    calls, diagnostics = [], {}
    result = synthesize_monthly(
        registry, "en", lambda *_: calls.append(1) or json.dumps(synthesis), diagnostics
    )
    assert calls == [1]
    assert [trend["scope"] for trend in result["trends"]] == ["source_specific", "cross_source"]
    assert result["trends"][0]["source"] == "Alpha News"
    assert diagnostics["monthly_trends_returned"] == 3
    assert diagnostics["monthly_trends_retained"] == 2
    assert diagnostics["monthly_trends_dropped"] == 1
    assert diagnostics["monthly_trends_reclassified"] == 1
    assert diagnostics["monthly_normalization_required"] is True


def test_rich_month_density_guidance_is_soft_and_evidence_limits_are_unchanged():
    registry = build_source_registry([
        item(f"Source {index % 15}", index % 28 + 1, f"rich-{index}")
        for index in range(72)
    ])
    system, _ = monthly_prompt(registry, "en")
    assert "aim for 4-6 supported Key Trends and 2-4 material Worth Noting items" in system
    assert "approximately 1,500-2,000 recipient-facing words" in system
    assert "soft editorial guidance, not a validation requirement" in system
    assert "up to roughly 2,500 words" in system

    sparse_system, _ = monthly_prompt(registry[:49], "en")
    assert "1,500-2,000" not in sparse_system
    assert len(select_evidence_pack(build_source_registry([
        item(f"Source {index % 15}", index % 28 + 1, f"limit-{index}")
        for index in range(100)
    ]))) == 72


def test_one_record_can_support_a_trend_and_distinct_el_nino_notable():
    iran_a = item("Canadian Prepper", 22, "cp-22", title="Iran and El Nino risk assessment")
    iran_a["bottom_line"] = (
        "Canadian Prepper discussed Iran and separately argued unusually warm El Nino-region "
        "waters could increase extreme-weather and food-system disruption risks."
    )
    registry = build_source_registry([
        iran_a, item("Global Affairs", 23, "iran-2"), item("Market Desk", 24, "market")
    ])
    synthesis = valid(registry)
    shared_ref = next(row["ref_id"] for row in registry if row["source"] == "Canadian Prepper")
    other_ref = next(row["ref_id"] for row in registry if row["source"] == "Global Affairs")
    synthesis["trends"][0].update({
        "heading": "Iran safeguards dispute", "synthesis": "Canadian Prepper and Global Affairs covered the Iran safeguards dispute.",
        "source_refs": [shared_ref, other_ref],
    })
    synthesis["notable_developments"] = [{
        "heading": "El Nino food-system risk",
        "synthesis": "Canadian Prepper argued unusually warm El Nino-region waters could increase extreme-weather and food-system disruption risks.",
        "source_refs": [shared_ref],
    }]
    result = validate_synthesis(synthesis, registry)
    assert shared_ref in result["trends"][0]["source_refs"]
    assert result["notable_developments"][0]["source_refs"] == [shared_ref]


def test_single_source_notable_gets_canonical_attribution_without_literal_name():
    registry = build_source_registry([
        item("Alpha News", 1, "a"), item("Beta News", 2, "b"), item("Risk Desk", 3, "risk")
    ])
    synthesis = valid(registry)
    synthesis["notable_developments"][0]["synthesis"] = "Unusually warm waters could disrupt harvests and fertilizer supplies."
    result = validate_synthesis(synthesis, registry)
    notable_item = result["notable_developments"][0]
    assert notable_item["attribution_source"] == "Risk Desk"
    report = render_monthly("Monthly", result, registry, "2026-08")
    assert "*Source: Risk Desk*" in report


def test_dropped_material_nontrend_is_reclassified_without_provider_call_and_cap_is_four():
    registry = build_source_registry([
        item("Alpha News", 1, "a"), item("Beta News", 2, "b"), item("Risk Desk", 3, "risk")
    ])
    synthesis = valid(registry)
    synthesis["notable_developments"] *= 4
    synthesis["trends"].append({
        "heading": "El Nino food and weather risk", "scope": "cross_source",
        "synthesis": "Risk Desk argued that unusually warm regional waters could disrupt harvests and increase extreme-weather risks.",
        "source_refs": [next(row["ref_id"] for row in registry if row["source"] == "Risk Desk")],
    })
    calls, diagnostics = [], {}
    result = synthesize_monthly(
        registry, "en", lambda *_: calls.append(1) or json.dumps(synthesis), diagnostics
    )
    assert calls == [1]
    assert len(result["notable_developments"]) == 4
    assert diagnostics["monthly_trends_dropped"] == 1
    assert diagnostics["monthly_trends_reclassified_to_notable"] == 1
    assert diagnostics["monthly_notable_retained"] == 4


def test_fewer_than_two_trends_uses_the_single_repair_call():
    registry = build_source_registry([
        item("Alpha News", 1, "a"), item("Beta News", 2, "b")
    ])
    insufficient = valid(registry)
    insufficient["trends"] = insufficient["trends"][:1]
    repaired = valid(registry)
    calls = []

    def provider(*_):
        calls.append(1)
        return json.dumps(insufficient if len(calls) == 1 else repaired)

    result = synthesize_monthly(registry, "en", provider)
    assert len(calls) == 2
    assert len(result["trends"]) == 2


def test_generic_optional_trend_is_dropped_but_url_remains_fatal():
    registry = build_source_registry([
        item("Alpha News", 1, "a"), item("Beta News", 2, "b")
    ])
    synthesis = valid(registry)
    synthesis["trends"].append({
        "heading": "Droppable", "scope": "cross_source",
        "synthesis": "The presenter described it.", "source_refs": [registry[0]["ref_id"]],
    })
    diagnostics = {}
    assert len(validate_synthesis(synthesis, registry, diagnostics)["trends"]) == 2
    assert diagnostics["monthly_optional_sections_dropped"] == 1

    synthesis = valid(registry)
    synthesis["trends"].append({
        "heading": "Droppable", "scope": "cross_source",
        "synthesis": "See https://invented.invalid", "source_refs": [registry[0]["ref_id"]],
    })
    with pytest.raises(MonthlySynthesisError, match="contains a URL"):
        validate_synthesis(synthesis, registry)


def test_provenance_normalizes_model_spelling_missing_source_and_invalid_scope():
    registry = build_source_registry([
        item("CanadianPrepper", day, f"cp-{day}") for day in (1, 2, 3)
    ] + [item("Alpha News", 4, "a"), item("Beta News", 5, "b")])
    cp_refs = [row["ref_id"] for row in registry if row["source"] == "CanadianPrepper"]
    synthesis = valid(registry)
    synthesis["trends"] = [
        {"heading": "Preparedness emphasis", "scope": "source_specific",
         "source": "Canadian Prepper", "synthesis": "Canadian Prepper argued that risks increased materially.",
         "source_refs": cp_refs},
        {"heading": "Second preparedness emphasis", "scope": "unexpected",
         "synthesis": "Coverage documented another material shift without naming its channel.",
         "source_refs": cp_refs},
    ]
    calls, diagnostics = [], {}
    result = synthesize_monthly(registry, "en", lambda *_: calls.append(1) or json.dumps(synthesis), diagnostics)
    assert calls == [1]
    assert [trend["source"] for trend in result["trends"]] == ["CanadianPrepper", "CanadianPrepper"]
    assert result["trends"][1]["scope"] == "source_specific"
    assert diagnostics["monthly_trend_scopes_canonicalized"] == 1


def test_cross_source_same_channel_and_reclassified_notable_use_canonical_refs():
    registry = build_source_registry([
        item("CanadianPrepper", day, f"cp-{day}") for day in (1, 2, 3)
    ] + [item("Alpha News", 4, "a"), item("Beta News", 5, "b")])
    cp_refs = [row["ref_id"] for row in registry if row["source"] == "CanadianPrepper"]
    cross_refs = [row["ref_id"] for row in registry if row["source"] in {"Alpha News", "Beta News"}]
    synthesis = valid(registry)
    synthesis["trends"] = [
        {"heading": "Preparedness emphasis", "scope": "cross_source",
         "synthesis": "Coverage repeatedly emphasized a material development.", "source_refs": cp_refs},
        {"heading": "Shared development", "scope": "cross_source",
         "synthesis": "Alpha News and Beta News documented a shared development.", "source_refs": cross_refs},
        {"heading": "Material isolated risk", "scope": "cross_source",
         "synthesis": "Unusually warm regional waters could materially disrupt harvests and supplies.",
         "source_refs": [cp_refs[0]]},
    ]
    result = validate_synthesis(synthesis, registry)
    assert result["trends"][0]["scope"] == "source_specific"
    assert result["trends"][0]["source"] == "CanadianPrepper"
    reclassified = next(item for item in result["notable_developments"] if item["heading"] == "Material isolated risk")
    assert reclassified["attribution_source"] == "CanadianPrepper"
    assert canonical_sources_for_refs(cp_refs, {row["ref_id"]: row for row in registry}) == ["CanadianPrepper"]


def test_monthly_evidence_limit_constants_remain_unchanged():
    assert (MONTHLY_EVIDENCE_TARGET, MONTHLY_EVIDENCE_MAX, MONTHLY_CHANNEL_SOFT_CAP) == (72, 80, 8)


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
