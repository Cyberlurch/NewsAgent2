from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from newsagent2.cyberlurch_monthly import (
    MONTHLY_EVIDENCE_LIMIT, MonthlySynthesisError, build_source_registry, render_monthly,
    select_monthly_evidence,
    source_abbreviations, synthesize_monthly, validate_synthesis,
)


def item(channel, day, ident, title="Title", topic="wrong-label"):
    return {"channel":channel, "published_at":datetime(2026, 8, day, tzinfo=timezone.utc),
            "id":ident, "title":title, "url":f"https://www.youtube.com/watch?v={ident}",
            "bottom_line":f"{channel} reported persisted fact {ident}.", "topic_primary":topic}


def valid(registry):
    refs = [row["ref_id"] for row in registry]
    return {"executive_summary":[{"synthesis":"Coverage developed across the month.", "source_refs":[refs[0], refs[1]]}],
            "trends":[{"heading":"Semantically regrouped theme", "synthesis":"Alpha News and Alpine Network returned to a shared development.", "source_refs":refs[:2]}],
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
    assert report.count("https://") == 3
    for ref in synthesis["source_refs_used"]: assert f"[{ref}]" in report
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


def test_large_month_is_bounded_diverse_temporally_spread_and_repeatable():
    rows = []
    # A prolific channel may retain enough items for a source-specific trend, but
    # cannot displace the lower-volume channels.
    for index in range(410):
        rows.append(item("High Volume", 1 + index % 31, f"hv-{index}"))
    for source_index in range(12):
        for index in range(3):
            row = item(f"Source {source_index:02d}", 1 + (source_index * 7 + index * 11) % 31, f"s-{source_index}-{index}")
            row.update(content_status="full_text", text_source="managed_transcript", transcript_full_summary="A concise persisted factual account.")
            rows.append(row)
    selected, diag = select_monthly_evidence(rows)
    selected_again, _ = select_monthly_evidence(reversed(rows))
    identity = lambda values: [(r["channel"], r["id"]) for r in values]
    assert len(selected) == MONTHLY_EVIDENCE_LIMIT
    assert identity(selected) == identity(selected_again)
    assert diag["persisted_records_available"] == 446
    assert diag["evidence_records_excluded"] == 446 - MONTHLY_EVIDENCE_LIMIT
    assert diag["unique_channels_represented"] == diag["unique_channels_available"] == 13
    high_volume_count = sum(r["channel"] == "High Volume" for r in selected)
    assert 2 <= high_volume_count < MONTHLY_EVIDENCE_LIMIT - 12
    days = {r["published_at"].day for r in selected}
    assert any(day <= 8 for day in days) and any(day >= 25 for day in days)


def test_compact_pack_keeps_full_registry_for_validation_and_cannot_invent_url():
    rows = [item("High Volume", 1 + i % 28, f"hv-{i}") for i in range(100)]
    full_registry = build_source_registry(rows)
    selected, _ = select_monthly_evidence(rows, limit=10)
    selected_urls = {row["url"] for row in selected}
    evidence = [row for row in full_registry if row["url"] in selected_urls]
    seen = {}
    refs = [row["ref_id"] for row in evidence]
    payload = {"executive_summary":[{"synthesis":"High Volume changed emphasis across several persisted items.","source_refs":refs[:2]}],
               "trends":[{"heading":"High Volume emphasis","synthesis":"High Volume repeatedly covered this theme.","source_refs":refs[:3]}],
               "notable_developments":[],"month_in_brief":"High Volume repeatedly returned to one theme.","source_refs_used":refs[:3]}
    def call(system, user): seen["user"] = user; return json.dumps(payload)
    result = synthesize_monthly(full_registry, "en", call, evidence_registry=evidence)
    assert len(full_registry) == 100 and len(evidence) == 10
    assert rows[-1]["url"] not in seen["user"]  # excluded provenance was not sent
    assert all(ref in {row["ref_id"] for row in full_registry} for ref in result["source_refs_used"])
    bad = dict(payload); bad["month_in_brief"] = "See https://invented.invalid"
    with pytest.raises(MonthlySynthesisError): validate_synthesis(bad, full_registry)
