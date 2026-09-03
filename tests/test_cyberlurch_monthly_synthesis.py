from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from newsagent2.cyberlurch_monthly import (
    MonthlySynthesisError, build_source_registry, render_monthly,
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
