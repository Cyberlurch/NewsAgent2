"""Paraphrased synthetic fixtures; no third-party article is reproduced."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsagent2 import main, summarizer
from newsagent2.pubmed_facts import build_pubmed_facts, evidence_input, candidate_issues


CORRECTION = json.loads((Path(__file__).parents[1] / "data/corrections/cybermed_42854247.json").read_text())


def paper(**extra):
    return {"source": "pubmed", "id": "42854247", "pmid": "42854247",
            "title": "Synthetic decision-support trial", "abstract": CORRECTION["abstract"], **extra}


def response(text, finish="stop"):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=text), finish_reason=finish)])


@pytest.fixture
def mock_provider(monkeypatch):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setattr(summarizer, "_get_client", lambda: object())
    calls = []
    def create(*args, **kw):
        calls.append(kw)
        return response("The intervention improved adherence and reduced delirium.")
    monkeypatch.setattr(summarizer, "_openai_chat_create", create)
    return calls


def test_late_results_have_priority_over_long_background(mock_provider):
    item = paper(abstract="Background: " + "Motivation for a future benefit. " * 110 + CORRECTION["abstract"])
    assert item["abstract"].index("225/641") > 2000
    core = summarizer.summarize_pubmed_bottom_line(item, language="en")
    payload = mock_provider[0]["messages"][1]["content"]
    assert "225/641" in payload and "P=.37" in payload and "all P>.05" in payload
    assert "225/641" in core and "201/614" in core and "1255" in core
    assert "does not prove absence" in core
    assert "unsupported_benefit" in item["_pubmed_evidence_reduction"]
    assert len(mock_provider) == 1


def test_audit_facts_preserve_primary_groups_and_missingness():
    facts = build_pubmed_facts(paper())
    assert facts.direction == "negative_or_null"
    assert "7294" in facts.population and "1255" in facts.primary_results
    assert "Missing delirium assessments" in facts.limitations
    assert "225/641 (35.1%)" in facts.primary_results
    assert "201/614 (32.7%)" in facts.primary_results
    assert "seven" in facts.primary_endpoint and facts.source_basis == "abstract"
    assert "not proof of no effect" in facts.core()


@pytest.mark.parametrize("overview,deep,category", [
    ("Improved outcomes", "No significant difference", "positive_overview_vs_negative_deep_dive"),
    ("Did not reduce events", "Reduced complications", "negative_overview_vs_positive_deep_dive"),
])
def test_conflict_detection_both_directions(overview, deep, category):
    assert category in main._detect_pubmed_bottom_line_conflicts(overview, deep)


def test_no_results_no_default_benefit():
    facts = build_pubmed_facts({"abstract": "Objectives: Improve survival. Methods: Randomized intervention trial."})
    assert facts.primary_results == "" and facts.direction == "unclear"
    assert "unknown" in facts.core() and "no benefit can be inferred" in facts.core()


def test_positive_secondary_does_not_replace_null_primary():
    facts = build_pubmed_facts({"abstract": "Primary endpoint: Mortality. Results: Primary mortality was not significantly reduced (P=.2); Secondary biomarker improved (P=.01)."})
    assert facts.direction == "negative_or_null"
    assert "biomarker" not in facts.primary_results
    assert "biomarker" in facts.secondary_results
    assert "P=.2" in facts.core() and "P=.01" not in facts.core()


def test_noninferiority_is_neither_superiority_nor_equivalence():
    facts = build_pubmed_facts({"abstract": "Design: Noninferiority trial. Results: Treatment A was noninferior to B for the primary endpoint within the prespecified margin."})
    assert facts.direction == "noninferiority"
    assert "does not establish superiority or equivalence" in facts.core()
    assert "noninferiority_overclaim" in candidate_issues(facts, "Treatment A was superior and as effective.")


def test_observational_association_does_not_establish_causation():
    facts = build_pubmed_facts({"abstract": "Design: Observational cohort study. Results: Exposure was associated with lower mortality."})
    assert facts.observational and "do not establish causality" in facts.core()
    assert "causal_overclaim" in candidate_issues(facts, "Exposure caused better survival.")


@pytest.mark.parametrize("finish", ["length", "content_filter"])
def test_incomplete_model_reply_is_reduced_without_extra_calls(mock_provider, monkeypatch, finish):
    monkeypatch.setattr(summarizer, "_openai_chat_create", lambda *a, **k: response("Reduced delirium", finish))
    item = paper()
    assert summarizer.summarize_pubmed_bottom_line(item) == build_pubmed_facts(item).core()
    assert "incomplete_response" in item["_pubmed_evidence_reduction"]


def test_provider_error_remains_content_safe(mock_provider, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("insufficient_quota api_key=sk-private")
    monkeypatch.setattr(summarizer, "_openai_chat_create", fail)
    with pytest.raises(Exception) as err:
        summarizer.summarize_pubmed_bottom_line(paper())
    assert "sk-private" not in str(err.value)


def test_fulltext_is_section_budgeted_and_disagreement_stays_unknown():
    item = paper(full_text_excerpt="Background: " + "irrelevant. " * 3000 + "\nMethods: Trial.\nResults: Intervention improved the primary outcome significantly.\nConclusions: Benefit reported.")
    text, basis, omitted = evidence_input(item, budget=3000)
    assert len(text) <= 3000 and "Fulltext excerpt / results:" in text
    assert omitted and basis == "abstract_and_fulltext_excerpt"
    assert build_pubmed_facts(item).primary_results == ""


def test_source_core_identical_in_overview_and_entire_deep_dive(mock_provider):
    item = paper()
    core = summarizer.summarize_pubmed_bottom_line(item)
    # Simulate a complete but contradictory provider JSON. No second
    # bottom-line assertion survives in the source-grounded deep dive.
    candidate = {"bottom_line": "Improved delirium.", "study_type": "RCT", "population_setting": "Adults",
        "intervention_or_exposure": "AI", "comparator": "Routine care", "primary_endpoint": "Delirium",
        "key_results": "Improved delirium.", "limitations": ["Missing assessments"], "clinical_interpretation": "Benefit."}
    from unittest.mock import patch
    with patch.object(summarizer, "_openai_chat_create", return_value=response(json.dumps(candidate))):
        deep = summarizer.summarize_item_detail(item, language="en", profile="medical")
    assert deep.startswith("BOTTOM LINE: " + core)
    assert "Improved delirium." not in deep and deep.count("BOTTOM LINE:") == 1
    overview = summarizer.summarize([item], language="en", profile="medical")
    assert core in overview and "intervention improved adherence" not in overview


def test_main_final_boundary_cannot_copy_an_unvalidated_overview(monkeypatch, tmp_path):
    import sys
    from datetime import datetime, timezone
    item = paper(published_at=datetime.now(timezone.utc), text=CORRECTION["abstract"])
    for name, value in {"REPORT_KEY": "cybermed", "REPORT_MODE": "daily", "REPORT_LANGUAGE": "en",
        "REPORT_TITLE": "Cybermed Report", "REPORT_SUBJECT": "Cybermed Report",
        "REPORT_PROFILE": "medical", "SEND_EMAIL": "0", "EMAIL_MODE": "none", "GITHUB_EVENT_NAME": "workflow_dispatch",
        "REPORT_DIR": str(tmp_path / "out"), "STATE_PATH": str(tmp_path / "state.json"),
        "CYBERMED_DAILY_DIGEST_STATE_PATH": str(tmp_path / "daily.json"),
        "CYBERLURCH_DIGEST_STATE_PATH": str(tmp_path / "lurch.json"), "ROLLUPS_STATE_PATH": str(tmp_path / "rollups.json")}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "argv", ["newsagent2"])
    monkeypatch.setattr(main, "load_channels_config", lambda *a: ([{"source": "pubmed", "name": "Test", "query": "mock"}], {}, {}))
    def collect(*args, **kwargs):
        return ([item], {}) if kwargs.get("return_metadata") else [item]
    monkeypatch.setattr(main, "search_recent_pubmed", collect)
    monkeypatch.setattr(main, "get_pmcids_for_pmids", lambda *a, **k: {})
    monkeypatch.setattr(main, "collect_foamed_items", lambda *a, **k: ([], {}))
    monkeypatch.setattr(main, "_select_pubmed_deep_dives_with_content", lambda rows, **kw: rows[:1])
    monkeypatch.setattr(main, "summarize_pubmed_bottom_line", lambda *a, **k: "The intervention reduced delirium.")
    monkeypatch.setattr(main, "summarize", lambda *a, **k: "## Executive Summary\n\nThe intervention reduced delirium.")
    monkeypatch.setattr(main, "summarize_item_detail", lambda *a, **k: "BOTTOM LINE: No significant difference.\n\nYet the intervention reduced delirium.")
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: None)
    monkeypatch.setattr(main, "_save_youtube_channel_id_cache", lambda *a, **k: None)
    main.main()
    report = next((tmp_path / "out").glob("cybermed_daily_*.md")).read_text()
    assert "intervention reduced delirium" not in report
    assert "225/641" in report and "201/614" in report and "P=.37" in report
    assert not (tmp_path / "daily.json").exists()
    local = next((tmp_path / "out").rglob("cybermed_daily_digests.json"))
    stored = json.loads(local.read_text())["digests"][0]
    assert stored["items"]["pubmed"][0]["bottom_line"] == stored["deep_dives"][0]["bottom_line"]
