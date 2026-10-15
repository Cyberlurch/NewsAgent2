"""Clinical meaning and grounding of reader prose; all provider calls mocked."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsagent2 import main, reporter, summarizer
from newsagent2.clinical_reader import capture_reader_summary, reader_summary_issues, verified_reader_summary
from newsagent2.pubmed_facts import build_pubmed_facts
from newsagent2.pubmed_presentation import reader_bottom_line
from newsagent2.cybermed_digest_store import aggregate_cybermed_digest_inputs


CORRECTION = json.loads((Path(__file__).parents[1] / "data/corrections/cybermed_42854247.json").read_text())
SUMMARY = ("Surgical patients with cognitive impairment were included in a randomized comparison of AI-guided decision support with routine care. "
           "The authors reported no significant reduction in postoperative delirium, and missing assessments limit the usefulness of this evidence for prevention decisions.")
QUOTE = "In those assessed, delirium occurred in 225/641 (35.1%) in the intervention group versus 201/614 (32.7%) in the control group, P=.37; a statistically significant difference was not demonstrated."


def paper(**extra):
    return {"source": "pubmed", "source_type": "pubmed", "id": "42854247", "pmid": "42854247",
            "title": "Perioperative AI decision support for delirium prevention", "abstract": CORRECTION["abstract"], **extra}


def bind(item, text=SUMMARY, quote=QUOTE, finish="stop"):
    facts = build_pubmed_facts(item)
    item.update(pubmed_facts=facts.to_dict(), facts_version=facts.version, bottom_line=facts.core())
    capture_reader_summary(item, facts, json.dumps({"summary": text, "result_quote": quote}), language="en", finish_reason=finish)
    return facts


def test_existing_model_call_supplies_complete_grounded_context_not_a_generic_sentence(monkeypatch):
    calls = []
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setattr(summarizer, "_get_client", lambda: object())
    def create(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps({"summary": SUMMARY, "result_quote": QUOTE})), finish_reason="stop")])
    monkeypatch.setattr(summarizer, "_openai_chat_create", create)
    item = paper()
    core = summarizer.summarize_pubmed_bottom_line(item)
    assert core == build_pubmed_facts(item).core() and "225/641" in core
    assert item["_reader_summary_issues"] == []
    assert reader_bottom_line(item) == SUMMARY
    assert len(calls) == 1 and calls[0]["max_tokens"] == 240
    assert calls[0]["model"] == summarizer.OPENAI_MODEL_CYBERMED_BOTTOM_LINE
    assert "generic statement" in calls[0]["messages"][0]["content"]


@pytest.mark.parametrize("text,quote,finish,issue", [
    ("In adults, the clinical implication of the reported findings remains uncertain. Further research will be needed before any meaningful conclusion can be drawn.", QUOTE, "stop", "missing_study_context"),
    (SUMMARY.replace("no significant reduction", "a meaningful improvement"), QUOTE, "stop", "unsupported_reader_benefit"),
    (SUMMARY, "Adherence to best practices showed no significant differences (all P>.05).", "stop", "unbound_result_quote"),
    (SUMMARY, QUOTE, "length", "incomplete_reader_summary"),
    (SUMMARY.replace("Surgical patients", "Pediatric patients"), QUOTE, "stop", "reader_population_mismatch"),
    (SUMMARY + " The IQR was 3 to 7.", QUOTE, "stop", "reader_detail_wall"),
    (SUMMARY, "These data do not establish a delirium-prevention benefit.", "stop", "unbound_result_quote"),
])
def test_reader_rejects_generic_or_unbound_proposals(text, quote, finish, issue):
    item = paper(clinical_context={"population": "adult"})
    facts = bind(item, text, quote, finish)
    assert issue in item["_reader_summary_issues"]
    assert not verified_reader_summary(item)
    assert "225/641" in facts.primary_results
    fallback = reader_bottom_line(item)
    assert "decision support" in fallback and "routine care" in fallback


def test_unlabelled_primary_outcome_still_explains_the_actual_comparison():
    item = paper(id="context", pmid="context", title="Norepinephrine versus dopamine for shock",
        abstract="Design: Randomized trial. Population: Adult patients with shock. Interventions: Norepinephrine versus dopamine. Results: Vasopressor requirements were 5 versus 6 units, P=.37.")
    text = reader_bottom_line(item)
    assert "norepinephrine" in text.lower() and "dopamine" in text.lower() and "shock" in text.lower()
    assert text.count(".") >= 2
    assert text != "In adults, the clinical implication of the reported findings remains uncertain."


def test_unlabelled_but_qualitative_results_are_attributed_in_the_fallback():
    item = paper(id="context", pmid="context", title="Decision support versus routine care in surgical patients",
        abstract="Design: Randomized trial. Population: Surgical patients. Interventions: Decision support versus routine care. Findings: No significant difference in delirium was demonstrated.")
    text = reader_bottom_line(item)
    assert "decision support" in text.lower() and "routine care" in text.lower()
    assert "No significant difference in delirium was demonstrated." in text
    assert "primary" not in text.lower() and "remains uncertain" not in text


def test_missing_results_summary_names_the_question_and_explicitly_says_results_are_missing():
    item = paper(id="protocol", pmid="protocol", title="Decision support versus routine care for delirium",
        abstract="Design: Randomized trial protocol. Population: Surgical patients. Interventions: Decision support versus routine care. Primary outcome: Postoperative delirium.")
    text = "The trial protocol compares decision support with routine care for postoperative delirium in surgical patients. Results are not available, so this source cannot answer whether the intervention improves prevention."
    bind(item, text, "")
    assert verified_reader_summary(item) == text
    bind(item, text.replace("Results are not available, so this source cannot answer whether the intervention improves prevention.", "Its clinical implications remain uncertain and further research may clarify the findings."), "")
    assert "missing_reported_finding" in item["_reader_summary_issues"]


@pytest.mark.parametrize("winner,expected", [("norepinephrine", []), ("dopamine", ["unverified_reader_group_direction"])])
def test_real_named_comparison_cannot_be_reversed(winner, expected):
    item = paper(id="pressors", pmid="pressors", title="Norepinephrine versus dopamine in shock",
        abstract="Design: Randomized trial. Participants: Adult patients with shock. Interventions: Norepinephrine versus dopamine. Primary outcome: Mortality. Results: Mortality was lower with norepinephrine than dopamine.")
    quote = "Mortality was lower with norepinephrine than dopamine."
    text = f"A randomized trial compared norepinephrine with dopamine in adults with shock. The authors reported that {winner} reduced mortality, a finding limited to the studied population."
    facts = build_pubmed_facts(item)
    issues = reader_summary_issues(item, facts, text, quote)
    assert issues == expected


def test_unstructured_reported_measurement_is_not_called_a_missing_result():
    item = paper(id="unstructured", pmid="unstructured", title="Decision support versus routine care for delirium",
        abstract="A randomized trial compared decision support with routine care in adult surgical patients. We found no significant reduction in delirium, P=.37, and incomplete assessments limited interpretation.")
    quote = "We found no significant reduction in delirium, P=.37, and incomplete assessments limited interpretation."
    text = "The trial compared decision support with routine care in adult surgical patients. The authors reported no significant reduction in delirium; incomplete assessments limit the clinical interpretation of this finding."
    facts = bind(item, text, quote)
    assert not facts.primary_results  # Do not invent a canonical primary mapping.
    assert verified_reader_summary(item) == text
    assert reader_bottom_line(item) == text


def test_background_objective_cannot_be_used_as_an_unstructured_result():
    item = paper(id="protocol", pmid="protocol", title="Decision support for delirium prevention",
        abstract="The objective is to improve delirium prevention. A randomized trial will assess decision support in adult surgical patients; 500 patients are planned.")
    quote = "A randomized trial will assess decision support in adult surgical patients; 500 patients are planned."
    text = "The study evaluates decision support for adult surgical patients. The authors reported improved delirium prevention and support routine clinical implementation based on the available evidence."
    bind(item, text, quote)
    assert "results_unavailable" in item["_reader_summary_issues"]
    assert not verified_reader_summary(item)


def test_capsule_survives_the_persisted_daily_weekly_monthly_chain_without_generation(monkeypatch):
    item = paper()
    bind(item)
    original = copy.deepcopy(item)
    monkeypatch.setattr(summarizer, "_get_client", lambda: pytest.fail("aggregate provider call"))
    daily = main._sanitize_cybermed_pubmed_item(item)
    aggregate = aggregate_cybermed_digest_inputs([{"run_date": "2026-10-15", "items": {"pubmed": [daily], "foamed": []}}], pubmed_cap=20, foamed_cap=12, deep_dive_cap=8)
    weekly = aggregate["pubmed"][0]
    monthly = main._sanitize_cybermed_monthly_rollup_item(weekly)
    assert reader_bottom_line(daily) == reader_bottom_line(weekly) == reader_bottom_line(monthly) == SUMMARY
    from newsagent2.rollups import render_yearly_markdown
    year = render_yearly_markdown(report_title="Cybermed", report_language="en", year=2026,
        rollups=[{"month": "2026-10", "cybermed_items": [monthly]}])
    assert SUMMARY in year and "225/641" in year.split("## Deep Dives", 1)[1] and item == original


def test_duplicate_or_truncated_json_never_becomes_reader_prose():
    item = paper()
    facts = bind(item)
    for content in ('{"summary":"first","summary":"second","result_quote":""}', '{"summary":'):
        capture_reader_summary(item, facts, content, language="en")
        assert item["_reader_summary_issues"] == ["invalid_reader_summary_json"]
        assert not verified_reader_summary(item)


@pytest.mark.parametrize("design,result,text,issue", [
    ("Noninferiority randomized trial", "Drug A was noninferior to Drug B for mortality.",
     "A randomized trial compared Drug A with Drug B in adults with shock. Drug A was noninferior for mortality; superiority was not established.", None),
    ("Noninferiority randomized trial", "Drug A was noninferior to Drug B for mortality.",
     "A randomized trial compared Drug A with Drug B in adults with shock. Drug A was superior for mortality and should replace Drug B in routine care.", "reader_noninferiority_overclaim"),
    ("Observational cohort", "Drug A was associated with lower mortality than Drug B.",
     "An observational study compared Drug A with Drug B in adults with shock. The authors reported an association with lower mortality, without establishing causality.", None),
    ("Observational cohort", "Drug A was associated with lower mortality than Drug B.",
     "An observational study compared Drug A with Drug B in adults with shock. Drug A prevented deaths and caused lower mortality in the studied population.", "reader_causal_overclaim"),
])
def test_reader_keeps_noninferiority_and_association_boundaries(design, result, text, issue):
    item = paper(id="design", pmid="design", title="Drug A versus Drug B in shock",
        abstract=f"Design: {design}. Population: Adults with shock. Interventions: Drug A versus Drug B. Primary outcome: Mortality. Results: {result}")
    facts = build_pubmed_facts(item)
    issues = reader_summary_issues(item, facts, text, result)
    assert (not issues) if issue is None else issue in issues


def test_renderer_uses_same_contextual_text_in_overview_and_deep_dive(monkeypatch):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    item = paper(cybermed_deep_dive=True)
    facts = bind(item)
    md = reporter.to_markdown([item], "", {item["id"]: facts.deep_dive()}, report_title="Cybermed Daily", report_language="en", report_mode="daily")
    overview, deep = md.split("## Deep Dives", 1)
    assert SUMMARY in overview and SUMMARY in deep
    assert "225/641" not in overview and "225/641" in deep and "P=.37" in deep


@pytest.mark.parametrize("change", ["quote", "identity", "version", "error"])
def test_stale_or_wrong_paper_reader_metadata_is_not_trusted(change):
    item = paper()
    bind(item)
    if change == "quote": item["clinical_summary"]["result_quote"] = "Unrelated findings."
    if change == "identity": item["pmid"] = "999"
    if change == "version": item["clinical_summary"]["facts_version"] = "0" * 64
    if change == "error": item["clinical_summary"]["text"] = "Provider error: insufficient_quota. Contact support."
    assert not verified_reader_summary(item)
