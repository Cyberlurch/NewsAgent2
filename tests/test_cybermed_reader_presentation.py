"""Readable Cybermed output from source facts, without new provider calls."""
import copy
import json
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from newsagent2 import clinical_editorial, emailer, reporter, summarizer
from newsagent2.pubmed_facts import build_pubmed_facts
from newsagent2.pubmed_presentation import reader_bottom_line, reader_deep_dive, reader_rating_line


AUDIT = json.loads((Path(__file__).parents[1] / "data/corrections/cybermed_42854247.json").read_text())


def paper(abstract=None, **extra):
    return {"source": "pubmed", "id": "42854247", "pmid": "42854247",
            "title": "Illustrative perioperative decision-support trial", "journal": "Example Journal",
            "url": "https://pubmed.ncbi.nlm.nih.gov/42854247/",
            "abstract": abstract or AUDIT["abstract"], **extra}


def test_delirium_core_is_short_specific_and_preserves_null_finding():
    item = paper()
    facts = build_pubmed_facts(item)
    brief = reader_bottom_line(item)
    assert len(brief.split()) <= 60
    assert "surgical patients with cognitive impairment" in brief
    assert "delirium" in brief and "do not establish" in brief
    assert not any(s in brief for s in ("225/641", "201/614", "P=.37", "IQR", "does not prove absence", "Interpret secondary"))
    deep = reader_deep_dive(item, facts.deep_dive())
    assert deep.startswith("**BOTTOM LINE:** " + brief)
    assert all(s in deep for s in ("225/641", "201/614", "P=.37", "Missing delirium assessments"))
    assert "limited to these source facts" not in deep
    assert facts.core() == build_pubmed_facts(item).core()  # Internal audit core unchanged.


@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly"])
def test_real_reporter_has_classic_header_stars_short_core_and_detailed_results(monkeypatch, cadence):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    item = paper(cybermed_deep_dive=True, top_pick=True,
        evidence_strength_label="A", clinical_relevance_1_5=5,
        practice_change_potential_1_5=5, text_confidence_label="high")
    clinical_editorial.attach_context(item)
    original = copy.deepcopy(item)
    facts = build_pubmed_facts(item)
    md = reporter.to_markdown([item], "", {item["id"]: facts.deep_dive()},
        report_title="Cybermed " + cadence.title(), report_language="en", report_mode=cadence)
    overview, deep = md.split("## Deep Dives", 1)
    assert "⭐ Top pick · Evidence A · Relevance 5/5 · Practice impact 5/5 · Confidence High" in overview
    brief = reader_bottom_line(item)
    assert "**BOTTOM LINE:** " + brief in overview and "**BOTTOM LINE:** " + brief in deep
    assert "225/641" not in overview and "225/641" in deep and "P=.37" in deep
    assert not any(s in md for s in ("Design hint", "not GRADE", "not effect certainty", "Population adult ·", "mapping unknown", "PubMed:"))
    assert item == original
    html = emailer._render_email_html(md, report_key="cybermed", cadence=cadence)
    soup = BeautifulSoup(html, "html.parser")
    assert soup.find("table", role="presentation")
    assert soup.find("a", href=item["url"])


@pytest.mark.parametrize("result,conclusion,expected", [
    ("Primary mortality was not significantly reduced (P=.20). Secondary biomarker improved (P=.01).",
     "A better biomarker may support treatment.", "not significantly reduced"),
    ("Treatment A was noninferior to treatment B for 28-day mortality within the prespecified margin.",
     "Treatment A was equivalent and superior.", "noninferior"),
])
def test_secondary_and_noninferiority_do_not_become_benefit_claims(result, conclusion, expected):
    item = paper(abstract="Design: Randomized trial. Population: Adult patients. Primary outcome: 28-day mortality. Results: " + result + " Conclusions: " + conclusion)
    brief = reader_bottom_line(item)
    assert expected.lower() in brief.lower()
    assert not any(s in brief.lower() for s in ("biomarker", "equivalent", "was superior"))


def test_observation_keeps_association_and_removes_statistics():
    item = paper(abstract="Design: Observational cohort. Participants: Adult ICU patients. Primary outcome: Mortality. Results: Exposure was associated with lower mortality (HR 0.7, 95% CI 0.5-0.9).")
    brief = reader_bottom_line(item)
    assert "associated with lower mortality" in brief
    assert "adult ICU patients" in brief and "HR" not in brief and "CI" not in brief
    assert "caused" not in brief and "prevents" not in brief


def test_numerical_subgroup_qualifier_is_not_stripped_into_overall_benefit():
    item = paper(abstract="Design: Randomized trial. Participants: Adult patients. Primary endpoint: Mortality. Results: Mortality was lower (only in a subgroup aged 70 years, P=.03).")
    brief = reader_bottom_line(item)
    assert "overall benefit" in brief and "uncertain" in brief
    assert "An improvement" not in brief


@pytest.mark.parametrize("abstract", [
    "Objectives: Prevent delirium. Methods: Randomized trial.",
    "Methods: Randomized trial. Results: Secondary biomarker improved. Conclusions: Promising intervention.",
])
def test_missing_primary_result_does_not_acquire_a_positive_finding(abstract):
    brief = reader_bottom_line(paper(abstract=abstract))
    assert "not reported" in brief or "uncertain" in brief
    assert "Promising" not in brief and "improvement" not in brief


def test_statistics_only_result_uses_known_endpoint_and_population():
    item = paper(abstract="Participants: ICU patients. Primary endpoint: ICU length of stay. Results: Median stay was 5 (IQR 3-7) days versus 6 (IQR 4-8), P=.37.")
    brief = reader_bottom_line(item)
    assert "ICU patients" in brief and "ICU length of stay" in brief
    assert "No clear improvement" in brief or "no clear improvement" in brief
    assert "IQR" not in brief and "P=" not in brief


def test_reported_guideline_keeps_conditional_recommendation_without_trial_claim():
    item = paper(abstract="Participants: Adult ICU patients. Conclusions: We conditionally suggest a structured assessment process, with limited evidence.",
                 publication_types=["Practice Guideline"])
    clinical_editorial.attach_context(item)
    brief = reader_bottom_line(item)
    assert "The guideline authors conditionally suggest" in brief
    assert "limited evidence" in brief
    assert "proven" not in brief and "prevents" not in brief


def test_classic_labels_never_invent_high_confidence_or_missing_scores():
    assert reader_rating_line({"evidence_strength_label": "B", "clinical_relevance_1_5": 3,
                               "practice_change_potential_1_5": 4, "text_confidence_label": "low"}) == (
        "Evidence B · Relevance 3/5 · Practice impact 4/5 · Confidence Low")
    assert reader_rating_line({}) == ""
    assert reader_rating_line({"clinical_relevance_1_5": True}) == ""


def test_selected_important_null_result_can_keep_star_without_primary_label():
    item = paper(abstract="Methods: Randomized trial of adult ICU patients. Results: Delirium did not differ significantly, P=.37.",
        top_pick=True, evidence_strength_label="A", clinical_relevance_1_5=4, practice_change_potential_1_5=4)
    clinical_editorial.attach_context(item)
    assert item["clinical_context"]["result_basis"] == "unknown"
    clinical_editorial.finalize_favorite(item, cadence="daily")
    assert item["top_pick"]
    assert "uncertain" in reader_bottom_line(item)  # A star supplies no primary finding.
    for key in ("clinical_relevance_1_5", "practice_change_potential_1_5"):
        weaker = {**item, key: 1}
        clinical_editorial.finalize_favorite(weaker, cadence="daily")
        assert not weaker["top_pick"]


def test_legacy_projection_is_additive_and_keeps_saved_detail_without_source_invention():
    legacy = {"source": "pubmed", "bottom_line": "A useful stored clinical statement."}
    assert reader_bottom_line(legacy) == legacy["bottom_line"]
    dense = {"source": "pubmed", "bottom_line": "Mortality was 12% versus 15%, P=.2."}
    original = copy.deepcopy(dense)
    deep = reader_deep_dive(dense, "BOTTOM LINE: " + dense["bottom_line"])
    assert "12% versus 15%, P=.2" in deep
    assert "not reported" in reader_bottom_line(dense)
    assert dense == original


def test_renderer_never_calls_a_provider_and_foamed_learning_stays_intact(monkeypatch):
    monkeypatch.setattr(summarizer, "_get_client", lambda: pytest.fail("presentation provider call"))
    assert reader_bottom_line(paper())
    foam = {"source": "foamed", "bottom_line": "The authors explain a bedside assessment approach."}
    assert reader_bottom_line(foam) == foam["bottom_line"]
