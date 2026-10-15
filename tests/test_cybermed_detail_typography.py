"""Reader Markdown and recipient HTML stay clear without changing evidence."""
import copy

import pytest
from bs4 import BeautifulSoup

from newsagent2 import emailer, reporter
from newsagent2.pubmed_presentation import format_reader_detail
from test_cybermed_reader_presentation import paper


DETAIL = """- Study type: Randomized comparison.
- Key results: Mortality was not significantly different, P=.37. CONCLUSIONS AND RELEVANCE: Routine adoption is not supported by these findings.
1. Limitations:
2. Not explicitly reported in the supplied evidence.
- Source basis: abstract
"""


def dom(detail):
    return BeautifulSoup(emailer._render_email_html(detail, report_key="cybermed", cadence="daily"), "html.parser")


def test_bold_fields_inline_limitation_and_separate_conclusion_are_idempotent():
    formatted = format_reader_detail(DETAIL)
    assert format_reader_detail(formatted) == formatted
    soup = dom(formatted)
    for label in ("Study type:", "Key results:", "Limitations:", "Conclusions and relevance:"):
        assert soup.find("strong", string=label)
    result = soup.find("strong", string="Key results:").find_parent("li")
    conclusion = soup.find("strong", string="Conclusions and relevance:").find_parent("li")
    assert "P=.37" in result.get_text() and "Routine adoption" not in result.get_text()
    assert "Routine adoption is not supported" in conclusion.get_text() and result is not conclusion
    limitation = soup.find("strong", string="Limitations:").find_parent("li")
    assert "Not explicitly reported in the supplied evidence." in limitation.get_text()
    assert not soup.find("ol") and not limitation.find(["ol", "ul"])
    assert "Source basis" not in soup.get_text() and "abstract" not in soup.get_text()


@pytest.mark.parametrize("label", ["Limitations:", "- Limitations:", "1. **Limitations:**", "- **Limitations**:"])
def test_multiline_limitations_are_one_readable_row_with_every_qualifier(label):
    value = label + "\n- Missing assessments in most patients.\n\n- No reliable subgroup estimate.\n- Source: Abstract\n\nA separate reader paragraph."
    result = format_reader_detail(value)
    soup = dom(result)
    row = soup.find("strong", string="Limitations:").find_parent("li")
    assert "Missing assessments in most patients. No reliable subgroup estimate." in row.get_text()
    assert "Source:" not in result and "A separate reader paragraph." in result
    assert len(soup.find_all("li")) == 1


@pytest.mark.parametrize("heading", ["RESULTS", "FINDINGS", "CONCLUSIONS AND RELEVANCE", "INTERPRETATION"])
def test_source_subheadings_are_not_left_in_a_running_paragraph_or_an_empty_bullet(heading):
    formatted = format_reader_detail(f"- {heading}: No significant difference was reported.")
    soup = dom(formatted)
    assert len(soup.find_all("li")) == 1
    assert soup.find("strong") and "No significant difference was reported." in soup.get_text()
    assert all(li.get_text(strip=True) for li in soup.find_all("li"))


def test_source_label_is_hidden_but_a_real_reference_link_is_retained():
    result = format_reader_detail("- Source: [Trial](https://example.test/trial)\n\nSeparate paragraph.")
    assert "Source:" not in result
    assert dom(result).find("a", href="https://example.test/trial")
    assert "Separate paragraph." in result


@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly"])
def test_actual_pubmed_reader_uses_same_detail_typography_for_each_cadence(monkeypatch, cadence):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    item = paper(cybermed_deep_dive=True)
    before = copy.deepcopy(item)
    md = reporter.to_markdown([item], "", {item["id"]: DETAIL}, report_title="Cybermed " + cadence,
                             report_language="en", report_mode=cadence)
    deep = md.split("## Deep Dives", 1)[1]
    soup = dom(deep)
    assert soup.find("strong", string="Key results:") and soup.find("strong", string="Limitations:")
    assert not soup.find("strong", string="Source:") and "Source basis:" not in deep
    assert "P=.37" in deep and item == before


def test_foamed_details_get_the_same_labels_without_rewriting_the_learning_statement(monkeypatch):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    item = {"source": "foamed", "id": "learning", "title": "Bedside assessment",
        "url": "https://example.test/learning", "bottom_line": "Check and document delirium assessments consistently.",
        "cybermed_deep_dive": True}
    md = reporter.to_markdown([item], "", {"learning": "**BOTTOM LINE:** Check and document delirium assessments consistently.\n\n" + DETAIL},
                             report_title="Cybermed Daily", report_language="en", report_mode="daily")
    assert "**Key results:**" in md and "**Limitations:**" in md
    assert item["bottom_line"] in md and "Source basis:" not in md


def test_favorites_have_no_one_star_cap_or_percentage_quota(monkeypatch):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    items = [paper(id=str(i), pmid=str(i), top_pick=True, evidence_strength_label="A",
        clinical_relevance_1_5=5, practice_change_potential_1_5=4) for i in range(3)]
    from newsagent2.clinical_editorial import attach_context
    for item in items:
        attach_context(item)
    md = reporter.to_markdown(items, "", {}, report_title="Cybermed Daily", report_language="en", report_mode="daily")
    assert md.count("⭐ Top pick") == 3
    for item in items:
        item["practice_change_potential_1_5"] = 1
    md = reporter.to_markdown(items, "", {}, report_title="Cybermed Daily", report_language="en", report_mode="daily")
    assert "⭐ Top pick" not in md
