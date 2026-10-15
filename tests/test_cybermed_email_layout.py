"""Exercise recipient HTML, not just the Markdown report artifact (no SMTP)."""
import hashlib
import json
import re
from email import message_from_string
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from newsagent2 import emailer, reporter


# Synthetic layout text, not a copied article or a medical evidence fixture.
LAYOUT_MD = """# Cybermed layout example

## Papers

**[Illustrative study](https://example.test/paper)**

**Design:** Randomized example.
**Population:** Adults in an illustrative setting.
**BOTTOM LINE:** Primary benefit remains uncertain; P=.37.

### Deep Dive

- Study type: Illustrative randomized comparison.
- Key results: No demonstrated primary benefit.
- Limitations:
    - Missing assessments.
    - Uncertain generalizability.

Second paragraph with **important uncertainty**.

---

## FOAMed & Commentary

**Summary:** An illustrative educational discussion.
**Limitations:** Commentary does not establish causality.

| Field | Value |
| --- | --- |
| Source basis | Abstract |
"""


def dom(html):
    return BeautifulSoup(html, "html.parser")


@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly", "yearly"])
def test_cybermed_soft_breaks_and_block_spacing_survive(cadence):
    html = emailer._render_email_html(LAYOUT_MD, report_key="cybermed", cadence=cadence)
    soup = dom(html)
    labels = soup.find("strong", string="Design:").parent
    assert len(labels.find_all("br")) == 2
    assert labels.find("strong", string="BOTTOM LINE:")
    assert soup.find("strong", string="Summary:").find_next_sibling("br")
    paragraphs = soup.find_all("p")
    assert any(p.get_text().startswith("Second paragraph") for p in paragraphs)
    assert all("margin:" in p["style"] and "line-height:" in p["style"] for p in paragraphs)
    assert soup.h1 and soup.h2 and soup.h3 and soup.hr
    assert soup.find("table", role="presentation")["width"] == "100%"
    assert "table-layout:fixed" in soup.find("table", role="presentation")["style"]
    assert soup.find("li", string="Missing assessments.").parent.parent.name == "li"
    assert soup.find("th", string="Field")["style"]


def test_layout_preserves_text_numbers_negation_links_and_list_structure():
    baseline = dom(emailer._safe_markdown_to_html(LAYOUT_MD))
    styled = dom(emailer._render_email_html(LAYOUT_MD, report_key="cybermed", cadence="daily"))
    text = lambda soup: re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    assert text(styled) == text(baseline)
    assert "Primary benefit remains uncertain; P=.37." in text(styled)
    assert [a["href"] for a in styled.find_all("a")] == [a["href"] for a in baseline.find_all("a")]
    assert len(styled.find_all("li")) == len(baseline.find_all("li"))
    assert not styled.find_all(["style", "script", "link", "img"])


@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly", "yearly"])
def test_cyberlurch_keeps_its_existing_html_and_soft_break_behavior(cadence):
    original = emailer._safe_markdown_to_html(LAYOUT_MD)
    expected = (emailer._style_cyberlurch_longform_html(original)
                if cadence in {"monthly", "yearly"} else original)
    assert emailer._render_email_html(LAYOUT_MD, report_key="cyberlurch", cadence=cadence) == expected
    assert not dom(original).find("br")


def test_actual_reporter_bottom_line_and_detail_list_stay_separate(monkeypatch):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    item = {"source": "pubmed", "id": "layout-1", "title": "Illustrative paper",
            "url": "https://example.test/paper", "bottom_line": "Primary benefit is unknown.",
            "cybermed_deep_dive": True}
    detail = "BOTTOM LINE: Primary benefit is unknown.\n\n- Study type: Illustrative comparison."
    md = reporter.to_markdown([item], "", {"layout-1": detail},
                             report_title="Cybermed Daily", report_language="en", report_mode="daily")
    baseline = dom(emailer._safe_markdown_to_html(md))
    baseline_paragraph = baseline.find("h2", string="Deep Dives").find_next("p")
    assert baseline_paragraph.find("strong", string="BOTTOM LINE:") and not baseline_paragraph.find("br")
    styled = dom(emailer._render_email_html(md, report_key="cybermed", cadence="daily"))
    paragraph = styled.find("h2", string="Deep Dives").find_next("p")
    assert paragraph.find("strong", string="BOTTOM LINE:")
    assert "Population unknown" not in paragraph.get_text()
    assert paragraph.find_next_sibling("ul")
    assert styled.find("strong", string="Study type:").find_parent("li")


def test_conversion_failure_keeps_escaped_readable_multiline_fallback():
    with patch.object(emailer, "markdown", side_effect=ValueError("private details")):
        html = emailer._render_email_html("<unsafe>\nSecond line & detail", report_key="cybermed", cadence="daily")
    soup = dom(html)
    assert soup.pre.get_text() == "<unsafe>\nSecond line & detail"
    assert "white-space:pre-wrap" in soup.pre["style"]
    assert not soup.find("unsafe")
    assert "private details" not in html


@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly", "yearly"])
def test_actual_mime_has_styled_html_plaintext_and_unchanged_receipt_identity(monkeypatch, tmp_path, cadence):
    messages = []

    class MockSMTP:
        def __init__(self, *args, **kwargs):
            pass

        def starttls(self, **kwargs):
            pass

        def login(self, *args):
            pass

        def sendmail(self, sender, recipients, payload):
            assert recipients == ["layout@example.test"]
            messages.append(message_from_string(payload))
            return {}

        def quit(self):
            pass

    monkeypatch.setattr(emailer.smtplib, "SMTP", MockSMTP)
    for key, value in {
        "EMAIL_MODE": "test", "SEND_EMAIL": "1", "REPORT_KEY": "cybermed", "REPORT_MODE": cadence,
        "SMTP_HOST": "smtp.example.test", "SMTP_USER": "sender@example.test", "SMTP_PASS": "dummy",
        "DELIVERY_RECIPIENT_HMAC_KEY": "offline-layout-key-at-least-thirty-two-bytes",
        "TEST_RECIPIENTS_CONFIG_JSON": json.dumps({"cybermed": {cadence: ["layout@example.test"]}}),
        "DELIVERY_JOURNAL_PATH": str(tmp_path / "journal.json"), "REPORT_DIR": str(tmp_path / "out"),
    }.items():
        monkeypatch.setenv(key, value)
    md = LAYOUT_MD + "\n## Run Metadata\n<!-- RUN_METADATA_ATTACHMENT_START -->\nprivate diagnostic\n<!-- RUN_METADATA_ATTACHMENT_END -->\n"
    clean, _, removed = emailer._extract_run_metadata_for_email(md)
    assert removed
    receipt = emailer.send_markdown("Cybermed layout example", md)
    retry = emailer.send_markdown("Cybermed layout example", md)
    assert len(messages) == 1
    assert receipt.status == retry.status == "accepted_by_smtp"
    assert receipt.message_id == retry.message_id
    assert receipt.content_version == hashlib.sha256(clean.encode()).hexdigest()
    parts = {part.get_content_type(): part.get_payload(decode=True).decode("utf-8")
             for part in messages[0].walk() if part.get_content_type() in {"text/plain", "text/html"}}
    assert dom(parts["text/html"]).find("table", role="presentation")
    assert dom(parts["text/html"]).find("br")
    assert "**Population:**" in parts["text/plain"]
    assert not any("private diagnostic" in body or "Run Metadata" in body for body in parts.values())
