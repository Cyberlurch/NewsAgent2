import pathlib
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import reporter


def test_pubmed_compact_line_and_top_pick_rendered():
    items = [{
        "source": "pubmed", "id": "p1", "title": "Paper", "url": "https://x",
        "journal": "J", "evidence_strength_label": "b", "clinical_relevance_1_5": 4,
        "practice_change_potential_1_5": 3, "text_confidence_label": "high", "top_pick": True,
        "bottom_line": "Useful"
    }]
    md = reporter.to_markdown(items, "", {}, report_title="Cybermed Daily", report_language="en", report_mode="daily")
    assert "⭐ Top pick · Evidence B · Relevance 4/5 · Practice impact 3/5 · Confidence High" in md


def test_compact_lines_omit_missing_without_dangling_separators():
    items = [
        {"source": "pubmed", "id": "p1", "title": "Paper", "clinical_relevance_1_5": 4, "bottom_line": "x"},
        {"source": "foamed", "id": "f1", "title": "Post", "url": "https://f", "foamed_source": "S", "text_confidence_label": "moderate", "bottom_line": "y", "published_at": datetime.now(timezone.utc)},
    ]
    md = reporter.to_markdown(items, "", {}, report_title="Cybermed Daily", report_language="en", report_mode="daily")
    assert "Relevance 4/5" in md
    assert "Source quality" not in md
    assert "· ·" not in md
    assert "Confidence Moderate" in md


def test_foamed_compact_line_rendered_and_top_pick_false_not_shown():
    items = [{
        "source": "foamed", "id": "f1", "title": "Post", "url": "https://f", "foamed_source": "CoreSrc",
        "source_quality_label": "core", "clinical_usefulness_1_5": 4, "practice_relevance_1_5": 3,
        "text_confidence_label": "high", "bottom_line": "Useful", "published_at": datetime.now(timezone.utc)
    }]
    md = reporter.to_markdown(items, "", {}, report_title="Cybermed Daily", report_language="en", report_mode="daily")
    assert "Source quality Core · Usefulness 4/5 · Practice relevance 3/5 · Confidence High" in md
    assert "⭐ Top pick" not in md


def test_empty_report_has_no_badge_lines():
    md = reporter.to_markdown([], "", {}, report_title="Cybermed Daily", report_language="en", report_mode="daily")
    assert "No new papers selected" in md
    assert "No new FOAMed posts" in md
    assert "Evidence " not in md
    assert "Source quality " not in md
