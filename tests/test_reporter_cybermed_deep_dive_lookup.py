import os
import pathlib
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest import TestCase
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import reporter  # noqa: E402


class CybermedDeepDiveDetailLookupTests(TestCase):
    def test_pubmed_deep_dive_uses_source_prefixed_detail_key(self):
        published_at = datetime(2025, 1, 5, 12, 0, tzinfo=ZoneInfo("Europe/Stockholm"))
        items = [
            {
                "id": "12345",
                "source": "pubmed",
                "channel": "PubMed: Anesthesiology",
                "journal": "Anesthesiology",
                "year": 2025,
                "title": "Frequency and Management of Maternal Peripartum Cardiac Arrest",
                "url": "https://example.org/x",
                "published_at": published_at,
                "cybermed_deep_dive": True,
            }
        ]
        details_by_id = {
            "pubmed:12345": (
                "BOTTOM LINE: ...\n\n"
                "Study type: cohort\n"
                "Population/setting: not reported\n"
                "Intervention/exposure & comparator: not reported\n"
                "Primary endpoints: not reported\n"
                "Key results: not reported\n"
                "Limitations:\n"
                "- not reported\n"
                "Why this matters: not reported\n"
            )
        }

        with patch.dict(os.environ, {"REPORT_KEY": "cybermed"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id=details_by_id,
                report_title="Cybermed Weekly",
                report_language="en",
                report_mode="weekly",
            )

        self.assertIn("## Deep Dives", md)
        self.assertIn("Study type:", md)


    def test_deep_dive_bottom_line_label_is_bold(self):
        items = [
            {
                "id": "12345",
                "source": "pubmed",
                "channel": "PubMed: Anesthesiology",
                "journal": "Anesthesiology",
                "year": 2025,
                "title": "Frequency and Management of Maternal Peripartum Cardiac Arrest",
                "url": "https://example.org/x",
                "cybermed_deep_dive": True,
            }
        ]
        details_by_id = {
            "pubmed:12345": (
                "BOTTOM LINE: plain label\n\n"
                "Study type: cohort\n"
                "Population/setting: not reported\n"
                "Intervention/exposure & comparator: not reported\n"
                "Primary endpoints: not reported\n"
                "Key results: not reported\n"
                "Limitations:\n"
                "- not reported\n"
                "Why this matters: not reported\n"
            )
        }

        with patch.dict(os.environ, {"REPORT_KEY": "cybermed"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id=details_by_id,
                report_title="Cybermed Weekly",
                report_language="en",
                report_mode="weekly",
            )

        self.assertIn("**BOTTOM LINE:** plain label", md)

if __name__ == "__main__":
    import unittest

    unittest.main()
