import os
import pathlib
import sys
from unittest import TestCase
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import reporter


class CybermedMetadataRenderingTests(TestCase):
    def test_cybermed_run_metadata_section_is_minimal_but_contains_markers(self):
        overview_md = "**Cybermed report metadata**\nLookback window: 24 hours\n"
        with patch.dict(
            os.environ, {"REPORT_KEY": "cybermed", "REPORT_MODE": "daily"}, clear=False
        ):
            rendered = reporter.to_markdown(
                [],
                overview_md,
                {},
                report_title="Cybermed Daily",
                report_language="en",
                report_mode="daily",
            )

        self.assertIn("## Run Metadata", rendered)
        self.assertNotIn("Run Metadata Summary", rendered)
        self.assertIn("<!-- RUN_METADATA_ATTACHMENT_START -->", rendered)
        self.assertIn("<!-- RUN_METADATA_ATTACHMENT_END -->", rendered)


if __name__ == "__main__":
    import unittest

    unittest.main()
