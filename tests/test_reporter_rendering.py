import os
import pathlib
import sys
from unittest import TestCase
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import reporter


class CybermedMetadataRenderingTests(TestCase):
    def test_cybermed_readable_report_has_no_inline_metadata_markers(self):
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

        self.assertNotIn("Run Metadata Summary", rendered)
        self.assertNotIn("## Run Metadata", rendered)
        self.assertNotIn("Run Metadata (click to expand)", rendered)
        self.assertNotIn("<!-- RUN_METADATA_ATTACHMENT_START -->", rendered)
        self.assertNotIn("<!-- RUN_METADATA_ATTACHMENT_END -->", rendered)


if __name__ == "__main__":
    import unittest

    unittest.main()
