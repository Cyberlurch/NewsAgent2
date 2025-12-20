import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import emailer


class MarkdownConversionTests(unittest.TestCase):
    def test_details_and_lists_render_without_error(self):
        md = """# Report\n\n<details markdown=\"1\">\n<summary>Run Metadata (click to expand)</summary>\n\n- bullet one\n- bullet two\n\n`inline code`\n</details>\n"""
        html = emailer._safe_markdown_to_html(md)
        self.assertIsInstance(html, str)
        self.assertIn("Run Metadata", html)
        self.assertIn("<details", html)

    def test_markdown_conversion_falls_back_to_pre_on_error(self):
        md = "````"  # intentionally odd markdown
        with patch("newsagent2.emailer.markdown", side_effect=Exception("boom")):
            html = emailer._safe_markdown_to_html(md)
        self.assertTrue(html.startswith("<pre>"))
        self.assertIn("````", html)


class RunMetadataExtractionTests(unittest.TestCase):
    def test_marker_based_attachment_and_summary_notice(self):
        md = (
            "# Report\n\n"
            "## Run Metadata\n\n"
            "### Run Metadata Summary\n"
            "- Lookback window: 24h\n"
            "- PubMed items (overview + details): 2\n"
            "- FOAMed items: 1\n"
            "- FOAMed source health: ok_rss=1, ok_html=0, blocked_403=0, not_found_404=0, parse_failed=0, other=0\n\n"
            "<!-- RUN_METADATA_ATTACHMENT_START -->\n"
            "first line\nsecond line\n"
            "<!-- RUN_METADATA_ATTACHMENT_END -->\n"
        )

        new_md, meta, markers = emailer._extract_run_metadata_for_email(md)
        html = emailer._safe_markdown_to_html(new_md)
        plain = emailer._strip_details_tags(new_md)

        self.assertIn("Run Metadata Summary", new_md)
        self.assertNotIn("<details", html)
        self.assertIn("Run metadata is attached as a text file.", html)
        self.assertIn("Lookback window: 24h", html)
        self.assertIn("Run metadata is attached as a text file.", plain)
        self.assertEqual("first line\nsecond line", meta)
        self.assertTrue(markers)

    def test_metadata_block_becomes_attachment_placeholder(self):
        md = (
            "# Report\n\n"
            "## Run Metadata\n"
            "<details markdown=\"1\">\n"
            "  <summary>Run Metadata (click to expand)</summary>\n\n"
            "  <pre>\n"
            "first line\nsecond line\n"
            "  </pre>\n"
            "</details>\n\n"
            "## Next\nContent\n"
        )

        new_md, meta, markers_found = emailer._extract_run_metadata_for_email(md)

        self.assertIn("Run metadata is attached as a text file.", new_md)
        self.assertNotIn("<details", new_md)
        self.assertEqual("first line\nsecond line", meta)
        self.assertFalse(markers_found)

    def test_no_metadata_block_returns_original(self):
        md = "# Report\n\nNothing to see here.\n"
        new_md, meta, markers_found = emailer._extract_run_metadata_for_email(md)

        self.assertEqual(md, new_md)
        self.assertEqual("", meta)
        self.assertFalse(markers_found)


if __name__ == "__main__":
    unittest.main()
