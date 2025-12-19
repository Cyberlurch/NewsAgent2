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


if __name__ == "__main__":
    unittest.main()
