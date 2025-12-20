import os
import sys
import unittest
from pathlib import Path


# Allow running tests without installing the package.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


try:
    import markdown  # noqa: F401
    HAS_MARKDOWN = True
except Exception:
    HAS_MARKDOWN = False


class TestEmailMetadata(unittest.TestCase):
    def setUp(self) -> None:
        # Ensure the module can resolve relative paths in a CI runner.
        os.environ.setdefault("REPORT_KEY", "cybermed")

    @unittest.skipUnless(HAS_MARKDOWN, "python-markdown is not installed")
    def test_details_survives_markdown_to_html(self):
        from newsagent2.emailer import _safe_markdown_to_html

        md = """
<h1>Cybermed Report</h1>

## Run Metadata
<details>
<summary><b>Run Metadata (click to expand)</b></summary>
<pre>
- example_key: 123
</pre>
</details>
""".strip()

        html = _safe_markdown_to_html(md)
        self.assertIn("<details", html)
        self.assertIn("<summary", html)

    @unittest.skipUnless(HAS_MARKDOWN, "python-markdown is not installed")
    def test_plaintext_replaces_metadata_with_attachment_notice(self):
        from newsagent2.emailer import _extract_run_metadata_for_email

        md = """
# Cybermed Report

## Run Metadata
<details>
<summary><b>Run Metadata (click to expand)</b></summary>
<pre>
- a: 1
- b: 2
</pre>
</details>

## Body
Hello.
""".strip()

        md_plain, meta, markers_found = _extract_run_metadata_for_email(md)
        self.assertTrue(meta.strip().startswith("- a:"))
        self.assertIn("Run metadata is attached", md_plain)
        self.assertNotIn("<details", md_plain)
        self.assertFalse(markers_found)


if __name__ == "__main__":
    unittest.main()
