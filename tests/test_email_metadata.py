import os
import sys
import unittest
import unittest.mock
from email import message_from_string
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
        os.environ.setdefault("REPORT_MODE", "daily")

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
    def test_comment_wrapped_run_metadata_is_removed_and_attached(self):
        from newsagent2.emailer import _extract_run_metadata_for_email

        md = """
Intro
<!-- RUN_METADATA_ATTACHMENT_START
- first: 1
- second: 2
RUN_METADATA_ATTACHMENT_END -->

Body continues.
""".strip()

        cleaned, meta, removed = _extract_run_metadata_for_email(md)
        self.assertTrue(removed)
        self.assertIn("first: 1", meta)
        self.assertIn("Body continues.", cleaned)
        self.assertNotIn("second: 2", cleaned)
        self.assertNotIn("RUN_METADATA_ATTACHMENT_START", cleaned)

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
        self.assertNotIn("Run Metadata", md_plain)
        self.assertNotIn("Run metadata is attached", md_plain)
        self.assertNotIn("<details", md_plain)
        self.assertTrue(markers_found)

    @unittest.skipUnless(HAS_MARKDOWN, "python-markdown is not installed")
    def test_cybermed_email_omits_placeholder_but_attaches_metadata(self):
        from newsagent2 import emailer

        class DummySMTP:
            last_instance = None

            def __init__(self, *args, **kwargs):
                self.sent = []
                DummySMTP.last_instance = self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def starttls(self):
                return None

            def login(self, user, pw):
                return None

            def sendmail(self, from_addr, to_addrs, msg_data):
                self.sent.append(msg_data)

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

        with unittest.mock.patch("smtplib.SMTP", DummySMTP):
            with unittest.mock.patch.dict(
                os.environ,
                {
                    "SEND_EMAIL": "1",
                    "REPORT_KEY": "cybermed",
                    "REPORT_MODE": "daily",
                    "SMTP_HOST": "localhost",
                    "SMTP_PORT": "25",
                    "SMTP_USER": "user",
                    "SMTP_PASS": "pass",
                    "EMAIL_FROM": "from@example.com",
                    "RECIPIENTS_JSON_CYBERMED": "[\"to@example.com\"]",
                },
                clear=False,
            ):
                emailer.send_markdown("Cybermed Report", md)
                dummy = DummySMTP.last_instance
                self.assertIsNotNone(dummy)
                self.assertTrue(dummy.sent)
                payload = dummy.sent[0]
                msg = message_from_string(payload)

                plain_parts = [
                    part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8")
                    for part in msg.walk()
                    if part.get_content_type() == "text/plain" and part.get_content_disposition() != "attachment"
                ]
                plain_body = "\n".join(plain_parts)

                self.assertNotIn("Run metadata is attached", plain_body)

                attachment_names = [
                    part.get_filename() for part in msg.walk() if part.get_content_disposition() == "attachment"
                ]
                self.assertTrue(any(name and "cybermed_run_metadata_" in name for name in attachment_names))

    @unittest.skipUnless(HAS_MARKDOWN, "python-markdown is not installed")
    def test_non_cybermed_email_keeps_placeholder(self):
        from newsagent2 import emailer

        class DummySMTP:
            last_instance = None

            def __init__(self, *args, **kwargs):
                self.sent = []
                DummySMTP.last_instance = self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def starttls(self):
                return None

            def login(self, user, pw):
                return None

            def sendmail(self, from_addr, to_addrs, msg_data):
                self.sent.append(msg_data)

        md = """
# Cyberlurch Report

## Run Metadata
<details>
<summary><b>Run Metadata (click to expand)</b></summary>
<pre>
- c: 3
- d: 4
</pre>
</details>

## Body
Hi.
""".strip()

        with unittest.mock.patch("smtplib.SMTP", DummySMTP):
            with unittest.mock.patch.dict(
                os.environ,
                {
                    "SEND_EMAIL": "1",
                    "REPORT_KEY": "cyberlurch",
                    "REPORT_MODE": "weekly",
                    "SMTP_HOST": "localhost",
                    "SMTP_PORT": "25",
                    "SMTP_USER": "user",
                    "SMTP_PASS": "pass",
                    "EMAIL_FROM": "from@example.com",
                    "RECIPIENTS_JSON_CYBERLURCH": "[\"to@example.com\"]",
                },
                clear=False,
            ):
                emailer.send_markdown("Cyberlurch Report", md)
                dummy = DummySMTP.last_instance
                self.assertIsNotNone(dummy)
                self.assertTrue(dummy.sent)
                payload = dummy.sent[0]
                msg = message_from_string(payload)

                plain_parts = [
                    part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8")
                    for part in msg.walk()
                    if part.get_content_type() == "text/plain" and part.get_content_disposition() != "attachment"
                ]
                plain_body = "\n".join(plain_parts)

                self.assertIn("Run metadata is attached", plain_body)

                attachment_names = [
                    part.get_filename() for part in msg.walk() if part.get_content_disposition() == "attachment"
                ]
                self.assertTrue(any(name and "cyberlurch_run_metadata_" in name for name in attachment_names))


if __name__ == "__main__":
    unittest.main()
