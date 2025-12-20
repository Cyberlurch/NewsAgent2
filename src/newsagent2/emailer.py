# src/newsagent2/emailer.py
from __future__ import annotations

import html as html_module
import json
import os
import re
import smtplib
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Tuple

from markdown import markdown


def _clean_recipient_list(value: object) -> List[str]:
    """Normalize recipient input into a clean list of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []

    out: List[str] = []
    for x in value:
        s = str(x).strip()
        if s:
            out.append(s)
    return out


def _parse_recipients_from_json_env(report_key: str) -> Tuple[List[str], str]:
    """Load recipients from JSON environment variables.

    Priority:
      1) RECIPIENTS_JSON_<REPORT_KEY_UPPER>
      2) RECIPIENTS_JSON

    Accepted formats:
      - list: ["a@b.com", "c@d.com"]
      - dict: {"default": [...], "cyberlurch": [...], "cybermed": [...]} (keys are case-insensitive)

    Returns:
      (recipients, source_label)
    """
    rk = (report_key or "default").strip() or "default"
    rk_upper = rk.upper()
    rk_lower = rk.lower()

    specific_var = f"RECIPIENTS_JSON_{rk_upper}"
    raw = (os.getenv(specific_var) or "").strip()
    source = ""

    if raw:
        source = f"env:{specific_var}"
    else:
        raw = (os.getenv("RECIPIENTS_JSON") or "").strip()
        if raw:
            source = "env:RECIPIENTS_JSON"

    if not raw:
        return [], source

    try:
        data = json.loads(raw)
    except Exception:
        # Invalid JSON -> treat as absent and fall back to other sources.
        return [], source

    if isinstance(data, list):
        return _clean_recipient_list(data), source

    if isinstance(data, dict):
        # Try report-specific key (case-insensitive), then default.
        candidates = [rk_lower, rk, rk_upper, "default", "DEFAULT"]
        val = None
        for k in candidates:
            if k in data:
                val = data.get(k)
                break
        return _clean_recipient_list(val), source

    return [], source


def _parse_recipients_from_env() -> Tuple[List[str], str]:
    raw = (os.getenv("EMAIL_TO", "") or "").strip()
    if not raw:
        return [], ""
    return [x.strip() for x in raw.split(",") if x.strip()], "env:EMAIL_TO"


def _load_recipients_from_file(report_key: str) -> Tuple[List[str], str]:
    """Load recipients from data/recipients.json (optional local/private fallback)."""
    path = Path("data") / "recipients.json"
    if not path.exists():
        return [], "file:data/recipients.json"

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [], "file:data/recipients.json"

    if not isinstance(data, dict):
        return [], "file:data/recipients.json"

    rk = (report_key or "default").strip() or "default"
    rk_lower = rk.lower()

    candidates = [rk_lower, rk, rk.upper(), "default", "DEFAULT"]
    val = None
    for k in candidates:
        if k in data:
            val = data.get(k)
            break

    return _clean_recipient_list(val), "file:data/recipients.json"


def _get_recipients(report_key: str) -> Tuple[List[str], str]:
    # 1) JSON via Secrets (preferred)
    rec, src = _parse_recipients_from_json_env(report_key)
    if rec:
        return rec, src

    # 2) Backward compatible: EMAIL_TO (only safe if passed via Secrets)
    rec, src2 = _parse_recipients_from_env()
    if rec:
        return rec, src2

    # 3) Optional local/private fallback: file-based recipients
    rec, src3 = _load_recipients_from_file(report_key)
    return rec, (src or src2 or src3)

def _strip_details_tags(md_text: str) -> str:
    """Remove HTML <details>/<summary> tags while keeping readable text."""
    if not md_text:
        return ""

    def _strip_html_tags(text: str) -> str:
        return re.sub(r"<[^>]+>", "", text or "")

    def _details_repl(match: re.Match[str]) -> str:
        inner = match.group(1) or ""
        summary_match = re.search(r"<summary[^>]*>(.*?)</summary>", inner, flags=re.IGNORECASE | re.DOTALL)
        summary_text = _strip_html_tags(summary_match.group(1) if summary_match else "").strip() or "Run Metadata"
        heading = f"{summary_text} (collapsed in HTML email):"

        body = re.sub(r"<summary[^>]*>.*?</summary>", "", inner, flags=re.IGNORECASE | re.DOTALL)
        body = re.sub(r"</?pre[^>]*>", "", body, flags=re.IGNORECASE)
        body = _strip_html_tags(body).strip()

        return f"{heading}\n{body}\n" if body else f"{heading}\n"

    text = re.sub(r"<details[^>]*>(.*?)</details>", _details_repl, md_text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(
        r"<summary[^>]*>(.*?)</summary>",
        lambda m: f"{_strip_html_tags(m.group(1)).strip() or 'Run Metadata'}:\n",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(r"</?details[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</?pre[^>]*>", "", text, flags=re.IGNORECASE)
    text = _strip_html_tags(text)
    return text


def _extract_metadata_text(block: str) -> str:
    if not block:
        return ""

    pre_match = re.search(r"<pre[^>]*>(.*?)</pre>", block, flags=re.IGNORECASE | re.DOTALL)
    if pre_match:
        return html_module.unescape((pre_match.group(1) or "").strip())

    cleaned = re.sub(r"<[^>]+>", "", block)
    return html_module.unescape(cleaned.strip())


def _extract_run_metadata_for_email(md_body: str) -> Tuple[str, str, bool]:
    """Split out the Run Metadata block for attachment use in emails.

    Returns (markdown_without_metadata_block, metadata_text, metadata_removed).
    If no metadata block exists, returns the original markdown, empty text, and False.
    """

    if not md_body:
        return md_body, "", False

    original_trailing_newline = md_body.endswith("\n")
    working_body = md_body
    metadata_text = ""
    metadata_removed = False

    marker_pattern = re.compile(
        r"<!--\s*RUN_METADATA_ATTACHMENT_START\s*-->(.*?)<!--\s*RUN_METADATA_ATTACHMENT_END\s*-->",
        flags=re.IGNORECASE | re.DOTALL,
    )

    marker_match = marker_pattern.search(working_body)
    if marker_match:
        metadata_text = marker_match.group(1) or ""
        before = working_body[: marker_match.start()] or ""
        after = working_body[marker_match.end() :] or ""

        working_body = f"{before}{after.lstrip(' \n')}"
        metadata_removed = True

    lines = working_body.splitlines()
    start_idx = None
    for idx, line in enumerate(lines):
        if re.match(r"^##\s*Run Metadata\s*$", line.strip(), flags=re.IGNORECASE):
            start_idx = idx
            break

    if start_idx is not None:
        end_idx = len(lines)
        for j in range(start_idx + 1, len(lines)):
            if re.match(r"^##\s+", lines[j]):
                end_idx = j
                break

        metadata_block = "\n".join(lines[start_idx:end_idx])
        if re.search(r"run metadata", metadata_block, flags=re.IGNORECASE):
            new_lines = lines[:start_idx] + lines[end_idx:]
            working_body = "\n".join(new_lines)
            if not metadata_text:
                metadata_text = _extract_metadata_text(metadata_block)
            metadata_removed = True

    if not metadata_removed:
        details_match = re.search(
            r"<details[^>]*>.*?Run Metadata.*?</details>", working_body, flags=re.IGNORECASE | re.DOTALL
        )
        if not details_match:
            return working_body, metadata_text, False

        metadata_block = details_match.group(0)

        before = working_body[: details_match.start()]
        after = working_body[details_match.end() :]

        if before and not before.endswith("\n"):
            before += "\n"

        working_body = f"{before}{after.lstrip(' \n')}"
        if not metadata_text:
            metadata_text = _extract_metadata_text(metadata_block)
        metadata_removed = True

    if metadata_removed and original_trailing_newline and not working_body.endswith("\n"):
        working_body += "\n"

    return working_body, metadata_text.strip(), metadata_removed


def _safe_markdown_to_html(md_body: str) -> str:
    """Convert Markdown to HTML safely, falling back to a preformatted block.

    - Tries minimal extension sets that work with current Markdown versions.
    - Logs concise stack traces on failures (without dumping the whole report).
    - Never raises; always returns HTML (escaped as <pre> if conversion fails).
    """

    attempts = [
        {"extensions": ["extra", "sane_lists", "md_in_html"]},
        {"extensions": ["extra", "sane_lists"]},
        {"extensions": ["extra"]},
    ]

    for attempt in attempts:
        try:
            return markdown(
                md_body,
                extensions=attempt["extensions"],
                output_format="html5",
            )
        except Exception as exc:
            print(
                f"[email] WARN: markdown conversion failed "
                f"(extensions={attempt['extensions']}): {exc!r}"
            )
            print(traceback.format_exc())

    escaped = html_module.escape(md_body or "")
    print("[email] WARN: falling back to <pre> HTML rendering for email body.")
    return f"<pre>{escaped}</pre>"


def send_markdown(subject: str, md_body: str) -> None:
    """Send the Markdown report as email (plain + HTML).

    Env vars:
      SEND_EMAIL = "1" -> send
                  "0" -> disable
                  empty/unset -> defaults to "1"

      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
      EMAIL_FROM

      Recipients (preferred):
        RECIPIENTS_JSON_<REPORT_KEY_UPPER>  (JSON list or dict)
        RECIPIENTS_JSON                    (fallback)

      Backward compatible:
        EMAIL_TO (comma-separated)
        data/recipients.json
    """
    send_flag = (os.getenv("SEND_EMAIL") or "1").strip()
    print(f"[email] SEND_EMAIL={send_flag!r}")
    if send_flag != "1":
        print("[email] SEND_EMAIL != '1' -> email sending disabled.")
        return

    report_key = (os.getenv("REPORT_KEY", "default") or "default").strip() or "default"

    host = os.getenv("SMTP_HOST")
    port_str = os.getenv("SMTP_PORT", "587")
    user = os.getenv("SMTP_USER")
    pw = os.getenv("SMTP_PASS")
    from_addr = os.getenv("EMAIL_FROM", user or "newsagent@localhost")

    to_list, recipients_source = _get_recipients(report_key)

    try:
        port = int(port_str)
    except ValueError:
        print(f"[email] Invalid SMTP_PORT={port_str!r} -> abort.")
        return

    if not (host and port and user and pw and to_list):
        print("[email] SMTP configuration incomplete -> NOT sending.")
        print(
            "[email] Missing fields:",
            {
                "host": bool(host),
                "port": bool(port),
                "user": bool(user),
                "pw": bool(pw),
                "recipients_count": len(to_list),
                "report_key": report_key,
                "recipients_source": recipients_source or "(none)",
            },
        )
        return

    # Keep the HTML part as close to the original Markdown as possible.
    # We only extract the Run Metadata for (a) a plaintext-friendly replacement
    # and (b) attaching the full metadata as a .txt file.
    md_without_metadata, metadata_text, metadata_removed = _extract_run_metadata_for_email(md_body)

    try:
        html_source = md_without_metadata if metadata_removed else md_body
        html = _safe_markdown_to_html(html_source)
    except Exception as exc:  # pragma: no cover - ultra-safety guard
        print(f"[email] WARN: unexpected markdown conversion failure: {exc!r}")
        print(traceback.format_exc())
        html = f"<pre>{html_module.escape((html_source or ''))}</pre>"

    if "<details" in (md_body or "") and "<details" not in (html or ""):
        print("[email] WARN: '<details>' did not survive markdown->HTML conversion; metadata may not be collapsible.")

    plain = _strip_details_tags(md_without_metadata)

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)

    alternative = MIMEMultipart("alternative")
    alternative.attach(MIMEText(plain, "plain", "utf-8"))
    alternative.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alternative)

    if metadata_text:
        safe_report_key = re.sub(r"[^a-z0-9_-]+", "_", (report_key or "report").strip().lower())
        if not safe_report_key:
            safe_report_key = "report"
        filename = f"{safe_report_key}_run_metadata_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.txt"
        attachment = MIMEText(metadata_text, "plain", "utf-8")
        attachment.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(attachment)

    try:
        # Do not log host/from/to addresses.
        print(
            f"[email] Sending email (report_key={report_key}, recipients_count={len(to_list)}, source={recipients_source})"
        )
        with smtplib.SMTP(host, port, timeout=60) as s:
            s.starttls()
            s.login(user, pw)
            s.sendmail(from_addr, to_list, msg.as_string())
        print("[email] Email sent.")
    except Exception as e:
        print(f"[email] ERROR during email send: {e!r}")
