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
from typing import Dict, List, Tuple, Union

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


def _resolve_recipients_from_mapping(
    data: Union[Dict[str, object], List[str], object],
    report_key: str,
    report_mode: str,
) -> List[str]:
    """Resolve recipients from nested/flattened mappings or simple lists.

    Supported shapes:
      - list: ["a@b.com", "c@d.com"]
      - dict: nested {"cybermed": {"daily": [...]}} or flattened {"cybermed_daily": [...]}
      - dict: per-report {"cybermed": [...]} or {"default": [...]}
    """
    if isinstance(data, list):
        return _clean_recipient_list(data)

    if not isinstance(data, dict):
        return []

    key_map: Dict[str, object] = {str(k).lower(): v for k, v in data.items()}
    rk = (report_key or "default").strip().lower() or "default"
    mode = (report_mode or "").strip().lower()

    def _from_nested(obj: object) -> List[str]:
        if isinstance(obj, dict):
            mode_map = {str(k).lower(): v for k, v in obj.items()}
            if mode and mode in mode_map:
                return _clean_recipient_list(mode_map.get(mode))
            for default_key in ("default", "all"):
                if default_key in mode_map:
                    return _clean_recipient_list(mode_map.get(default_key))
            return []
        return _clean_recipient_list(obj)

    if mode:
        for sep in ("_", "-", ""):
            flat_key = f"{rk}{sep}{mode}"
            if flat_key in key_map:
                return _clean_recipient_list(key_map.get(flat_key))

    if rk in key_map:
        recipients = _from_nested(key_map.get(rk))
        if recipients:
            return recipients

    for default_key in ("default", "all"):
        if default_key in key_map:
            recipients = _from_nested(key_map.get(default_key))
            if recipients:
                return recipients

    return []


def _parse_recipients_json_var(var_name: str, report_key: str, report_mode: str) -> Tuple[List[str], str]:
    raw = (os.getenv(var_name) or "").strip()
    if not raw:
        return [], ""

    try:
        data = json.loads(raw)
    except Exception:
        return [], f"env:{var_name}"

    recipients = _resolve_recipients_from_mapping(data, report_key, report_mode)
    return recipients, f"env:{var_name}"


def _parse_recipients_from_env() -> Tuple[List[str], str]:
    raw = (os.getenv("EMAIL_TO", "") or "").strip()
    if not raw:
        return [], ""
    return [x.strip() for x in raw.split(",") if x.strip()], "env:EMAIL_TO"


def _load_recipients_from_file(report_key: str, report_mode: str) -> Tuple[List[str], str]:
    """Load recipients from data/recipients.json (optional local/private fallback)."""
    path = Path("data") / "recipients.json"
    if not path.exists():
        return [], "file:data/recipients.json"

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [], "file:data/recipients.json"

    recipients = _resolve_recipients_from_mapping(data, report_key, report_mode)
    return recipients, "file:data/recipients.json"


def _get_recipients(report_key: str, report_mode: str) -> Tuple[List[str], str]:
    rk = (report_key or "default").strip()
    mode = (report_mode or "").strip()
    rk_upper = rk.upper() or "DEFAULT"
    mode_upper = mode.upper()

    # Preferred: combined config for all reports/modes
    raw_config = (os.getenv("RECIPIENTS_CONFIG_JSON") or "").strip()
    if raw_config:
        try:
            data = json.loads(raw_config)
            recipients = _resolve_recipients_from_mapping(data, rk, mode)
            return recipients, "env:RECIPIENTS_CONFIG_JSON"
        except Exception:
            # Fall back only when config is missing or invalid
            pass

    # 1) Mode-specific env var
    if mode_upper:
        rec, src = _parse_recipients_json_var(f"RECIPIENTS_JSON_{rk_upper}_{mode_upper}", rk, mode)
        if rec:
            return rec, src

    # 2) Per-report env var
    rec, src = _parse_recipients_json_var(f"RECIPIENTS_JSON_{rk_upper}", rk, mode)
    if rec:
        return rec, src

    # 3) Combined config JSON
    # (handled above as the highest-priority source; reach here only if missing/invalid)

    # 4) Generic env (nested or flattened)
    rec, src = _parse_recipients_json_var("RECIPIENTS_JSON", rk, mode)
    if rec:
        return rec, src

    # 5) Optional file fallback
    rec, src = _load_recipients_from_file(rk, mode)
    if rec:
        return rec, src

    # 6) EMAIL_TO (legacy)
    rec, src = _parse_recipients_from_env()
    if rec:
        return rec, src

    return [], src or "env:EMAIL_TO"

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

        replacement = "Run metadata is attached as a text file."
        prefix = before
        if prefix and not prefix.endswith("\n"):
            prefix += "\n"
        suffix = after.lstrip(" \n")
        working_body = f"{prefix}{replacement}"
        if suffix:
            if not working_body.endswith("\n"):
                working_body += "\n"
            working_body += suffix
        metadata_removed = True
    else:
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
    report_mode = (os.getenv("REPORT_MODE", "daily") or "daily").strip() or "daily"

    host = os.getenv("SMTP_HOST")
    port_str = os.getenv("SMTP_PORT", "587")
    user = os.getenv("SMTP_USER")
    pw = os.getenv("SMTP_PASS")
    from_addr = os.getenv("EMAIL_FROM", user or "newsagent@localhost")

    to_list, recipients_source = _get_recipients(report_key, report_mode)

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

    placeholder_block = ""
    if (
        metadata_text
        and metadata_removed
        and report_key.lower() != "cybermed"
        and "Run metadata is attached as a text file." not in (md_without_metadata or "")
    ):
        placeholder_block = "\n\n_Run metadata is attached as a .txt file._\n"

    if placeholder_block:
        md_without_metadata = f"{md_without_metadata.rstrip()}{placeholder_block}"

    try:
        html_source = md_without_metadata if metadata_removed or placeholder_block else md_body
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
