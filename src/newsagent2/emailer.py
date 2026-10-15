# src/newsagent2/emailer.py
from __future__ import annotations

import html as html_module
import json
import os
import re
import smtplib
import ssl
import hashlib
import hmac
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Tuple, Union

from markdown import markdown

from .cybermed_quality import assert_no_generation_error_text
from .delivery import Coordinator, DeliveryResult, DeliveryError
from .run_context import email_mode, send_enabled
from .store_io import StoreError


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


def _dedupe_preserve(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _get_recipients_union(report_key: str) -> Tuple[List[str], str]:
    rk = (report_key or "default").strip()
    raw_config = (os.getenv("RECIPIENTS_CONFIG_JSON") or "").strip()
    if raw_config:
        try:
            data = json.loads(raw_config)
            if isinstance(data, dict):
                block = data.get(rk) or data.get("default") or data.get("all")
                if isinstance(block, dict):
                    all_modes = []
                    for key in ("daily", "weekly", "monthly", "all", "default"):
                        val = block.get(key)
                        if val:
                            all_modes.extend(_clean_recipient_list(val))
                    deduped = _dedupe_preserve(all_modes)
                    if deduped:
                        return deduped, "env:RECIPIENTS_CONFIG_JSON"
                elif isinstance(block, list):
                    deduped = _dedupe_preserve(_clean_recipient_list(block))
                    if deduped:
                        return deduped, "env:RECIPIENTS_CONFIG_JSON"
        except Exception:
            pass

    recipients: List[str] = []
    sources: List[str] = []
    for mode in ("daily", "weekly", "monthly"):
        rec, src = _get_recipients(rk, mode)
        if rec:
            recipients.extend(rec)
            if src and src not in sources:
                sources.append(src)

    return _dedupe_preserve(recipients), "union:" + ",".join(sources) if sources else "union"

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
    """Remove Run Metadata blocks from outgoing email markdown.

    Returns (markdown_without_metadata_block, metadata_text, metadata_removed).
    If no metadata block exists, returns the original markdown, empty text, and False.
    """

    if not md_body:
        return md_body, "", False

    original_trailing_newline = md_body.endswith("\n")
    working_body = md_body
    metadata_text = ""
    metadata_removed = False

    def _splice_out_block(match: re.Match[str], captured: str) -> None:
        nonlocal working_body, metadata_text, metadata_removed
        before = working_body[: match.start()] or ""
        after = working_body[match.end() :] or ""

        prefix = before
        if prefix and not prefix.endswith("\n"):
            prefix += "\n"
        suffix = after.lstrip(" \n")
        working_body = prefix
        if suffix:
            working_body += suffix

        if captured and not metadata_text:
            cleaned = re.sub(r"^\\s*-->\\s*", "", captured)
            cleaned = re.sub(r"\\s*<!--\\s*$", "", cleaned)
            metadata_text = cleaned.strip()
        metadata_removed = True

    comment_block_pattern = re.compile(
        r"<!--\s*RUN_METADATA_ATTACHMENT_START\b(.*?)RUN_METADATA_ATTACHMENT_END\s*-->",
        flags=re.IGNORECASE | re.DOTALL,
    )

    marker_pattern = re.compile(
        r"<!--\s*RUN_METADATA_ATTACHMENT_START\s*-->(.*?)<!--\s*RUN_METADATA_ATTACHMENT_END\s*-->",
        flags=re.IGNORECASE | re.DOTALL,
    )

    comment_match = comment_block_pattern.search(working_body)
    marker_match = marker_pattern.search(working_body)
    if marker_match:
        _splice_out_block(marker_match, (marker_match.group(1) or "").strip())
    elif comment_match:
        _splice_out_block(comment_match, (comment_match.group(1) or "").strip())
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

    if metadata_removed:
        working_body = re.sub(
            r"(?im)^[ \t]*##[ \t]*Run Metadata[ \t]*$\n?",
            "",
            working_body,
        )
        working_body = re.sub(r"\n{3,}", "\n\n", working_body)

    if metadata_removed and original_trailing_newline and not working_body.endswith("\n"):
        working_body += "\n"

    return working_body, metadata_text.strip(), metadata_removed


def _safe_markdown_to_html(md_body: str, *, preserve_line_breaks: bool = False) -> str:
    """Convert Markdown to HTML safely, falling back to a preformatted block.

    - Tries minimal extension sets that work with current Markdown versions.
    - Logs safe failure categories without exception text or report contents.
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
                extensions=attempt["extensions"] + (["nl2br"] if preserve_line_breaks else []),
                output_format="html5",
            )
        except Exception as exc:
            print(
                f"[email] WARN: markdown conversion failed "
                f"(extensions={attempt['extensions']}, category=rendering_failure)"
            )

    escaped = html_module.escape(md_body or "")
    print("[email] WARN: falling back to <pre> HTML rendering for email body.")
    return f"<pre>{escaped}</pre>"


def _style_cyberlurch_longform_html(html: str) -> str:
    """Apply the shared, restrained Cyberlurch long-form email presentation."""
    styled = html
    replacements = {
        "<h1>": '<h1 style="font-size:30px;line-height:1.2;margin:0 0 18px;">',
        "<h2>": '<h2 style="font-size:22px;line-height:1.3;margin:32px 0 14px;padding-top:16px;border-top:1px solid #d9dde3;">',
        "<h3>": '<h3 style="font-size:18px;line-height:1.35;margin:24px 0 10px;">',
        "<p>": '<p style="margin:0 0 16px;">',
        "<hr>": '<hr style="border:0;border-top:1px solid #d9dde3;margin:28px 0;">',
        "<a ": '<a style="color:#1558a6;text-decoration:underline;" ',
    }
    for original, replacement in replacements.items():
        styled = styled.replace(original, replacement)
    return (
        '<div style="background:#ffffff;color:#202124;font-family:-apple-system,BlinkMacSystemFont,'
        "&quot;Segoe UI&quot;,Arial,sans-serif;font-size:16px;line-height:1.55;"
        'max-width:700px;margin:0 auto;padding:20px 18px;">'
        + styled + "</div>"
    )


# Compatibility for callers and tests which used the original Monthly name.
_style_cyberlurch_monthly_html = _style_cyberlurch_longform_html


def _style_cybermed_html(html: str) -> str:
    """Give Cybermed explicit spacing without relying on mail-client defaults.

    Styles are inline because many email clients discard document stylesheets.
    This only changes presentation; source text, links and list structure stay
    intact. No external fonts, images or stylesheets are requested.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    styles = {
        "h1": "font-size:28px;line-height:1.25;margin:0 0 12px;font-weight:700;",
        "h2": "font-size:23px;line-height:1.3;margin:32px 0 16px;padding-bottom:10px;border-bottom:1px solid #d9e2ec;",
        "h3": "font-size:20px;line-height:1.4;margin:26px 0 12px;",
        "h4": "font-size:17px;line-height:1.4;margin:22px 0 12px;",
        "h5": "font-size:16px;line-height:1.4;margin:20px 0 10px;",
        "h6": "font-size:16px;line-height:1.4;margin:18px 0 10px;",
        "p": "font-size:16px;line-height:1.6;margin:0 0 16px;",
        "ul": "margin:0 0 18px;padding-left:24px;",
        "ol": "margin:0 0 18px;padding-left:24px;",
        "li": "font-size:16px;line-height:1.6;margin:0 0 8px;",
        "a": "color:#145c91;text-decoration:underline;overflow-wrap:break-word;word-wrap:break-word;",
        "hr": "border:0;border-top:1px solid #d9e2ec;height:0;margin:28px 0;",
        "blockquote": "border-left:3px solid #d9e2ec;margin:0 0 18px;padding:0 0 0 16px;",
        "table": "border-collapse:collapse;width:100%;margin:0 0 18px;",
        "th": "border:1px solid #d9e2ec;padding:8px;text-align:left;vertical-align:top;",
        "td": "border:1px solid #d9e2ec;padding:8px;vertical-align:top;",
        "pre": "white-space:pre-wrap;overflow-wrap:break-word;word-wrap:break-word;font-size:14px;line-height:1.6;margin:0 0 16px;",
        "code": "font-family:Consolas,monospace;font-size:14px;",
    }
    for tag in soup.find_all(list(styles)):
        previous = str(tag.get("style") or "").rstrip("; ")
        tag["style"] = (previous + ";" if previous else "") + styles[tag.name]

    # A presentation table works without flex/grid or a retained <head> block.
    wrapper = soup.new_tag("table", attrs={
        "role": "presentation", "width": "100%", "align": "center",
        "cellpadding": "0", "cellspacing": "0", "border": "0",
        "style": "width:100%;max-width:720px;table-layout:fixed;border-collapse:collapse;background:#ffffff;",
    })
    row = soup.new_tag("tr")
    cell = soup.new_tag("td", attrs={"style": (
        "padding:20px 16px;color:#243247;font-family:Arial,Helvetica,sans-serif;"
        "font-size:16px;line-height:1.6;overflow-wrap:break-word;word-wrap:break-word;"
    )})
    for node in list(soup.contents):
        cell.append(node)
    row.append(cell)
    wrapper.append(row)
    return str(wrapper)


def _render_email_html(md_body: str, *, report_key: str, cadence: str) -> str:
    """Use the same deterministic presentation for sending and offline previews."""
    report_key, cadence = report_key.lower(), cadence.lower()
    html = _safe_markdown_to_html(md_body, preserve_line_breaks=report_key == "cybermed")
    if report_key == "cybermed":
        return _style_cybermed_html(html)
    if report_key == "cyberlurch" and cadence in {"monthly", "yearly"}:
        return _style_cyberlurch_longform_html(html)
    return html


def _delivery_recipients(report_key: str, report_mode: str, mode: str):
    if mode == "test":
        try:
            config = json.loads(os.getenv("TEST_RECIPIENTS_CONFIG_JSON") or "null")
        except ValueError:
            config = None
        if report_mode == "yearly":
            to_list = _dedupe_preserve([r for cadence in ("daily", "weekly", "monthly", "yearly")
                for r in _resolve_recipients_from_mapping(config, report_key, cadence)])
        else:
            to_list = _resolve_recipients_from_mapping(config, report_key, report_mode)
        recipients_source = "test_config"
    else:
        to_list, recipients_source = (_get_recipients_union(report_key) if report_mode == "yearly"
                                      else _get_recipients(report_key, report_mode))
    return _dedupe_preserve(to_list), recipients_source


def current_audience_refs(report_key: str, cadence: str) -> set[str]:
    key = os.getenv("DELIVERY_RECIPIENT_HMAC_KEY") or ""
    if len(key.encode()) < 32:
        raise RuntimeError("recipient_hmac_key_missing")
    recipients, _ = _delivery_recipients(report_key, cadence, email_mode())
    if not recipients:
        raise RuntimeError("smtp_recipient_configuration_missing")
    return {hmac.new(key.encode(), recipient.strip().casefold().encode(), hashlib.sha256).hexdigest() for recipient in recipients}


def send_markdown(subject: str, md_body: str, *, coordinator: Coordinator | None = None,
                  finalization: dict | None = None, retry_rejected: bool = False) -> DeliveryResult:
    """Return a journalled SMTP outcome; required errors fail without raw logs.

    Test mode resolves ONLY TEST_RECIPIENTS_CONFIG_JSON, never production
    recipient variables, EMAIL_TO or the private production recipients file.
    """
    report_key = (os.getenv("REPORT_KEY") or "cyberlurch").strip().lower()
    report_mode = (os.getenv("REPORT_MODE") or "daily").strip().lower()
    mode = email_mode()
    coordinator = coordinator or Coordinator(report_key, report_mode, mode=mode)
    if report_key == "cybermed":
        assert_no_generation_error_text(md_body, boundary="email_body")
    md_without_metadata, _, _ = _extract_run_metadata_for_email(md_body)
    if mode == "none" or not send_enabled():
        return coordinator.skip(md_without_metadata, "send_disabled", finalization=finalization)

    host, user, pw = os.getenv("SMTP_HOST"), os.getenv("SMTP_USER"), os.getenv("SMTP_PASS")
    from_addr = os.getenv("EMAIL_FROM") or user
    try:
        port = int(os.getenv("SMTP_PORT") or "587")
    except ValueError:
        port = 0
    to_list, recipients_source = _delivery_recipients(report_key, report_mode, mode)
    to_list = _dedupe_preserve(to_list)
    if not (host and user and pw and from_addr and to_list and 0 < port <= 65535):
        coordinator.generated(md_without_metadata, finalization)
        raise DeliveryError(coordinator.failure("smtp_configuration_missing", generated=True))
    # Keep recipient-facing email bodies clean by removing run metadata from outgoing markdown.
    md_without_metadata, _, metadata_removed = _extract_run_metadata_for_email(md_body)

    try:
        html_source = md_without_metadata if metadata_removed else md_body
        html = _render_email_html(html_source, report_key=report_key, cadence=report_mode)
    except Exception as exc:  # pragma: no cover - ultra-safety guard
        print("[email] WARN: markdown rendering failure; using escaped text")
        html = f"<pre>{html_module.escape((html_source or ''))}</pre>"

    if "<details" in (md_body or "") and "<details" not in (html or ""):
        print("[email] WARN: '<details>' did not survive markdown->HTML conversion; metadata may not be collapsible.")

    plain = _strip_details_tags(md_without_metadata)

    disclose_recipients = (os.getenv("EMAIL_DISCLOSE_RECIPIENTS") or "").strip() == "1"
    if len(to_list) <= 1:
        header_to_value = ", ".join(to_list)
        header_to_mode = "single"
    elif disclose_recipients:
        header_to_value = ", ".join(to_list)
        header_to_mode = "disclosed"
    else:
        header_to_value = from_addr
        header_to_mode = "undisclosed"

    print(f"[email] header_to_mode={header_to_mode}")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = header_to_value

    alternative = MIMEMultipart("alternative")
    alternative.attach(MIMEText(plain, "plain", "utf-8"))
    alternative.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alternative)

    def transport(message_id, recipients, inflight, outcome):
        msg["Message-ID"] = message_id
        smtp = None
        data_started = False
        try:
            smtp = smtplib.SMTP(host, port, timeout=60)
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(user, pw)
            inflight()
            data_started = True
            refused = smtp.sendmail(from_addr, recipients, msg.as_string())
            if not isinstance(refused, dict) or not set(refused).issubset(set(recipients)):
                outcome("outcome_unknown", [], [], "invalid_smtp_response")
            else:
                rejected = [r for r in recipients if r in refused]
                accepted = [r for r in recipients if r not in refused]
                status = "accepted_by_smtp" if not rejected else ("partially_accepted" if accepted else "failed")
                outcome(status, accepted, rejected, "recipient_rejection" if rejected else "")
        except (StoreError, DeliveryError):
            raise
        except smtplib.SMTPRecipientsRefused as exc:
            if set(exc.recipients) == set(recipients):
                outcome("failed", [], recipients, "all_recipients_rejected")
            else:
                outcome("outcome_unknown", [], [], "ambiguous_recipient_rejection")
        except smtplib.SMTPResponseException:
            outcome("failed", [], recipients, "smtp_explicit_rejection")
        except Exception:
            outcome("outcome_unknown" if data_started else "failed", [], [] if data_started else recipients,
                    "smtp_completion_unknown" if data_started else "smtp_connection_failed")
        finally:
            if smtp is not None:
                try:
                    smtp.quit()
                except Exception:
                    # QUIT failure cannot reverse a known DATA acceptance.
                    pass
    result = coordinator.deliver(md_without_metadata, to_list, transport, finalization=finalization,
                                 retry_rejected=retry_rejected)
    print(f"[email] smtp_result={result.status} accepted_count={len(result.accepted)} rejected_count={len(result.rejected)}")
    return result
