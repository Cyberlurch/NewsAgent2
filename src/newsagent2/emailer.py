from __future__ import annotations

import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List

from markdown import markdown


def _parse_recipients_from_env() -> List[str]:
    raw = (os.getenv("EMAIL_TO", "") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _load_recipients_from_file(report_key: str) -> List[str]:
    """Load recipients from data/recipients.json.

    Structure:
      {
        "default": ["a@b.com"],
        "cyberlurch": ["..."],
        "cybermed": ["..."]
      }

    The file is intended to be git-ignored (public repo safety).
    """
    path = Path("data") / "recipients.json"
    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return []
    except Exception:
        return []

    # Prefer exact report_key, then default.
    val = data.get(report_key) or data.get("default") or []
    if isinstance(val, str):
        val = [val]
    if not isinstance(val, list):
        return []
    return [str(x).strip() for x in val if str(x).strip()]


def _get_recipients(report_key: str) -> List[str]:
    # 1) Explicit env var (compat with current GitHub Actions secrets/workflow)
    env_list = _parse_recipients_from_env()
    if env_list:
        return env_list

    # 2) Fallback: local file (git-ignored)
    return _load_recipients_from_file(report_key)


def send_markdown(subject: str, md_body: str) -> None:
    """Send the Markdown report as email (plain + HTML).

    Controlled via environment variables:
      SEND_EMAIL = "1"  -> send
                   anything else -> do not send

      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
      EMAIL_FROM, EMAIL_TO (optional; overrides recipients.json)

    Recipients fallback (if EMAIL_TO is empty):
      data/recipients.json  (keyed by REPORT_KEY, with optional "default")
    """
    send_flag = os.getenv("SEND_EMAIL", "1")
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

    to_list = _get_recipients(report_key)

    try:
        port = int(port_str)
    except ValueError:
        print(f"[email] Invalid SMTP_PORT={port_str!r} -> abort.")
        return

    if not (host and port and user and pw and to_list):
        print("[email] SMTP configuration incomplete -> NOT sending.")
        # Never log passwords or recipient addresses.
        print(
            "[email] Missing fields:",
            {
                "host": bool(host),
                "port": bool(port),
                "user": bool(user),
                "pw": bool(pw),
                "recipients_count": len(to_list),
                "report_key": report_key,
                "recipients_source": ("env:EMAIL_TO" if _parse_recipients_from_env() else "file:data/recipients.json"),
            },
        )
        return

    # Convert Markdown to HTML; keep Markdown as plain part.
    html = markdown(md_body, extensions=["extra", "tables", "fenced_code"])
    plain = md_body

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    # Header must contain recipients; this is not logged by us.
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        print(
            f"[email] Sending via {host}:{port} from {from_addr} to {len(to_list)} recipient(s) "
            f"(report_key={report_key})"
        )
        with smtplib.SMTP(host, port, timeout=60) as s:
            s.starttls()
            s.login(user, pw)
            s.sendmail(from_addr, to_list, msg.as_string())
        print("[email] Email sent.")
    except Exception as e:
        print(f"[email] ERROR during email send: {e!r}")
