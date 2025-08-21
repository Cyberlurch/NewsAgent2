from __future__ import annotations
import os, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from markdown import markdown

def send_markdown(subject: str, md_body: str) -> None:
    if os.getenv("SEND_EMAIL", "1") != "1":
        return
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pw   = os.getenv("SMTP_PASS")
    from_addr = os.getenv("EMAIL_FROM", user or "newsagent@localhost")
    to_list = [x.strip() for x in os.getenv("EMAIL_TO", "").split(",") if x.strip()]
    if not (host and port and user and pw and to_list):
        raise RuntimeError("SMTP/.env unvollständig (HOST/PORT/USER/PASS/EMAIL_TO)")

    html = markdown(md_body, output_format="xhtml")
    plain = md_body

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(host, port, timeout=60) as s:
        s.starttls()
        s.login(user, pw)
        s.sendmail(from_addr, to_list, msg.as_string())

