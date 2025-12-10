from __future__ import annotations
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from markdown import markdown


def send_markdown(subject: str, md_body: str) -> None:
    """
    Sendet den Markdown-Report als E-Mail (Text + HTML).

    Steuerung über Umgebungsvariablen:
      SEND_EMAIL = "1"  -> senden
                   alles andere -> nicht senden

      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
      EMAIL_FROM, EMAIL_TO
    """
    send_flag = os.getenv("SEND_EMAIL", "1")
    print(f"[NewsAgent2] SEND_EMAIL={send_flag!r}")
    if send_flag != "1":
        print("[NewsAgent2] SEND_EMAIL != '1' -> E-Mail-Versand deaktiviert, breche ab.")
        return

    host = os.getenv("SMTP_HOST")
    port_str = os.getenv("SMTP_PORT", "587")
    user = os.getenv("SMTP_USER")
    pw = os.getenv("SMTP_PASS")
    from_addr = os.getenv("EMAIL_FROM", user or "newsagent@localhost")
    to_raw = os.getenv("EMAIL_TO", "")
    to_list = [x.strip() for x in to_raw.split(",") if x.strip()]

    try:
        port = int(port_str)
    except ValueError:
        print(f"[NewsAgent2] Ungültiger SMTP_PORT={port_str!r}, breche ab.")
        return

    if not (host and port and user and pw and to_list):
        print("[NewsAgent2] SMTP-Konfiguration unvollständig – E-Mail wird NICHT gesendet.")
        print(f"[NewsAgent2] host={host!r}, user={user!r}, to_list={to_list!r}")
        # Passwort NICHT loggen!
        return

    # Markdown -> HTML
    html = markdown(md_body, output_format="xhtml")
    plain = md_body

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        print(f"[NewsAgent2] Sende E-Mail via {host}:{port} von {from_addr} an {to_list}")
        with smtplib.SMTP(host, port, timeout=60) as s:
            s.starttls()
            s.login(user, pw)
            s.sendmail(from_addr, to_list, msg.as_string())
        print("[NewsAgent2] E-Mail erfolgreich gesendet.")
    except Exception as e:
        print(f"[NewsAgent2] Fehler beim E-Mail-Versand: {e!r}")
