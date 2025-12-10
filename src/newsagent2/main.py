# src/newsagent2/main.py
from __future__ import annotations

import os
import json
import argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict

from dotenv import load_dotenv

from .collectors_youtube import list_recent_videos, fetch_transcript
from .summarizer import summarize
from .reporter import to_markdown
from .emailer import send_markdown


def load_channels(path: str) -> List[Dict]:
    """
    Lädt die Kanalkonfiguration aus einer JSON-Datei im Format
    { "topic_buckets": [ { "channels": [ {"name": ..., "url": ...}, ... ] } ] }.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    chans: List[Dict] = []
    for bucket in data.get("topic_buckets", []):
        for c in bucket.get("channels", []):
            chans.append({"name": c["name"], "url": c["url"]})
    return chans


def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--channels", default="data/channels.json")
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument(
        "--max-per-channel",
        type=int,
        default=int(os.getenv("MAX_ITEMS_PER_CHANNEL", "5")),
    )
    args = ap.parse_args()

    sto = ZoneInfo("Europe/Stockholm")

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=args.hours)
    # cutoff wird aktuell nur zu Debug-/Dokumentationszwecken gehalten;
    # die eigentliche Filterung passiert in list_recent_videos.

    channels = load_channels(args.channels)
    items: List[Dict] = []

    for ch in channels:
        vids = list_recent_videos(
            ch["url"],
            hours=args.hours,
            max_items=args.max_per_channel,
        )

        for v in vids:
            text = fetch_transcript(v["id"])
            desc = v.get("description") or ""

            # Hard-Skip, falls wirklich weder Transkript noch Beschreibung verfügbar
            if not text and not desc:
                continue

            items.append(
                {
                    "id": v["id"],
                    "title": v["title"],
                    "channel": v["channel"],
                    "url": v["url"],
                    "published_at": v["published_at"].astimezone(sto),
                    "text": text,
                    "description": desc,
                }
            )

    if not items:
        summary = "Keine neuen Inhalte in den letzten 24 Stunden."
    else:
        # Grobe Deduplikation anhand (titel, kanal) – harte Duplikate rausfiltern
        seen = set()
        unique: List[Dict] = []
        for it in items:
            key = (it["title"].strip().lower(), it["channel"].strip().lower())
            if key in seen:
                continue
            seen.add(key)
            unique.append(it)
        items = unique

        summary = summarize(items)

    # Ablegen im Repo-Artefaktverzeichnis
    os.makedirs("reports", exist_ok=True)
    fn = datetime.now(sto).strftime("reports/daily_summary_%Y-%m-%d_%H-%M.md")
    with open(fn, "w", encoding="utf-8") as f:
        f.write(md := to_markdown(items, summary))

    # E-Mail (wenn SEND_EMAIL=1 und SMTP-ENV gesetzt)
    subject = "Daily Summary – NewsAgent2"
    send_markdown(subject, md)


if __name__ == "__main__":
    main()
