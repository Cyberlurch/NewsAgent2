from __future__ import annotations

import os
import json
import argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict

from dotenv import load_dotenv

from .collectors_youtube import list_recent_videos, fetch_transcript
from .summarizer import summarize, summarize_item_detail
from .reporter import to_markdown
from .emailer import send_markdown


def load_channels(path: str) -> List[Dict]:
    """
    Lädt die Kanäle aus einer JSON-Konfiguration im Format:
    {
      "topic_buckets": [
        {
          "topic": "...",
          "channels": [
            {"name": "...", "url": "..."},
            ...
          ]
        },
        ...
      ]
    }
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
    # cutoff wird aktuell nur zu Dokumentationszwecken gehalten;
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

            # Skip, falls wirklich weder Transkript noch Beschreibung verfügbar
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
        overview = "Keine neuen Inhalte in den letzten 24 Stunden."
        details_by_id: Dict[str, str] = {}
    else:
        # Harte Deduplikation anhand (titel, kanal) – doppelte Items im selben Lauf entfernen
        seen = set()
        unique: List[Dict] = []
        for it in items:
            key = (it["title"].strip().lower(), it["channel"].strip().lower())
            if key in seen:
                continue
            seen.add(key)
            unique.append(it)
        items = unique

        # Gesamt-Overview mit allen Items
        overview = summarize(items)

        # Detail-Zusammenfassungen für eine begrenzte Anzahl Videos
        max_detail = int(os.getenv("DETAIL_ITEMS_PER_DAY", "8"))
        details_by_id: Dict[str, str] = {}

        if max_detail > 0:
            # Neueste Videos zuerst (absteigend nach Veröffentlichungszeit)
            sorted_items = sorted(
                items,
                key=lambda it: it["published_at"],
                reverse=True,
            )
            for it in sorted_items[:max_detail]:
                try:
                    details_by_id[it["id"]] = summarize_item_detail(it)
                except Exception as e:
                    details_by_id[it["id"]] = (
                        f"[Fehler bei Detailzusammenfassung: {e!r}]"
                    )

    # Report als Markdown erzeugen
    os.makedirs("reports", exist_ok=True)
    fn = datetime.now(sto).strftime("reports/daily_summary_%Y-%m-%d_%H-%M.md")
    md = to_markdown(items, overview, details_by_id)
    with open(fn, "w", encoding="utf-8") as f:
        f.write(md)

    # E-Mail mit dem gesamten Markdown-Inhalt
    subject = "Daily Summary – NewsAgent2"
    send_markdown(subject, md)


if __name__ == "__main__":
    main()
