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
    Für die aktuelle Logik wird das Topic ignoriert; wir flatten nur die Kanalliste.
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
        max_per_channel_detail = int(os.getenv("DETAIL_ITEMS_PER_CHANNEL_MAX", "3"))
        details_by_id: Dict[str, str] = {}

        if max_detail > 0 and max_per_channel_detail > 0:
            # Gruppiere Items pro Kanal
            items_by_channel: Dict[str, List[Dict]] = {}
            for it in items:
                ch_name = it["channel"]
                items_by_channel.setdefault(ch_name, []).append(it)

            # Sortiere pro Kanal nach Veröffentlichungszeit (neueste zuerst)
            for ch_items in items_by_channel.values():
                ch_items.sort(key=lambda it: it["published_at"], reverse=True)

            # Round-Robin-Auswahl über Kanäle, mit Limit pro Kanal
            channel_order = sorted(items_by_channel.keys())
            per_channel_count: Dict[str, int] = {
                ch: 0 for ch in channel_order
            }

            selected_count = 0
            while selected_count < max_detail:
                made_progress = False
                for ch in channel_order:
                    if selected_count >= max_detail:
                        break
                    if per_channel_count[ch] >= max_per_channel_detail:
                        continue

                    ch_items = items_by_channel[ch]
                    if not ch_items:
                        continue

                    candidate = ch_items.pop(0)
                    if candidate["id"] in details_by_id:
                        continue

                    try:
                        details_by_id[candidate["id"]] = summarize_item_detail(candidate)
                    except Exception as e:
                        details_by_id[candidate["id"]] = (
                            f"[Fehler bei Detailzusammenfassung: {e!r}]"
                        )

                    per_channel_count[ch] += 1
                    selected_count += 1
                    made_progress = True

                if not made_progress:
                    # Keine weiteren Items mehr verfügbar, die Bedingungen erfüllen
                    break

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