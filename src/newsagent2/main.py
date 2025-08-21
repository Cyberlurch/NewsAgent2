from __future__ import annotations
import os, json, argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict

from dotenv import load_dotenv
from .collectors_youtube import list_recent_videos, fetch_transcript
from .summarizer import summarize
from .reporter import to_markdown
from .emailer import send_markdown

def load_channels(path: str) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    chans = []
    for b in data.get("topic_buckets", []):
        for c in b.get("channels", []):
            chans.append({"name": c["name"], "url": c["url"]})
    return chans

def main():
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--channels", default="data/channels.json")
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--max-per-channel", type=int, default=int(os.getenv("MAX_ITEMS_PER_CHANNEL", "5")))
    args = ap.parse_args()

    sto = ZoneInfo("Europe/Stockholm")
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=args.hours)

    channels = load_channels(args.channels)
    items: List[Dict] = []

    for ch in channels:
        vids = list_recent_videos(ch["url"], hours=args.hours, max_items=args.max_per_channel)
        for v in vids:
            text = fetch_transcript(v["id"])
            items.append({
                "id": v["id"],
                "title": v["title"],
                "channel": v["channel"],
                "url": v["url"],
                "published_at": v["published_at"].astimezone(sto),
                "text": text
            })

    # nur Items mit Text ODER zumindest Titel (falls gar kein Transkript zu kriegen war)
    if not items:
        summary = "Keine neuen Inhalte in den letzten 24 Stunden."
    else:
        # dedupliziere grob anhand (titel, kanal) – harte Duplikate
        seen = set()
        unique = []
        for it in items:
            key = (it["title"].strip().lower(), it["channel"].strip().lower())
            if key in seen:
                continue
            seen.add(key)
            unique.append(it)
        items = unique
        summary = summarize(items)

    md = to_markdown(items, summary)
    # Ablegen ins Repo-Artefaktverzeichnis
    os.makedirs("reports", exist_ok=True)
    from zoneinfo import ZoneInfo
    fn = datetime.now(ZoneInfo("Europe/Stockholm")).strftime("reports/daily_summary_%Y-%m-%d_%H-%M.md")
    with open(fn, "w", encoding="utf-8") as f:
        f.write(md)

    # E-Mail
    subject = "Daily Summary – NewsAgent2"
    send_markdown(subject, md)

if __name__ == "__main__":
    main()

