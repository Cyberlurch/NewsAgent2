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
from .state_manager import load_state, save_state, is_processed, mark_processed, prune_state


def load_channels(path: str):
    """
    Lädt Kanal- und Themenkonfiguration aus einer JSON-Datei im Format:

    {
      "topic_buckets": [
        {
          "topic": "Geo-Politik",
          "weight": 1.5,              # optional, Default 1.0
          "channels": [
            {"name": "preppernewsflash", "url": "..."},
            {"name": "klartextwinkler", "url": "..."}
          ]
        },
        ...
      ]
    }

    Rückgabe:
      - channels: Liste von Dicts mit 'name' und 'url'
      - channel_topics: Mapping von Kanalname -> Topic
      - topic_weights: Mapping von Topic -> Gewicht (float, Default 1.0)
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    channels: List[Dict] = []
    channel_topics: Dict[str, str] = {}
    topic_weights: Dict[str, float] = {}

    for bucket in data.get("topic_buckets", []):
        topic = bucket.get("topic", "Allgemein")
        try:
            weight = float(bucket.get("weight", 1.0))
        except (TypeError, ValueError):
            weight = 1.0
        # keine negativen oder 0-Gewichte
        topic_weights[topic] = max(weight, 0.0) or 1.0

        for c in bucket.get("channels", []):
            name = c["name"]
            url = c["url"]
            channels.append({"name": name, "url": url})
            channel_topics[name] = topic

    return channels, channel_topics, topic_weights


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

    # Memory/State gegen doppelte Verarbeitung (persistentes JSON im Repo)
    report_key = os.getenv("REPORT_KEY", "cyberlurch").strip() or "cyberlurch"
    state_path = os.getenv("STATE_PATH", "state/processed_items.json").strip() or "state/processed_items.json"
    try:
        retention_days = int(os.getenv("STATE_RETENTION_DAYS", "120"))
    except ValueError:
        retention_days = 120
    try:
        max_entries = int(os.getenv("STATE_MAX_ENTRIES_PER_BUCKET", "0"))
    except ValueError:
        max_entries = 0

    print(f"[NewsAgent2] report_key={report_key!r}")
    print(f"[NewsAgent2] state_path={state_path!r}")
    print(f"[NewsAgent2] retention_days={retention_days}")
    if max_entries > 0:
        print(f"[NewsAgent2] max_entries_per_bucket={max_entries}")

    state = load_state(state_path)
    removed_age, removed_cap = prune_state(state, retention_days=retention_days, max_entries_per_bucket=max_entries)
    if removed_age or removed_cap:
        print(f"[NewsAgent2] State prune: removed_by_age={removed_age}, removed_by_cap={removed_cap}")

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=args.hours)
    # cutoff aktuell nur informativ; Filterung passiert in list_recent_videos

    channels, channel_topics, topic_weights = load_channels(args.channels)
    items: List[Dict] = []

    skipped_by_state = 0

    # Sammeln aller Items aus allen Kanälen
    for ch in channels:
        vids = list_recent_videos(
            ch["url"],
            hours=args.hours,
            max_items=args.max_per_channel,
        )

        for v in vids:
            # Persistenter Memory-Check (YouTube Video-ID)
            if is_processed(state, report_key, "youtube", v.get("id") or ""):
                skipped_by_state += 1
                continue

            text = fetch_transcript(v["id"])
            desc = v.get("description") or ""

            # Skip, falls weder Transkript noch Beschreibung verfügbar
            if not text and not desc:
                continue

            items.append(
                {
                    "id": v["id"],
                    "title": v["title"],
                    # Im Report verwenden wir den konfigurierten Kanalnamen
                    "channel": ch["name"],
                    "topic": channel_topics.get(ch["name"], "Allgemein"),
                    "url": v["url"],
                    "published_at": v["published_at"].astimezone(sto),
                    "text": text,
                    "description": desc,
                }
            )

    print(f"[NewsAgent2] Collected {len(items)} item(s) after filtering (state + availability)")

    if not items:
        if skipped_by_state:
            print(f"[NewsAgent2] Skipped {skipped_by_state} item(s) due to state/memory")
        overview = "Keine neuen Inhalte in den letzten 24 Stunden."
        details_by_id: Dict[str, str] = {}
    else:
        # Deduplikation innerhalb eines Laufs anhand (Titel, Kanal)
        seen = set()
        unique: List[Dict] = []
        for it in items:
            key = (it["title"].strip().lower(), it["channel"].strip().lower())
            if key in seen:
                continue
            seen.add(key)
            unique.append(it)
        items = unique

        print(f"[NewsAgent2] Items after in-run dedup (title+channel): {len(items)}")

        if skipped_by_state:
            print(f"[NewsAgent2] Skipped {skipped_by_state} item(s) due to state/memory")

        # Gesamt-Overview mit allen Items
        overview = summarize(items)

        # Detail-Zusammenfassungen
        max_detail = int(os.getenv("DETAIL_ITEMS_PER_DAY", "8"))
        max_per_channel_detail = int(os.getenv("DETAIL_ITEMS_PER_CHANNEL_MAX", "3"))
        details_by_id: Dict[str, str] = {}

        if max_detail > 0 and max_per_channel_detail > 0:
            # Items nach Topic und Kanal gruppieren
            items_by_topic: Dict[str, Dict[str, List[Dict]]] = {}
            for it in items:
                topic = it.get("topic", "Allgemein")
                ch_name = it["channel"]
                items_by_topic.setdefault(topic, {}).setdefault(ch_name, []).append(it)

            # Innerhalb jedes Kanals nach Zeit sortieren (neueste zuerst)
            for topic_map in items_by_topic.values():
                for ch_name, ch_items in topic_map.items():
                    ch_items.sort(key=lambda it: it["published_at"], reverse=True)

            # Aktive Topics (nur solche mit Items)
            active_topics = sorted(items_by_topic.keys())

            # Gewichte für aktive Topics (Default 1.0)
            weights: Dict[str, float] = {}
            for t in active_topics:
                w = topic_weights.get(t, 1.0)
                try:
                    w = float(w)
                except (TypeError, ValueError):
                    w = 1.0
                weights[t] = max(w, 0.0) or 1.0

            total_weight = sum(weights.values())

            # Grobe Slotverteilung pro Topic
            slots_by_topic: Dict[str, int] = {}
            if total_weight > 0:
                for t in active_topics:
                    proportion = weights[t] / total_weight
                    est = int(round(max_detail * proportion))
                    slots_by_topic[t] = max(est, 1)
            else:
                base = max_detail // max(1, len(active_topics))
                extra = max_detail % max(1, len(active_topics))
                for i, t in enumerate(active_topics):
                    slots_by_topic[t] = base + (1 if i < extra else 0)

            def total_slots() -> int:
                return sum(slots_by_topic.values())

            # Falls Summe der Slots > max_detail: etwas zurückschneiden
            while total_slots() > max_detail and len(slots_by_topic) > 0:
                t_max = max(slots_by_topic, key=slots_by_topic.get)
                if slots_by_topic[t_max] > 1:
                    slots_by_topic[t_max] -= 1
                else:
                    # Alle stehen schon auf 1 – Rest regeln wir über max_detail
                    break

            # Falls Summe < max_detail: Restslots in Round-Robin auffüllen
            while total_slots() < max_detail and active_topics:
                for t in active_topics:
                    if total_slots() >= max_detail:
                        break
                    slots_by_topic[t] = slots_by_topic.get(t, 0) + 1

            # Topics nach Gewicht sortieren (wichtigere zuerst)
            topics_by_priority = sorted(
                active_topics,
                key=lambda t: weights.get(t, 1.0),
                reverse=True,
            )

            selected_count = 0

            # Pro Topic Slots vergeben, innerhalb Topic Round-Robin über Kanäle
            for topic in topics_by_priority:
                if selected_count >= max_detail:
                    break

                topic_quota = slots_by_topic.get(topic, 0)
                if topic_quota <= 0:
                    continue

                channel_map = items_by_topic[topic]
                channel_order = sorted(channel_map.keys())
                per_channel_count: Dict[str, int] = {
                    ch_name: 0 for ch_name in channel_order
                }

                topic_selected = 0
                while topic_selected < topic_quota and selected_count < max_detail:
                    made_progress = False
                    for ch_name in channel_order:
                        if topic_selected >= topic_quota or selected_count >= max_detail:
                            break
                        if per_channel_count[ch_name] >= max_per_channel_detail:
                            continue

                        ch_items = channel_map[ch_name]
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

                        per_channel_count[ch_name] += 1
                        topic_selected += 1
                        selected_count += 1
                        made_progress = True

                    if not made_progress:
                        # In diesem Topic sind keine weiteren Items mehr verfügbar
                        break

    # Report als Markdown erzeugen
    os.makedirs("reports", exist_ok=True)
    fn = datetime.now(sto).strftime("reports/daily_summary_%Y-%m-%d_%H-%M.md")
    md = to_markdown(items, overview, details_by_id)
    with open(fn, "w", encoding="utf-8") as f:
        f.write(md)

    # State aktualisieren (nur für Items, die im Report verarbeitet wurden)
    if items:
        for it in items:
            try:
                mark_processed(
                    state,
                    report_key=report_key,
                    source="youtube",
                    item_id=str(it.get("id") or ""),
                    meta={
                        "title": it.get("title") or "",
                        "channel": it.get("channel") or "",
                        "url": it.get("url") or "",
                        "published_at": (
                            it.get("published_at").isoformat() if it.get("published_at") else ""
                        ),
                    },
                )
            except Exception as e:
                print(f"[NewsAgent2] WARN: failed to mark processed for id={it.get('id')!r}: {e!r}")

        save_state(state_path, state)
    else:
        # Auch ohne neue Items: nur speichern, falls wir beim Pruning etwas entfernt haben.
        if removed_age or removed_cap:
            save_state(state_path, state)

    # E-Mail mit dem gesamten Markdown-Inhalt
    subject = "The Cyberlurch Report"
    send_markdown(subject, md)


if __name__ == "__main__":
    main()
