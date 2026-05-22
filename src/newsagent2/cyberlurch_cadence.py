from __future__ import annotations

import re
from typing import Any, Dict, List


def cyberlurch_cadence_profile(report_mode: str) -> Dict[str, Any]:
    mode = (report_mode or "daily").strip().lower()
    profiles: Dict[str, Dict[str, Any]] = {
        "daily": {
            "name": "daily",
            "focus": "new_items",
            "source_link_style": "top_videos",
            "allow_item_deep_dives": True,
            "allow_news_item_deep_dives": True,
            "allow_evergreen_deep_dives": True,
        },
        "weekly": {
            "name": "weekly",
            "focus": "week_in_review",
            "source_link_style": "capped_top_videos",
            "allow_item_deep_dives": True,
            "allow_news_item_deep_dives": True,
            "allow_evergreen_deep_dives": True,
        },
        "monthly": {
            "name": "monthly",
            "focus": "trend_report",
            "source_link_style": "representative_links_by_topic",
            "allow_item_deep_dives": False,
            "allow_news_item_deep_dives": False,
            "allow_evergreen_deep_dives": True,
        },
        "yearly": {
            "name": "yearly",
            "focus": "annual_analysis",
            "source_link_style": "representative_links_by_theme",
            "allow_item_deep_dives": False,
            "allow_news_item_deep_dives": False,
            "allow_evergreen_deep_dives": True,
        },
    }
    return profiles.get(mode, profiles["daily"])


def classify_cyberlurch_item_temporality(item: dict) -> str:
    title = str(item.get("title") or "").lower()
    topic = str(item.get("topic_primary") or "").lower()
    topics = " ".join(str(t).lower() for t in (item.get("topics") or []))
    channel = str(item.get("channel") or "").lower()
    content = " ".join([title, topic, topics, channel, str(item.get("content") or "").lower()])


    if "mainstream" in topic or channel in {"tagesschau", "zdfheute", "vanessawingardh"}:
        return "current_affairs"

    is_news_urgent = any(k in content for k in ["breaking", "alert", "live", "today", "attack", "war update", "eilmeldung", "heute"])
    if is_news_urgent:
        return "breaking_news" if "breaking" in content or "alert" in content else "current_affairs"

    if any(k in content for k in ["dante", "bible", "old testament", "theology", "philosophy", "apologetics", "god", "bibel", "apologetik", "glaube", "worldview", "christlicher glaube"]):
        if any(k in content for k in ["breaking", "news", "krieg", "attack"]):
            return "mixed"
        return "evergreen"

    if any(k in content for k in ["geopolitik", "krieg", "machtbl", "israel", "nahost", "sicherheitslage"]):
        return "trend_analysis" if not is_news_urgent else "current_affairs"
    if any(k in content for k in ["preparedness", "survival", "sicherheit & survival"]):
        return "mixed"
    if any(k in content for k in ["finanzen", "wirtschaft", "krypto", "finance", "econom"]):
        return "trend_analysis"
    if any(k in content for k in ["prophetie", "endzeit", "weltdeutung"]):
        return "mixed"
    if any(k in content for k in ["society", "media", "politik", "gesellschaft"]):
        return "trend_analysis"
    return "current_affairs"


def annotate_cyberlurch_temporality(items: List[dict]) -> List[dict]:
    for item in items:
        existing = str(item.get("temporality") or "").strip()
        if existing:
            item["temporality"] = existing
            continue
        item["temporality"] = classify_cyberlurch_item_temporality(item)
    return items
