import pathlib, sys
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))

from newsagent2.cyberlurch_editorial import classify_cyberlurch_item_temporality
from newsagent2.reporter import to_markdown


def test_temporality_christian_apologetics_is_evergreen():
    item = {"title": "Apologetik und Bibel: Grundlagen des Glaubens", "channel": "WesHuff", "topic_primary": "Christlicher Glaube"}
    assert classify_cyberlurch_item_temporality(item) == "evergreen"


def test_temporality_mainstream_news_is_current_affairs():
    item = {"title": "Tagesschau: Lage in Europa", "channel": "tagesschau", "topic_primary": "Mainstream DE News"}
    assert classify_cyberlurch_item_temporality(item) == "current_affairs"


def test_weekly_top_links_are_capped(monkeypatch):
    monkeypatch.setenv("CYBERLURCH_WEEKLY_TOP_LINKS_MAX", "2")
    items = []
    for i in range(5):
        items.append({"id": str(i), "title": f"t{i}", "url": f"https://e/{i}", "channel": "c", "published_at": datetime.now(timezone.utc)})
    md = to_markdown(items, "overview", {}, report_title="The Cyberlurch Report — Weekly", report_language="en", report_mode="weekly")
    assert md.count("https://e/") <= 2
