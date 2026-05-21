import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))

from newsagent2.main import _annotate_cyberlurch_item_topics


def test_annotate_cyberlurch_item_topics_sets_primary_and_fallback():
    items = [
        {"channel": "A"},
        {"channel": "B"},
    ]
    channel_topics = {
        "A": ["Israel, Nahost & Sicherheitslage", "Geopolitik, Krieg & Machtblöcke"],
    }

    _annotate_cyberlurch_item_topics(items, channel_topics)

    assert items[0]["topics"] == ["Israel, Nahost & Sicherheitslage", "Geopolitik, Krieg & Machtblöcke"]
    assert items[0]["topic_primary"] == "Israel, Nahost & Sicherheitslage"
    assert items[0]["topic"] == "Israel, Nahost & Sicherheitslage"

    assert items[1]["topics"] == ["Other"]
    assert items[1]["topic_primary"] == "Other"
    assert items[1]["topic"] == "Other"
