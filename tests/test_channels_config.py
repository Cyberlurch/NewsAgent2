import json
from pathlib import Path


def test_channels_config_integrity():
    config_path = Path(__file__).resolve().parent.parent / "data" / "channels.json"
    with config_path.open(encoding="utf-8") as f:
        config = json.load(f)

    buckets = config.get("topic_buckets", [])

    # Ensure weights are positive and gather all channels
    all_channel_names = []
    for bucket in buckets:
        assert bucket["weight"] > 0
        for channel in bucket.get("channels", []):
            all_channel_names.append(channel["name"])

    assert len(all_channel_names) == 54
    assert len(all_channel_names) == len(set(all_channel_names))

    all_urls = []
    for bucket in buckets:
        for channel in bucket.get("channels", []):
            all_urls.append(channel["url"])
    assert len(all_urls) == len(set(all_urls))


def test_expected_topic_buckets_and_existing_channels_once():
    config_path = Path(__file__).resolve().parent.parent / "data" / "channels.json"
    with config_path.open(encoding="utf-8") as f:
        config = json.load(f)

    expected_topics = {
        "Mainstream DE/SE News",
        "Geopolitik, Krieg & Machtblöcke",
        "Israel, Nahost & Sicherheitslage",
        "Christlicher Glaube, Bibel & Apologetik",
        "Prophetie, Endzeit & Weltdeutung",
        "Gesellschaft, Medienkritik & Debatte",
        "Preparedness, Sicherheit & Survival",
        "Finanzen, Wirtschaft & Krypto",
    }
    buckets = config.get("topic_buckets", [])
    assert {b.get("topic") for b in buckets} == expected_topics

    names = [c.get("name") for b in buckets for c in b.get("channels", [])]
    for must_once in ["IsraelEnglishNews", "judgingfreedom", "klartextwinkler", "Riks", "SlingandStoneVideos", "sdwebbtv", "SwebbTV2"]:
        assert names.count(must_once) == 1

    debate_bucket = next(b for b in buckets if b.get("topic") == "Gesellschaft, Medienkritik & Debatte")
    debate_names = [c.get("name") for c in debate_bucket.get("channels", [])]
    assert "sdwebbtv" in debate_names
    assert "SwebbTV2" in debate_names
