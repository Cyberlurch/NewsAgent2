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

    assert len(all_channel_names) == 30
    assert len(all_channel_names) == len(set(all_channel_names))
