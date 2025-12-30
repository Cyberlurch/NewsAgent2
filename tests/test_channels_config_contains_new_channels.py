import json
from pathlib import Path


def test_channels_config_contains_new_channels():
    channels_path = Path(__file__).resolve().parent.parent / "data" / "channels.json"
    with channels_path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)

    all_urls = {
        channel["url"]
        for bucket in data.get("topic_buckets", [])
        for channel in bucket.get("channels", [])
    }

    expected_urls = {
        "https://www.youtube.com/@AndreiJikh",
        "https://www.youtube.com/@3of7Project",
        "https://www.youtube.com/@COMPACTTV",
        "https://www.youtube.com/@endzeit-kanal",
        "https://www.youtube.com/@frihetsnytt2745",
        "https://www.youtube.com/@IsraelNationalTV",
        "https://www.youtube.com/@judgingfreedom",
        "https://www.youtube.com/@krissyrieger",
        "https://www.youtube.com/@MarcFriedrich7",
        "https://www.youtube.com/@profjiangclips",
        "https://www.youtube.com/@Riks",
        "https://www.youtube.com/@russianmediamonitor",
        "https://www.youtube.com/@SlingandStoneVideos",
        "https://www.youtube.com/@tagesschau",
        "https://www.youtube.com/@VanessaWing%C3%A5rdh",
        "https://www.youtube.com/@ZDFheute",
    }

    missing = expected_urls - all_urls
    assert not missing, f"Missing channels: {sorted(missing)}"
