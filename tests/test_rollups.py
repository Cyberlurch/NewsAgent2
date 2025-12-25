import pathlib
from datetime import datetime
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import rollups


def test_upsert_monthly_rollup_overwrites_month():
    state = {}
    rollups.upsert_monthly_rollup(
        state,
        report_key="cybermed",
        month="2024-01",
        generated_at="2024-02-01T00:00:00Z",
        executive_summary=["initial"],
        top_items=[
            {"title": "One", "url": "https://a", "channel": "ch", "source": "youtube", "published_at": "2024-01-05"},
        ],
    )
    rollups.upsert_monthly_rollup(
        state,
        report_key="cybermed",
        month="2024-01",
        generated_at="2024-02-02T00:00:00Z",
        executive_summary=["updated"],
        top_items=[
            {"title": "Two", "url": "https://b", "channel": "ch2", "source": "youtube", "published_at": "2024-01-06"},
        ],
    )

    entries = state["reports"]["cybermed"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["generated_at"] == "2024-02-02T00:00:00Z"
    assert entry["executive_summary"] == ["updated"]
    assert entry["top_items"][0]["title"] == "Two"
    assert entry["top_items"][0]["date"] == "2024-01-06"


def test_yearly_markdown_uses_previous_year_rollups():
    base_state = {
        "reports": {
            "cyberlurch": [
                {"month": "2023-12", "generated_at": "2024-01-01T00:00:00Z", "executive_summary": ["wrap"], "top_items": []},
                {"month": "2024-01", "generated_at": "2024-02-01T00:00:00Z", "executive_summary": ["jan"], "top_items": []},
            ]
        }
    }

    entries = rollups.rollups_for_year(base_state, "cyberlurch", 2024)
    assert len(entries) == 1
    assert entries[0]["month"] == "2024-01"

    md = rollups.render_yearly_markdown(
        report_title="The Cyberlurch Year in Review — 2024",
        report_language="en",
        year=2024,
        rollups=[
            {"month": f"2024-{i:02d}", "generated_at": "2025-01-01T00:00:00Z", "executive_summary": [f"m{i}"], "top_items": []}
            for i in range(1, 13)
        ],
    )

    for i in range(1, 13):
        label = datetime(year=2024, month=i, day=1).strftime("%B 2024")
        assert label in md
        assert f"m{i}" in md
