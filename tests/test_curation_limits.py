import pathlib
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.main import (
    CYBERMED_MONTHLY_MAX_FOAMED,
    CYBERMED_WEEKLY_MAX_FOAMED,
    _curate_cyberlurch_overview,
    _trim_foamed_overview,
)
from newsagent2.reporter import to_markdown


def _fake_item(idx: int, *, top_pick: bool = False, score: float = 0.0) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "id": f"vid-{idx}",
        "title": f"Video {idx}",
        "channel": "Test Channel",
        "url": f"https://example.com/video-{idx}",
        "published_at": now - timedelta(hours=idx),
        "top_pick": top_pick,
        "score": score,
    }


def test_cyberlurch_weekly_sources_use_curated_overview():
    items = [_fake_item(i, top_pick=(i == 5), score=float(i)) for i in range(6)]

    curated = _curate_cyberlurch_overview(items, "weekly", overview_items_max=3)

    assert len(curated) <= 3
    assert any(it.get("top_pick") for it in curated)

    md = to_markdown(
        curated,
        "## Executive Summary\n\nTest.",
        {},
        report_title="The Cyberlurch Report — Weekly",
        report_language="en",
        report_mode="weekly",
    )

    curated_urls = {it["url"] for it in curated}
    sources_section = md.split("## Sources")[-1]

    for url in curated_urls:
        assert url in sources_section

    # Ensure an omitted URL from the original pool is not present
    assert "https://example.com/video-0" not in sources_section


def test_trim_foamed_overview_preserves_top_picks_and_limit():
    foamed_items = []
    for i in range(20):
        foamed_items.append(
            {
                "id": f"foam-{i}",
                "title": f"Foam {i}",
                "foamed_score": float(i),
                "published_at": datetime.now(timezone.utc) - timedelta(hours=i),
                "top_pick": i in {2, 3},
            }
        )

    weekly_trimmed = _trim_foamed_overview(list(foamed_items), "weekly")
    monthly_trimmed = _trim_foamed_overview(list(foamed_items), "monthly")

    assert len(weekly_trimmed) <= CYBERMED_WEEKLY_MAX_FOAMED
    assert len(monthly_trimmed) <= CYBERMED_MONTHLY_MAX_FOAMED

    for pick_id in {"foam-2", "foam-3"}:
        assert any(it["id"] == pick_id for it in weekly_trimmed)

    # Monthly view should be tighter than weekly.
    assert len(monthly_trimmed) <= len(weekly_trimmed)
