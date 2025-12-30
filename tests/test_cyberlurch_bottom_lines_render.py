import pathlib
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.reporter import to_markdown


def test_cyberlurch_weekly_bottom_line_rendering():
    item = {
        "id": "vid1",
        "url": "https://example.com/video1",
        "title": "Interesting Video",
        "channel": "Example Channel",
        "published_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
        "bottom_line": "Something meaningful.",
    }

    md = to_markdown(
        [item],
        overview_markdown="## Executive Summary\n\nOverview body.",
        details_by_id={},
        report_title="Cyberlurch Report — Weekly",
        report_language="en",
        report_mode="weekly",
    )

    assert "## Top videos (this period)" in md
    assert "BOTTOM LINE: Something meaningful." in md
