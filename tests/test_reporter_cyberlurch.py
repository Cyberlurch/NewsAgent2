import os
import pathlib
import sys
import unittest
from datetime import datetime
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import reporter


class CyberlurchPeriodicRenderingTests(unittest.TestCase):
    def test_weekly_top_videos_are_links_and_sources_removed(self):
        items = [
            {
                "id": "v1",
                "title": "Video One",
                "url": "https://example.com/one",
                "channel": "Channel A",
                "published_at": datetime(2024, 1, 2),
                "top_pick": True,
            },
            {
                "id": "v2",
                "title": "Video Two",
                "url": "https://example.com/two",
                "channel": "Channel B",
                "published_at": datetime(2024, 1, 3),
            },
        ]

        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id={},
                report_title="Cyberlurch Weekly",
                report_language="en",
                report_mode="weekly",
            )

        self.assertIn("## Top videos (this period)", md)
        self.assertIn("[Video One](https://example.com/one)", md)
        self.assertIn("[Video Two](https://example.com/two)", md)
        self.assertNotIn("## Sources", md)


    def test_monthly_sources_removed_but_top_videos_present(self):
        items = [
            {
                "id": "v3",
                "title": "Video Three",
                "url": "https://example.com/three",
                "channel": "Channel C",
                "published_at": datetime(2024, 2, 1),
            }
        ]

        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id={},
                report_title="Cyberlurch — Monthly",
                report_language="en",
                report_mode="monthly",
            )

        self.assertIn("## Top videos (this period)", md)
        self.assertIn("[Video Three](https://example.com/three)", md)
        self.assertNotIn("## Sources", md)


    def test_daily_sources_remain_present(self):
        items = [
            {
                "id": "d1",
                "title": "Daily Video",
                "url": "https://example.com/daily",
                "channel": "Channel D",
                "published_at": datetime(2024, 3, 1),
            }
        ]

        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id={},
                report_title="Cyberlurch Daily",
                report_language="en",
                report_mode="daily",
            )

        self.assertIn("## Sources", md)
        self.assertIn("https://example.com/daily", md)


if __name__ == "__main__":
    unittest.main()
