import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running tests without installing the package.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2 import main  # noqa: E402


class TestFoamedAutoDisable(unittest.TestCase):
    def test_blocked_403_reaches_disable_threshold(self):
        state = {}
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # First failure: increments but does not disable yet.
        main._update_foamed_health_state(
            state,
            {"source1": {"health": "blocked_403"}},
            now,
            auto_disable_enabled=True,
            disable_after_403=2,
            disable_days_403=7,
            disable_after_404=2,
            disable_days_404=30,
        )
        entry = state["foamed_source_health"]["source1"]
        self.assertFalse(main._foamed_source_disabled(entry, now))
        self.assertEqual(entry["consecutive_failures"], 1)

        # Second consecutive 403 triggers disable window.
        main._update_foamed_health_state(
            state,
            {"source1": {"health": "blocked_403"}},
            now,
            auto_disable_enabled=True,
            disable_after_403=2,
            disable_days_403=7,
            disable_after_404=2,
            disable_days_404=30,
        )
        entry = state["foamed_source_health"]["source1"]
        self.assertTrue(main._foamed_source_disabled(entry, now))
        disabled_until = main._parse_iso_utc(entry["disabled_until_utc"])
        self.assertIsNotNone(disabled_until)
        self.assertGreater(disabled_until, now)

    def test_disabled_sources_are_filtered(self):
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        state = {
            "foamed_source_health": {
                "skipme": {
                    "disabled_until_utc": (now + timedelta(days=5)).isoformat(),
                    "consecutive_failures": 3,
                }
            }
        }
        sources = [{"name": "skipme", "feed_url": "http://example.com"}]
        filtered, stats = main._filter_disabled_foamed_sources(
            sources, state, now, auto_disable_enabled=True
        )
        self.assertEqual(filtered, [])
        self.assertEqual(stats.get("skipped_disabled_count"), 1)
        self.assertEqual(stats.get("disabled_active_count"), 1)

    def test_ok_resets_failure_and_clears_disable(self):
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        state = {
            "foamed_source_health": {
                "healme": {
                    "consecutive_failures": 4,
                    "disabled_until_utc": (now + timedelta(days=10)).isoformat(),
                    "last_health": "blocked_403",
                }
            }
        }
        main._update_foamed_health_state(
            state,
            {"healme": {"health": "ok_rss"}},
            now,
            auto_disable_enabled=True,
            disable_after_403=3,
            disable_days_403=7,
            disable_after_404=2,
            disable_days_404=30,
        )
        entry = state["foamed_source_health"]["healme"]
        self.assertEqual(entry.get("consecutive_failures"), 0)
        self.assertEqual(entry.get("disabled_until_utc"), "")
        self.assertEqual(entry.get("last_health"), "ok_rss")
        self.assertTrue(entry.get("last_ok_utc"))
        self.assertFalse(main._foamed_source_disabled(entry, now))


if __name__ == "__main__":
    unittest.main()
