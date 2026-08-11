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

    def test_disabled_source_strategy_override_in_audit(self):
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        state = {
            "foamed_source_health": {
                "journalfeed": {"disabled_until_utc": (now + timedelta(days=3)).isoformat()}
            }
        }
        sources = [{
            "name": "journalfeed",
            "feed_url": "https://example.com/feed",
            "homepage": "https://example.com",
            "extraction_strategy": "html_only",
            "ignore_auto_disable_if_strategy_viable": True,
        }]
        import os
        os.environ["FOAMED_AUDIT"] = "1"
        filtered, stats = main._filter_disabled_foamed_sources(sources, state, now, auto_disable_enabled=True)
        self.assertEqual(len(filtered), 1)
        self.assertTrue(filtered[0]["strategy_override_disabled"])
        self.assertTrue(filtered[0]["disabled_state_present"])
        self.assertEqual(stats.get("strategy_override_disabled_count"), 1)

    def test_disabled_source_override_not_allowed_for_non_html_only(self):
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        state = {"foamed_source_health": {"src": {"disabled_until_utc": (now + timedelta(days=3)).isoformat()}}}
        sources = [{
            "name": "src",
            "feed_url": "https://example.com/feed",
            "homepage": "https://example.com",
            "extraction_strategy": "rss_then_article",
            "ignore_auto_disable_if_strategy_viable": True,
        }]
        import os
        os.environ["FOAMED_AUDIT"] = "1"
        filtered, stats = main._filter_disabled_foamed_sources(sources, state, now, auto_disable_enabled=True)
        self.assertEqual(filtered, [])
        self.assertEqual(stats.get("strategy_override_disabled_count"), 0)

    def test_health_epoch_retries_source_after_endpoint_repair(self):
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        state = {
            "foamed_source_health": {
                "Taming the SRU": {
                    "consecutive_failures": 4,
                    "disabled_until_utc": (now + timedelta(days=12)).isoformat(),
                    "last_health": "not_found_404",
                }
            }
        }
        sources = [{
            "name": "Taming the SRU",
            "feed_url": "https://www.tamingthesru.com/blog?format=rss",
            "health_epoch": "2026-08-squarespace-rss-v1",
        }]

        filtered, stats = main._filter_disabled_foamed_sources(
            sources,
            state,
            now,
            auto_disable_enabled=True,
        )

        self.assertEqual(len(filtered), 1)
        self.assertEqual(stats.get("health_epoch_reset_count"), 1)
        entry = state["foamed_source_health"]["Taming the SRU"]
        self.assertEqual(entry["consecutive_failures"], 0)
        self.assertEqual(entry["disabled_until_utc"], "")
        self.assertEqual(entry["health_epoch"], "2026-08-squarespace-rss-v1")

    def test_budget_truncated_source_does_not_change_health_state(self):
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        existing = {
            "consecutive_failures": 2,
            "disabled_until_utc": "",
            "last_health": "ok_rss",
            "last_seen_utc": "2026-08-10T00:00:00+00:00",
        }
        state = {"foamed_source_health": {"Source": dict(existing)}}

        main._update_foamed_health_state(
            state,
            {
                "Source": {
                    "health": "budget_exhausted",
                    "collection_incomplete_due_to_budget": True,
                }
            },
            now,
            auto_disable_enabled=True,
            disable_after_403=3,
            disable_days_403=7,
            disable_after_404=2,
            disable_days_404=30,
            source_names={"Source"},
        )

        self.assertEqual(state["foamed_source_health"]["Source"], existing)


if __name__ == "__main__":
    unittest.main()
