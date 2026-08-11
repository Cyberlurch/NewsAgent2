import json
import sys
import unittest
from pathlib import Path
from unittest import mock

# Allow running tests without installing the package.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2 import main  # noqa: E402


class TestReadOnlyMode(unittest.TestCase):
    def test_cybermed_lookback_ignores_shared_marker_and_uses_daily_store(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            digest_path = Path(tmp) / "cybermed_daily_digests.json"
            digest_path.write_text(
                json.dumps({
                    "schema_version": 1,
                    "digests": [{
                        "digest_id": "cybermed_daily_2026-08-07",
                        "report_key": "cybermed",
                        "cadence": "daily",
                        "generated_at_utc": "2026-08-07T05:18:55+00:00",
                    }],
                }),
                encoding="utf-8",
            )
            state = {"last_successful_daily_run_utc": "2026-08-10T05:08:00+00:00"}

            marker, source = main._cybermed_daily_lookback_marker(state, str(digest_path))

        self.assertEqual(marker, "2026-08-07T05:18:55+00:00")
        self.assertEqual(source, "cybermed_daily_digest_store")

    def test_update_state_read_only_skips_writes(self):
        state = {}
        items = [{"source": "youtube", "id": "abc", "title": "x"}]

        with mock.patch("newsagent2.main.save_state") as save_mock, mock.patch(
            "newsagent2.main.mark_processed"
        ) as mark_mock, mock.patch("newsagent2.main.mark_sent") as mark_sent_mock, mock.patch(
            "newsagent2.main.mark_screened"
        ) as mark_screened_mock:
            main._update_state_after_run(
                state_path="state/processed_items.json",
                state=state,
                items_all_new=items,
                overview_items=[],
                detail_items=[],
                foamed_overview_items=[],
                report_key="cyberlurch",
                report_mode="weekly",
                now_utc_iso="2024-01-01T00:00:00+00:00",
                read_only=True,
            )

        save_mock.assert_not_called()
        mark_mock.assert_not_called()
        mark_sent_mock.assert_not_called()
        mark_screened_mock.assert_not_called()
        self.assertNotIn("last_successful_daily_run_utc", state)

    def test_cybermed_daily_uses_its_own_success_marker(self):
        state = {"last_successful_daily_run_utc": "2026-08-10T05:08:00+00:00"}

        with mock.patch("newsagent2.main.save_state") as save_mock:
            main._update_state_after_run(
                state_path="state/processed_items.json",
                state=state,
                items_all_new=[],
                overview_items=[],
                detail_items=[],
                foamed_overview_items=[],
                report_key="cybermed",
                report_mode="daily",
                now_utc_iso="2026-08-10T05:12:00+00:00",
                read_only=False,
            )

        self.assertEqual(state["last_successful_daily_run_utc"], "2026-08-10T05:08:00+00:00")
        self.assertEqual(
            state["last_successful_daily_run_utc_by_report"]["cybermed"],
            "2026-08-10T05:12:00+00:00",
        )
        save_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
