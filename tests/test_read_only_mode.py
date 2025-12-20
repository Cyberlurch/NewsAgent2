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


if __name__ == "__main__":
    unittest.main()
