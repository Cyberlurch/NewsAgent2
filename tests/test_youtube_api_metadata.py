import pathlib
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.collectors_youtube_api import fetch_video_snippets
from newsagent2.main import _load_youtube_channel_id_cache, _save_youtube_channel_id_cache


class YouTubeApiMetadataTests(unittest.TestCase):
    def test_fetch_video_snippets_parses_response(self):
        diag = {}
        fake = Mock()
        fake.status_code = 200
        fake.content = b"1"
        fake.json.return_value = {
            "items": [
                {
                    "id": "abc123",
                    "snippet": {
                        "title": "T",
                        "description": "Long description text",
                        "channelTitle": "C",
                        "channelId": "UC12345678901234567890",
                        "publishedAt": "2026-01-01T00:00:00Z",
                    },
                    "contentDetails": {"caption": "true", "duration": "PT10M"},
                    "status": {},
                }
            ]
        }
        with patch("newsagent2.collectors_youtube_api.requests.get", return_value=fake):
            out = fetch_video_snippets(["abc123"], "secret", diag)
        self.assertEqual(out["abc123"]["channel_id"], "UC12345678901234567890")
        self.assertEqual(diag["youtube_api_metadata_attempted_total"], 1)
        self.assertEqual(diag["youtube_api_metadata_success_total"], 1)

    def test_missing_key_means_no_call_path(self):
        diag = {}
        with patch("newsagent2.collectors_youtube_api.requests.get") as get_mock:
            out = fetch_video_snippets([], "", diag)
        self.assertEqual(out, {})
        get_mock.assert_not_called()

    def test_channel_id_cache_read_write_without_secrets(self):
        with tempfile.TemporaryDirectory() as td:
            path = str(pathlib.Path(td) / "state.json")
            cache = {"channels": {"x": {"channel_id": "UC123", "source": "youtube_api", "updated_at_utc": "2026-01-01T00:00:00+00:00"}}}
            _save_youtube_channel_id_cache(cache, read_only_mode=False, path=path)
            loaded = _load_youtube_channel_id_cache(path)
            self.assertEqual(loaded["channels"]["x"]["channel_id"], "UC123")
            self.assertNotIn("api_key", str(loaded).lower())

    def test_read_only_does_not_write_cache(self):
        with tempfile.TemporaryDirectory() as td:
            path = pathlib.Path(td) / "state.json"
            _save_youtube_channel_id_cache({"channels": {"x": {"channel_id": "UC123"}}}, read_only_mode=True, path=str(path))
            self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
