import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.youtube_content_providers import fetch_video_content


class YouTubeContentProviderTests(unittest.TestCase):
    def test_provider_order_first_success_stops_chain(self):
        with tempfile.TemporaryDirectory() as td:
            cache = pathlib.Path(td) / "youtube_content_cache.json"
            diag = {}
            with patch("newsagent2.youtube_content_providers.CACHE_PATH", cache), patch.dict(os.environ, {"CYBERLURCH_CONTENT_PROVIDERS": "description,yt_dlp_captions", "YOUTUBE_CONTENT_CACHE_DAYS": "1"}, clear=False):
                res = fetch_video_content(video_id="abc-order", video_url="https://x", description="hello world", diagnostics=diag)
        self.assertEqual(res.source, "description")
        self.assertEqual(diag["provider_attempted_by_name"].get("description"), 1)
        self.assertIsNone(diag["provider_attempted_by_name"].get("yt_dlp_captions"))

    def test_cache_hit_skips_provider(self):
        with tempfile.TemporaryDirectory() as td:
            state = pathlib.Path(td) / "state"
            state.mkdir()
            cache = state / "youtube_content_cache.json"
            cache.write_text(json.dumps({"v1": {"status": "success", "source": "description", "fetched_at_utc": "2999-01-01T00:00:00+00:00", "text": "cached"}}))
            with patch("newsagent2.youtube_content_providers.CACHE_PATH", cache):
                diag = {}
                res = fetch_video_content(video_id="v1", video_url="https://x", description="", diagnostics=diag)
                self.assertEqual(res.text, "cached")
                self.assertEqual(diag.get("cache_hit_total"), 1)

    def test_cache_expiry_triggers_provider_call(self):
        with tempfile.TemporaryDirectory() as td:
            state = pathlib.Path(td) / "state"
            state.mkdir()
            cache = state / "youtube_content_cache.json"
            cache.write_text(json.dumps({"v1": {"status": "success", "source": "description", "fetched_at_utc": "2000-01-01T00:00:00+00:00", "text": "old"}}))
            with patch("newsagent2.youtube_content_providers.CACHE_PATH", cache), patch.dict(os.environ, {"CYBERLURCH_CONTENT_PROVIDERS": "description", "YOUTUBE_CONTENT_CACHE_DAYS": "1"}):
                diag = {}
                res = fetch_video_content(video_id="v1", video_url="https://x", description="fresh", diagnostics=diag)
                self.assertEqual(res.text, "fresh")
                self.assertEqual(diag.get("cache_miss_total"), 1)


if __name__ == "__main__":
    unittest.main()
