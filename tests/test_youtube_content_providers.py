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
            with patch("newsagent2.youtube_content_providers.CACHE_PATH", cache), patch.dict(os.environ, {"CYBERLURCH_CONTENT_PROVIDERS": "description,yt_dlp_captions", "YOUTUBE_CONTENT_CACHE_DAYS": "1", "DESCRIPTION_PROVIDER_MIN_CHARS": "5"}, clear=False):
                res = fetch_video_content(video_id="abc-order", video_url="https://x", description="substantive content " * 20, diagnostics=diag)
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
            with patch("newsagent2.youtube_content_providers.CACHE_PATH", cache), patch.dict(os.environ, {"CYBERLURCH_CONTENT_PROVIDERS": "description", "YOUTUBE_CONTENT_CACHE_DAYS": "1", "DESCRIPTION_PROVIDER_MIN_CHARS": "5"}):
                diag = {}
                res = fetch_video_content(video_id="v1", video_url="https://x", description="substantive content " * 20, diagnostics=diag)
                self.assertIn("substantive content", res.text)
                self.assertEqual(diag.get("cache_miss_total"), 1)

    def test_description_provider_quality_threshold(self):
        with tempfile.TemporaryDirectory() as td:
            cache = pathlib.Path(td) / "youtube_content_cache.json"
            long_ok = "This is substantive content. " * 30
            short_bad = "short blurb"
            with patch("newsagent2.youtube_content_providers.CACHE_PATH", cache), patch.dict(os.environ, {"CYBERLURCH_CONTENT_PROVIDERS": "description", "DESCRIPTION_PROVIDER_MIN_CHARS": "300"}, clear=False):
                res_ok = fetch_video_content(video_id="v-ok", video_url="https://x", description=long_ok, diagnostics={})
                res_bad = fetch_video_content(video_id="v-bad", video_url="https://x", description=short_bad, diagnostics={})
        self.assertEqual(res_ok.status, "success")
        self.assertEqual(res_bad.status, "empty")


if __name__ == "__main__":
    unittest.main()


def test_cache_text_default_off_for_managed_transcript(tmp_path, monkeypatch):
    from newsagent2.youtube_content_providers import fetch_video_content
    monkeypatch.setenv("CYBERLURCH_CONTENT_PROVIDERS", "managed_transcript")
    monkeypatch.setenv("YOUTUBE_TRANSCRIPT_PROVIDER", "transcriptapi")
    monkeypatch.setenv("YOUTUBE_TRANSCRIPT_API_KEY", "k")
    monkeypatch.setenv("MANAGED_TRANSCRIPT_MIN_CHARS", "1")
    from unittest.mock import Mock, patch
    with patch("newsagent2.youtube_content_providers.CACHE_PATH", tmp_path/"cache.json"):
        resp=Mock(status_code=200,content=b"x"); resp.json.return_value={"text":"abc"}
        with patch("newsagent2.managed_transcripts.requests.get", return_value=resp):
            with patch("newsagent2.managed_transcripts.ATTEMPTS_PATH", tmp_path/"attempts.json"):
                fetch_video_content(video_id="v-cache-1", video_url="u", description="", diagnostics={})
    data=(tmp_path/"cache.json").read_text(encoding="utf-8")
    assert '"text": ""' in data


def test_providers_override_skips_managed_transcript(tmp_path, monkeypatch):
    monkeypatch.setenv("CYBERLURCH_CONTENT_PROVIDERS", "managed_transcript,description")
    with patch("newsagent2.youtube_content_providers.CACHE_PATH", tmp_path/"cache.json"):
        diag = {}
        res = fetch_video_content(
            video_id="v-ovr",
            video_url="https://x",
            description="substantive content " * 30,
            diagnostics=diag,
            providers_override="description,metadata_only",
        )
    assert res.source == "description"
    assert diag["provider_attempted_by_name"].get("managed_transcript") is None
