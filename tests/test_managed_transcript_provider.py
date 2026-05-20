import os
import pathlib
import sys
import tempfile
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.youtube_content_providers import fetch_video_content


def _env(**extra):
    base = {
        "CYBERLURCH_CONTENT_PROVIDERS": "managed_transcript,metadata_only",
        "YOUTUBE_TRANSCRIPT_PROVIDER": "generic",
        "YOUTUBE_TRANSCRIPT_API_KEY": "super-secret-key",
        "YOUTUBE_TRANSCRIPT_API_BASE_URL": "https://api.example.invalid/transcript",
        "MANAGED_TRANSCRIPT_MIN_CHARS": "5",
        "YOUTUBE_CONTENT_CACHE_DAYS": "1",
        "YOUTUBE_CONTENT_CACHE_TEXT": "1",
    }
    base.update(extra)
    return base


def test_generic_parses_text():
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(), clear=False):
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", pathlib.Path(td) / "cache.json"):
            resp = Mock(status_code=200, content=b"x")
            resp.json.return_value = {"text": "hello world text"}
            with patch("newsagent2.youtube_content_providers.requests.post", return_value=resp):
                out = fetch_video_content(video_id="v1", video_url="https://youtube.com/watch?v=v1", description="", diagnostics={})
    assert out.status == "success"
    assert out.source == "managed_transcript"


def test_generic_parses_content_and_segments():
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(), clear=False):
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", pathlib.Path(td) / "cache.json"):
            resp1 = Mock(status_code=200, content=b"x"); resp1.json.return_value = {"content": "abcdef"}
            resp2 = Mock(status_code=200, content=b"x"); resp2.json.return_value = {"segments": [{"text": "abc"}, {"text": "def"}]}
            with patch("newsagent2.youtube_content_providers.requests.post", side_effect=[resp1, resp2]):
                out1 = fetch_video_content(video_id="v2", video_url="https://youtube.com/watch?v=v2", description="", diagnostics={})
                out2 = fetch_video_content(video_id="v3", video_url="https://youtube.com/watch?v=v3", description="", diagnostics={})
    assert out1.status == "success"
    assert out2.status == "success"


def test_429_rate_limited_soft_fail_and_budget():
    diag = {}
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN="1"), clear=False):
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", pathlib.Path(td) / "cache.json"):
            resp = Mock(status_code=429, headers={"Retry-After": "1"}, content=b"")
            with patch("newsagent2.youtube_content_providers.requests.post", return_value=resp):
                out = fetch_video_content(video_id="v4", video_url="https://youtube.com/watch?v=v4", description="", diagnostics=diag)
                out2 = fetch_video_content(video_id="v5", video_url="https://youtube.com/watch?v=v5", description="", diagnostics=diag)
    assert out.status in {"empty", "error"}
    assert diag.get("managed_transcript_rate_limited_total", 0) == 1
    assert diag.get("managed_transcript_skipped_budget_total", 0) == 1
    assert out2.source == "metadata_only"


def test_cache_hit_avoids_api_call():
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(), clear=False):
        cache = pathlib.Path(td) / "cache.json"
        cache.write_text('{"v6": {"status":"success","source":"description","fetched_at_utc":"2999-01-01T00:00:00+00:00","text":"cached"}}')
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", cache), patch("newsagent2.youtube_content_providers.requests.post") as post:
            out = fetch_video_content(video_id="v6", video_url="https://youtube.com/watch?v=v6", description="", diagnostics={})
    assert out.text == "cached"
    post.assert_not_called()


def test_transcriptapi_with_key_is_configured():
    diag = {}
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(YOUTUBE_TRANSCRIPT_PROVIDER="transcriptapi"), clear=False):
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", pathlib.Path(td) / "cache.json"):
            resp = Mock(status_code=200, content=b"x")
            resp.json.return_value = {"text": "hello transcript api"}
            with patch("newsagent2.youtube_content_providers.requests.get", return_value=resp):
                out = fetch_video_content(video_id="v7", video_url="https://youtube.com/watch?v=v7", description="", diagnostics=diag)
    assert out.status == "success"
    assert diag.get("youtube_transcript_provider") == "transcriptapi"
    assert diag.get("managed_transcript_configured") is True
    assert diag.get("managed_transcript_api_key_present") is True


def test_provider_set_but_missing_key_increments_misconfigured():
    diag = {}
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(YOUTUBE_TRANSCRIPT_PROVIDER="supadata", YOUTUBE_TRANSCRIPT_API_KEY=""), clear=False):
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", pathlib.Path(td) / "cache.json"):
            out = fetch_video_content(video_id="v8", video_url="https://youtube.com/watch?v=v8", description="", diagnostics=diag)
    assert out.source == "metadata_only"
    assert diag.get("managed_transcript_misconfigured_total", 0) >= 1


def test_manual_override_none_disables_provider():
    diag = {}
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(YOUTUBE_TRANSCRIPT_PROVIDER="none"), clear=False):
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", pathlib.Path(td) / "cache.json"):
            out = fetch_video_content(video_id="v9", video_url="https://youtube.com/watch?v=v9", description="", diagnostics=diag)
    assert out.source == "metadata_only"
    assert diag.get("managed_transcript_attempted_total", 0) == 0

