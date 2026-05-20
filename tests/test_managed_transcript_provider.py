import os
import pathlib
import sys
import tempfile
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.managed_transcripts import fetch_managed_transcript
from newsagent2.youtube_content_providers import fetch_video_content


def _env(**extra):
    base = {
        "CYBERLURCH_CONTENT_PROVIDERS": "managed_transcript,metadata_only",
        "YOUTUBE_TRANSCRIPT_PROVIDER": "transcriptapi",
        "YOUTUBE_TRANSCRIPT_API_KEY": "super-secret-key",
        "MANAGED_TRANSCRIPT_MIN_CHARS": "5",
        "MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN": "2",
        "MANAGED_TRANSCRIPT_LANGS": "de,en,sv",
    }
    base.update(extra)
    return base


def test_transcriptapi_parses_transcript_list():
    diag = {}
    with patch.dict(os.environ, _env(), clear=False):
        resp = Mock(status_code=200, content=b"x")
        resp.json.return_value = {"transcript": [{"text": "hello"}, {"text": "world"}]}
        with patch("newsagent2.managed_transcripts.requests.get", return_value=resp):
            out = fetch_managed_transcript("abc", diagnostics=diag)
    assert out["status"] == "success"
    assert out["chars"] >= 5


def test_transcriptapi_401_auth_error():
    diag = {}
    with patch.dict(os.environ, _env(), clear=False):
        resp = Mock(status_code=401, content=b"")
        with patch("newsagent2.managed_transcripts.requests.get", return_value=resp):
            out = fetch_managed_transcript("abc", diagnostics=diag)
    assert out["error_kind"] == "auth_error"
    assert diag.get("managed_transcript_auth_error_total") == 1


def test_transcriptapi_429_rate_limited():
    diag = {}
    with patch.dict(os.environ, _env(), clear=False):
        resp = Mock(status_code=429, content=b"")
        with patch("newsagent2.managed_transcripts.requests.get", return_value=resp):
            out = fetch_managed_transcript("abc", diagnostics=diag)
    assert out["error_kind"] == "rate_limited"
    assert diag.get("managed_transcript_rate_limited_total") == 1


def test_supadata_parses_content_string():
    diag = {}
    with patch.dict(os.environ, _env(YOUTUBE_TRANSCRIPT_PROVIDER="supadata"), clear=False):
        resp = Mock(status_code=200, content=b"x")
        resp.json.return_value = {"content": "plain text ok"}
        with patch("newsagent2.managed_transcripts.requests.get", return_value=resp):
            out = fetch_managed_transcript("abc", diagnostics=diag)
    assert out["status"] == "success"


def test_generic_requires_base_url():
    diag = {}
    with patch.dict(os.environ, _env(YOUTUBE_TRANSCRIPT_PROVIDER="generic", YOUTUBE_TRANSCRIPT_API_BASE_URL=""), clear=False):
        out = fetch_managed_transcript("abc", diagnostics=diag)
    assert out["status"] == "misconfigured"


def test_missing_api_key_misconfigured():
    diag = {}
    with patch.dict(os.environ, _env(YOUTUBE_TRANSCRIPT_API_KEY=""), clear=False):
        out = fetch_managed_transcript("abc", diagnostics=diag)
    assert out["status"] == "misconfigured"


def test_provider_success_sets_source_in_content_pipeline():
    with tempfile.TemporaryDirectory() as td, patch.dict(os.environ, _env(), clear=False):
        with patch("newsagent2.youtube_content_providers.CACHE_PATH", pathlib.Path(td) / "cache.json"):
            resp = Mock(status_code=200, content=b"x")
            resp.json.return_value = {"text": "hello transcript managed"}
            with patch("newsagent2.managed_transcripts.requests.get", return_value=resp):
                out = fetch_video_content(video_id="v1", video_url="https://youtube.com/watch?v=v1", description="", diagnostics={})
    assert out.status == "success"
    assert out.source == "managed_transcript"


def test_budget_respected():
    diag = {"managed_transcript_attempted_total": 1}
    with patch.dict(os.environ, _env(MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN="1"), clear=False):
        out = fetch_managed_transcript("abc", diagnostics=diag)
    assert out["status"] == "budget_exhausted"
    assert diag.get("managed_transcript_skipped_budget_total") == 1


def test_no_secret_or_transcript_in_logs():
    diag = {}
    secret = "super-secret-key"
    transcript = "private transcript text"
    with patch.dict(os.environ, _env(), clear=False):
        resp = Mock(status_code=200, content=b"x")
        resp.json.return_value = {"text": transcript}
        with patch("newsagent2.managed_transcripts.requests.get", return_value=resp):
            fetch_managed_transcript("abc", diagnostics=diag)
    dumped = str(diag)
    assert secret not in dumped
    assert transcript not in dumped
