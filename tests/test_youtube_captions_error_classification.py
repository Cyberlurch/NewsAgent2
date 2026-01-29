import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2.collectors_youtube import classify_captions_error_kind


def test_classify_captions_error_timeout():
    assert classify_captions_error_kind("ERROR: Read timed out while fetching data") == "timeout"


def test_classify_captions_error_no_subtitles():
    message = "There are no subtitles for the requested languages"
    assert classify_captions_error_kind(message) == "no_subtitles"


def test_classify_captions_error_http_403():
    assert classify_captions_error_kind("HTTP Error 403: Forbidden") == "http_403"


def test_classify_captions_error_http_429():
    assert classify_captions_error_kind("HTTP Error 429: Too Many Requests") == "http_429"


def test_classify_captions_error_bot_check():
    message = "Sign in to confirm you're not a bot"
    assert classify_captions_error_kind(message) == "bot_check"


def test_classify_captions_error_cli_option():
    assert classify_captions_error_kind("yt-dlp: error: no such option --bad") == "cli_option_error"


def test_classify_captions_error_extract_failed():
    assert classify_captions_error_kind("ERROR: Unable to extract nSig parameter") == "extract_failed"


def test_classify_captions_error_unknown():
    assert classify_captions_error_kind("unexpected failure") == "unknown"
