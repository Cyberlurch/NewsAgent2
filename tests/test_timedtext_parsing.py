import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2.collectors_youtube_timedtext import _parse_timedtext_list, _parse_timedtext_track


def test_parse_timedtext_list_tracks():
    xml_text = """
    <transcript_list>
        <track id="123" name="English" lang_code="en" lang_default="true" />
        <track id="456" name="" lang_code="de" kind="asr" />
    </transcript_list>
    """
    tracks = _parse_timedtext_list(xml_text)
    assert len(tracks) == 2
    assert tracks[0].lang_code == "en"
    assert tracks[0].name == "English"
    assert tracks[0].track_id == "123"
    assert tracks[0].lang_default == "true"
    assert tracks[1].lang_code == "de"
    assert tracks[1].kind == "asr"


def test_parse_timedtext_track_text():
    xml_text = """
    <transcript>
        <text start="0.0" dur="1.2">Hello &amp; welcome</text>
        <text start="1.2" dur="1.3">to the show</text>
    </transcript>
    """
    parsed = _parse_timedtext_track(xml_text)
    assert parsed == "Hello & welcome to the show"
