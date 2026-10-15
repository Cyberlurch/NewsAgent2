import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.summarizer import _SYS_DETAIL_YOUTUBE_CYBERLURCH_EN, _SYS_OVERVIEW_CYBERLURCH_EN


def test_christian_prompt_is_respectful_not_mocking():
    txt = (_SYS_OVERVIEW_CYBERLURCH_EN + "\n" + _SYS_DETAIL_YOUTUBE_CYBERLURCH_EN).lower()
    assert "christian/theological/apologetic" in txt
    assert "respectful" in txt
    assert "mock" not in txt


def test_all_channels_keep_source_tone_without_added_ideological_labels_or_irony():
    txt = (_SYS_OVERVIEW_CYBERLURCH_EN + "\n" + _SYS_DETAIL_YOUTUBE_CYBERLURCH_EN).lower()
    assert "same respectful editorial treatment" in txt
    assert "without adding irony, ideological labels or judgments" in txt
    assert "preserve each channel's own argument and tone" in txt
    assert "light dry tone" not in txt and "fringe/alien" not in txt
    assert "negation and forecasts" in txt
