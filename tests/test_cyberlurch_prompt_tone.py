import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.summarizer import _SYS_DETAIL_YOUTUBE_CYBERLURCH_EN, _SYS_OVERVIEW_CYBERLURCH_EN


def test_christian_prompt_is_respectful_not_mocking():
    txt = (_SYS_OVERVIEW_CYBERLURCH_EN + "\n" + _SYS_DETAIL_YOUTUBE_CYBERLURCH_EN).lower()
    assert "christian/theological/apologetic" in txt
    assert "respectful" in txt
    assert "mock" not in txt


def test_fringe_prompt_allows_dry_tone_without_fact_promotion():
    txt = (_SYS_OVERVIEW_CYBERLURCH_EN + "\n" + _SYS_DETAIL_YOUTUBE_CYBERLURCH_EN).lower()
    assert "light dry tone" in txt
    assert "without presenting claims as established fact" in txt
