import textwrap

from src.newsagent2.summarizer import _ensure_pubmed_deep_dive_template


def test_bottom_line_only_rebuilt_with_template():
    md = "BOTTOM LINE: Promising early signal"
    fixed = _ensure_pubmed_deep_dive_template(md, lang="en")

    required = [
        "BOTTOM LINE:",
        "Study type:",
        "Population/setting:",
        "Intervention/exposure & comparator:",
        "Primary endpoints:",
        "Key results:",
        "Limitations:",
        "Why this matters:",
    ]
    for heading in required:
        assert heading in fixed

    assert "Limitations:\n- " in fixed


def test_structured_output_is_preserved_and_normalized():
    md = textwrap.dedent(
        """
        **BOTTOM LINE:** Benefit observed

        **Study type:** RCT
        **Population/setting:** ICU adults
        **Intervention/Exposure & comparator:** Drug vs placebo
        **Primary endpoints:** Mortality at 28 days
        **Key results:** No difference in mortality
        **Limitations:** Small sample
        **Why this matters:** Could guide future trials
        """
    ).strip()

    fixed = _ensure_pubmed_deep_dive_template(md, lang="en")

    assert "BOTTOM LINE: Benefit observed" in fixed
    assert "Study type: RCT" in fixed
    assert "Limitations:\n- Small sample" in fixed
    assert fixed.count("Limitations:") == 1
