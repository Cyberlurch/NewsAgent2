from src.newsagent2 import summarizer


def test_parse_structured_pubmed_abstract_sections_extracts_headings():
    abstract = (
        "Objectives: Assess efficacy of treatment A.\n"
        "Design: Randomized controlled trial.\n"
        "Setting: Single tertiary hospital.\n"
        "Subjects: 120 adult ICU patients.\n"
        "Interventions: Treatment A vs placebo.\n"
        "Measurements and main results: Treatment A reduced mortality compared with placebo.\n"
        "Conclusions: Treatment A improved survival.\n"
    )

    sections = summarizer._parse_structured_pubmed_abstract_sections(abstract)

    assert sections["objectives"].startswith("Assess efficacy")
    assert "Randomized controlled trial" in sections["design"]
    assert "Single tertiary hospital." in sections["setting"]
    assert "120 adult ICU patients." in sections["subjects"]
    assert sections["interventions"].startswith("Treatment A vs placebo")
    assert sections["measurements"].startswith("Treatment A reduced mortality")
    assert sections["conclusions"].startswith("Treatment A improved survival")


def test_structured_abstract_rescue_reduces_missing_fields():
    placeholder_md = "\n".join(
        [
            "BOTTOM LINE: Not reported",
            "- Study type: Not reported",
            "- Population/setting: Not reported",
            "- Intervention/exposure & comparator: Not reported",
            "- Primary endpoints: Not reported",
            "- Key results: Not reported",
            "- Limitations:",
            "- Not reported",
            "- Why this matters: Not reported",
        ]
    )
    abstract = (
        "Objectives: Evaluate mortality impact of treatment A in critical illness. "
        "Design: Multicenter randomized controlled trial. "
        "Setting: ICU patients across three hospitals. "
        "Subjects: 300 adults with severe disease. "
        "Interventions: Treatment A versus placebo. "
        "Measurements and main results: 30-day mortality was lower with treatment A without excess adverse events. "
        "Conclusions: Treatment A improved short-term survival in this population."
    )

    base_md, base_missing = summarizer._normalize_pubmed_field_values(placeholder_md, lang="en")
    rescued_md, rescued_missing, used = summarizer._heuristic_fill_pubmed_deep_dive_from_structured_abstract(
        bottom_line="", lang="en", abstract_text=abstract, current_md=base_md
    )

    assert used is True
    assert rescued_missing < base_missing
    assert "Multicenter randomized controlled trial" in rescued_md
    assert "ICU patients across three hospitals" in rescued_md
    assert "300 adults with severe disease" in rescued_md
    assert "30-day mortality was lower with treatment A without excess adverse events." in rescued_md
