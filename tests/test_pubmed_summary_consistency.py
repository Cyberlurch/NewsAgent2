from newsagent2.main import _build_pubmed_shared_synopsis, _detect_pubmed_bottom_line_conflicts


def test_shared_synopsis_negative_primary_is_cautious():
    item = {"abstract": "Objectives: Evaluate intervention. Measurements and main results: Primary endpoint was not significant (P=.12). Results: Secondary markers improved."}
    syn = _build_pubmed_shared_synopsis(item)
    assert syn["primary_result_direction"] == "negative_or_null"
    assert syn["primary_result_significance"] == "not_significant"
    assert "not significantly" in syn["bottom_line"].lower() or "cautious" in syn["bottom_line"].lower()


def test_shared_synopsis_mixed_composite_is_cautious():
    item = {"abstract": "Results: Composite endpoint was significant, but individual components were mixed."}
    syn = _build_pubmed_shared_synopsis(item)
    assert syn["primary_result_direction"] in {"mixed_or_unclear", "unclear"}


def test_contradiction_guard_detects_negative_vs_positive():
    conflicts = _detect_pubmed_bottom_line_conflicts("did not reduce primary endpoint", "BOTTOM LINE: reduced complications")
    assert "negative_overview_vs_positive_deep_dive" in conflicts


def test_contradiction_guard_detects_no_benefit_vs_beneficial():
    conflicts = _detect_pubmed_bottom_line_conflicts("no benefit for primary outcome", "beneficial and improved outcomes")
    assert "negative_overview_vs_positive_deep_dive" in conflicts


def test_contradiction_guard_does_not_flag_consistent_sentiment():
    assert not _detect_pubmed_bottom_line_conflicts("no significant benefit", "no significant improvement")
    assert not _detect_pubmed_bottom_line_conflicts("significant improvement", "reduced events significantly")
