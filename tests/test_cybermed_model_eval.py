from __future__ import annotations

from pathlib import Path

from newsagent2.maintenance.evaluate_cybermed_models import evaluate_output


def test_model_eval_objective_checks_detect_format_facts_and_new_numbers():
    case = {
        "source_text": "A randomized study enrolled 100 people; 30% improved.",
        "required_markers": ["BOTTOM LINE:", "Limitations:"],
        "critical_terms": ["100", "30%", "randomized"],
        "max_words": 30,
    }
    result = evaluate_output(
        case,
        "BOTTOM LINE: In this randomized study of 100 people, 30% improved. Limitations: follow-up was short; 77% was not reported.",
    )

    assert result["required_markers_passed"] is True
    assert result["critical_term_coverage"] == 1.0
    assert result["unsupported_numeric_tokens"] == ["77%"]
    assert result["word_limit_passed"] is True


def test_model_eval_workflow_is_manual_non_mutating_and_separate_from_production():
    eval_text = Path(".github/workflows/cybermed-model-eval.yml").read_text(
        encoding="utf-8"
    )
    production_text = Path(".github/workflows/cybermed.yml").read_text(
        encoding="utf-8"
    )

    assert "workflow_dispatch:" in eval_text
    assert "schedule:" not in eval_text
    assert 'permissions:\n  contents: read' in eval_text
    assert 'CYBERMED_MODEL_EVAL_MODE: "1"' in eval_text
    assert "gpt-4.1-2025-04-14" in eval_text
    assert "gpt-4.1-mini-2025-04-14" in eval_text
    for forbidden in (
        "SEND_EMAIL",
        "RECIPIENTS",
        "SMTP_",
        "state/",
        "git commit",
        "git push",
    ):
        assert forbidden not in eval_text

    assert "gpt-4.1-mini" not in production_text
    assert 'CYBERMED_MODEL_EVAL_MODE: "0"' in production_text
    assert "OPENAI_MODEL_CYBERMED: ${{ vars.OPENAI_MODEL_CYBERMED || 'gpt-4.1-2025-04-14' }}" in production_text
    assert "CYBERMED_OPENAI_MAX_ESTIMATED_COST_USD_PER_RUN" in production_text


def test_model_eval_fixture_is_synthetic_and_bounded():
    text = Path("data/cybermed_model_eval_cases.json").read_text(encoding="utf-8")

    assert text.count("Synthetic benchmark") == 3
    assert '"max_output_tokens": 600' in text
    assert "patient name" not in text.lower()
