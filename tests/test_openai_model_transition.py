"""Regression checks for the November 2026 model migration."""
from types import SimpleNamespace

import pytest

from newsagent2 import cybermed_openai as med, summarizer
from newsagent2.cybermed_quality import CybermedGenerationError
from newsagent2.openai_model_compat import prepare_chat_request


@pytest.mark.parametrize("legacy", [
    "gpt-5-mini", "gpt-5-mini-2025-08-07", "gpt-4.1",
    "gpt-4.1-2025-04-14", "gpt-4.1-mini",
])
def test_stale_cyberlurch_model_variables_are_normalized(monkeypatch, legacy):
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    assert summarizer._cyberlurch_model(legacy) == "gpt-6.1-terra"
    assert summarizer._cyberlurch_model("gpt-6.2-sol") == "gpt-6.2-sol"
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    assert summarizer._cyberlurch_model(legacy) == legacy


@pytest.mark.parametrize("model,effort", [
    ("gpt-6.1-terra", "none"),
    ("gpt-6-luna", "none"),
    ("gpt-6.2-sol", "low"),
])
def test_new_model_request_preserves_json_and_completion_limit(model, effort):
    original = {"temperature": 0.2, "max_tokens": 1800,
                "messages": [{"role": "user", "content": "JSON"}],
                "response_format": {"type": "json_object"}}
    req = prepare_chat_request(model, original)
    assert req["reasoning_effort"] == effort
    assert req["max_completion_tokens"] == 1800
    assert req["response_format"] == original["response_format"]
    assert req["messages"] == original["messages"]
    assert "temperature" not in req and "max_tokens" not in req
    assert original["temperature"] == 0.2 and original["max_tokens"] == 1800
    assert prepare_chat_request("gpt-4.1", original) == original


def test_cyberlurch_direct_digest_calls_new_model_with_json(monkeypatch):
    calls = []
    def create(**kw):
        calls.append(kw)
        return SimpleNamespace(
            choices=[SimpleNamespace(finish_reason="stop",
                     message=SimpleNamespace(content='{"transcript_full_summary":"The council approved 25 sites.","transcript_key_points":["The council approved 25 sites."]}'))],
            usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        )
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    monkeypatch.setattr(summarizer, "OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST",
                        summarizer._cyberlurch_model("gpt-5-mini"))
    monkeypatch.setattr(summarizer, "_get_client", lambda: SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))))
    out = summarizer.summarize_youtube_transcript_direct(
        {"text": "The council approved 25 sites.", "language": "en"}, language="en")
    assert out["transcript_generation_status"] == "validated"
    assert len(calls) == 1
    assert calls[0]["model"] == "gpt-6.1-terra"
    assert calls[0]["reasoning_effort"] == "none"
    assert calls[0]["response_format"] == {"type": "json_object"}
    assert "temperature" not in calls[0]


@pytest.mark.parametrize("model", ["gpt-6.2-sol", "gpt-6.1-terra", "gpt-6-luna"])
def test_clinical_candidate_only_allowed_in_eval_and_budgeted(monkeypatch, model):
    calls = []
    def create(**kw):
        calls.append(kw)
        return SimpleNamespace(
            choices=[SimpleNamespace(finish_reason="stop",
                     message=SimpleNamespace(content="Synthetic output."))],
            usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        )
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    monkeypatch.setattr(med, "CYBERMED_OPENAI_DIAGNOSTICS",
                        med.CybermedOpenAIDiagnostics(med.CybermedOpenAIBudget.from_env()))
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)
    monkeypatch.delenv("CYBERMED_APPROVED_MODEL", raising=False)
    with pytest.raises(CybermedGenerationError, match="model_policy_violation"):
        med.cybermed_chat_create(client, stage="overview", model=model, messages=[])
    monkeypatch.setenv("CYBERMED_MODEL_EVAL_MODE", "1")
    with pytest.raises(CybermedGenerationError, match="model_policy_violation"):
        med.cybermed_chat_create(client, stage="overview", model=model, messages=[])
    med.cybermed_chat_create(client, stage="model_eval", model=model,
                            messages=[{"role":"user","content":"Synthetic"}],
                            temperature=0, max_tokens=1800)
    assert len(calls) == 1 and calls[0]["model"] == model
    assert calls[0]["max_completion_tokens"] == 1800 and "max_tokens" not in calls[0]
    report = med.CYBERMED_OPENAI_DIAGNOSTICS.to_dict()
    assert report["provider_calls_attempted_total"] == 1
    assert report["budget_accounted"]["estimated_cost_usd"] > 0


def test_explicit_clinical_approval_enables_only_reviewed_model(monkeypatch):
    calls = []
    def create(**kw):
        calls.append(kw)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
                               usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5, total_tokens=15))
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    monkeypatch.setattr(med, "CYBERMED_OPENAI_DIAGNOSTICS",
                        med.CybermedOpenAIDiagnostics(med.CybermedOpenAIBudget.from_env()))
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)
    monkeypatch.setenv("CYBERMED_APPROVED_MODEL", "gpt-6.2-sol")
    for wrong in ("gpt-4.1-2025-04-14", "gpt-6.1-terra"):
        with pytest.raises(CybermedGenerationError, match="model_policy_violation"):
            med.cybermed_chat_create(client, stage="overview", model=wrong, messages=[])
    med.cybermed_chat_create(client, stage="overview", model="gpt-6.2-sol",
                            messages=[{"role":"user","content":"Synthetic"}],
                            max_tokens=240, temperature=0.2)
    assert len(calls) == 1
    assert calls[0]["max_completion_tokens"] == 1440
    assert calls[0]["reasoning_effort"] == "low"
    assert "temperature" not in calls[0]
