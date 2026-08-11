from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from newsagent2 import cybermed_openai
from newsagent2 import main as main_module
from newsagent2.cybermed_openai import (
    CybermedOpenAIBudget,
    CybermedOpenAIDiagnostics,
    cybermed_chat_create,
)
from newsagent2.cybermed_quality import CybermedGenerationError


def _response(*, prompt_tokens: int = 12, completion_tokens: int = 4):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="safe output"))],
        usage=SimpleNamespace(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        ),
    )


class _Client:
    def __init__(self, call):
        self.calls = 0

        def create(**kwargs):
            self.calls += 1
            return call(kwargs)

        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=create)
        )


def _tracker(*, calls: int = 4, input_tokens: int = 10_000, output_tokens: int = 10_000, cost: str = "1"):
    return CybermedOpenAIDiagnostics(
        CybermedOpenAIBudget(
            max_calls_per_run=calls,
            max_input_tokens_per_run=input_tokens,
            max_output_tokens_per_run=output_tokens,
            max_estimated_cost_usd_per_run=Decimal(cost),
        )
    )


def test_budget_blocks_before_second_provider_call(monkeypatch):
    tracker = _tracker(calls=1)
    monkeypatch.setattr(cybermed_openai, "CYBERMED_OPENAI_DIAGNOSTICS", tracker)
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)
    seen = []
    client = _Client(lambda kwargs: seen.append(kwargs) or _response())

    cybermed_chat_create(
        client,
        stage="overview",
        model="gpt-4.1-2025-04-14",
        messages=[{"role": "user", "content": "clinical input"}],
    )
    with pytest.raises(CybermedGenerationError) as exc_info:
        cybermed_chat_create(
            client,
            stage="overview",
            model="gpt-4.1-2025-04-14",
            messages=[{"role": "user", "content": "second input"}],
        )

    assert client.calls == 1
    assert seen[0]["max_tokens"] == 2200
    assert exc_info.value.category == "budget_exceeded_calls"
    diag = tracker.to_dict()
    assert diag["provider_calls_attempted_total"] == 1
    assert diag["budget_blocked_total"] == 1
    assert diag["budget_blocks_by_limit"] == {"calls": 1}


def test_zero_call_budget_is_an_emergency_generation_kill_switch(monkeypatch):
    tracker = _tracker(calls=0)
    monkeypatch.setattr(cybermed_openai, "CYBERMED_OPENAI_DIAGNOSTICS", tracker)
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)
    client = _Client(lambda _kwargs: _response())

    with pytest.raises(CybermedGenerationError) as exc_info:
        cybermed_chat_create(
            client,
            stage="overview",
            model="gpt-4.1-2025-04-14",
            messages=[{"role": "user", "content": "input"}],
        )

    assert exc_info.value.category == "budget_exceeded_calls"
    assert client.calls == 0


@pytest.mark.parametrize(
    ("tracker", "expected_category"),
    [
        (_tracker(input_tokens=1), "budget_exceeded_input_tokens"),
        (_tracker(output_tokens=5), "budget_exceeded_output_tokens"),
        (_tracker(cost="0.000001"), "budget_exceeded_estimated_cost_usd"),
    ],
)
def test_token_and_cost_limits_block_before_provider(
    monkeypatch, tracker, expected_category
):
    monkeypatch.setattr(cybermed_openai, "CYBERMED_OPENAI_DIAGNOSTICS", tracker)
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)
    client = _Client(lambda _kwargs: _response())

    with pytest.raises(CybermedGenerationError) as exc_info:
        cybermed_chat_create(
            client,
            stage="pubmed_bottom_line",
            model="gpt-4.1-2025-04-14",
            messages=[{"role": "user", "content": "bounded clinical input"}],
            max_tokens=10,
        )

    assert exc_info.value.category == expected_category
    assert client.calls == 0


def test_provider_failure_telemetry_never_contains_exception_text(monkeypatch):
    tracker = _tracker()
    monkeypatch.setattr(cybermed_openai, "CYBERMED_OPENAI_DIAGNOSTICS", tracker)
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)

    class RateLimitError(RuntimeError):
        status_code = 429

    secret_error = "insufficient_quota request_id=req-secret api_key=sk-secret"
    client = _Client(lambda _kwargs: (_ for _ in ()).throw(RateLimitError(secret_error)))

    with pytest.raises(RateLimitError):
        cybermed_chat_create(
            client,
            stage="pubmed_bottom_line",
            model="gpt-4.1-2025-04-14",
            messages=[{"role": "user", "content": "input"}],
            max_tokens=90,
        )

    serialized = json.dumps(tracker.to_dict(), sort_keys=True)
    assert "req-secret" not in serialized
    assert "sk-secret" not in serialized
    assert tracker.to_dict()["errors_by_category"] == {"quota": 1}


def test_production_model_policy_blocks_mini_but_eval_mode_allows_it(monkeypatch):
    tracker = _tracker()
    monkeypatch.setattr(cybermed_openai, "CYBERMED_OPENAI_DIAGNOSTICS", tracker)
    client = _Client(lambda _kwargs: _response())
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)

    with pytest.raises(CybermedGenerationError) as exc_info:
        cybermed_chat_create(
            client,
            stage="overview",
            model="gpt-4.1-mini-2025-04-14",
            messages=[{"role": "user", "content": "input"}],
        )
    assert exc_info.value.category == "model_policy_violation"
    assert client.calls == 0

    monkeypatch.setenv("CYBERMED_MODEL_EVAL_MODE", "1")
    cybermed_chat_create(
        client,
        stage="model_eval",
        model="gpt-4.1-mini-2025-04-14",
        messages=[{"role": "user", "content": "input"}],
        max_tokens=20,
    )
    assert client.calls == 1


def test_token_and_cost_telemetry_reconciles_to_provider_usage(monkeypatch):
    tracker = _tracker()
    monkeypatch.setattr(cybermed_openai, "CYBERMED_OPENAI_DIAGNOSTICS", tracker)
    monkeypatch.delenv("CYBERMED_MODEL_EVAL_MODE", raising=False)
    client = _Client(lambda _kwargs: _response(prompt_tokens=100, completion_tokens=25))

    cybermed_chat_create(
        client,
        stage="overview",
        model="gpt-4.1-2025-04-14",
        messages=[{"role": "user", "content": "input"}],
        max_tokens=100,
    )

    diag = tracker.to_dict()
    assert diag["provider_reported_usage"]["input_tokens"] == 100
    assert diag["provider_reported_usage"]["output_tokens"] == 25
    assert diag["budget_accounted"]["input_tokens"] == 100
    assert diag["budget_accounted"]["output_tokens"] == 25
    assert diag["provider_reported_usage"]["estimated_cost_usd"] == pytest.approx(0.0004)
    assert diag["content_free_telemetry"] is True


def test_scheduled_cybermed_run_writes_openai_telemetry_artifact(
    tmp_path, monkeypatch
):
    tracker = _tracker()
    monkeypatch.setattr(cybermed_openai, "CYBERMED_OPENAI_DIAGNOSTICS", tracker)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")

    main_module._write_cybermed_diagnostics(
        str(tmp_path),
        "daily",
        {"report_mode": "daily"},
    )

    payload = json.loads(
        (tmp_path / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8")
    )
    assert payload["cybermed_openai"]["content_free_telemetry"] is True
    assert payload["cybermed_openai"]["budget_limits"]["calls"] == 4
