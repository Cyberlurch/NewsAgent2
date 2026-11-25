"""Chat Completions parameter compatibility for explicitly supported GPT-6 models.

Keep the existing request unchanged for legacy models. In particular, do not
pass unsupported sampling controls to reasoning models or confuse visible
output tokens with the completion-token budget (which includes reasoning).
"""
from __future__ import annotations

from typing import Any, Mapping


def prepare_chat_request(model: str, request: Mapping[str, Any]) -> dict[str, Any]:
    prepared = dict(request)
    if model not in {"gpt-6.1-terra", "gpt-6.2-sol", "gpt-6-luna"}:
        return prepared

    # Terra and Luna support none; extraction is a low-latency workload.
    # Sol requires reasoning, so use low for the bounded clinical evaluation.
    effort = "low" if model == "gpt-6.2-sol" else "none"
    prepared.setdefault("reasoning_effort", effort)
    for key in ("temperature", "top_p", "logprobs", "top_logprobs"):
        prepared.pop(key, None)
    old_limit = prepared.pop("max_tokens", None)
    if old_limit is not None and prepared.get("max_completion_tokens") is None:
        prepared["max_completion_tokens"] = old_limit
    return prepared
