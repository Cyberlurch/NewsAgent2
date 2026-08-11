from __future__ import annotations

import os
import re
import threading
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .cybermed_quality import CybermedGenerationError, classify_generation_error


DEFAULT_MAX_CALLS_PER_RUN = 96
DEFAULT_MAX_INPUT_TOKENS_PER_RUN = 350_000
DEFAULT_MAX_OUTPUT_TOKENS_PER_RUN = 80_000
DEFAULT_MAX_ESTIMATED_COST_USD_PER_RUN = Decimal("1.50")

_DEFAULT_OUTPUT_LIMIT_BY_STAGE = {
    "overview": 2_200,
    "pubmed_deep_dive_json": 1_800,
    "pubmed_deep_dive_markdown": 1_800,
    "pubmed_deep_dive_best_effort": 1_800,
    "deep_dive": 1_600,
    "pubmed_bottom_line": 120,
    "foamed_bottom_line": 160,
    "model_eval": 600,
}
_SAFE_LABEL_RE = re.compile(r"[^A-Za-z0-9_.:@/-]+")
_GPT_41_MODEL_RE = re.compile(r"^gpt-4\.1(?:-\d{4}-\d{2}-\d{2})?$")
_GPT_41_MINI_MODEL_RE = re.compile(r"^gpt-4\.1-mini(?:-\d{4}-\d{2}-\d{2})?$")


def _nonnegative_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise CybermedGenerationError(
            stage="budget_configuration",
            category="invalid_budget_configuration",
        ) from None
    if value < 0:
        raise CybermedGenerationError(
            stage="budget_configuration",
            category="invalid_budget_configuration",
        ) from None
    return value


def _nonnegative_decimal_env(name: str, default: Decimal) -> Decimal:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = Decimal(raw)
    except Exception:
        raise CybermedGenerationError(
            stage="budget_configuration",
            category="invalid_budget_configuration",
        ) from None
    if not value.is_finite() or value < 0:
        raise CybermedGenerationError(
            stage="budget_configuration",
            category="invalid_budget_configuration",
        ) from None
    return value


@dataclass(frozen=True)
class CybermedOpenAIBudget:
    max_calls_per_run: int
    max_input_tokens_per_run: int
    max_output_tokens_per_run: int
    max_estimated_cost_usd_per_run: Decimal

    @classmethod
    def from_env(cls) -> "CybermedOpenAIBudget":
        return cls(
            max_calls_per_run=_nonnegative_int_env(
                "CYBERMED_OPENAI_MAX_CALLS_PER_RUN",
                DEFAULT_MAX_CALLS_PER_RUN,
            ),
            max_input_tokens_per_run=_nonnegative_int_env(
                "CYBERMED_OPENAI_MAX_INPUT_TOKENS_PER_RUN",
                DEFAULT_MAX_INPUT_TOKENS_PER_RUN,
            ),
            max_output_tokens_per_run=_nonnegative_int_env(
                "CYBERMED_OPENAI_MAX_OUTPUT_TOKENS_PER_RUN",
                DEFAULT_MAX_OUTPUT_TOKENS_PER_RUN,
            ),
            max_estimated_cost_usd_per_run=_nonnegative_decimal_env(
                "CYBERMED_OPENAI_MAX_ESTIMATED_COST_USD_PER_RUN",
                DEFAULT_MAX_ESTIMATED_COST_USD_PER_RUN,
            ),
        )


@dataclass(frozen=True)
class _Reservation:
    stage: str
    model: str
    row_key: str
    input_tokens: int
    output_tokens: int
    input_usd_per_million: Decimal
    output_usd_per_million: Decimal
    estimated_cost_usd: Decimal


def _safe_label(value: Any, *, fallback: str) -> str:
    label = _SAFE_LABEL_RE.sub("_", str(value or "").strip())[:100]
    return label or fallback


def _request_content_bytes(value: Any) -> int:
    """Count request bytes without retaining or returning request content."""

    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, Mapping):
        return sum(
            _request_content_bytes(key) + _request_content_bytes(item)
            for key, item in value.items()
        )
    if isinstance(value, Sequence):
        return sum(_request_content_bytes(item) for item in value)
    return len(str(value).encode("utf-8"))


def estimate_request_input_tokens(request: Mapping[str, Any]) -> int:
    """Hard upper-bound estimate; the request itself is never persisted."""

    messages = request.get("messages") or []
    message_count = len(messages) if isinstance(messages, Sequence) else 1
    # A tokenizer cannot emit more ordinary text tokens than there are UTF-8
    # bytes.  Count the full request (including response-format keys), then add
    # a deliberately generous allowance for chat framing tokens.
    request_bytes = _request_content_bytes(request)
    return max(1, request_bytes + 32 + (message_count * 12))


def _pricing_for_model(model: str) -> tuple[Decimal, Decimal, str]:
    normalized = str(model or "").strip().lower()
    if _GPT_41_MINI_MODEL_RE.fullmatch(normalized):
        return Decimal("0.40"), Decimal("1.60"), "gpt-4.1-mini"
    if _GPT_41_MODEL_RE.fullmatch(normalized):
        return Decimal("2.00"), Decimal("8.00"), "gpt-4.1"
    raise CybermedGenerationError(
        stage="budget_configuration",
        category="unknown_model_pricing",
    )


def _cost_usd(
    input_tokens: int,
    output_tokens: int,
    input_usd_per_million: Decimal,
    output_usd_per_million: Decimal,
) -> Decimal:
    million = Decimal(1_000_000)
    return (
        (Decimal(input_tokens) * input_usd_per_million / million)
        + (Decimal(output_tokens) * output_usd_per_million / million)
    )


def _usage_int(usage: Any, names: tuple[str, ...]) -> int | None:
    for name in names:
        if isinstance(usage, Mapping) and usage.get(name) is not None:
            try:
                return max(0, int(usage[name]))
            except (TypeError, ValueError):
                continue
        value = getattr(usage, name, None)
        if value is not None:
            try:
                return max(0, int(value))
            except (TypeError, ValueError):
                continue
    return None


class CybermedOpenAIDiagnostics:
    """Per-process Cybermed budget guard and content-free usage telemetry."""

    def __init__(self, budget: CybermedOpenAIBudget):
        self.budget = budget
        self._lock = threading.Lock()
        self.preflight_checks_total = 0
        self.provider_calls_attempted_total = 0
        self.provider_calls_succeeded_total = 0
        self.provider_calls_failed_total = 0
        self.budget_blocked_total = 0
        self.model_policy_blocked_total = 0
        self.usage_missing_total = 0
        self.request_output_limit_injected_total = 0
        self.accounted_input_tokens = 0
        self.accounted_output_tokens = 0
        self.reported_input_tokens = 0
        self.reported_output_tokens = 0
        self.accounted_estimated_cost_usd = Decimal("0")
        self.reported_cost_usd = Decimal("0")
        self.by_stage_model: dict[str, dict[str, Any]] = {}
        self.error_categories: Counter[str] = Counter()
        self.budget_blocks_by_limit: Counter[str] = Counter()
        self.pricing_sources: Counter[str] = Counter()

    @staticmethod
    def _new_row() -> dict[str, Any]:
        return {
            "preflight_checks": 0,
            "provider_calls": 0,
            "successes": 0,
            "errors": 0,
            "budget_blocks": 0,
            "accounted_input_tokens": 0,
            "accounted_output_tokens": 0,
            "reported_input_tokens": 0,
            "reported_output_tokens": 0,
            "usage_missing": 0,
            "accounted_estimated_cost_usd": 0.0,
            "reported_cost_usd": 0.0,
        }

    def _reserve(self, *, stage: str, model: str, request: Mapping[str, Any]) -> _Reservation:
        safe_stage = _safe_label(stage, fallback="unknown")
        safe_model = _safe_label(model, fallback="unknown")
        row_key = f"{safe_stage}|{safe_model}"
        input_tokens = estimate_request_input_tokens(request)
        raw_output_tokens = request.get("max_completion_tokens")
        if raw_output_tokens is None:
            raw_output_tokens = request.get("max_tokens")
        if raw_output_tokens is None:
            raw_output_tokens = _DEFAULT_OUTPUT_LIMIT_BY_STAGE.get(safe_stage, 1_800)
        try:
            output_tokens = int(raw_output_tokens)
        except (TypeError, ValueError):
            raise CybermedGenerationError(
                stage=safe_stage,
                category="invalid_request_budget",
            ) from None
        if output_tokens <= 0:
            raise CybermedGenerationError(
                stage=safe_stage,
                category="invalid_request_budget",
            )
        input_rate, output_rate, pricing_source = _pricing_for_model(safe_model)
        estimated_cost = _cost_usd(
            input_tokens,
            output_tokens,
            input_rate,
            output_rate,
        )

        with self._lock:
            self.preflight_checks_total += 1
            row = self.by_stage_model.setdefault(row_key, self._new_row())
            row["preflight_checks"] += 1
            self.pricing_sources[pricing_source] += 1

            violation = ""
            if self.provider_calls_attempted_total + 1 > self.budget.max_calls_per_run:
                violation = "calls"
            elif self.accounted_input_tokens + input_tokens > self.budget.max_input_tokens_per_run:
                violation = "input_tokens"
            elif self.accounted_output_tokens + output_tokens > self.budget.max_output_tokens_per_run:
                violation = "output_tokens"
            elif (
                self.accounted_estimated_cost_usd + estimated_cost
                > self.budget.max_estimated_cost_usd_per_run
            ):
                violation = "estimated_cost_usd"

            if violation:
                self.budget_blocked_total += 1
                self.budget_blocks_by_limit[violation] += 1
                row["budget_blocks"] += 1
                raise CybermedGenerationError(
                    stage=safe_stage,
                    category=f"budget_exceeded_{violation}",
                )

            # Reserve before the provider call so concurrent requests cannot
            # independently pass against the same remaining budget.
            self.provider_calls_attempted_total += 1
            self.accounted_input_tokens += input_tokens
            self.accounted_output_tokens += output_tokens
            self.accounted_estimated_cost_usd += estimated_cost
            row["provider_calls"] += 1
            row["accounted_input_tokens"] += input_tokens
            row["accounted_output_tokens"] += output_tokens
            row["accounted_estimated_cost_usd"] = round(
                float(Decimal(str(row["accounted_estimated_cost_usd"])) + estimated_cost),
                8,
            )

        return _Reservation(
            stage=safe_stage,
            model=safe_model,
            row_key=row_key,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            input_usd_per_million=input_rate,
            output_usd_per_million=output_rate,
            estimated_cost_usd=estimated_cost,
        )

    def _reconcile(
        self,
        reservation: _Reservation,
        *,
        input_tokens: int,
        output_tokens: int,
        usage_reported: bool,
    ) -> tuple[Decimal, Decimal]:
        accounted_cost = _cost_usd(
            input_tokens,
            output_tokens,
            reservation.input_usd_per_million,
            reservation.output_usd_per_million,
        )
        reported_cost = accounted_cost if usage_reported else Decimal("0")
        row = self.by_stage_model[reservation.row_key]

        self.accounted_input_tokens += input_tokens - reservation.input_tokens
        self.accounted_output_tokens += output_tokens - reservation.output_tokens
        self.accounted_estimated_cost_usd += accounted_cost - reservation.estimated_cost_usd
        row["accounted_input_tokens"] += input_tokens - reservation.input_tokens
        row["accounted_output_tokens"] += output_tokens - reservation.output_tokens
        row["accounted_estimated_cost_usd"] = round(
            float(
                Decimal(str(row["accounted_estimated_cost_usd"]))
                + accounted_cost
                - reservation.estimated_cost_usd
            ),
            8,
        )
        if usage_reported:
            self.reported_input_tokens += input_tokens
            self.reported_output_tokens += output_tokens
            self.reported_cost_usd += reported_cost
            row["reported_input_tokens"] += input_tokens
            row["reported_output_tokens"] += output_tokens
            row["reported_cost_usd"] = round(
                float(Decimal(str(row["reported_cost_usd"])) + reported_cost),
                8,
            )
        else:
            self.usage_missing_total += 1
            row["usage_missing"] += 1
        return accounted_cost, reported_cost

    def record_success(self, reservation: _Reservation, response: Any) -> None:
        usage = getattr(response, "usage", None)
        input_tokens = _usage_int(usage, ("prompt_tokens", "input_tokens"))
        output_tokens = _usage_int(usage, ("completion_tokens", "output_tokens"))
        usage_reported = input_tokens is not None and output_tokens is not None
        if input_tokens is None:
            input_tokens = reservation.input_tokens
        if output_tokens is None:
            # Keep the preflight reservation when the provider omits usage.
            output_tokens = reservation.output_tokens
        with self._lock:
            self.provider_calls_succeeded_total += 1
            self.by_stage_model[reservation.row_key]["successes"] += 1
            self._reconcile(
                reservation,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                usage_reported=usage_reported,
            )

    def record_error(self, reservation: _Reservation, exc: BaseException) -> None:
        category = (
            exc.category
            if isinstance(exc, CybermedGenerationError)
            else classify_generation_error(exc)
        )
        with self._lock:
            self.provider_calls_failed_total += 1
            self.error_categories[_safe_label(category, fallback="other")] += 1
            self.by_stage_model[reservation.row_key]["errors"] += 1
            # A failed request can still consume prompt tokens.  Retain the
            # conservative input estimate but release its output reservation.
            self._reconcile(
                reservation,
                input_tokens=reservation.input_tokens,
                output_tokens=0,
                usage_reported=False,
            )

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            remaining_calls = max(
                0,
                self.budget.max_calls_per_run - self.provider_calls_attempted_total,
            )
            remaining_input = max(
                0,
                self.budget.max_input_tokens_per_run - self.accounted_input_tokens,
            )
            remaining_output = max(
                0,
                self.budget.max_output_tokens_per_run - self.accounted_output_tokens,
            )
            remaining_cost = max(
                Decimal("0"),
                self.budget.max_estimated_cost_usd_per_run
                - self.accounted_estimated_cost_usd,
            )
            return {
                "telemetry_schema_version": 1,
                "content_free_telemetry": True,
                "production_model_policy": "gpt-4.1_only_until_ab_validation",
                "preflight_checks_total": self.preflight_checks_total,
                "provider_calls_attempted_total": self.provider_calls_attempted_total,
                "provider_calls_succeeded_total": self.provider_calls_succeeded_total,
                "provider_calls_failed_total": self.provider_calls_failed_total,
                "budget_blocked_total": self.budget_blocked_total,
                "model_policy_blocked_total": self.model_policy_blocked_total,
                "budget_blocks_by_limit": dict(self.budget_blocks_by_limit),
                "errors_by_category": dict(self.error_categories),
                "usage_missing_total": self.usage_missing_total,
                "request_output_limit_injected_total": self.request_output_limit_injected_total,
                "calls_by_stage_model": {
                    key: dict(value) for key, value in self.by_stage_model.items()
                },
                "pricing_sources": dict(self.pricing_sources),
                "pricing_usd_per_million_tokens": {
                    "gpt-4.1": {"input": 2.0, "output": 8.0},
                    "gpt-4.1-mini": {"input": 0.4, "output": 1.6},
                },
                "budget_limits": {
                    "calls": self.budget.max_calls_per_run,
                    "input_tokens": self.budget.max_input_tokens_per_run,
                    "output_tokens": self.budget.max_output_tokens_per_run,
                    "estimated_cost_usd": float(
                        self.budget.max_estimated_cost_usd_per_run
                    ),
                },
                "budget_accounted": {
                    "calls": self.provider_calls_attempted_total,
                    "input_tokens": self.accounted_input_tokens,
                    "output_tokens": self.accounted_output_tokens,
                    "estimated_cost_usd": round(
                        float(self.accounted_estimated_cost_usd), 8
                    ),
                },
                "provider_reported_usage": {
                    "input_tokens": self.reported_input_tokens,
                    "output_tokens": self.reported_output_tokens,
                    "estimated_cost_usd": round(float(self.reported_cost_usd), 8),
                },
                "budget_remaining": {
                    "calls": remaining_calls,
                    "input_tokens": remaining_input,
                    "output_tokens": remaining_output,
                    "estimated_cost_usd": round(float(remaining_cost), 8),
                },
            }


CYBERMED_OPENAI_DIAGNOSTICS = CybermedOpenAIDiagnostics(
    CybermedOpenAIBudget(
        max_calls_per_run=DEFAULT_MAX_CALLS_PER_RUN,
        max_input_tokens_per_run=DEFAULT_MAX_INPUT_TOKENS_PER_RUN,
        max_output_tokens_per_run=DEFAULT_MAX_OUTPUT_TOKENS_PER_RUN,
        max_estimated_cost_usd_per_run=DEFAULT_MAX_ESTIMATED_COST_USD_PER_RUN,
    )
)


def reset_cybermed_openai_diagnostics() -> CybermedOpenAIDiagnostics:
    global CYBERMED_OPENAI_DIAGNOSTICS
    CYBERMED_OPENAI_DIAGNOSTICS = CybermedOpenAIDiagnostics(
        CybermedOpenAIBudget.from_env()
    )
    return CYBERMED_OPENAI_DIAGNOSTICS


def cybermed_chat_create(
    client: Any,
    *,
    stage: str,
    model: str,
    **request: Any,
) -> Any:
    """Budget and record a Cybermed Chat Completions call."""

    normalized_model = str(model or "").strip().lower()
    safe_stage = _safe_label(stage, fallback="unknown")
    eval_mode = (os.getenv("CYBERMED_MODEL_EVAL_MODE") or "").strip() == "1"
    production_model_allowed = bool(_GPT_41_MODEL_RE.fullmatch(normalized_model))
    eval_model_allowed = eval_mode and safe_stage == "model_eval" and bool(
        _GPT_41_MODEL_RE.fullmatch(normalized_model)
        or _GPT_41_MINI_MODEL_RE.fullmatch(normalized_model)
    )
    if not production_model_allowed and not eval_model_allowed:
        with CYBERMED_OPENAI_DIAGNOSTICS._lock:
            CYBERMED_OPENAI_DIAGNOSTICS.model_policy_blocked_total += 1
        raise CybermedGenerationError(
            stage=safe_stage,
            category="model_policy_violation",
        )

    request_for_provider = dict(request)
    if (
        request_for_provider.get("max_tokens") is None
        and request_for_provider.get("max_completion_tokens") is None
    ):
        request_for_provider["max_tokens"] = _DEFAULT_OUTPUT_LIMIT_BY_STAGE.get(
            safe_stage, 1_800
        )
        with CYBERMED_OPENAI_DIAGNOSTICS._lock:
            CYBERMED_OPENAI_DIAGNOSTICS.request_output_limit_injected_total += 1

    reservation = CYBERMED_OPENAI_DIAGNOSTICS._reserve(
        stage=stage,
        model=model,
        request=request_for_provider,
    )
    try:
        response = client.chat.completions.create(
            model=model,
            **request_for_provider,
        )
    except Exception as exc:
        CYBERMED_OPENAI_DIAGNOSTICS.record_error(reservation, exc)
        raise
    CYBERMED_OPENAI_DIAGNOSTICS.record_success(reservation, response)
    return response
