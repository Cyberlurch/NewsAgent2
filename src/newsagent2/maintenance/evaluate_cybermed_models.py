from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

from openai import OpenAI

from .. import cybermed_openai as cybermed_openai_module
from ..cybermed_openai import cybermed_chat_create
from ..cybermed_quality import (
    CybermedGenerationError,
    classify_generation_error,
    validate_generated_text,
)


BASELINE_MODEL = "gpt-4.1-2025-04-14"
CANDIDATE_MODEL = "gpt-4.1-mini-2025-04-14"

_SYSTEM_PROMPTS = {
    "pubmed_bottom_line": (
        "You are a careful clinical evidence summarizer. Return only a one- or "
        "two-sentence bottom line. State the design, central result, important "
        "harm or limitation, and avoid causal language not supported by the text."
    ),
    "pubmed_deep_dive": (
        "You are a careful clinical evidence summarizer. Return concise Markdown "
        "using exactly these field labels: BOTTOM LINE:, Study type:, Population/setting:, "
        "Intervention/exposure & comparator:, Primary endpoints:, Key results:, "
        "Limitations:, Why this matters:. Use only the supplied evidence and preserve "
        "important numbers and uncertainty."
    ),
    "foamed_bottom_line": (
        "You are a careful clinical content summarizer. Return one sentence of at "
        "most 55 words beginning with BOTTOM LINE:. Explicitly distinguish education "
        "or commentary from original evidence. Do not invent findings."
    ),
}
_NUMBER_RE = re.compile(r"(?<![A-Za-z])[-+]?\d[\d,.]*(?:%|\b)")


def _normalize_number(value: str) -> str:
    return str(value or "").strip().replace(",", "")


def evaluate_output(case: dict[str, Any], output: str) -> dict[str, Any]:
    text = str(output or "").strip()
    lower = text.lower()
    required_markers = [str(v) for v in case.get("required_markers") or []]
    critical_terms = [str(v) for v in case.get("critical_terms") or []]
    marker_hits = [marker for marker in required_markers if marker.lower() in lower]
    term_hits = [term for term in critical_terms if term.lower() in lower]
    source_numbers = {
        _normalize_number(value)
        for value in _NUMBER_RE.findall(str(case.get("source_text") or ""))
    }
    output_numbers = {
        _normalize_number(value) for value in _NUMBER_RE.findall(text)
    }
    unsupported_numbers = sorted(output_numbers - source_numbers)
    word_count = len(text.split())
    max_words = max(1, int(case.get("max_words") or 300))
    return {
        "nonempty": bool(text),
        "required_markers_total": len(required_markers),
        "required_markers_matched_total": len(marker_hits),
        "required_markers_passed": len(marker_hits) == len(required_markers),
        "critical_terms_total": len(critical_terms),
        "critical_terms_matched_total": len(term_hits),
        "critical_term_coverage": round(
            len(term_hits) / len(critical_terms), 4
        )
        if critical_terms
        else 1.0,
        "unsupported_numeric_tokens": unsupported_numbers[:20],
        "unsupported_numeric_tokens_total": len(unsupported_numbers),
        "word_count": word_count,
        "max_words": max_words,
        "word_limit_passed": word_count <= max_words,
    }


def _safe_failure_category(exc: BaseException) -> str:
    if isinstance(exc, CybermedGenerationError):
        return exc.category
    return classify_generation_error(exc)


def _run_case(
    client: OpenAI,
    *,
    model: str,
    case: dict[str, Any],
) -> dict[str, Any]:
    task = str(case.get("task") or "")
    system_prompt = _SYSTEM_PROMPTS.get(task)
    if not system_prompt:
        return {
            "status": "error",
            "error_category": "invalid_eval_case",
            "output": "",
            "objective": evaluate_output(case, ""),
        }
    user_prompt = (
        f"Title: {str(case.get('title') or '').strip()}\n\n"
        f"Evidence text:\n{str(case.get('source_text') or '').strip()}"
    )
    try:
        response = cybermed_chat_create(
            client,
            stage="model_eval",
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_tokens=max(1, int(case.get("max_output_tokens") or 600)),
        )
        output = validate_generated_text(
            (response.choices[0].message.content or "").strip(),
            stage="model_eval",
        )
        return {
            "status": "ok",
            "error_category": "",
            "output": output,
            "objective": evaluate_output(case, output),
        }
    except Exception as exc:
        return {
            "status": "error",
            "error_category": _safe_failure_category(exc),
            "output": "",
            "objective": evaluate_output(case, ""),
        }


def _blind_labels(case_id: str) -> tuple[str, str]:
    baseline_is_a = hashlib.sha256(case_id.encode("utf-8")).digest()[0] % 2 == 0
    return ("A", "B") if baseline_is_a else ("B", "A")


def build_evaluation(
    *,
    cases: list[dict[str, Any]],
    baseline_model: str,
    candidate_model: str,
    client: OpenAI,
) -> tuple[dict[str, Any], dict[str, Any]]:
    evaluated: list[dict[str, Any]] = []
    mapping: dict[str, Any] = {
        "schema_version": 1,
        "baseline_model": baseline_model,
        "candidate_model": candidate_model,
        "case_labels": {},
    }
    for case in cases:
        case_id = str(case.get("case_id") or "").strip()
        baseline_label, candidate_label = _blind_labels(case_id)
        baseline = _run_case(client, model=baseline_model, case=case)
        candidate = _run_case(client, model=candidate_model, case=case)
        by_label = {baseline_label: baseline, candidate_label: candidate}
        mapping["case_labels"][case_id] = {
            baseline_label: baseline_model,
            candidate_label: candidate_model,
        }
        evaluated.append(
            {
                "case_id": case_id,
                "task": str(case.get("task") or ""),
                "title": str(case.get("title") or ""),
                "source_sha256": hashlib.sha256(
                    str(case.get("source_text") or "").encode("utf-8")
                ).hexdigest(),
                "outputs": by_label,
            }
        )
    payload = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "evaluation_type": "manual_blinded_ab",
        "production_model_change_executed": False,
        "production_decision": "hold_gpt_4_1_pending_manual_validation",
        "manual_review_required": True,
        "manual_review_dimensions": [
            "factual_fidelity",
            "important_omissions",
            "clinical_usefulness",
            "uncertainty_calibration",
            "format_adherence",
        ],
        "cases": evaluated,
        "cybermed_openai": cybermed_openai_module.CYBERMED_OPENAI_DIAGNOSTICS.to_dict(),
    }
    return payload, mapping


def render_blinded_markdown(
    payload: dict[str, Any],
    cases_by_id: dict[str, dict[str, Any]],
) -> str:
    lines = [
        "# Cybermed blinded model A/B review",
        "",
        "Production remains on GPT-4.1. This artifact cannot promote a model.",
        "",
        "For each case, review factual fidelity, important omissions, clinical usefulness, uncertainty calibration, and format adherence before opening the separate mapping file.",
        "",
    ]
    for row in payload.get("cases") or []:
        case_id = str(row.get("case_id") or "")
        case = cases_by_id.get(case_id, {})
        lines.extend(
            [
                f"## {case_id}",
                "",
                f"Task: `{row.get('task', '')}`",
                "",
                "### Source text",
                "",
                str(case.get("source_text") or ""),
                "",
            ]
        )
        outputs = row.get("outputs") or {}
        for label in ("A", "B"):
            result = outputs.get(label) or {}
            lines.extend(
                [
                    f"### Output {label}",
                    "",
                    f"Status: `{result.get('status', 'error')}`",
                    "",
                    str(result.get("output") or "(no safe output)"),
                    "",
                    "Objective checks:",
                    "",
                    "```json",
                    json.dumps(
                        result.get("objective") or {},
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    ),
                    "```",
                    "",
                    "Manual score (1–5): ___  Preferred output: ___  Notes: ___",
                    "",
                ]
            )
    return "\n".join(lines).rstrip() + "\n"


def _load_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or not isinstance(payload.get("cases"), list):
        raise SystemExit("Invalid Cybermed model-evaluation fixture")
    cases = [row for row in payload["cases"] if isinstance(row, dict)]
    if not cases or any(not str(row.get("case_id") or "").strip() for row in cases):
        raise SystemExit("Cybermed model-evaluation fixture has no valid cases")
    return cases


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a non-production blinded Cybermed GPT-4.1 A/B evaluation."
    )
    parser.add_argument(
        "--cases",
        default="data/cybermed_model_eval_cases.json",
    )
    parser.add_argument("--output-dir", default="out/cybermed-model-eval")
    parser.add_argument("--baseline-model", default=BASELINE_MODEL)
    parser.add_argument("--candidate-model", default=CANDIDATE_MODEL)
    args = parser.parse_args()

    if (os.getenv("CYBERMED_MODEL_EVAL_MODE") or "").strip() != "1":
        raise SystemExit("Refusing model evaluation without CYBERMED_MODEL_EVAL_MODE=1")

    cases = _load_cases(Path(args.cases))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cybermed_openai_module.reset_cybermed_openai_diagnostics()
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    payload, mapping = build_evaluation(
        cases=cases,
        baseline_model=str(args.baseline_model),
        candidate_model=str(args.candidate_model),
        client=client,
    )
    cases_by_id = {str(row["case_id"]): row for row in cases}
    (output_dir / "blinded-review.md").write_text(
        render_blinded_markdown(payload, cases_by_id),
        encoding="utf-8",
    )
    (output_dir / "results.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "model-mapping.json").write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    failures = sum(
        1
        for row in payload["cases"]
        for result in (row.get("outputs") or {}).values()
        if result.get("status") != "ok"
    )
    print(
        "[cybermed-model-eval] "
        f"cases={len(cases)} safe_generation_failures={failures} "
        f"output_dir={output_dir} production_change=0"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
