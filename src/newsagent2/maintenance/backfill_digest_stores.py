from __future__ import annotations

import argparse
import copy
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class BackfillConfig:
    report: str
    from_date: date
    to_date: date
    apply: bool
    output_dir: Path
    enable_openai: bool


def _parse_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> list[str]:
    out = []
    d = start
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _load_json(path: Path, default: Any, warnings: list[str] | None = None) -> Any:
    if not path.exists():
        if warnings is not None:
            warnings.append(f"Missing state file {path}; using empty audit default.")
        return copy.deepcopy(default)
    return json.loads(path.read_text(encoding="utf-8"))


def _is_nonempty_digest(entry: dict[str, Any]) -> bool:
    if (entry.get("items") or {}).get("pubmed") or (entry.get("items") or {}).get("foamed"):
        return True
    return bool((entry.get("deep_dives") or []) or (entry.get("top_picks") or []))


def _plan_cybermed(state: dict[str, Any], wanted_dates: list[str]) -> dict[str, Any]:
    digests = [d for d in state.get("digests", []) if isinstance(d, dict)]
    by_date = {str(d.get("run_date") or ""): d for d in digests if str(d.get("run_date") or "")}
    missing = [d for d in wanted_dates if d not in by_date]

    to_insert = [{"run_date": d, "action": "skip", "reason": "missing_source_data"} for d in missing]
    to_update = []
    skipped = []
    for d in wanted_dates:
        existing = by_date.get(d)
        if not existing:
            continue
        if _is_nonempty_digest(existing):
            skipped.append({"run_date": d, "reason": "existing_nonempty_digest"})
        else:
            to_update.append({"run_date": d, "action": "skip", "reason": "no_reconstruction_source"})

    return {
        "existing_digest_count": len(digests),
        "covered_dates": sorted([k for k in by_date.keys() if k in wanted_dates]),
        "missing_dates": missing,
        "candidate_source_files": ["state/cybermed_daily_digests.json"],
        "would_insert_or_update": {"insert": to_insert, "update": to_update},
        "would_skip": skipped,
        "warnings": ["No reconstruction source is implemented; audit-only missing_source_data markers are emitted."],
    }


def _plan_cyberlurch(cyberlurch_state: dict[str, Any], rollups_state: dict[str, Any], wanted_dates: list[str]) -> dict[str, Any]:
    digests = [d for d in cyberlurch_state.get("digests", []) if isinstance(d, dict)]
    covered = set()
    for d in digests:
        raw = str(d.get("published_at") or d.get("date") or "")[:10]
        if len(raw) == 10:
            covered.add(raw)
    missing = [d for d in wanted_dates if d not in covered]
    rollup_reports = sorted((rollups_state.get("reports") or {}).keys()) if isinstance(rollups_state, dict) else []

    return {
        "existing_digest_count": len(digests),
        "covered_dates": sorted([d for d in covered if d in wanted_dates]),
        "missing_dates": missing,
        "candidate_source_files": ["state/cyberlurch_digests.json", "state/rollups.json"],
        "would_insert_or_update": {"insert": [], "update": []},
        "would_skip": [{"run_date": d, "reason": "missing_source_data"} for d in missing],
        "warnings": [
            "No Cyberlurch summary reconstruction is implemented; no summaries are fabricated.",
            f"rollups_reports_present={','.join(rollup_reports) if rollup_reports else 'none'}",
        ],
    }


def build_backfill_plan(config: BackfillConfig, state_dir: Path = Path("state")) -> dict[str, Any]:
    wanted_dates = _date_range(config.from_date, config.to_date)
    warnings: list[str] = []
    cybermed_state = _load_json(
        state_dir / "cybermed_daily_digests.json",
        {"schema_version": 1, "digests": []},
        warnings,
    )
    cyberlurch_state = _load_json(
        state_dir / "cyberlurch_digests.json",
        {"version": 1, "digests": []},
        warnings,
    )
    rollups_state = _load_json(state_dir / "rollups.json", {"version": 1, "reports": {}}, warnings)

    plan = {
        "report": config.report,
        "from_date": config.from_date.isoformat(),
        "to_date": config.to_date.isoformat(),
        "apply": False,
        "safe_to_apply": False,
        "apply_executed": False,
        "openai_enabled": bool(config.enable_openai),
        "warnings": warnings,
        "reports": {},
    }
    if config.enable_openai:
        plan["warnings"].append("OpenAI backfill reconstruction flag was set, but no OpenAI reconstruction path is implemented.")

    if config.report in {"cybermed", "both"}:
        plan["reports"]["cybermed"] = _plan_cybermed(cybermed_state, wanted_dates)
    if config.report in {"cyberlurch", "both"}:
        plan["reports"]["cyberlurch"] = _plan_cyberlurch(cyberlurch_state, rollups_state, wanted_dates)
    return plan


def maybe_apply(plan: dict[str, Any], config: BackfillConfig, state_dir: Path = Path("state")) -> bool:
    _ = plan
    _ = state_dir
    if config.apply:
        raise SystemExit("Apply mode is not implemented; refusing to mutate state.")
    return False


def run(config: BackfillConfig, state_dir: Path = Path("state")) -> int:
    plan = build_backfill_plan(config, state_dir=state_dir)
    applied = maybe_apply(plan, config, state_dir=state_dir)
    plan["apply_executed"] = applied

    config.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = config.output_dir / "backfill_plan.json"
    out_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[backfill] wrote audit plan: {out_path}")
    return 0


def parse_args() -> BackfillConfig:
    parser = argparse.ArgumentParser(description="Maintenance-only backfill audit for digest stores.")
    parser.add_argument("--report", choices=["cybermed", "cyberlurch", "both"], required=True)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--dry-run", action="store_true", default=True, help="Audit only; retained for compatibility.")
    parser.add_argument("--apply", action="store_true", default=False, help="Unsupported; exits without mutating state.")
    parser.add_argument("--output-dir", default="out/backfill_audit")
    parser.add_argument("--enable-openai-reconstruction", action="store_true", default=False)
    args = parser.parse_args()

    start = _parse_date(args.from_date)
    end = _parse_date(args.to_date)
    if end < start:
        raise SystemExit("Invalid date range: to-date must be >= from-date")

    return BackfillConfig(
        report=args.report,
        from_date=start,
        to_date=end,
        apply=bool(args.apply),
        output_dir=Path(args.output_dir),
        enable_openai=bool(args.enable_openai_reconstruction),
    )


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
