from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"


def _run_cli(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_PATH)
    return subprocess.run(
        [sys.executable, "-m", "newsagent2.maintenance.backfill_digest_stores", *args],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, sort_keys=True) + "\n", encoding="utf-8")


def test_cli_audit_only_writes_backfill_plan_json(tmp_path: Path) -> None:
    result = _run_cli(
        tmp_path,
        "--report",
        "both",
        "--from-date",
        "2026-01-01",
        "--to-date",
        "2026-01-02",
        "--output-dir",
        "out/backfill_audit",
    )

    assert result.returncode == 0, result.stderr
    plan_path = tmp_path / "out" / "backfill_audit" / "backfill_plan.json"
    assert plan_path.exists()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["apply"] is False
    assert plan["safe_to_apply"] is False
    assert plan["apply_executed"] is False


def test_cli_apply_exits_nonzero_and_writes_no_state(tmp_path: Path) -> None:
    processed_items = tmp_path / "state" / "processed_items.json"
    _write_json(processed_items, {"sentinel": ["unchanged"]})
    before = processed_items.read_text(encoding="utf-8")

    result = _run_cli(
        tmp_path,
        "--report",
        "both",
        "--from-date",
        "2026-01-01",
        "--to-date",
        "2026-01-01",
        "--apply",
        "--output-dir",
        "out/backfill_audit",
    )

    assert result.returncode != 0
    assert "Apply mode is not implemented; refusing to mutate state." in result.stderr
    assert processed_items.read_text(encoding="utf-8") == before
    assert not (tmp_path / "state" / "cybermed_daily_digests.json").exists()
    assert not (tmp_path / "state" / "cyberlurch_digests.json").exists()
    assert not (tmp_path / "state" / "rollups.json").exists()


def test_missing_state_files_produce_warnings_but_no_crash(tmp_path: Path) -> None:
    result = _run_cli(
        tmp_path,
        "--report",
        "both",
        "--from-date",
        "2026-01-01",
        "--to-date",
        "2026-01-01",
        "--output-dir",
        "out/backfill_audit",
    )

    assert result.returncode == 0, result.stderr
    plan = json.loads((tmp_path / "out" / "backfill_audit" / "backfill_plan.json").read_text(encoding="utf-8"))
    assert any("Missing state file" in warning for warning in plan["warnings"])


def test_date_range_validation_still_fails(tmp_path: Path) -> None:
    result = _run_cli(
        tmp_path,
        "--report",
        "both",
        "--from-date",
        "2026-01-03",
        "--to-date",
        "2026-01-01",
        "--output-dir",
        "out/backfill_audit",
    )

    assert result.returncode != 0
    assert "Invalid date range: to-date must be >= from-date" in result.stderr


def test_processed_items_json_is_never_touched(tmp_path: Path) -> None:
    processed_items = tmp_path / "state" / "processed_items.json"
    _write_json(processed_items, {"sentinel": ["unchanged"]})
    before = processed_items.read_text(encoding="utf-8")

    result = _run_cli(
        tmp_path,
        "--report",
        "cybermed",
        "--from-date",
        "2026-01-01",
        "--to-date",
        "2026-01-02",
        "--output-dir",
        "out/backfill_audit",
    )

    assert result.returncode == 0, result.stderr
    assert processed_items.read_text(encoding="utf-8") == before
