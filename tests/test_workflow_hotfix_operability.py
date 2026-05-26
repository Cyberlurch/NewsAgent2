from pathlib import Path
import re


def _workflow_text() -> str:
    return Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')


def test_workflow_dispatch_inputs_present_and_scheduler_not_wired():
    y = _workflow_text()
    assert 'workflow_dispatch:' in y
    assert 'report_mode:' in y
    assert 'which_report:' in y
    assert 'email_mode:' in y
    assert 'compute_scheduled_run_plan' not in y


def test_single_run_modes_loop_present_and_split_modes_removed():
    y = _workflow_text()
    assert 'for mode in $RUN_MODES; do' in y
    assert 'RUN_MODES_CYBERMED' not in y
    assert 'RUN_MODES_CYBERLURCH' not in y


def test_workflow_yaml_dispatch_shape_and_schedule_crons():
    y = _workflow_text()
    # Parse-like structural guard: top-level mapping keys should be unique.
    top_level_keys = []
    for raw_line in y.splitlines():
        if not raw_line or raw_line.lstrip().startswith("#"):
            continue
        if raw_line.startswith(" ") or raw_line.startswith("\t"):
            continue
        if ":" in raw_line:
            top_level_keys.append(raw_line.split(":", 1)[0].strip())
    assert len(top_level_keys) == len(set(top_level_keys))

    assert "workflow_dispatch:" in y
    assert "inputs:" in y

    required_inputs = [
        "report_mode:",
        "lookback_hours:",
        "year_in_review_year:",
        "which_report:",
        "email_mode:",
        "cyberlurch_channels_file:",
        "managed_transcript_provider:",
    ]
    for input_name in required_inputs:
        assert input_name in y

    assert "default: daily" in y
    assert "- monthly" in y
    assert "- cybermed" in y
    assert "default: none" in y
    assert "- none" in y

    crons = []
    for line in y.splitlines():
        m = re.search(r'cron:\s*"([^"]+)"', line)
        if m:
            crons.append(m.group(1))
    assert "30 2 * * *" in crons
    assert "30 3 * * *" in crons
    assert "30 4 * * *" not in crons
