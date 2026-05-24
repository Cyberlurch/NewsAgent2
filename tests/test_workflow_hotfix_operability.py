from pathlib import Path


def _workflow_text() -> str:
    return Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')


def test_workflow_dispatch_still_exists_and_manual_path_does_not_call_scheduler():
    y = _workflow_text()
    assert 'workflow_dispatch:' in y
    assert 'if [ "${{ github.event_name }}" = "workflow_dispatch" ]; then' in y
    manual_block = y.split('if [ "${{ github.event_name }}" = "workflow_dispatch" ]; then', 1)[1].split('else', 1)[0]
    assert 'compute_scheduled_run_plan' not in manual_block


def test_global_weekend_gate_removed_and_no_old_verify_gate():
    y = _workflow_text()
    assert 'weekend-nonfirst' not in y
    assert 'if [ "$dow" -ge 6 ] && [ "$dom" != "01" ]; then' not in y


def test_schedule_path_has_split_modes_and_safe_loops():
    y = _workflow_text()
    assert 'run_modes_cybermed' in y and 'run_modes_cyberlurch' in y
    assert 'cyberlurch_modes="${RUN_MODES_CYBERLURCH:-}"' in y
    assert 'cybermed_modes="${RUN_MODES_CYBERMED:-}"' in y
    assert 'Skipping Cyberlurch: no run modes resolved.' in y
    assert 'Skipping Cybermed: no run modes resolved.' in y


def test_verification_respects_run_skip_without_weekend_gate():
    y = _workflow_text()
    verify = y.split('name: Verify reports and email attempts', 1)[1]
    assert 'run_skip != \'true\'' in verify
    assert 'weekend-nonfirst' not in verify
    assert 'if [ "$dow" -ge 6 ] && [ "$dom" != "01" ]; then' not in verify


def test_cyberlurch_not_globally_blocked_by_swedish_calendar_logic():
    y = _workflow_text()
    gate = y.split('name: Gate scheduled runs to 05:30 Europe/Stockholm', 1)[1].split('name: Compute run plan', 1)[0]
    assert 'is_swedish_no_send_day' not in gate
    assert 'weekend-nonfirst' not in gate
