from pathlib import Path


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
