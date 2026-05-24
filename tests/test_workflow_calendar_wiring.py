from pathlib import Path


def test_workflow_rollback_restores_manual_dispatch_and_single_modes_loop():
    y = Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')

    assert 'workflow_dispatch:' in y
    assert 'report_mode:' in y
    assert 'which_report:' in y
    assert 'email_mode:' in y

    assert 'for mode in $RUN_MODES; do' in y

    assert 'compute_scheduled_run_plan' not in y
    assert 'RUN_MODES_CYBERMED' not in y
    assert 'RUN_MODES_CYBERLURCH' not in y
    assert 'CYBERMED_SEASONAL_GREETING' not in y
