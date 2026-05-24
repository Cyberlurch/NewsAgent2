from pathlib import Path


def test_workflow_wires_seasonal_greeting_and_per_report_modes():
    y = Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')
    assert 'CYBERMED_SEASONAL_GREETING' in y
    assert 'RUN_MODES_CYBERMED' in y
    assert 'RUN_MODES_CYBERLURCH' in y
    assert 'for mode in ${RUN_MODES_CYBERLURCH' in y
    assert 'for mode in ${RUN_MODES_CYBERMED' in y
