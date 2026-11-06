from datetime import datetime
from pathlib import Path
import json

import pytest
import yaml

from newsagent2 import delivery, operational_readiness as op, workflow_delivery as wd
from newsagent2.run_context import path_for
from test_recovery_scope import result, packet
from test_readiness_external_evidence import plan_job
from test_readiness_schedule_delay import seed


def test_reconciled_pre_smtp_failure_is_not_a_resend_or_a_permanent_block(tmp_path):
    row = result(start='2026-10-22', run='999')
    seed('cybermed', prior_day='2026-10-21')
    source = packet(tmp_path, [row])
    assert wd.import_recovery(source, report_key='cybermed', source_run_id='999')
    journal = delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))
    assert journal['editions'][row['edition_id']] == row
    assert wd._completed_plan_receipts({'id': 999}, [plan_job('cybermed', [row['edition_id']])], 'cybermed', journal)
    report = op.assess('cybermed', as_of=datetime.fromisoformat('2026-10-23T08:00:00+00:00'))
    assert report['ok'] and report['status'] == 'degraded'
    assert report['checks'][0]['warnings'] == ['operational_daily_publication_stale']
    assert not report['unresolved_delivery']
    assert row['accepted'] == [] and row['smtp_phase'] == 'not_handed_off'


@pytest.mark.parametrize('status', ['outcome_unknown', 'partially_accepted', 'accepted_by_smtp'])
def test_publication_warning_never_clears_unsafe_delivery(status):
    row = result(start='2026-10-22', body='Saved original content')
    row.update(status=status, smtp_phase='handoff_started' if status=='outcome_unknown' else 'completed')
    if status == 'outcome_unknown': row['unknown'] = ['a'*64]
    else: row['accepted'] = ['a'*64]
    if status == 'partially_accepted': row['rejected'] = ['b'*64]
    seed('cybermed', row, prior_day='2026-10-21')
    report = op.assess('cybermed', as_of=datetime.fromisoformat('2026-10-23T08:00:00+00:00'))
    assert not report['ok'] and report['status']=='blocked'
    assert report['warnings'] and report['unresolved_delivery']


def test_automatic_recovery_remains_serial_without_sender_credentials():
    workflow = yaml.safe_load(Path('.github/workflows/newsletter-recover-state.yml').read_text())
    events = workflow.get('on', workflow.get(True))
    assert events['workflow_run']['branches'] == ['main']
    assert events['push']['paths'] == ['.github/workflows/newsletter-recover-state.yml']
    job = workflow['jobs']['recover']
    assert job['strategy']['max-parallel'] == 1
    assert job['env']['SEND_EMAIL'] == '0'
    assert not any(name.startswith(('SMTP_', 'OPENAI_', 'RECIPIENTS_')) for name in job['env'])
    scripts = '\n'.join(step.get('run', '') for step in job['steps'])
    assert 'src.newsagent2.main' not in scripts and 'replay-pre-smtp' not in scripts
    bind = next(s for s in job['steps'] if s.get('name') == 'Bind recovery to this run and newsletter')
    assert '--maintenance' in bind['run']


@pytest.mark.parametrize('event', ['push', 'workflow_run', 'workflow_dispatch'])
def test_recovery_plan_accepts_real_trigger_without_overriding_github_env(monkeypatch, tmp_path, event):
    from newsagent2 import edition_plan as ep
    import sys
    monkeypatch.setenv('GITHUB_EVENT_NAME', event)
    monkeypatch.setenv('EMAIL_MODE', 'real')
    monkeypatch.setenv('SEND_EMAIL', '0')
    output = tmp_path / 'plan.json'
    monkeypatch.setattr(sys, 'argv', ['plan', '--report-key', 'cybermed', '--maintenance',
        '--created-at', '2026-11-06T10:00:00Z', '--job-started-at', '2026-11-06T10:01:00Z', '--output', str(output)])
    ep.main()
    plan = json.loads(output.read_text())
    assert plan['trigger'] == 'manual' and plan['expected_slot'] is None
    assert plan['workflow_run_id'] == 'local' and ep.validate_plan(plan)
    monkeypatch.setenv('SEND_EMAIL', '1')
    output.unlink()
    with pytest.raises(RuntimeError, match="edition_plan_invalid"):
        ep.main()
    assert not output.exists()
