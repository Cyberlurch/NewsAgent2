import pytest

from newsagent2 import workflow_delivery as wd
from newsagent2.store_io import StoreError


@pytest.mark.parametrize('error,category', [
    (RuntimeError('recovery_recipient_key_missing'), 'recovery_recipient_key_missing'),
    (RuntimeError('recovery_readiness_binding_invalid'), 'recovery_readiness_binding_invalid'),
    (StoreError('generated_delta', 'newer_state_requires_operator_review'),
     'store_error:generated_delta:newer_state_requires_operator_review'),
    (RuntimeError('delivery_recovery_missing_for_run:71;operator_reconciliation_required'),
     'delivery_recovery_missing_for_run:71;operator_reconciliation_required'),
    (RuntimeError('smtp.private.test person@example.test password=private-value\n::notice::untrusted'),
     'newsletter_operation_failed'),
    (StoreError('person@example.test', 'private-value'), 'newsletter_operation_failed'),
    (StoreError('delivery_recovery', 'smtp.private.test'), 'newsletter_operation_failed'),
    (ValueError({'person@example.test': (550, b'private-value')}), 'newsletter_operation_failed'),
])
def test_cli_failure_annotation_is_useful_and_contains_only_allowlisted_categories(error, category, monkeypatch, capsys):
    def fail(): raise error
    monkeypatch.setattr(wd, 'main', fail)
    with pytest.raises(SystemExit) as outcome: wd.cli()
    assert outcome.value.code == 1
    output = capsys.readouterr()
    assert output.out == ''
    assert output.err == f'::error title=Newsletter operation failed::{category}\n'
    assert not any(value in output.err for value in ('person@example.test', 'smtp.private.test', 'private-value', '::notice::'))


def test_successful_cli_keeps_ordinary_exit_and_has_no_error_annotation(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(wd, 'main', lambda: calls.append('main'))
    assert wd.cli() is None and calls == ['main']
    output = capsys.readouterr()
    assert output.err == '' and output.out == ''
