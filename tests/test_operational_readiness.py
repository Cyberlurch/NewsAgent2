from copy import deepcopy
from datetime import date, datetime, timezone
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
import sys

import pytest
import yaml

from newsagent2 import aggregate_policy as policy, delivery, edition_plan as ep, operational_readiness as op
from newsagent2 import history_coverage as hc, workflow_delivery as wd
from newsagent2.cybermed_digest_store import make_cybermed_weekly_digest
from newsagent2.run_context import PATHS, path_for
from newsagent2.store_io import atomic_store


def snapshot():
    return {k: path_for(k).read_bytes() if path_for(k).exists() else None for k in PATHS}


def sources():
    daily = [{'digest_id': 'cybermed_daily_'+d, 'report_key': 'cybermed', 'cadence': 'daily', 'run_date': d,
        'items': {'pubmed': [], 'foamed': []}} for d in ['2026-10-05', '2026-10-06', '2026-10-07',
            '2026-10-08', '2026-10-09', '2026-10-12', '2026-10-13']]
    week = make_cybermed_weekly_digest(period_start='2026-10-05', period_end='2026-10-09',
        generated_at_utc='2026-10-09T10:00:00Z', source_digests=daily[:5],
        pubmed_items=[], foamed_items=[], deep_dives=[], top_picks=[])
    path_for('CYBERMED_DAILY_DIGEST_STATE_PATH').write_text(json.dumps({'digests': daily}))
    path_for('CYBERMED_WEEKLY_DIGEST_STATE_PATH').write_text(json.dumps({'digests': [week]}))
    path_for('ROLLUPS_STATE_PATH').write_text(json.dumps({'reports': {
        'cybermed': [{'month': '2026-09', 'top_items': [{'title': 'Saved clinical review'}]}],
        'cyberlurch': [{'month': '2026-09', 'full_text_count': 1, 'representative_items': [{
            'video_id': 'v', 'date': '2026-09-10', 'channel': 'Channel A', 'title': 'Saved development',
            'url': 'https://example.test/v', 'bottom_line': 'A persisted source describes a concrete development.'}]}]}}))
    path_for('CYBERLURCH_DIGEST_STATE_PATH').write_text(json.dumps({'digests': [{
        'video_id': 'v', 'published_at': '2026-10-13T10:00:00Z', 'title': 'Saved report'}]}))
    return daily, week


@pytest.mark.parametrize('key', ['cybermed', 'cyberlurch'])
def test_four_cadences_forecast_without_network_mutation_or_false_complete(key):
    sources(); before = snapshot()
    result = op.assess(key, as_of=datetime(2026, 10, 16, 12, tzinfo=timezone.utc))
    assert not result['ok'] and [r['cadence'] for r in result['checks']] == list(ep.CADENCES)
    assert snapshot() == before
    assert not any(r.get('delivery_status') == 'accepted_by_smtp' for r in result['checks'])
    if key == 'cybermed':
        weekly = result['checks'][1]
        assert weekly['status'] == 'recovery_required' and weekly['source_status'] == 'partial_ready'
        assert weekly['known_missing_dates'] == ['2026-10-14', '2026-10-15']
        assert weekly['coverage']['status'] == 'incomplete'
    assert all(not row['automatic_resend'] for row in result['historical_delivery'])


@pytest.mark.parametrize('defect', ['orphan', 'foreign', 'duplicate', 'corrupt', 'upstream_opaque'])
def test_partial_policy_never_downgrades_structural_failures(defect):
    daily, week = sources()
    if defect == 'orphan': week['source_daily_digest_ids'].append('lost')
    if defect == 'foreign': daily[0]['report_key'] = 'cyberlurch'
    if defect == 'duplicate': daily.append(deepcopy(daily[0]))
    coverage = hc.monthly_coverage({'month': '2026-10', 'calendar_policy': ep.POLICY,
        'source_weekly_digest_ids': [week['digest_id']]}, [week], daily)
    if defect == 'corrupt': coverage = hc.result([('incomplete', 'corrupted_existing_store')])
    if defect == 'upstream_opaque': coverage = hc.result([('incomplete', 'upstream_incomplete')])
    with pytest.raises(RuntimeError):
        policy.require_review_sources(coverage, available=1, category='test')


@pytest.mark.parametrize('cadence', ['monthly', 'yearly'])
@pytest.mark.parametrize('key', ['cybermed', 'cyberlurch'])
def test_open_period_never_becomes_a_final_review(cadence, key):
    with pytest.raises(RuntimeError, match='period_still_open'):
        policy.require_closed('2026-12-31', '2026-10-16', cadence)
    entry = ep.entry_for(key, cadence, date(2026, 10, 16), 'workflow_dispatch', 'real')
    with pytest.raises(RuntimeError, match='period_still_open'):
        ep.check_ready(entry)


@pytest.mark.parametrize('key', ['cybermed', 'cyberlurch'])
def test_bad_existing_json_is_byte_preserved_and_each_cadence_is_red(key):
    sources()
    path = path_for('ROLLUPS_STATE_PATH'); path.write_bytes(b'{"reports":broken')
    before = snapshot()
    result = op.assess(key, as_of=datetime(2026, 10, 16, 12, tzinfo=timezone.utc))
    assert not result['ok'] and all(r['status'] == 'blocked' for r in result['checks'])
    assert any('corrupted_existing_store' in r for r in result['errors'])
    assert snapshot() == before


def test_stale_publication_is_not_hidden_by_an_available_partial_week():
    sources()
    path_for('STATE_PATH').write_text(json.dumps({'reports': {},
        'last_successful_daily_run_utc_by_report': {'cybermed': '2026-10-13T10:00:00Z'}}))
    result = op.assess('cybermed', as_of=datetime(2026, 10, 16, 12, tzinfo=timezone.utc))
    assert not result['ok'] and result['checks'][0]['status'] == 'recovery_required'
    assert result['checks'][0]['marker_is_not_smtp_proof'] is True
    assert result['checks'][1]['source_status'] == 'partial_ready'


def test_configuration_checks_every_audience_without_disclosing_private_values(monkeypatch):
    sources()
    for name, value in {'REPORT_KEY': 'cybermed', 'SMTP_HOST': 'private-host.invalid',
        'SMTP_USER': 'private-user', 'SMTP_PASS': 'private-password', 'SMTP_PORT': '587',
        'DELIVERY_RECIPIENT_HMAC_KEY': 'offline-key-at-least-32-characters-long',
        'OPENAI_API_KEY': 'offline-model-key',
        'RECIPIENTS_CONFIG_JSON': json.dumps({'cybermed': {'daily': ['dummy@example.test'],
            'weekly': [], 'monthly': ['dummy@example.test']}})}.items():
        monkeypatch.setenv(name, value)
    result = op.assess('cybermed', as_of=datetime(2026, 10, 16, 12, tzinfo=timezone.utc), check_config=True)
    assert result['checks'][0]['status'] == 'ready'
    assert 'operational_recipients_missing' in result['checks'][1]['categories']
    assert not result['ok']
    encoded = json.dumps(result)
    assert all(value not in encoded for value in ['private-host', 'private-user', 'private-password', 'dummy@example.test'])


def test_scheduled_archive_is_explicitly_quarantined_without_smtp_success(monkeypatch, tmp_path):
    monkeypatch.setenv('GITHUB_REPOSITORY', 'Cyberlurch/NewsAgent2')
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'schedule'); monkeypatch.setenv('EMAIL_MODE', 'real')
    output = tmp_path/'plan.json'
    monkeypatch.setattr(sys, 'argv', ['plan', '--report-key', 'cybermed', '--created-at', '2026-10-16T12:00:00Z',
        '--job-started-at', '2026-10-16T12:01:00Z', '--output', str(output)])
    before = snapshot(); ep.main()
    plan = json.loads(output.read_text())
    assert [r['cadence'] for r in plan['entries']] == ['daily', 'weekly']
    assert plan['quarantined_editions'] == [{'edition_id': 'cybermed:monthly:2026-09-01:2026-09-30:production',
        'category': 'legacy_delivery_history_unknown', 'automatic_resend': False}]
    assert not plan['already_finalized_editions'] and snapshot() == before and ep.validate_plan(plan)


@pytest.mark.parametrize('language,word', [('en','Partial review'),('de','Teilrückblick'),('sv','Partiell översikt')])
def test_reader_note_is_short_localized_and_does_not_invent_known_counts(language, word):
    coverage = hc.result([('unknown', 'edition_references_unknown')], expected=['a','b'], observed=[])
    note = policy.review_note(coverage, cadence='yearly', language=language)
    assert word in note and '0 of 2' not in note and len(note.splitlines()) == 2


def test_recovery_workflow_has_writer_lock_and_no_sender_capability():
    text = Path('.github/workflows/newsletter-recover-state.yml').read_text()
    workflow = yaml.safe_load(text)
    assert workflow['concurrency']['group'] == 'newsagent2-state-writers-${{ github.ref }}'
    assert 'ref: ${{ github.ref_name }}' in text and 'finalize-pending' in text
    assert 'SMTP_' not in text and 'OPENAI_' not in text and 'RECIPIENTS_CONFIG_JSON' not in text
    assert 'workflow_dispatch:' in text and 'schedule:' not in text
    assert 'src.newsagent2.main ' not in text and 'git push' not in text


def test_finalize_pending_cli_rejects_nonproduction_or_foreign_scope(monkeypatch):
    before = snapshot()
    monkeypatch.setenv('EMAIL_MODE', 'none'); monkeypatch.setenv('REPORT_KEY', 'cybermed')
    monkeypatch.setattr(sys, 'argv', ['delivery', 'finalize-pending', '--report-key', 'cybermed'])
    with pytest.raises(RuntimeError, match='scope_mismatch'): wd.main()
    assert snapshot() == before


@pytest.mark.parametrize('kind', ['publication_failure', 'preview_failure', 'sender_skipped', 'api_denied'])
def test_actions_metadata_finds_pending_recovery_before_the_next_send(kind, monkeypatch):
    import requests
    from test_recovery_scope import job
    for name, value in {'GITHUB_REPOSITORY': 'offline/repo', 'GITHUB_REF_NAME': 'main', 'GITHUB_TOKEN': 'offline-token'}.items():
        monkeypatch.setenv(name, value)
    class Session:
        def __init__(self): self.headers = {}
        def get(self, url, **kwargs):
            if kind == 'api_denied':
                raise requests.ConnectionError('private-host.invalid token-secret')
            if url.endswith('/runs'):
                data = {'workflow_runs': [{'id': 71, 'conclusion': 'failure',
                    'event': 'workflow_dispatch' if kind == 'preview_failure' else 'schedule'}]}
            else:
                data = {'jobs': [job(mode='test' if kind == 'preview_failure' else 'real')]}
                if kind == 'sender_skipped':
                    data['jobs'][0]['steps'][1]['conclusion'] = 'skipped'
            return SimpleNamespace(json=lambda: data, raise_for_status=lambda: None)
    monkeypatch.setattr(requests, 'Session', Session)
    sources(); before = snapshot()
    result = op.assess('cybermed', as_of=datetime(2026, 10, 16, 12, tzinfo=timezone.utc), check_actions=True)
    # The synthetic due Weekly has no receipt; preview/skipped jobs must not
    # add an unrelated recovery hold, while the real publication failure does.
    assert not result['ok']
    assert bool(result['pending_recovery_runs']) is (kind == 'publication_failure')
    assert 'private-host' not in json.dumps(result) and 'token-secret' not in json.dumps(result)
    assert snapshot() == before


@pytest.mark.parametrize('status', ['outcome_unknown', 'partially_accepted', 'accepted_by_smtp'])
def test_unresolved_previous_edition_is_not_hidden_by_the_next_date(status, monkeypatch):
    from test_newsletter_job_scope import receipt
    sources()
    row, _ = receipt('cybermed')
    row.status = status; row.state_persisted = False
    if status == 'partially_accepted': row.rejected = ['b'*64]
    if status == 'outcome_unknown': row.unknown = ['b'*64]
    atomic_store(path_for('DELIVERY_JOURNAL_PATH'), {'version': 1, 'editions': {row.edition_id: row.public()}},
        kind='delivery_journal', validate=delivery.journal_valid)
    result = op.assess('cybermed', as_of=datetime(2026, 10, 19, 12, tzinfo=timezone.utc))
    assert not result['ok'] and result['unresolved_delivery'][0]['edition_id'] == row.edition_id


def test_quarantine_cannot_hide_a_structured_unknown_receipt(monkeypatch, tmp_path):
    from test_recovery_scope import result as receipt
    monkeypatch.setenv('GITHUB_REPOSITORY', 'Cyberlurch/NewsAgent2')
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'schedule'); monkeypatch.setenv('EMAIL_MODE', 'real')
    row = receipt(start='2026-09-01', end='2026-09-30', cadence='monthly')
    row['status'] = 'outcome_unknown'
    atomic_store(path_for('DELIVERY_JOURNAL_PATH'), {'editions': {row['edition_id']: row}},
        kind='delivery_journal', validate=delivery.journal_valid)
    output = tmp_path/'plan.json'
    monkeypatch.setattr(sys, 'argv', ['plan', '--report-key', 'cybermed', '--created-at', '2026-10-16T12:00:00Z',
        '--job-started-at', '2026-10-16T12:01:00Z', '--output', str(output)])
    ep.main(); plan = json.loads(output.read_text())
    assert 'monthly' in [r['cadence'] for r in plan['entries']] and not plan.get('quarantined_editions')
    assert delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row['edition_id']]['status'] == 'outcome_unknown'


@pytest.mark.parametrize('key', ['cybermed', 'cyberlurch'])
def test_state_only_finalization_cli_repairs_only_known_acceptance(key, monkeypatch, tmp_path):
    from test_newsletter_job_scope import receipt
    row, _ = receipt(key); row.state_persisted = False
    atomic_store(path_for('DELIVERY_JOURNAL_PATH'), {'editions': {row.edition_id: row.public()}},
        kind='delivery_journal', validate=delivery.journal_valid)
    monkeypatch.setenv('REPORT_KEY', key); monkeypatch.setenv('EMAIL_MODE', 'real'); monkeypatch.setenv('SEND_EMAIL', '0')
    monkeypatch.setattr(sys, 'argv', ['delivery', 'finalize-pending', '--report-key', key])
    wd.main(); wd.main()
    saved = delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][row.edition_id]
    assert saved['state_persisted'] and saved['status'] == 'accepted_by_smtp'
    assert saved['message_id'] == row.message_id and saved['accepted'] == row.accepted


def test_actual_readonly_cli_reports_stale_state_without_mutation(monkeypatch, tmp_path):
    directory = tmp_path/'state'; directory.mkdir()
    for key, name in PATHS.items(): monkeypatch.setenv(key, str(directory/name))
    sources()
    path_for('STATE_PATH').write_text(json.dumps({'reports': {},
        'last_successful_daily_run_utc_by_report': {'cybermed': '2026-10-13T10:00:00Z'}}))
    before = snapshot(); output = tmp_path/'evidence'/'readiness.json'
    result = subprocess.run([sys.executable, '-m', 'newsagent2.operational_readiness', '--report-key', 'cybermed',
        '--as-of', '2026-10-16T12:00:00Z', '--output', str(output)], text=True, capture_output=True)
    assert result.returncode == 1, result.stderr
    assert json.loads(output.read_text())['checks'][0]['status'] == 'recovery_required'
    assert snapshot() == before and not list(directory.glob('*.lock'))


def test_missing_edition_note_does_not_count_holidays_as_required_editions():
    coverage = hc.result([('incomplete', 'required_daily_missing_or_unreferenced')],
        expected=['2026-04-07', '2026-04-08'], observed=['2026-04-06', '2026-04-07'])
    assert '1 of 2' in policy.review_note(coverage, cadence='weekly')


def test_partial_reviews_still_wait_for_the_scheduled_month_year_close():
    with pytest.raises(RuntimeError, match='closing_slot_not_reached'):
        policy.require_review_period('cybermed', 'monthly', '2026-10-31', '2026-11-02')
    policy.require_review_period('cybermed', 'monthly', '2026-10-31', '2026-11-06')
    pending = hc.yearly_coverage('cybermed', 2026, [], as_of='2027-01-01T12:00:00Z')
    with pytest.raises(RuntimeError, match='closing_slot_not_reached'):
        policy.require_year_closed(pending)


def test_malformed_month_is_not_misdiagnosed_as_an_available_partial_year():
    sources()
    path_for('ROLLUPS_STATE_PATH').write_text(json.dumps({'reports': {'cybermed': [{'month': '2026-13'}]}}))
    before = snapshot()
    result = op.assess('cybermed', as_of=datetime(2026, 10, 16, 12, tzinfo=timezone.utc))
    assert not result['ok'] and 'store_error:operational_monthly:invalid_existing_store' in result['errors']
    assert snapshot() == before
