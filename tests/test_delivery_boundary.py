import hashlib
import json
import smtplib
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsagent2 import emailer, main, state_manager, rollups, workflow_delivery
from newsagent2.delivery import Coordinator, DeliveryError, load_journal, verify_reports
from newsagent2.run_context import PATHS, execution_context, path_for
from newsagent2.store_io import StoreError, atomic_store, read_store, generated_store, digest_store


@pytest.fixture
def smtp(monkeypatch, tmp_path):
    for key, value in {"EMAIL_MODE": "real", "SEND_EMAIL": "1", "REPORT_KEY": "cybermed", "REPORT_MODE": "daily",
        "SMTP_HOST": "smtp.private.test", "SMTP_USER": "sender@private.test", "SMTP_PASS": "private-password",
        "EMAIL_FROM": "sender@private.test", "SMTP_PORT": "587", "RECIPIENTS_JSON": '["one@private.test","two@private.test"]',
        "DELIVERY_RECIPIENT_HMAC_KEY": "offline-test-key-never-used-for-live-smtp-32",
        "DELIVERY_JOURNAL_PATH": str(tmp_path / "journal.json"), "REPORT_DIR": str(tmp_path / "out"),
        "DELIVERY_PERIOD_START": "2026-10-12", "DELIVERY_PERIOD_END": "2026-10-12"}.items():
        monkeypatch.setenv(key, value)
    calls = []
    class SMTP:
        refused = {}
        error = None
        quit_error = None
        def __init__(self, *a, **kw): pass
        def starttls(self, **kw): pass
        def login(self, *a): pass
        def sendmail(self, sender, recipients, message):
            calls.append((list(recipients), message))
            if self.error: raise self.error
            return self.refused
        def quit(self):
            if self.quit_error: raise self.quit_error
    monkeypatch.setattr(emailer.smtplib, "SMTP", SMTP)
    return SMTP, calls, tmp_path


def test_known_acceptance_stable_message_id_and_no_duplicate_retry(smtp):
    cls, calls, root = smtp
    first = emailer.send_markdown("Newsletter", "Validated content.")
    second = emailer.send_markdown("Newsletter", "Validated content.")
    assert first.status == second.status == "accepted_by_smtp" and len(calls) == 1
    assert first.message_id == second.message_id and first.content_version == second.content_version
    assert first.message_id in calls[0][1]
    stored = (root / "journal.json").read_text()
    assert "@private.test" not in stored and "private-password" not in stored
    assert all(len(ref) == 64 for ref in first.accepted)


@pytest.mark.parametrize("exception", [False, True])
def test_complete_rejection_fails_and_records_all_refusals(smtp, exception):
    cls, calls, root = smtp
    refused = {"one@private.test": (550, b"rejected"), "two@private.test": (550, b"rejected")}
    if exception: cls.error = smtplib.SMTPRecipientsRefused(refused)
    else: cls.refused = refused
    with pytest.raises(DeliveryError) as err:
        emailer.send_markdown("Newsletter", "Validated content.")
    assert err.value.result.status == "failed" and len(err.value.result.rejected) == 2
    assert not err.value.result.accepted and len(calls) == 1


def test_partial_acceptance_blocks_automatic_resend(smtp):
    cls, calls, root = smtp
    cls.refused = {"two@private.test": (550, b"refused with private server details")}
    for _ in range(2):
        with pytest.raises(DeliveryError) as err:
            emailer.send_markdown("Newsletter", "Validated content.")
        assert err.value.result.status == "partially_accepted"
        assert len(err.value.result.accepted) == len(err.value.result.rejected) == 1
    assert len(calls) == 1


def test_timeout_is_unknown_and_never_blindly_retried(smtp, capsys):
    cls, calls, root = smtp
    cls.error = TimeoutError("one@private.test smtp.private.test private-password")
    for _ in range(2):
        with pytest.raises(DeliveryError) as err:
            emailer.send_markdown("Newsletter", "Validated content.")
        assert err.value.result.status == "outcome_unknown"
        assert "private" not in str(err.value)
    assert len(calls) == 1
    assert "private" not in capsys.readouterr().out


@pytest.mark.parametrize("partial", [False, True])
def test_disabling_send_cannot_erase_an_unresolved_smtp_handoff(smtp, monkeypatch, partial):
    cls, calls, root = smtp
    if partial:
        cls.refused = {"two@private.test": (550, b"refused")}
    else:
        cls.error = TimeoutError("private SMTP details")
    with pytest.raises(DeliveryError):
        emailer.send_markdown("Newsletter", "Validated content.")
    original = next(iter(load_journal(root / "journal.json")["editions"].values()))
    monkeypatch.setenv("SEND_EMAIL", "0")
    with pytest.raises(DeliveryError):
        emailer.send_markdown("Newsletter", "Validated content.")
    retained = next(iter(load_journal(root / "journal.json")["editions"].values()))
    assert retained == original and len(calls) == 1
    monkeypatch.setenv("SEND_EMAIL", "1")
    with pytest.raises(DeliveryError):
        emailer.send_markdown("Newsletter", "Validated content.")
    assert len(calls) == 1


def test_disabled_invocation_skips_without_replacing_a_known_acceptance(smtp, monkeypatch):
    cls, calls, root = smtp
    emailer.send_markdown("Newsletter", "Validated content.")
    original = (root / "journal.json").read_bytes()
    monkeypatch.setenv("SEND_EMAIL", "0")
    coordinator = Coordinator("cybermed", "daily")
    result = emailer.send_markdown("Newsletter", "Validated content.", coordinator=coordinator)
    assert result.status == "intentionally_skipped"
    coordinator.finalize([lambda _: pytest.fail("disabled attempt must not finalize markers")])
    assert (root / "journal.json").read_bytes() == original and len(calls) == 1
    monkeypatch.setenv("SEND_EMAIL", "1")
    assert emailer.send_markdown("Newsletter", "Validated content.").status == "accepted_by_smtp"
    assert len(calls) == 1


def test_missing_configuration_is_failure_without_smtp(smtp, monkeypatch):
    cls, calls, root = smtp
    monkeypatch.delenv("SMTP_PASS")
    with pytest.raises(DeliveryError) as err:
        emailer.send_markdown("Newsletter", "Validated content.")
    assert err.value.result.status == "failed" and err.value.result.category == "smtp_configuration_missing"
    assert not calls


def test_crash_after_server_acceptance_keeps_inflight_unknown(smtp):
    cls, calls, root = smtp
    cls.error = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        emailer.send_markdown("Newsletter", "Validated content.")
    journal = load_journal(root / "journal.json")
    assert next(iter(journal["editions"].values()))["status"] == "outcome_unknown"
    cls.error = None
    with pytest.raises(DeliveryError): emailer.send_markdown("Newsletter", "Validated content.")
    assert len(calls) == 1


def test_quit_failure_does_not_reverse_data_acceptance(smtp):
    cls, calls, root = smtp; cls.quit_error = TimeoutError("private")
    assert emailer.send_markdown("Newsletter", "Validated content.").status == "accepted_by_smtp"


def test_state_failure_after_acceptance_can_finalize_without_sending_again(smtp):
    cls, calls, root = smtp
    coordinator = Coordinator("cybermed", "daily")
    emailer.send_markdown("Newsletter", "Validated content.", coordinator=coordinator)
    def failed(_): raise OSError("private state failure")
    with pytest.raises(DeliveryError): coordinator.finalize([failed])
    receipt = next(iter(load_journal(root / "journal.json")["editions"].values()))
    assert receipt["status"] == "accepted_by_smtp" and not receipt["state_persisted"]
    repaired = []
    restarted = Coordinator("cybermed", "daily")
    emailer.send_markdown("Newsletter", "Validated content.", coordinator=restarted)
    restarted.finalize([lambda result: repaired.append(result.edition_id)])
    assert repaired and len(calls) == 1 and restarted.result.state_persisted


def test_changed_content_or_recipients_never_license_resend(smtp, monkeypatch):
    cls, calls, root = smtp
    emailer.send_markdown("Newsletter", "Validated content.")
    with pytest.raises(DeliveryError, match="content_version_changed"):
        emailer.send_markdown("Newsletter", "Changed content.")
    monkeypatch.setenv("RECIPIENTS_JSON", '["new@private.test"]')
    with pytest.raises(DeliveryError, match="recipient_set_changed"):
        emailer.send_markdown("Newsletter", "Validated content.")
    assert len(calls) == 1
    assert next(iter(load_journal(root / "journal.json")["editions"].values()))["status"] == "accepted_by_smtp"


def test_daily_success_cannot_mask_a_failed_weekly_in_multi_cadence(smtp, monkeypatch):
    cls, calls, root = smtp
    for cadence in ("daily", "weekly", "monthly"):
        monkeypatch.setenv("REPORT_MODE", cadence)
        cls.refused = {"two@private.test": (550, b"refused")} if cadence == "weekly" else {}
        coordinator = Coordinator("cybermed", cadence)
        try:
            emailer.send_markdown("Newsletter", "Validated content.", coordinator=coordinator)
            coordinator.finalize([])
        except DeliveryError: pass
    with pytest.raises(RuntimeError, match="cybermed:weekly"):
        verify_reports(root / "out", "cybermed", ["daily", "weekly", "monthly"], mode="real", send_required=True)
    assert len(list((root / "out/recovery").glob("*.md"))) == 3


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}


@pytest.mark.parametrize("report_key", ["cybermed", "cyberlurch"])
@pytest.mark.parametrize("mode", ["none", "test"])
@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly", "yearly"])
def test_none_test_isolate_all_paths_and_use_only_test_recipients(smtp, monkeypatch, report_key, mode, cadence):
    cls, calls, root = smtp
    prod = root / "production"; prod.mkdir()
    monkeypatch.delenv("DELIVERY_JOURNAL_PATH")
    monkeypatch.setenv("NEWSAGENT_STATE_DIR", str(prod))
    monkeypatch.setenv("REPORT_KEY", report_key); monkeypatch.setenv("REPORT_MODE", cadence); monkeypatch.setenv("EMAIL_MODE", mode)
    monkeypatch.setenv("TEST_RECIPIENTS_CONFIG_JSON", '["test@private.test"]')
    for name in PATHS.values():
        (prod / name).write_text(json.dumps({"version": 1, "editions": {}} if name == "delivery_journal.json" else {"legacy": "retained"}))
    before = hashes(prod)
    with execution_context():
        for variable in PATHS:
            if variable != "DELIVERY_JOURNAL_PATH":
                atomic_store(path_for(variable), {"local": True}, kind="local_test")
        result = emailer.send_markdown("Newsletter", "Validated content.")
    assert before == hashes(prod)
    assert result.status == ("intentionally_skipped" if mode == "none" else "accepted_by_smtp")
    assert not calls if mode == "none" else calls[0][0] == ["test@private.test"]


@pytest.mark.parametrize("loader", [state_manager.load_state, rollups.load_rollups_state, rollups.load_rollups_state_readonly,
                                    main._load_cyberlurch_digest_state, main._load_cybermed_daily_digest_state])
def test_corrupted_existing_store_is_unchanged_and_missing_store_is_not_created(tmp_path, loader):
    path = tmp_path / "history.json"; path.write_bytes(b'{"history":truncated')
    raw = path.read_bytes()
    with pytest.raises((StoreError, RuntimeError)): loader(str(path))
    assert path.read_bytes() == raw and len(list(tmp_path.iterdir())) == 1
    missing = tmp_path / "new.json"; loader(str(missing)); assert not missing.exists()


def test_valid_legacy_state_is_additive_and_does_not_invent_delivery_history(tmp_path):
    path = tmp_path / "state.json"; data = {"version": 1, "reports": {}, "legacy_extra": {"keep": True}}
    path.write_text(json.dumps(data)); loaded = state_manager.load_state(str(path))
    state_manager.save_state(str(path), loaded)
    assert json.loads(path.read_text())["legacy_extra"] == data["legacy_extra"]
    assert load_journal(tmp_path / "missing-journal.json")["editions"] == {}


def test_atomic_save_rejects_invalid_preimage_without_overwriting(tmp_path):
    path = tmp_path / "state.json"; path.write_bytes(b'not-json')
    with pytest.raises(StoreError): state_manager.save_state(str(path), {"reports": {}})
    assert path.read_bytes() == b'not-json'


def test_git_push_failure_is_visible_with_recovery_metadata(smtp, monkeypatch):
    cls, calls, root = smtp
    monkeypatch.setenv("GITHUB_REF_NAME", "test-branch")
    state = root / "processed_items.json"; state.write_text('{"reports": {}}')
    commands = []
    def run(args, **kw):
        commands.append(args)
        if args[1] == "status": return SimpleNamespace(returncode=0, stdout=" M state.json", stderr="")
        if args[1] == "rev-parse": return SimpleNamespace(returncode=0, stdout="a" * 40 + "\n", stderr="")
        if args[1] in {"push", "ls-remote"}: return SimpleNamespace(returncode=1, stdout="", stderr="private")
        return SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(workflow_delivery.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="git_push_failed_or_unknown"):
        workflow_delivery.publish_state([str(state)], report_dir=root / "out")
    result = json.loads((root / "out/recovery/git_persistence.json").read_text())
    assert result["category"] == "git_push_failed_or_unknown"
    assert all("--force" not in cmd and "rebase" not in cmd and "reset" not in cmd for cmd in commands)


def test_journal_persistence_failure_prevents_smtp(smtp, monkeypatch):
    cls, calls, root = smtp
    from newsagent2 import delivery
    monkeypatch.setattr(delivery, "atomic_store", lambda *a, **k: (_ for _ in ()).throw(StoreError("journal", "persistence_failed")))
    with pytest.raises(StoreError): emailer.send_markdown("Newsletter", "Validated content.")
    assert calls == []


def test_explicit_partial_retry_sends_only_previously_rejected_recipients(smtp):
    cls, calls, root = smtp
    cls.refused = {"two@private.test": (550, b"refused")}
    with pytest.raises(DeliveryError): emailer.send_markdown("Newsletter", "Validated content.")
    cls.refused = {}
    result = emailer.send_markdown("Newsletter", "Validated content.", retry_rejected=True)
    assert result.status == "accepted_by_smtp" and len(result.accepted) == 2
    assert calls[1][0] == ["two@private.test"] and result.prior_attempts


def test_partial_retry_timeout_retains_prior_acceptance_and_blocks_further_retry(smtp):
    cls, calls, root = smtp
    cls.refused = {"two@private.test": (550, b"refused")}
    with pytest.raises(DeliveryError): emailer.send_markdown("Newsletter", "Validated content.")
    cls.error = TimeoutError("unknown")
    with pytest.raises(DeliveryError) as err:
        emailer.send_markdown("Newsletter", "Validated content.", retry_rejected=True)
    assert err.value.result.status == "outcome_unknown" and len(err.value.result.accepted) == 1
    assert len(err.value.result.unknown) == 1 and calls[1][0] == ["two@private.test"]
    with pytest.raises(DeliveryError): emailer.send_markdown("Newsletter", "Validated content.", retry_rejected=True)
    assert len(calls) == 2


def test_recovery_import_does_not_downgrade_known_acceptance(smtp):
    cls, calls, root = smtp
    emailer.send_markdown("Newsletter", "Validated content.")
    coordinator = Coordinator("cybermed", "daily"); coordinator.result = next(iter(load_journal(root / "journal.json")["editions"].values()))
    incoming = root / "old-artifact"; incoming.mkdir()
    data = load_journal(root / "journal.json")
    row = next(iter(data["editions"].values())); row["status"] = "outcome_unknown"; row["unknown"] = row["accepted"]; row["accepted"] = []
    (incoming / "journal.json").write_text(json.dumps(data))
    workflow_delivery.import_recovery(incoming, journal_path=root / "journal.json")
    assert next(iter(load_journal(root / "journal.json")["editions"].values()))["status"] == "accepted_by_smtp"


def test_missing_recovery_never_licenses_fresh_send(smtp):
    cls, calls, root = smtp
    with pytest.raises(StoreError, match="missing_journal"):
        workflow_delivery.import_recovery(root / "absent", journal_path=root / "journal.json")
    assert not calls and not (root / "journal.json").exists()


def test_recovery_import_preserves_unknown_and_checks_generated_hash(smtp):
    cls, calls, root = smtp; cls.error = TimeoutError("unknown")
    with pytest.raises(DeliveryError): emailer.send_markdown("Newsletter", "Validated content.")
    archive = root / "out/recovery"
    target = root / "restored.json"
    workflow_delivery.import_recovery(archive, journal_path=target)
    assert next(iter(load_journal(target)["editions"].values()))["status"] == "outcome_unknown"
    next(archive.glob("*.md")).write_text("Tampered content.")
    with pytest.raises(StoreError, match="missing_or_invalid_generated_content"):
        workflow_delivery.import_recovery(archive, journal_path=root / "other.json")


def test_missing_later_cadence_gets_separate_failed_result(smtp):
    cls, calls, root = smtp
    coordinator = Coordinator("cybermed", "daily")
    emailer.send_markdown("Newsletter", "Validated content.", coordinator=coordinator); coordinator.finalize([])
    with pytest.raises(RuntimeError, match="weekly"):
        verify_reports(root / "out", "cybermed", ["daily", "weekly", "monthly"], mode="real", send_required=True)
    for cadence in ("weekly", "monthly"):
        result = json.loads((root / f"out/cybermed_{cadence}_delivery_result.json").read_text())
        assert result["status"] == "failed" and result["category"] == "planned_result_missing"


def test_generated_delta_contains_records_not_intermediate_state(smtp, monkeypatch):
    cls, calls, root = smtp
    state = root / "generated.json"
    state.write_text('{"schema_version":1,"digests":[{"digest_id":"old","extra":"keep"}]}')
    raw = state.read_bytes()
    from newsagent2.run_context import execution_context
    with execution_context():
        generated_store(state, {"schema_version": 1, "digests": [{"digest_id": "old", "extra": "keep"}, {"digest_id": "new", "bottom_line": "Validated."}]}, kind="cybermed_daily", validate=digest_store)
    delta = json.loads(next((root / "out/recovery").glob("*cybermed_daily_generated_delta.json")).read_text())
    assert delta["records"] == [{"digest_id": "new", "bottom_line": "Validated."}]
    assert delta["base_sha256"] == hashlib.sha256(raw).hexdigest()
    assert delta["filesystem_persisted"] and "processed" not in delta and not calls


def test_pre_smtp_generation_failure_can_retry_without_unknown_send(smtp):
    cls, calls, root = smtp
    Coordinator("cybermed", "daily").failure("generation_failed")
    assert emailer.send_markdown("Newsletter", "Validated content.").status == "accepted_by_smtp"
    assert len(calls) == 1


def test_no_sent_or_daily_markers_without_known_acceptance(tmp_path):
    from newsagent2.delivery import DeliveryResult
    path = tmp_path / "state.json"; path.write_text('{"reports":{},"legacy":"keep"}')
    raw = path.read_bytes()
    result = DeliveryResult("cybermed", "daily", "2026-10-12", "2026-10-12", "cybermed:daily:2026-10-12:2026-10-12:production", "a" * 64, "<safe@newsagent2.invalid>", status="partially_accepted", accepted=["b" * 64], rejected=["c" * 64])
    main._update_state_after_run(state_path=str(path), state=state_manager.load_state(str(path)), items_all_new=[{"source":"pubmed","id":"paper"}], overview_items=[{"source":"pubmed","id":"paper"}], detail_items=[], foamed_overview_items=[], report_key="cybermed", report_mode="daily", now_utc_iso="2026-10-12T05:00:00Z", read_only=False, delivery_result=result)
    assert path.read_bytes() == raw


def test_workflows_keep_shared_lock_ref_and_commit_only_after_validation():
    for filename, key in (("newsagent.yml", "cyberlurch"), ("cybermed.yml", "cybermed")):
        text = (Path(".github/workflows") / filename).read_text()
        assert 'group: newsagent2-state-writers-${{ github.ref }}' in text and 'ref: ${{ github.ref_name }}' in text
        assert "workflow_delivery verify --report-key " + key in text
        section = text.split("- name: Commit and publish", 1)[1].split("- name: Upload", 1)[0]
        assert "success() &&" in section and "always()" not in section
        assert "env.EMAIL_MODE == 'real'" in section and "--force" not in text
        assert "state/delivery_journal.json" in section and "out/recovery/" in text


def test_main_resumes_known_acceptance_without_models_or_smtp(smtp, monkeypatch):
    cls, calls, root = smtp
    monkeypatch.setenv("STATE_PATH", str(root / "processed_items.json"))
    intents = {"report_key": "cybermed", "report_mode": "daily", "now_utc_iso": "2026-10-12T05:00:00Z",
               "items_all_new": [{"source":"pubmed","id":"paper"}], "overview_items": [{"source":"pubmed","id":"paper"}],
               "detail_items": [], "foamed_overview_items": []}
    emailer.send_markdown("Newsletter", "Validated content.", finalization=intents)
    monkeypatch.setattr(main, "_main", lambda: pytest.fail("resumption must not regenerate or call providers"))
    main.main()
    state = json.loads((root / "processed_items.json").read_text())
    assert state["last_successful_daily_run_utc_by_report"]["cybermed"] == intents["now_utc_iso"]
    assert len(calls) == 1


def test_legacy_generation_without_delivery_history_is_unknown_not_sent(smtp, monkeypatch):
    cls, calls, root = smtp
    monkeypatch.setenv("STATE_PATH", str(root / "processed_items.json"))
    daily = root / "cybermed_daily_digests.json"
    daily.write_text('{"schema_version":1,"digests":[{"digest_id":"cybermed_daily_2026-10-12","run_date":"2026-10-12"}]}')
    raw = daily.read_bytes()
    monkeypatch.setattr(main, "_main", lambda: pytest.fail("legacy uncertainty must stop before providers"))
    with pytest.raises(RuntimeError, match="legacy_delivery_history_unknown"): main.main()
    receipt = next(iter(load_journal(root / "journal.json")["editions"].values()))
    assert receipt["status"] == "outcome_unknown" and receipt["accepted"] == []
    assert daily.read_bytes() == raw and not calls


def test_operator_reconciliation_retains_unknown_attempt_history(smtp):
    cls, calls, root = smtp; cls.error = TimeoutError("unknown")
    with pytest.raises(DeliveryError): emailer.send_markdown("Newsletter", "Validated content.")
    result = next(iter(load_journal(root / "journal.json")["editions"].values()))
    workflow_delivery.confirm_pending_not_accepted(result["edition_id"])
    fixed = next(iter(load_journal(root / "journal.json")["editions"].values()))
    assert fixed["status"] == "failed" and fixed["category"] == "operator_confirmed_not_accepted"
    assert fixed["prior_attempts"][0]["status"] == "outcome_unknown" and not fixed["accepted"]


def test_true_backfill_flag_keeps_only_authorized_rollup_path(monkeypatch, tmp_path):
    prod = tmp_path / "state"; prod.mkdir()
    monkeypatch.setenv("NEWSAGENT_STATE_DIR", str(prod))
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("EMAIL_MODE", "none"); monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "monthly")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("ROLLUP_MONTH_OVERRIDE", "2026-08")
    monkeypatch.setenv("CYBERLURCH_MONTHLY_MANUAL_STATE_WRITE", "true")
    with execution_context() as ctx:
        assert path_for("ROLLUPS_STATE_PATH") == prod / "rollups.json"
        assert path_for("STATE_PATH").parent != prod and path_for("YOUTUBE_CONTENT_CACHE_PATH").parent != prod


def test_failed_smtp_run_without_artifact_blocks_recovery_before_new_sender(monkeypatch, tmp_path):
    monkeypatch.setenv("EMAIL_MODE", "real"); monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_TOKEN", "offline-test-token"); monkeypatch.setenv("GITHUB_REF_NAME", "main")
    monkeypatch.setenv("GITHUB_RUN_ID", "2")
    class Session:
        def __init__(self): self.headers = {}
        def get(self, url, **kwargs):
            if url.endswith('/runs'):
                payload = {"workflow_runs": [{"id": 1, "conclusion": "cancelled"}]}
            elif url.endswith('/jobs'):
                payload = {"jobs": [{"steps": [{"name": "Run Cybermed reports", "started_at": "2026-10-12", "conclusion": "cancelled"}]}]}
            else: payload = {"artifacts": []}
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)
    monkeypatch.setattr(workflow_delivery.requests, "Session", Session)
    with pytest.raises(RuntimeError, match="delivery_recovery_missing_for_run:1"):
        workflow_delivery.recover_failed_runs("cybermed", "cybermed.yml", report_dir=tmp_path)


def test_generated_record_recovery_requires_matching_preimage_and_is_idempotent(smtp, monkeypatch):
    cls, calls, root = smtp
    source = root / "cybermed_daily_digests.json"
    source.write_text('{"schema_version":1,"digests":[{"digest_id":"old","legacy":"keep"}]}')
    raw = source.read_bytes()
    monkeypatch.setenv("CYBERMED_DAILY_DIGEST_STATE_PATH", str(source))
    with execution_context():
        generated_store(source, {"schema_version":1,"digests":[{"digest_id":"old","legacy":"keep"},{"digest_id":"new","bottom_line":"Validated."}]}, kind="cybermed_daily", validate=digest_store)
    source.write_bytes(raw)  # Fresh checkout after failed Git push.
    workflow_delivery.restore_generated_records(root / "out/recovery", "cybermed")
    restored = source.read_bytes()
    assert len(json.loads(restored)["digests"]) == 2 and not calls
    workflow_delivery.restore_generated_records(root / "out/recovery", "cybermed")
    assert source.read_bytes() == restored
    source.write_text('{"schema_version":1,"digests":[{"digest_id":"different","newer":"keep"}]}')
    before = source.read_bytes()
    with pytest.raises(StoreError, match="newer_state_requires_operator_review"):
        workflow_delivery.restore_generated_records(root / "out/recovery", "cybermed")
    assert source.read_bytes() == before


def test_imported_filesystem_success_is_not_assumed_published_by_git(smtp):
    cls, calls, root = smtp
    coordinator = Coordinator("cybermed", "daily")
    emailer.send_markdown("Newsletter", "Validated content.", coordinator=coordinator); coordinator.finalize([])
    target = root / "fresh-checkout.json"
    workflow_delivery.import_recovery(root / "out/recovery", journal_path=target)
    row = next(iter(load_journal(target)["editions"].values()))
    assert row["status"] == "accepted_by_smtp" and row["state_persisted"] is False


def test_old_receipt_never_rolls_back_newer_sent_markers():
    state = {"reports": {}}
    state_manager.mark_sent(state, "cybermed", "pubmed", "paper", sent_overview=True, when_utc="2026-10-13T05:00:00Z")
    state_manager.mark_sent(state, "cybermed", "pubmed", "paper", sent_overview=True, when_utc="2026-10-12T05:00:00Z")
    meta = state_manager.get_processed_meta(state, "cybermed", "pubmed", "paper")
    assert meta["sent_overview_at_utc"] == "2026-10-13T05:00:00Z"
