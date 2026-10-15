from copy import deepcopy
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsagent2 import delivery, edition_plan as ep, legacy_transition as lt, main, workflow_delivery as wd
from newsagent2.store_io import atomic_store


def plan(monkeypatch, tmp_path, key="cybermed", day="2026-10-16", request="scheduled", mode="real"):
    monkeypatch.setenv("GITHUB_REPOSITORY", lt.REPOSITORY)
    monkeypatch.setenv("GITHUB_REF_NAME", "main")
    monkeypatch.setenv("GITHUB_TOKEN", "offline-token")
    monkeypatch.setenv("EMAIL_MODE", mode)
    monkeypatch.setenv("SEND_EMAIL", "1")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("REPORT_KEY", key)
    monkeypatch.setenv("REPORT_MODE", "daily")
    p = ep.make_plan(key, created_at=day+"T12:00:00Z", job_started_at=day+"T12:01:00Z",
                     trigger="schedule", request=request, mode=mode)
    path = tmp_path / "plan.json"
    atomic_store(path, p, kind="plan", validate=ep.validate_plan)
    monkeypatch.setenv("NEWSAGENT_PLAN_PATH", str(path))
    return p


def legacy_run(run_id):
    return {"id": int(run_id), "head_sha": lt.REVIEWED[run_id][2], "conclusion": "failure"}


@pytest.mark.parametrize("run_id", list(lt.REVIEWED))
def test_exact_five_reviewed_runs_reconcile_without_receipt_invention(run_id, monkeypatch, tmp_path):
    key, wf = lt.REVIEWED[run_id][:2]
    plan(monkeypatch, tmp_path, key=key)
    before = {p: p.read_bytes() for p in tmp_path.glob("*.json")}
    result = lt.reviewed_failure(legacy_run(run_id), key, wf, {"editions": {}})
    assert result["finding"] == lt.REVIEWED[run_id][4]
    assert result["log_archive_sha256"] == lt.REVIEWED[run_id][3]
    assert before == {p: p.read_bytes() for p in tmp_path.glob("*.json")}
    assert not Path(os.environ["DELIVERY_JOURNAL_PATH"]).exists()


@pytest.mark.parametrize("change", ["head", "run", "newsletter", "workflow", "repository", "branch", "modern_receipt"])
def test_exception_cannot_match_foreign_or_new_failure(change, monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path)
    rid = "33186081165"; run = legacy_run(rid); key = "cybermed"; wf = "cybermed.yml"; journal = {"editions": {}}
    if change == "head": run["head_sha"] = "0"*40
    if change == "run": run["id"] += 1
    if change == "newsletter": key = "cyberlurch"
    if change == "workflow": wf = "newsagent.yml"
    if change == "repository": monkeypatch.setenv("GITHUB_REPOSITORY", "another/repo")
    if change == "branch": monkeypatch.setenv("GITHUB_REF_NAME", "another-branch")
    if change == "modern_receipt": journal["editions"]["old"] = {"workflow_run_id": rid}
    assert lt.reviewed_failure(run, key, wf, journal) is None


def test_uncertain_run_requires_bound_fresh_production_plan(monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path, day="2026-10-14")
    run = legacy_run("33186081165")
    assert lt.reviewed_failure(run, "cybermed", "cybermed.yml", {"editions": {}}) is None
    monkeypatch.delenv("NEWSAGENT_PLAN_PATH")
    assert lt.reviewed_failure(run, "cybermed", "cybermed.yml", {"editions": {}}) is None


def recovery_api(monkeypatch, runs):
    requests = []
    class Session:
        def __init__(self): self.headers = {}
        def get(self, url, **kwargs):
            requests.append(url)
            if url.endswith("/runs"): data = {"workflow_runs": runs}
            elif url.endswith("/jobs"): data = {"jobs": [{"steps": [{"name": "Run Cybermed reports", "started_at": "now", "conclusion": "failure"}]}]}
            else: data = {"artifacts": []}
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: data)
    monkeypatch.setattr(wd.requests, "Session", Session)
    return requests


def test_recovery_retires_only_reviewed_runs_and_new_missing_artifact_still_fails(monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path)
    requests = recovery_api(monkeypatch, [legacy_run("33186081165"), legacy_run("33489323708"),
        {"id": 99999999999, "head_sha": "f"*40, "conclusion": "failure"}])
    with pytest.raises(RuntimeError, match="delivery_recovery_missing_for_run:99999999999"):
        wd.recover_failed_runs("cybermed", "cybermed.yml", report_dir=tmp_path / "out")
    proof = json.loads((tmp_path / "out/cybermed_legacy_plan_metadata.json").read_text())
    assert len(proof["reviewed_legacy_runs"]) == 2 and not proof["smtp_success_invented"]
    assert not any("33186081165/jobs" in s or "33489323708/jobs" in s for s in requests)
    assert not Path(os.environ["DELIVERY_JOURNAL_PATH"]).exists()


@pytest.mark.parametrize("cadence", ["weekly", "monthly"])
def test_historical_aggregate_hold_preserves_all_stores_and_verifies_per_run(cadence, monkeypatch, tmp_path):
    p = plan(monkeypatch, tmp_path)
    entry = next(e for e in p["entries"] if e["cadence"] == cadence)
    monkeypatch.setenv("REPORT_MODE", cadence)
    monkeypatch.setattr(main, "_main", lambda: pytest.fail("held report must not collect/generate"))
    monkeypatch.setattr(main, "send_markdown", lambda *a, **kw: pytest.fail("held report must not send"))
    paths = [Path(os.environ[v]) for v in ("STATE_PATH", "ROLLUPS_STATE_PATH", "CYBERMED_DAILY_DIGEST_STATE_PATH",
        "CYBERMED_WEEKLY_DIGEST_STATE_PATH", "DELIVERY_JOURNAL_PATH", "CYBERMED_PUBMED_INGESTION_PATH")]
    before = {str(p): p.read_bytes() if p.exists() else None for p in paths}
    main.main()
    result = json.loads((tmp_path / f"out/cybermed_{cadence}_delivery_result.json").read_text())
    assert result["kind"] == "reviewed_legacy_hold" and result["edition_id"] == entry["edition_id"]
    assert result["status"] == "intentionally_skipped" and result["historical_smtp_outcome"] == "unknown"
    assert result["content_version"] is None and result["message_id"] is None
    delivery.verify_reports(tmp_path / "out", "cybermed", [cadence], mode="real", send_required=True)
    assert before == {str(p): p.read_bytes() if p.exists() else None for p in paths}


@pytest.mark.parametrize("field,value", [("workflow_run_id", "123"), ("policy_sha256", "0"*64),
    ("period_start", "2026-01-01"), ("status", "accepted_by_smtp"), ("smtp_attempted", True)])
def test_forged_or_stale_hold_never_passes_verification(field, value, monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path)
    c = delivery.Coordinator("cybermed", "monthly", mode="real", period_dates=("2026-09-01", "2026-09-30"))
    assert lt.write_hold(c, ep.planned_entry("cybermed", "monthly"))
    path = tmp_path / "out/cybermed_monthly_delivery_result.json"
    result = json.loads(path.read_text()); result[field] = value; path.write_text(json.dumps(result))
    with pytest.raises(RuntimeError, match="legacy_hold_verification_failed"):
        delivery.verify_reports(tmp_path / "out", "cybermed", ["monthly"], mode="real", send_required=True)


@pytest.mark.parametrize("status", ["accepted_by_smtp", "partially_accepted", "outcome_unknown", "failed"])
def test_existing_receipt_always_outranks_hold(status, monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path)
    c = delivery.Coordinator("cybermed", "monthly", mode="real", period_dates=("2026-09-01", "2026-09-30"))
    c.journal["editions"][c.edition_id] = {"status": status}
    before = deepcopy(c.journal)
    assert lt.expected_hold(c, ep.planned_entry("cybermed", "monthly")) is None
    assert c.journal == before


def test_old_direct_smtp_cannot_bypass_hold_but_new_daily_can_send(monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path, request="daily")
    monkeypatch.delenv("NEWSAGENT_PLAN_PATH")
    old = delivery.Coordinator("cybermed", "daily", mode="real", period_dates=("2026-09-01", "2026-09-01"))
    with pytest.raises(RuntimeError, match="legacy_period_smtp_prohibited"):
        old.deliver("body", ["dummy.invalid"], lambda *a: pytest.fail("old SMTP"))
    assert not old.path.exists()
    fresh = delivery.Coordinator("cybermed", "daily", mode="real", period_dates=("2026-10-16", "2026-10-16"))
    monkeypatch.setenv("DELIVERY_RECIPIENT_HMAC_KEY", "offline-key"*8)
    attempts = []
    def transport(mid, recipients, inflight, outcome):
        attempts.append(mid); inflight(); outcome("accepted_by_smtp", recipients, [], "")
    result = fresh.deliver("facts", ["dummy.invalid"], transport)
    fresh.finalize([])
    fresh.deliver("facts", ["dummy.invalid"], transport)
    assert result.status == "accepted_by_smtp" and len(attempts) == 1


def test_new_week_coverage_error_is_not_hidden_by_legacy_transition(monkeypatch, tmp_path):
    p = plan(monkeypatch, tmp_path, day="2026-10-23")
    entry = next(e for e in p["entries"] if e["cadence"] == "weekly")
    assert lt.hold_reason("cybermed", "weekly", entry["period_start"], entry["period_end"], entry) is None
    with pytest.raises(RuntimeError, match="edition_weekly_daily_coverage_incomplete"):
        ep.check_ready(entry)


def test_transition_week_leaves_hold_when_all_required_dailies_exist(monkeypatch, tmp_path):
    p = plan(monkeypatch, tmp_path)
    entry = next(e for e in p["entries"] if e["cadence"] == "weekly")
    target = Path(os.environ["CYBERMED_DAILY_DIGEST_STATE_PATH"])
    target.write_text(json.dumps({"schema_version": 1, "digests": [
        {"run_date": d} for d in ep.required_daily_dates(entry["period_start"], entry["period_end"]) ]}))
    assert lt.hold_reason("cybermed", "weekly", entry["period_start"], entry["period_end"], entry) is None


def test_corrupt_store_is_not_downgraded_to_historical_hold(monkeypatch, tmp_path):
    from newsagent2.cybermed_digest_store import CybermedDigestStoreError
    plan(monkeypatch, tmp_path)
    monkeypatch.setenv("REPORT_MODE", "monthly")
    path = Path(os.environ["CYBERMED_WEEKLY_DIGEST_STATE_PATH"]); path.write_bytes(b'{broken')
    with pytest.raises(CybermedDigestStoreError, match="corrupted_existing_store"): main.main()
    assert path.read_bytes() == b'{broken'


@pytest.mark.parametrize("key", ["cybermed", "cyberlurch"])
@pytest.mark.parametrize("mode", ["none", "test"])
def test_preview_test_never_gets_production_hold_or_changes_production_paths(key, mode, monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path, key=key, mode=mode, request="monthly")
    c = delivery.Coordinator(key, "monthly", mode=mode, period_dates=("2026-09-01", "2026-09-30"))
    assert lt.expected_hold(c) is None
    assert not c.path.exists()
    assert not list((tmp_path / "out").glob("*delivery_result.json"))


def test_multi_cadence_hold_does_not_mask_another_missing_result(monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path)
    c = delivery.Coordinator("cybermed", "monthly", mode="real")
    lt.write_hold(c, ep.planned_entry("cybermed", "monthly"))
    with pytest.raises(RuntimeError, match="missing_planned_delivery_result:cybermed:weekly"):
        delivery.verify_reports(tmp_path / "out", "cybermed", ["monthly", "weekly"], mode="real", send_required=True)


@pytest.mark.parametrize("key", ["cybermed", "cyberlurch"])
def test_tomorrow_workflow_recovery_generation_receipts_and_verification(key, monkeypatch, tmp_path):
    p = plan(monkeypatch, tmp_path, key=key)
    runs = [legacy_run(i) for i, row in lt.REVIEWED.items() if row[0] == key]
    recovery_api(monkeypatch, runs)
    wd.recover_failed_runs(key, "cybermed.yml" if key == "cybermed" else "newsagent.yml", report_dir=tmp_path / "out")
    monkeypatch.setenv("DELIVERY_RECIPIENT_HMAC_KEY", "offline-key"*8)
    monkeypatch.setenv("RECIPIENTS_CONFIG_JSON", json.dumps({key: {
        cadence: ["offline@example.test"] for cadence in ("daily", "weekly", "monthly")}}))
    generated = []; submitted = []
    def generate():
        ctx = main.current(); ctx.finalization_policy = "no_markers"
        generated.append(ctx.coordinator.cadence)
        main._record_generation("Offline fixture", "Source-grounded facts")
        main._queue_report("Offline fixture", "Source-grounded facts")
    def send(subject, body, *, coordinator, finalization):
        def transport(mid, recipients, inflight, outcome):
            submitted.append((coordinator.cadence, mid)); inflight()
            outcome("accepted_by_smtp", recipients, [], "")
        return coordinator.deliver(body, ["offline@example.test"], transport, finalization=finalization)
    monkeypatch.setattr(main, "_main", generate); monkeypatch.setattr(main, "send_markdown", send)
    modes = [entry["cadence"] for entry in p["entries"]]
    for cadence in modes:
        monkeypatch.setenv("REPORT_MODE", cadence); main.main()
    delivery.verify_reports(tmp_path / "out", key, modes, mode="real", send_required=True)
    expected = ["daily"] if key == "cybermed" else ["daily", "weekly"]
    assert generated == expected and [c for c, _ in submitted] == expected
    journal = delivery.load_journal(Path(os.environ["DELIVERY_JOURNAL_PATH"]))
    assert len(journal["editions"]) == len(expected)
    assert all(row["status"] == "accepted_by_smtp" and row["state_persisted"] for row in journal["editions"].values())
    if key == "cybermed":
        assert not Path(os.environ["CYBERMED_WEEKLY_DIGEST_STATE_PATH"]).exists()
        assert not Path(os.environ["ROLLUPS_STATE_PATH"]).exists()


def test_ready_transition_week_still_fails_on_real_generation_error(monkeypatch, tmp_path):
    p = plan(monkeypatch, tmp_path)
    entry = next(e for e in p["entries"] if e["cadence"] == "weekly")
    Path(os.environ["CYBERMED_DAILY_DIGEST_STATE_PATH"]).write_text(json.dumps({"digests": [
        {"run_date": d} for d in ep.required_daily_dates(entry["period_start"], entry["period_end"]) ]}))
    monkeypatch.setenv("REPORT_MODE", "weekly")
    monkeypatch.setattr(main, "_main", lambda: (_ for _ in ()).throw(RuntimeError("offline generation failure")))
    with pytest.raises(RuntimeError, match="offline generation failure"):
        main.main()
    result = json.loads((tmp_path / "out/cybermed_weekly_delivery_result.json").read_text())
    assert result["status"] == "failed" and "kind" not in result
    with pytest.raises(RuntimeError, match="planned_delivery_not_finalized"):
        delivery.verify_reports(tmp_path / "out", "cybermed", ["weekly"], mode="real", send_required=True)


def test_future_month_and_year_do_not_acquire_coverage_exception(monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path, day="2026-11-06")
    assert lt.hold_reason("cybermed", "monthly", "2026-10-01", "2026-10-31") is None
    assert lt.hold_reason("cybermed", "yearly", "2026-01-01", "2026-12-31") is None


def test_staging_cannot_manufacture_historical_submission_permission(monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path, request="daily")
    monkeypatch.delenv("NEWSAGENT_PLAN_PATH")
    c = delivery.Coordinator("cybermed", "daily", mode="real", period_dates=("2026-09-01", "2026-09-01"))
    c.generated("untransmitted historical candidate")
    with pytest.raises(RuntimeError, match="legacy_period_smtp_prohibited"):
        c.deliver("untransmitted historical candidate", ["offline.invalid"], lambda *a: pytest.fail("SMTP"))


def test_preflight_failed_receipt_is_retained_without_licensing_old_send(monkeypatch, tmp_path):
    plan(monkeypatch, tmp_path)
    c = delivery.Coordinator("cybermed", "monthly", mode="real", period_dates=("2026-09-01", "2026-09-30"))
    c.failure("generation_or_finalization_failed")
    before = c.path.read_bytes()
    assert lt.write_hold(c, ep.planned_entry("cybermed", "monthly"))
    assert c.path.read_bytes() == before
    fresh = delivery.Coordinator("cybermed", "monthly", mode="real", period_dates=(c.start, c.end))
    with pytest.raises(RuntimeError, match="legacy_period_smtp_prohibited"):
        lt.guard_submission(fresh)
