"""Exercise separate Actions steps with the actual job's newsletter scope.

Do not inherit the report process's inline REPORT_KEY as older sender fixtures
did: recovery and publication run in different shells on GitHub Actions.
"""
from copy import deepcopy
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import shlex
import sys
from types import SimpleNamespace
import zipfile

import pytest
from _github_fakes import response as github_response, collection as github_collection
import yaml

from newsagent2 import delivery, pubmed_ingestion as ingestion, workflow_delivery as wd
from newsagent2.run_context import path_for

HMAC_KEY = "offline-only-recipient-key-32-bytes-long"
RECIPIENT = "operator@example.test"


def workflow(key, monkeypatch):
    filename = "cybermed.yml" if key == "cybermed" else "newsagent.yml"
    data = yaml.safe_load((Path(".github/workflows") / filename).read_text())
    job = next(iter(data["jobs"].values()))
    monkeypatch.delenv("REPORT_KEY", raising=False)
    if "REPORT_KEY" in job["env"]:
        monkeypatch.setenv("REPORT_KEY", job["env"]["REPORT_KEY"])
    monkeypatch.setenv("EMAIL_MODE", "real")
    monkeypatch.setenv("GITHUB_REF_NAME", "main")
    monkeypatch.setenv("GITHUB_REPOSITORY", "offline/repo")
    return filename, job


def command(job, action):
    for step in job["steps"]:
        for line in step.get("run", "").splitlines():
            if not line.strip().startswith(f"python -m src.newsagent2.workflow_delivery {action} "):
                continue
            args = shlex.split(line.strip())
            prefix = ["python", "-m", "src.newsagent2.workflow_delivery", action]
            if args[:4] == prefix and "--backfill-apply" not in args:
                return args[2:]
    pytest.fail("Missing workflow command: " + action)


def git_mock(monkeypatch, *, fail_push=False):
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        action = args[1]
        output = " M state" if action == "status" else "a" * 40 if action == "rev-parse" else ""
        code = int(fail_push and action in {"push", "ls-remote"})
        return SimpleNamespace(returncode=code, stdout=output, stderr="")
    monkeypatch.setattr(wd.subprocess, "run", run)
    return calls


def receipt(key):
    edition = f"{key}:daily:2026-10-16:2026-10-16:production"
    body = "Validated saved report."
    version = hashlib.sha256(body.encode()).hexdigest()
    mid = hashlib.sha256((edition + ":" + version).encode()).hexdigest()
    result = delivery.DeliveryResult(key, "daily", "2026-10-16", "2026-10-16", edition,
        version, f"<{mid}@newsagent2.invalid>", status="accepted_by_smtp",
        accepted=[hmac.new(HMAC_KEY.encode(), RECIPIENT.encode(), hashlib.sha256).hexdigest()],
        workflow_run_id="71", smtp_phase="completed", state_persisted=True)
    return result, body


@pytest.mark.parametrize("key", ["cybermed", "cyberlurch"])
def test_actual_publication_step_keeps_its_newsletter_scope(key, monkeypatch, tmp_path):
    _, job = workflow(key, monkeypatch)
    args = command(job, "publish")
    monkeypatch.chdir(tmp_path)
    Path("state").mkdir()
    journal = {"version": 1, "editions": {}}
    stores = {"processed_items.json": {"reports": {}}, "delivery_journal.json": journal}
    if key == "cybermed":
        stores[ingestion.RECOVERY] = deepcopy(ingestion.EMPTY)
    for name, data in stores.items():
        (Path("state") / name).write_text(json.dumps(data))
    calls = git_mock(monkeypatch)
    monkeypatch.setattr(sys, "argv", args)
    wd.main()
    assert os.environ.get("REPORT_KEY") == key
    assert ["git", "push", "origin", "HEAD:main"] in calls
    result = json.loads((Path(os.environ["REPORT_DIR"]) / "recovery/git_persistence.json").read_text())
    assert result["category"] == "git_persisted"


@pytest.mark.parametrize("key", ["cybermed", "cyberlurch"])
def test_actual_recovery_step_restores_acceptance_without_another_sender(key, monkeypatch, tmp_path):
    filename, job = workflow(key, monkeypatch)
    row, body = receipt(key)
    journal = {"version": 1, "editions": {row.edition_id: row.public()}}
    body_name = hashlib.sha256((row.edition_id + ":" + row.content_version).encode()).hexdigest() + ".md"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("journal.json", json.dumps(journal))
        archive.writestr(body_name, body)
        if key == "cybermed":
            data = {**ingestion.EMPTY, "rotation": 1}
            archive.writestr(ingestion.RECOVERY, json.dumps({"kind": ingestion.KIND, "mode": "real",
                "base_sha256": None, "data": data, "sha256": ingestion._seal(data)}))
    monkeypatch.setenv("GITHUB_TOKEN", "offline-token")
    monkeypatch.setenv("GITHUB_RUN_ID", "99")
    monkeypatch.setenv("DELIVERY_RECIPIENT_HMAC_KEY", HMAC_KEY)
    calls = []
    class Session:
        def __init__(self): self.headers = {}
        def get(self, url, **kwargs):
            calls.append(url)
            data = {}
            if url.endswith("/runs"):
                data = {"workflow_runs": [{"id": 71, "event": "schedule", "conclusion": "failure"}]}
            elif url.endswith("/jobs"):
                data = {"jobs": [{"steps": [{"name": "Run Cybermed reports" if key == "cybermed" else "Run reports",
                    "started_at": "2026-10-16T08:30:00Z", "conclusion": "success"}]}]}
            elif url.endswith("/artifacts"):
                data = {"artifacts": [{"id": 7, "name": f"{key}-delivery-recovery-71", "expired": False}]}
            return github_response(github_collection(data), content=buffer.getvalue() if url.endswith('/zip') else None)
    monkeypatch.setattr(wd.requests, "Session", Session)
    monkeypatch.setattr(sys, "argv", command(job, "recover"))
    # Separate invocations must both succeed. The second import is a no-op.
    for _ in range(2):
        wd.main()
        wd.finalize_pending_receipts(key)
    restored = delivery.load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"][row.edition_id]
    assert restored["status"] == "accepted_by_smtp" and restored["state_persisted"]
    assert restored["message_id"] == row.message_id and restored["content_version"] == row.content_version
    assert os.environ.get("REPORT_KEY") == key
    if key == "cybermed":
        assert ingestion.load()["rotation"] == 1
        assert len(ingestion.load()["recovered"]) == 1
    assert any(f"/workflows/{filename}/runs" in url for url in calls)
    co = delivery.Coordinator(key, "daily", mode="real", period_dates=(row.period_start, row.period_end))
    def forbidden_transport(*args, **kwargs):
        pytest.fail("Recovered acceptance must never submit SMTP again")
    result = co.deliver(body, [RECIPIENT], forbidden_transport)
    assert result.status == "accepted_by_smtp"


def test_foreign_scope_still_cannot_publish_cybermed_ingestion(monkeypatch, tmp_path):
    workflow("cyberlurch", monkeypatch)
    target = path_for("CYBERMED_PUBMED_INGESTION_PATH")
    target.write_text(json.dumps(ingestion.EMPTY))
    before = target.read_bytes()
    calls = git_mock(monkeypatch)
    with pytest.raises(RuntimeError, match="publication_newsletter_scope_mismatch"):
        wd.publish_state([str(target)], report_dir=tmp_path / "out")
    assert target.read_bytes() == before and not calls
