"""Recovery must not promote test/foreign State or misclassify real failures."""
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

from newsagent2 import delivery, workflow_delivery as wd
from newsagent2.run_context import PATHS, path_for
from newsagent2.store_io import StoreError


def result(key="cybermed", purpose="production", run="71", body=None, start="2026-10-20", end=None, cadence="daily"):
    end = end or start
    edition = f"{key}:{cadence}:{start}:{end}:{purpose}"
    version = hashlib.sha256((body or "").encode()).hexdigest()
    mid = hashlib.sha256((edition + ":" + version).encode()).hexdigest()
    return delivery.DeliveryResult(key, cadence, start, end, edition, version, f"<{mid}@newsagent2.invalid>",
        status="failed", generation_validated=body is not None, workflow_run_id=run, smtp_phase="not_handed_off").public()


def packet(tmp_path, rows, bodies=None, legacy=None):
    directory = tmp_path / "source"; directory.mkdir(exist_ok=True)
    journal = {"version": 1, "editions": {r["edition_id"]: r for r in rows}}
    if legacy: journal["paper_legacy_unknown"] = {"version": 1, "reports": legacy}
    assert delivery.journal_valid(journal)
    (directory / "journal.json").write_text(json.dumps(journal))
    for row, body in bodies or []:
        name = hashlib.sha256((row["edition_id"] + ":" + row["content_version"]).encode()).hexdigest()+".md"
        (directory / name).write_text(body)
    return directory


def snapshot():
    return {name: path_for(name).read_bytes() if path_for(name).exists() else None for name in PATHS}


@pytest.mark.parametrize("purpose", ["none", "test"])
@pytest.mark.parametrize("existing", [False, True])
def test_nonproduction_only_import_is_byte_exact_noop(purpose, existing, tmp_path):
    directory = packet(tmp_path, [result(purpose=purpose)])
    target = path_for("DELIVERY_JOURNAL_PATH")
    if existing: target.write_text('{"version": 1, "editions": {}, "legacy": "preserve bytes"}\n')
    before = snapshot(); export = tmp_path / "export"
    assert wd.import_recovery(directory, recovery_dir=export) is False
    assert snapshot() == before and not export.exists()


@pytest.mark.parametrize("purpose", ["none", "test"])
def test_source_test_run_cannot_reimport_inherited_production_rows_or_deltas(purpose, tmp_path):
    own = result(purpose=purpose)
    inherited = result(run="19", body="prior production")
    directory = packet(tmp_path, [own, inherited], [(inherited, "prior production")])
    (directory / "cybermed_daily_generated_delta.json").write_text(json.dumps({"production_target": True}))
    (directory / "cyberlurch_candidates.json").write_bytes(b'{broken')
    before = snapshot()
    assert wd.import_recovery(directory, report_key="cybermed", source_run_id="71", recovery_dir=tmp_path/"export") is False
    assert snapshot() == before and not (tmp_path/"export").exists()


def test_import_and_export_stay_within_newsletter_and_production_scope(tmp_path):
    med = result(run="70", body="medical facts")
    cybl = result(key="cyberlurch", body="video facts")
    test = result(key="cyberlurch", purpose="test", run="69", body="test content")
    directory = packet(tmp_path, [med, cybl, test], [(med,"medical facts"),(cybl,"video facts"),(test,"test content")],
                       {"cybermed": ["123"], "cyberlurch": ["456"]})
    (directory / "cybermed_daily_generated_delta.json").write_text(json.dumps({
        "report_key":"cybermed", "production_target":True, "generation_validated":True, "records":[]}))
    export = tmp_path / "export"
    assert wd.import_recovery(directory, report_key="cyberlurch", source_run_id="71", recovery_dir=export)
    restored = delivery.load_journal(path_for("DELIVERY_JOURNAL_PATH"))
    assert set(restored["editions"]) == {cybl["edition_id"]}
    assert restored["paper_legacy_unknown"]["reports"] == {"cyberlurch": ["456"]}
    expected = hashlib.sha256((cybl["edition_id"]+":"+cybl["content_version"]).encode()).hexdigest()+".md"
    assert [p.name for p in export.glob("*.md")] == [expected]
    assert not (export / "cybermed_daily_generated_delta.json").exists()


@pytest.mark.parametrize("kind", ["empty", "foreign", "unrelated_test", "mixed"])
def test_missing_or_contradictory_source_scope_fails_before_state_write(kind, tmp_path):
    rows = {"empty": [], "foreign": [result(key="cyberlurch")],
        "unrelated_test": [result(purpose="test", run="72")],
        "mixed": [result(purpose="test"), result()]}[kind]
    directory = packet(tmp_path, rows); before = snapshot()
    with pytest.raises(StoreError, match="production_scope_missing|mixed_source_run_modes"):
        wd.import_recovery(directory, report_key="cybermed", source_run_id="71")
    assert snapshot() == before


def job(key="cybermed", mode="test"):
    return {"steps": [
        {"name":f"NewsAgent2 recovery v1 / {key} / {mode}", "number":2, "started_at":"2026-10-15T12:00:00Z", "conclusion":"success"},
        {"name":"Run Cybermed reports" if key=="cybermed" else "Run reports", "number":8,
         "started_at":"2026-10-15T12:01:00Z", "conclusion":"failure"}]}


@pytest.mark.parametrize("key", ["cybermed", "cyberlurch"])
@pytest.mark.parametrize("mode", ["none", "test"])
def test_completed_early_mode_step_is_positive_nonproduction_proof(key, mode):
    assert wd._nonproduction_jobs({"event":"workflow_dispatch"}, [job(key, mode)], key)


@pytest.mark.parametrize("change", ["real", "missing", "failed", "skipped", "late", "no_start", "no_number",
    "foreign", "duplicate", "second_real_job", "schedule", "free_text_title", "maintenance"])
def test_ambiguous_or_production_metadata_cannot_bypass_recovery(change):
    run = {"event":"workflow_dispatch"}; jobs = [job()]; marker = jobs[0]["steps"][0]
    if change == "real": jobs = [job(mode="real")]
    if change == "missing": jobs[0]["steps"].pop(0)
    if change in {"failed", "skipped"}: marker["conclusion"] = change
    if change == "late": marker["number"] = 9
    if change == "no_start": marker["started_at"] = None
    if change == "no_number": marker.pop("number")
    if change == "foreign": marker["name"] = "NewsAgent2 recovery v1 / cyberlurch / test"
    if change == "duplicate": jobs[0]["steps"].append(deepcopy(marker))
    if change == "second_real_job": jobs.append(job(mode="real"))
    if change == "schedule": run["event"] = "schedule"
    if change == "free_text_title":
        run["display_title"] = marker["name"]; jobs[0]["steps"].pop(0)
    if change == "maintenance": marker["name"] = "NewsAgent2 recovery v1 / cybermed / maintenance:2026-09"
    assert not wd._nonproduction_jobs(run, jobs, "cybermed")


def api(monkeypatch, run, jobs, archive=None, key="cybermed"):
    calls=[]
    class Session:
        def __init__(self): self.headers={}
        def get(self,url,**kwargs):
            calls.append(url); data={}; content=b""
            if url.endswith("/runs"): data={"workflow_runs":[run]}
            elif url.endswith("/jobs"): data={"jobs":jobs}
            elif url.endswith("/artifacts"):
                data={"artifacts":[] if archive is None else [{"id":7,"name":f"{key}-delivery-recovery-71","expired":False}]}
            elif url.endswith("/zip"): content=archive
            return SimpleNamespace(raise_for_status=lambda:None,json=lambda:data,content=content)
    monkeypatch.setattr(wd.requests,"Session",Session)
    for name,value in {"EMAIL_MODE":"real","GITHUB_TOKEN":"offline-token","GITHUB_REPOSITORY":"owner/repo",
        "GITHUB_REF_NAME":"main","GITHUB_RUN_ID":"99"}.items(): monkeypatch.setenv(name,value)
    return calls


@pytest.mark.parametrize("mode", ["none", "test"])
def test_failed_nonproduction_run_without_artifact_never_blocks_or_writes_state(mode, monkeypatch, tmp_path):
    calls=api(monkeypatch,{"id":71,"event":"workflow_dispatch","conclusion":"failure"},[job(mode=mode)])
    before=snapshot()
    wd.recover_failed_runs("cybermed","cybermed.yml",report_dir=tmp_path/"out")
    assert not any(url.endswith("/artifacts") for url in calls)
    assert snapshot()==before


def test_real_mode_still_requires_recovery_artifact(monkeypatch,tmp_path):
    api(monkeypatch,{"id":71,"event":"workflow_dispatch","conclusion":"failure"},[job(mode="real")])
    with pytest.raises(RuntimeError,match="delivery_recovery_missing_for_run:71"):
        wd.recover_failed_runs("cybermed","cybermed.yml",report_dir=tmp_path/"out")


def test_real_scope_cannot_be_relabelled_by_test_only_artifact(monkeypatch,tmp_path):
    directory=packet(tmp_path,[result(purpose="test")])
    api(monkeypatch,{"id":71,"event":"workflow_dispatch","conclusion":"failure"},
        [job(mode="real")],zipped(directory))
    before=snapshot()
    with pytest.raises(StoreError,match="source_mode_conflict"):
        wd.recover_failed_runs("cybermed","cybermed.yml",report_dir=tmp_path/"out")
    assert snapshot()==before


def zipped(directory):
    buffer=io.BytesIO()
    with zipfile.ZipFile(buffer,"w") as z:
        for p in directory.iterdir(): z.writestr(p.name,p.read_bytes())
    return buffer.getvalue()


def delta(directory, *, key="cyberlurch", kind="rollups", month="2026-09", removed=None):
    target=path_for("ROLLUPS_STATE_PATH")
    data={"kind":kind,"report_key":key,"generation_validated":True,"production_target":True,
        "records":[{"month":month,"summary":"source-based summary"}],"removed_record_ids":removed or [],
        "base_sha256":hashlib.sha256(target.read_bytes()).hexdigest() if target.exists() else None}
    (directory/f"{key}_monthly_{kind}_generated_delta.json").write_text(json.dumps(data))


def test_authorized_monthly_maintenance_only_restores_its_rollup(monkeypatch,tmp_path):
    target=path_for("ROLLUPS_STATE_PATH")
    target.write_text(json.dumps({"version":1,"reports":{"cyberlurch":[],"cybermed":[{"month":"2026-09","summary":"preserve"}]}}))
    own=result(key="cyberlurch",purpose="none",start="2026-09-01",end="2026-09-30",cadence="monthly")
    directory=packet(tmp_path,[own]);delta(directory)
    api(monkeypatch,{"id":71,"event":"workflow_dispatch","conclusion":"failure"},
        [job("cyberlurch","maintenance:2026-09")],zipped(directory),key="cyberlurch")
    wd.recover_failed_runs("cyberlurch","newsagent.yml",report_dir=tmp_path/"out")
    after=json.loads(target.read_text())
    assert after["reports"]["cybermed"]==[{"month":"2026-09","summary":"preserve"}]
    assert after["reports"]["cyberlurch"]==[{"month":"2026-09","summary":"source-based summary"}]
    assert not path_for("DELIVERY_JOURNAL_PATH").exists()
    wd.recover_failed_runs("cyberlurch","newsagent.yml",report_dir=tmp_path/"out")
    assert json.loads(target.read_text())==after


@pytest.mark.parametrize("unsafe", ["month", "newsletter", "kind", "prune", "unvalidated"])
def test_maintenance_scope_is_checked_before_any_restore(unsafe,tmp_path):
    target=path_for("ROLLUPS_STATE_PATH");target.write_text('{"version":1,"reports":{}}\n')
    directory=packet(tmp_path,[]);delta(directory)
    kwargs={"month":"2026-08"} if unsafe=="month" else {"key":"cybermed"} if unsafe=="newsletter" else {
        "kind":"cyberlurch_digests"} if unsafe=="kind" else {"removed":["2026-08"]}
    if unsafe=="unvalidated":
        p=next(directory.glob("*_generated_delta.json"));d=json.loads(p.read_text());d["generation_validated"]=False;p.write_text(json.dumps(d))
    else: delta(directory,**kwargs)
    before=snapshot()
    with pytest.raises(StoreError,match="maintenance_scope_mismatch"):
        wd.restore_generated_records(directory,"cyberlurch",maintenance_month="2026-09")
    assert snapshot()==before


def test_old_nonproduction_artifact_does_not_restore_injected_production_cache(monkeypatch,tmp_path):
    directory=packet(tmp_path,[result(purpose="test"),result(run="18")])
    delta(directory,key="cybermed",kind="cybermed_daily")
    calls=api(monkeypatch,{"id":71,"event":"workflow_dispatch","conclusion":"failure"},
        [{"steps":[job()["steps"][1]]}],zipped(directory))
    before=snapshot()
    wd.recover_failed_runs("cybermed","cybermed.yml",report_dir=tmp_path/"out")
    assert snapshot()==before and any(url.endswith("/zip") for url in calls)


def test_workflow_mode_proof_is_derived_from_the_same_effective_email_mode():
    import yaml
    root=Path(__file__).resolve().parents[1]
    for filename,key in [("cybermed.yml","cybermed"),("newsagent.yml","cyberlurch")]:
        data=yaml.load((root/".github/workflows"/filename).read_text(),Loader=yaml.BaseLoader)
        j=next(iter(data["jobs"].values()))
        modes=[s for s in j["steps"] if s.get("name","").startswith(f"NewsAgent2 recovery v1 / {key} / ")]
        assert len(modes)==1 and "env.EMAIL_MODE" in modes[0]["name"]
        assert 'case "$EMAIL_MODE"' in modes[0]["run"]
        assert j["env"]["EMAIL_MODE"].endswith("|| 'real' }}")
        if key=="cyberlurch": assert "persist_monthly_rollup" in modes[0]["name"]
