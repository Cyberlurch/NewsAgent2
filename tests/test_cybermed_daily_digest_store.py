import json
import sys
from datetime import datetime, timezone

from newsagent2 import main


def _base_env(monkeypatch, tmp_path):
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("CYBERMED_DAILY_DIGEST_STATE_PATH", str(tmp_path / "state" / "cybermed_daily_digests.json"))
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])

    monkeypatch.setattr(main, "load_channels_config", lambda _p: ([{"name": "PubMed: Test", "source": "pubmed", "query": "test"}], {}, {}))
    monkeypatch.setattr(main, "search_recent_pubmed", lambda *a, **k: ([], {}) if k.get("return_metadata") else [])
    monkeypatch.setattr(main, "load_foamed_sources_config", lambda _p: [{"name": "Src", "rss_url": "https://example.com/rss"}])
    monkeypatch.setattr(main, "collect_foamed_items", lambda *a, **k: ([], {"sources_total": 1, "sources_ok": 1, "sources_failed": 0, "items_raw": 0, "items_with_date": 0, "items_date_unknown": 0, "kept_last24h": 0, "newly_disabled_count": 0, "per_source": {}}))


def _with_nonempty_selection(monkeypatch):
    import types
    from newsagent2 import selector_medical
    monkeypatch.setattr(main, "search_recent_pubmed", lambda *a, **k: ([{"id": "p1", "pmid": "1", "title": "T", "journal": "J", "url": "u"}], {}) if k.get("return_metadata") else [{"id": "p1", "pmid": "1", "title": "T", "journal": "J", "url": "u"}])
    monkeypatch.setattr(main, "collect_foamed_items", lambda *a, **k: ([{"id": "f1", "title": "F", "url": "fu"}], {"sources_total": 1, "sources_ok": 1, "sources_failed": 0, "items_raw": 1, "items_with_date": 1, "items_date_unknown": 0, "kept_last24h": 1, "newly_disabled_count": 0, "per_source": {}}))
    monkeypatch.setattr(selector_medical, "select_cybermed_pubmed_items", lambda _items: types.SimpleNamespace(overview_items=[{"id": "p1", "pmid": "1", "title": "T", "journal": "J", "url": "u", "top_pick": True}], deep_dive_items=[{"id": "p1", "pmid": "1", "title": "T", "journal": "J", "url": "u"}], stats={"selection_diagnostics": {}}))
    monkeypatch.setattr(selector_medical, "select_cybermed_foamed_items", lambda items, max_items=25: [{"id": "f1", "title": "F", "url": "fu", "top_pick": True}])


def test_cybermed_daily_digest_store_created_and_deterministic_id(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)

    main.main()

    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    assert dpath.exists()
    payload = json.loads(dpath.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert len(payload["digests"]) == 1
    digest = payload["digests"][0]
    expected_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert digest["digest_id"] == f"cybermed_daily_{expected_date}"
    assert digest["report_key"] == "cybermed"
    assert digest["items"]["pubmed"] == []
    assert digest["items"]["foamed"] == []

    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is True
    assert diag["cybermed_digest_store_write_verified"] is True
    assert diag["cybermed_digest_store_expected_digest_present"] is True
    assert diag["cybermed_digest_store_digest_count_after_write"] == 1
    assert diag["cybermed_digest_store_items_pubmed_total"] == 0
    assert diag["cybermed_digest_store_items_foamed_total"] == 0


def test_cybermed_daily_digest_store_skips_duplicate_without_overwrite(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    _with_nonempty_selection(monkeypatch)
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dpath.write_text(json.dumps({"schema_version": 1, "digests": [{"digest_id": f"cybermed_daily_{date}", "report_key": "cybermed", "cadence": "daily", "run_date": date}]}), encoding="utf-8")

    main.main()

    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is False
    assert diag["cybermed_digest_store_skipped_reason"] == "digest_already_exists"
    assert diag["cybermed_digest_store_expected_digest_present"] is True
    assert diag["cybermed_digest_store_write_verified"] is False
    assert diag["cybermed_digest_store_digest_count_after_write"] == 1
    assert diag["cybermed_digest_store_existing_digest_empty"] is True
    assert diag["cybermed_digest_store_current_digest_nonempty"] is True
    assert diag["cybermed_digest_store_existing_pubmed_total"] == 0
    assert diag["cybermed_digest_store_existing_foamed_total"] == 0
    assert diag["cybermed_digest_store_current_pubmed_total"] > 0
    assert diag["cybermed_digest_store_current_digest_nonempty"] is True


def test_cybermed_daily_digest_store_skips_qa_replay_by_default(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    monkeypatch.setenv("CYBERMED_QA_REPLAY_MODE", "1")

    main.main()

    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is False
    assert diag["cybermed_digest_store_skipped_reason"] == "qa_replay_mode"


def test_cybermed_daily_digest_store_overwrite_replaces_existing(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    _with_nonempty_selection(monkeypatch)
    monkeypatch.setenv("CYBERMED_DIGEST_STORE_OVERWRITE", "1")
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dpath.write_text(json.dumps({"schema_version": 1, "digests": [{"digest_id": f"cybermed_daily_{date}", "report_key": "cybermed", "cadence": "daily", "run_date": date, "items": {"pubmed": [{"id": "old"}], "foamed": []}}]}), encoding="utf-8")

    main.main()

    payload = json.loads(dpath.read_text(encoding="utf-8"))
    assert len(payload["digests"]) == 1
    assert len(payload["digests"][0]["items"]["pubmed"]) == 1
    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is True
    assert diag["cybermed_digest_store_skipped_reason"] == ""


def test_cybermed_daily_digest_store_overwrite_not_allowed_for_scheduled_real_email(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    _with_nonempty_selection(monkeypatch)
    monkeypatch.setenv("CYBERMED_DIGEST_STORE_OVERWRITE", "1")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("SEND_EMAIL", "0")
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dpath.write_text(json.dumps({"schema_version": 1, "digests": [{"digest_id": f"cybermed_daily_{date}", "items": {"pubmed": [{"id": "old"}], "foamed": []}, "deep_dives": [], "top_picks": []}]}), encoding="utf-8")
    main.main()
    payload = json.loads(dpath.read_text(encoding="utf-8"))
    assert payload["digests"][0]["items"]["pubmed"] == [{"id": "old"}]


def test_cybermed_daily_digest_store_replace_empty_flag_safely_replaces(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    _with_nonempty_selection(monkeypatch)
    monkeypatch.setenv("CYBERMED_DIGEST_STORE_REPLACE_EMPTY", "1")
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dpath.write_text(json.dumps({"schema_version": 1, "digests": [{"digest_id": f"cybermed_daily_{date}", "items": {"pubmed": [], "foamed": []}, "deep_dives": [], "top_picks": []}]}), encoding="utf-8")
    main.main()
    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_replace_empty_requested"] is True
    assert diag["cybermed_digest_store_replace_empty_allowed"] is True
    assert diag["cybermed_digest_store_written"] is True


def test_cybermed_daily_digest_store_replace_empty_does_not_replace_nonempty_existing(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    _with_nonempty_selection(monkeypatch)
    monkeypatch.setenv("CYBERMED_DIGEST_STORE_REPLACE_EMPTY", "1")
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dpath.write_text(json.dumps({"schema_version": 1, "digests": [{"digest_id": f"cybermed_daily_{date}", "items": {"pubmed": [{"id": "old"}], "foamed": []}, "deep_dives": [], "top_picks": []}]}), encoding="utf-8")
    main.main()
    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is False
    assert diag["cybermed_digest_store_replace_empty_allowed"] is False
    assert diag["cybermed_digest_store_skipped_reason"] == "digest_already_exists"


def test_cybermed_daily_digest_store_replace_empty_not_allowed_for_schedule(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    _with_nonempty_selection(monkeypatch)
    monkeypatch.setenv("CYBERMED_DIGEST_STORE_REPLACE_EMPTY", "1")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dpath.write_text(json.dumps({"schema_version": 1, "digests": [{"digest_id": f"cybermed_daily_{date}", "items": {"pubmed": [], "foamed": []}, "deep_dives": [], "top_picks": []}]}), encoding="utf-8")
    main.main()
    payload = json.loads(dpath.read_text(encoding="utf-8"))
    assert payload["digests"][0]["items"]["pubmed"] == []


def test_cybermed_daily_digest_store_write_failure_sets_verification_failed(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    dpath.write_text(json.dumps({"schema_version": 1, "digests": []}), encoding="utf-8")

    import builtins
    real_open = builtins.open

    writes = {"count": 0}
    def _failing_open(path, *args, **kwargs):
        if str(path) == str(dpath) and "w" in (args[0] if args else kwargs.get("mode", "r")):
            writes["count"] += 1
            if writes["count"] >= 1:
                raise OSError("forced failure")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _failing_open)
    main.main()

    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is False
    assert diag["cybermed_digest_store_skipped_reason"] == "write_verification_failed"
    assert diag["cybermed_digest_store_write_error_class"] != ""
