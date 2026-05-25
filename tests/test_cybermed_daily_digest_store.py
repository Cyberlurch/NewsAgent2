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


def test_cybermed_daily_digest_store_skips_duplicate_without_overwrite(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    dpath = tmp_path / "state" / "cybermed_daily_digests.json"
    dpath.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dpath.write_text(json.dumps({"schema_version": 1, "digests": [{"digest_id": f"cybermed_daily_{date}", "report_key": "cybermed", "cadence": "daily", "run_date": date}]}), encoding="utf-8")

    main.main()

    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is False
    assert diag["cybermed_digest_store_skipped_reason"] == "digest_already_exists"


def test_cybermed_daily_digest_store_skips_qa_replay_by_default(tmp_path, monkeypatch):
    _base_env(monkeypatch, tmp_path)
    monkeypatch.setenv("CYBERMED_QA_REPLAY_MODE", "1")

    main.main()

    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_digest_store_written"] is False
    assert diag["cybermed_digest_store_skipped_reason"] == "qa_replay_mode"
