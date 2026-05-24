import pathlib
import sys
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from newsagent2 import main
from newsagent2 import selector_medical


def _fake_channels(_path):
    return ([{"name": "PubMed: Test", "topic": "Critical Care", "source": "pubmed", "query": "test[Title]"}], {}, {})


def _fake_pubmed_factory(seen):
    def _fake_pubmed(*args, **kwargs):
        seen.append(kwargs.get("max_items"))
        rows = [{"id": "123", "title": "A paper", "url": "https://pubmed.ncbi.nlm.nih.gov/123/", "published_at": datetime.now(timezone.utc)}]
        if kwargs.get("return_metadata"):
            return rows, {"retmax": kwargs.get("max_items", 0), "esearch_count_total": 1, "idlist_count": 1, "parsed_article_count": 1, "possibly_truncated": False}
        return rows

    return _fake_pubmed


def _configure_common(monkeypatch, tmp_path, report_key="cybermed"):
    monkeypatch.setenv("REPORT_KEY", report_key)
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])
    monkeypatch.setattr(main, "load_channels_config", _fake_channels)
    monkeypatch.setattr(main, "collect_foamed_items", lambda *a, **k: ([], {"sources_total": 0, "sources_ok": 0, "sources_failed": 0, "items_raw": 0, "items_with_date": 0, "items_date_unknown": 0, "kept_last24h": 0, "newly_disabled_count": 0, "per_source": {}, "audit": {"enabled": False, "sources": {}}}))


def test_cybermed_pubmed_default_max_items_is_25(monkeypatch, tmp_path):
    _configure_common(monkeypatch, tmp_path, report_key="cybermed")
    monkeypatch.delenv("CYBERMED_MAX_ITEMS_PER_CHANNEL", raising=False)
    monkeypatch.setenv("MAX_ITEMS_PER_CHANNEL", "5")
    seen = []
    monkeypatch.setattr(main, "search_recent_pubmed", _fake_pubmed_factory(seen))

    main.main()

    assert seen and seen[0] == 25


def test_cybermed_pubmed_uses_override(monkeypatch, tmp_path):
    _configure_common(monkeypatch, tmp_path, report_key="cybermed")
    monkeypatch.setenv("CYBERMED_MAX_ITEMS_PER_CHANNEL", "33")
    monkeypatch.setenv("MAX_ITEMS_PER_CHANNEL", "5")
    seen = []
    monkeypatch.setattr(main, "search_recent_pubmed", _fake_pubmed_factory(seen))

    main.main()

    assert seen and seen[0] == 33


def test_general_report_uses_max_items_per_channel_unchanged(monkeypatch, tmp_path):
    _configure_common(monkeypatch, tmp_path, report_key="cyberlurch")
    monkeypatch.setenv("MAX_ITEMS_PER_CHANNEL", "7")
    monkeypatch.setenv("CYBERMED_MAX_ITEMS_PER_CHANNEL", "33")
    seen = []
    monkeypatch.setattr(main, "search_recent_pubmed", _fake_pubmed_factory(seen))

    main.main()

    assert seen and seen[0] == 7


def test_cybermed_config_logging_and_no_recipient_or_secret_dump(monkeypatch, tmp_path, capsys):
    _configure_common(monkeypatch, tmp_path, report_key="cybermed")
    monkeypatch.setenv("CYBERMED_MAX_ITEMS_PER_CHANNEL", "29")
    monkeypatch.setenv("FOAMED_AUDIT", "1")
    monkeypatch.setenv("FOAMED_AUDIT_CHECK_DISABLED", "1")
    monkeypatch.setenv("FOAMED_ARTICLE_FETCH", "1")
    monkeypatch.setenv("FOAMED_ARTICLE_FETCH_MAX_PER_RUN", "17")
    monkeypatch.setenv("FOAMED_RENDER_FALLBACK", "1")
    monkeypatch.setenv("RECIPIENTS_JSON_CYBERMED", '["doctor@example.com"]')
    monkeypatch.setenv("SMTP_PASS", "super-secret")
    seen = []
    monkeypatch.setattr(main, "search_recent_pubmed", _fake_pubmed_factory(seen))

    main.main()

    out = capsys.readouterr().out
    assert "CYBERMED_MAX_ITEMS_PER_CHANNEL=29" in out
    assert "FOAMED_AUDIT=True" in out
    assert "FOAMED_AUDIT_CHECK_DISABLED=True" in out
    assert "FOAMED_ARTICLE_FETCH=True" in out
    assert "FOAMED_ARTICLE_FETCH_MAX_PER_RUN=17" in out
    assert "FOAMED_RENDER_FALLBACK=True" in out
    assert "doctor@example.com" not in out
    assert "super-secret" not in out


def test_workflow_exposes_cybermed_intake_audit_env_vars():
    text = pathlib.Path(".github/workflows/newsagent.yml").read_text(encoding="utf-8")
    assert "CYBERMED_MAX_ITEMS_PER_CHANNEL: ${{ vars.CYBERMED_MAX_ITEMS_PER_CHANNEL || '25' }}" in text
    assert "FOAMED_AUDIT: ${{ vars.FOAMED_AUDIT || '0' }}" in text
    assert "FOAMED_AUDIT_CHECK_DISABLED: ${{ vars.FOAMED_AUDIT_CHECK_DISABLED || '0' }}" in text
    assert "FOAMED_ARTICLE_FETCH: ${{ vars.FOAMED_ARTICLE_FETCH || '0' }}" in text
    assert "FOAMED_ARTICLE_FETCH_MAX_PER_RUN: ${{ vars.FOAMED_ARTICLE_FETCH_MAX_PER_RUN || '25' }}" in text
    assert "FOAMED_RENDER_FALLBACK: ${{ vars.FOAMED_RENDER_FALLBACK || '0' }}" in text
    assert "CYBERMED_QA_REPLAY_MODE: ${{ vars.CYBERMED_QA_REPLAY_MODE || '0' }}" in text


def test_cybermed_qa_replay_safety_and_state_behavior(monkeypatch, tmp_path):
    _configure_common(monkeypatch, tmp_path, report_key="cybermed")
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"cybermed": {"pubmed": {"123": {"processed_at_utc": "2026-01-01T00:00:00+00:00"}}}}), encoding="utf-8")
    monkeypatch.setenv("CYBERMED_QA_REPLAY_MODE", "1")
    monkeypatch.setattr(main, "search_recent_pubmed", _fake_pubmed_factory([]))
    monkeypatch.setattr(
        selector_medical,
        "select_cybermed_pubmed_items",
        lambda items: SimpleNamespace(overview_items=list(items), deep_dive_items=list(items), stats={"selection_diagnostics": {}}),
    )

    main.main()

    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_qa_replay_enabled"] is True
    assert diag["cybermed_qa_replay_safety_passed"] is True
    assert diag["cybermed_qa_replay_state_mutation_disabled"] is True
    assert diag["cybermed_qa_replay_email_disabled_confirmed"] is True
    assert diag["cybermed_qa_replay_state_bypass_pubmed_total"] >= 1
    after = json.loads(state_path.read_text(encoding="utf-8"))
    assert after == {"cybermed": {"pubmed": {"123": {"processed_at_utc": "2026-01-01T00:00:00+00:00"}}}}


def test_cybermed_qa_replay_ignored_when_send_email_enabled(monkeypatch, tmp_path):
    _configure_common(monkeypatch, tmp_path, report_key="cybermed")
    monkeypatch.setenv("SEND_EMAIL", "1")
    monkeypatch.setenv("CYBERMED_QA_REPLAY_MODE", "1")
    monkeypatch.setattr(main, "search_recent_pubmed", _fake_pubmed_factory([]))
    main.main()
    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["cybermed_qa_replay_enabled"] is False
    assert "send_email_not_zero" in diag["cybermed_qa_replay_skipped_reason"]
