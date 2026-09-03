from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from newsagent2 import main
from test_cyberlurch_mode_artifacts_and_digest_primary import _channels, _common, _digest_state


def _setup(tmp_path: Path, monkeypatch, *, email_mode: str = "real") -> Path:
    channels = tmp_path / "channels.json"
    _channels(channels)
    _digest_state(tmp_path / "state" / "cyberlurch_digests.json")
    _common(monkeypatch, tmp_path, "monthly")
    monkeypatch.setenv("EMAIL_MODE", email_mode)
    monkeypatch.setenv("ROLLUPS_STATE_PATH", str(tmp_path / "state" / "rollups.json"))
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels), "--hours", "36"])
    return tmp_path / "state" / "rollups.json"


@pytest.mark.parametrize("event", ["workflow_dispatch", "schedule"])
def test_production_persists_and_verifies_before_email(event, tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    if event == "schedule":
        # Keep this fixture's current-month digest while exercising schedule semantics.
        from datetime import datetime
        monkeypatch.setenv("ROLLUP_MONTH_OVERRIDE", datetime.now(main.STO).strftime("%Y-%m"))
    events = []
    real_save = main.save_rollups_state
    real_load = main.load_rollups_state
    monkeypatch.setattr(main, "save_rollups_state", lambda *a, **k: (events.append("save"), real_save(*a, **k))[1])
    monkeypatch.setattr(main, "load_rollups_state", lambda *a, **k: (events.append("load"), real_load(*a, **k))[1])
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: events.append("email"))

    main.main()

    assert path.exists()
    assert events[-1] == "email"
    assert events.index("save") < events.index("email")
    assert events.index("load", events.index("save")) < events.index("email")
    if event == "workflow_dispatch":
        diag = json.loads((tmp_path / "out" / "cyberlurch_monthly_youtube_diagnostics.json").read_text())
        assert diag["cyberlurch_monthly_source_mode"] == "persisted_digest"
        assert diag["cyberlurch_monthly_rollup_write_verified"] is True
        assert diag["cyberlurch_monthly_live_collection_used"] is False


@pytest.mark.parametrize("event,email_mode", [("schedule", "real"), ("workflow_dispatch", "real")])
def test_production_empty_store_fails_closed(event, email_mode, tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch, email_mode=email_mode)
    (tmp_path / "state" / "cyberlurch_digests.json").write_text('{"version":1,"digests":[]}')
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    monkeypatch.setattr(main, "list_recent_videos", lambda *a, **k: pytest.fail("live collection called"))
    monkeypatch.setattr(main, "fetch_transcript", lambda *a, **k: pytest.fail("provider called"))
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: pytest.fail("email called"))

    with pytest.raises(RuntimeError, match="no valid persisted digest items"):
        main.main()
    assert not path.exists()


@pytest.mark.parametrize("email_mode,emails", [("none", 0), ("test", 1)])
def test_manual_audit_modes_do_not_mutate_rollups(email_mode, emails, tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch, email_mode=email_mode)
    original = b'{"version":1,"updated_at_utc":"fixed","reports":{}}\n'
    path.write_bytes(original)
    sent = []
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: sent.append(a))

    main.main()

    assert path.read_bytes() == original
    assert len(sent) == emails


def test_save_failure_blocks_email(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(main, "save_rollups_state", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: pytest.fail("email called"))
    with pytest.raises(RuntimeError, match="rollup persistence failed"):
        main.main()


def test_malformed_synthesis_blocks_delivery_after_one_retry(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(main, "synthesize_cyberlurch_monthly_json", lambda *a: calls.append(a) or "not json")
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: pytest.fail("email called"))
    with pytest.raises(RuntimeError, match="synthesis failed"):
        main.main()
    assert len(calls) == 2
    assert not path.exists()


def test_monthly_uses_one_synthesis_operation_and_persists_semantic_rollup(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    for name in (
        "summarize", "summarize_item_detail", "summarize_cyberlurch_bottom_line",
        "summarize_youtube_transcript_direct", "summarize_youtube_transcript_chunks",
    ):
        monkeypatch.setattr(main, name, lambda *a, _name=name, **k: pytest.fail(f"Monthly called {_name}"))

    main.main()

    rollup = json.loads(path.read_text())["reports"]["cyberlurch"][-1]
    assert rollup["schema"] == "cyberlurch-monthly-semantic-v2"
    assert rollup["executive_summary"]
    assert len(rollup["themes"]) <= 6
    assert "top_channels" not in rollup
    report = next((tmp_path / "out").glob("cyberlurch_monthly_summary_*.md")).read_text()
    assert "## Source/channel summary" not in report
    assert "## Monthly trend map" not in report


def test_newsagent_workflow_wires_month_override():
    workflow = Path(".github/workflows/newsagent.yml").read_text()
    assert "rollup_month_override:" in workflow
    assert "ROLLUP_MONTH_OVERRIDE: ${{ github.event_name == 'workflow_dispatch' && github.event.inputs.rollup_month_override || '' }}" in workflow
