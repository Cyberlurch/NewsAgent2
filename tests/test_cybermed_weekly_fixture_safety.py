import json
import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from newsagent2 import main


def _run_weekly(monkeypatch, tmp_path, *, report_key="cybermed", report_mode="weekly", event_name="workflow_dispatch", fixture_mode="1"):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({"version": 1, "updated_at_utc": "", "digests": []}), encoding="utf-8")
    monkeypatch.setenv("REPORT_KEY", report_key)
    monkeypatch.setenv("REPORT_MODE", report_mode)
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", event_name)
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_MODE", fixture_mode)
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_PATH", str(fixture))
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: None)
    main.main()
    diag_path = tmp_path / "out" / "cybermed_weekly_diagnostics.json"
    if diag_path.exists():
        return json.loads(diag_path.read_text(encoding="utf-8"))
    return None


def test_scheduled_weekly_fixture_requested_but_not_enabled(monkeypatch, tmp_path, capsys):
    _run_weekly(monkeypatch, tmp_path, event_name="schedule", fixture_mode="1")
    out = capsys.readouterr().out
    assert "weekly loaded 0 digest item(s) for report" in out
    assert "tests/fixtures/cybermed_weekly_digest_store_nonempty.json" not in out


def test_manual_weekly_fixture_enabled_with_safe_settings(monkeypatch, tmp_path):
    diag = _run_weekly(monkeypatch, tmp_path, event_name="workflow_dispatch", fixture_mode="1")
    assert diag["cybermed_weekly_qa_fixture_requested"] is True
    assert diag["cybermed_weekly_qa_fixture_mode"] is True
    assert diag["cybermed_weekly_qa_fixture_safety_passed"] is True


def test_manual_daily_ignores_weekly_fixture(monkeypatch, tmp_path):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({"version": 1, "updated_at_utc": "", "digests": []}), encoding="utf-8")
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_MODE", "1")
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_PATH", str(fixture))
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: None)
    main.main()
    diag = json.loads((tmp_path / "out" / "cybermed_daily_diagnostics.json").read_text(encoding="utf-8"))
    assert "cybermed_weekly_qa_fixture_mode" not in diag


def test_cyberlurch_ignores_weekly_fixture(monkeypatch, tmp_path):
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_MODE", "1")
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])
    monkeypatch.setattr(main, "send_markdown", lambda *a, **k: None)
    main.main()
    diag = json.loads(next((tmp_path / "out").glob("cyberlurch_*_youtube_diagnostics.json")).read_text(encoding="utf-8"))
    assert "cybermed_weekly_qa_fixture_mode" not in diag
