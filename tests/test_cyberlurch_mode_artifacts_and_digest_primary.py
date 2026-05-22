from __future__ import annotations
import json, datetime as dt, pathlib, sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from newsagent2 import main as main_mod

def _channels(p):
    p.write_text(json.dumps({"topic_buckets":[{"topic":"intel","channels":[{"name":"tagesschau","url":"https://youtube.com/@tagesschau"}]}]}), encoding="utf-8")

def _digest_state(p):
    p.parent.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    p.write_text(json.dumps({"version":1,"updated_at_utc":now,"digests":[{"video_id":"d1","title":"Digest one","channel":"tagesschau","url":"https://www.youtube.com/watch?v=d1","published_at":now,"summary":"s","text_source":"managed_transcript","topic_primary":"intel","topics":["intel"]}]}, indent=2), encoding="utf-8")

def _common(monkeypatch, tmp_path, mode):
    monkeypatch.setenv("REPORT_KEY","cyberlurch"); monkeypatch.setenv("REPORT_MODE",mode); monkeypatch.setenv("REPORT_DIR",str(tmp_path/"out")); monkeypatch.setenv("STATE_PATH",str(tmp_path/"s.json")); monkeypatch.setenv("SEND_EMAIL","0"); monkeypatch.setenv("GITHUB_EVENT_NAME","workflow_dispatch")
    monkeypatch.setenv("CYBERLURCH_DIGEST_STATE_PATH", str(tmp_path/"state"/"cyberlurch_digests.json"))
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "## Executive Summary\n\nok")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")


def test_weekly_digest_primary_skips_collection(tmp_path, monkeypatch):
    ch = tmp_path / "channels.json"; _channels(ch)
    _digest_state(tmp_path/"state"/"cyberlurch_digests.json")
    _common(monkeypatch, tmp_path, "weekly")
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not collect")))
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(ch), "--hours", "36"])
    main_mod.main()
    assert list((tmp_path/"out").glob("cyberlurch_weekly_summary_*.md"))
    d = json.loads((tmp_path/"out"/"cyberlurch_weekly_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert d["digest_store_used_as_primary"] is True
    assert d["digest_store_collection_skipped_due_to_primary"] is True
    assert d.get("videos_listed_total", 0) == 0
    assert d.get("youtube_api_metadata_attempted_total", 0) == 0


def test_monthly_digest_primary_skips_collection(tmp_path, monkeypatch):
    ch = tmp_path / "channels.json"; _channels(ch)
    _digest_state(tmp_path/"state"/"cyberlurch_digests.json")
    _common(monkeypatch, tmp_path, "monthly")
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not collect")))
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(ch), "--hours", "36"])
    main_mod.main()
    d = json.loads((tmp_path/"out"/"cyberlurch_monthly_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert d["digest_store_used_as_primary"] is True
    assert d["digest_store_collection_skipped_due_to_primary"] is True
    assert d.get("managed_transcript_attempted_total", 0) == 0
    t = d.get("items_by_temporality", {})
    assert sum(int(v) for v in t.values()) > 0


def test_monthly_digest_empty_falls_back_collection(tmp_path, monkeypatch):
    ch = tmp_path / "channels.json"; _channels(ch)
    (tmp_path/"state").mkdir(parents=True, exist_ok=True)
    (tmp_path/"state"/"cyberlurch_digests.json").write_text(json.dumps({"version":1,"updated_at_utc":"","digests":[]}), encoding="utf-8")
    _common(monkeypatch, tmp_path, "monthly")
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: [{"id":"x1","title":"x","channel":"tagesschau","published_at":dt.datetime.now(dt.timezone.utc),"url":"https://www.youtube.com/watch?v=x1","description":""}])
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"success","text":"t"*1000,"source":"description"})())
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(ch), "--hours", "36"])
    main_mod.main()
    assert list((tmp_path/"out").glob("cyberlurch_monthly_summary_*.md"))
    d = json.loads((tmp_path/"out"/"cyberlurch_monthly_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert d["digest_store_collection_skipped_due_to_primary"] is False


def test_weekly_digest_supplement_allows_collection(tmp_path, monkeypatch):
    ch = tmp_path / "channels.json"; _channels(ch)
    _digest_state(tmp_path/"state"/"cyberlurch_digests.json")
    _common(monkeypatch, tmp_path, "weekly")
    monkeypatch.setenv("CYBERLURCH_WEEKLY_SUPPLEMENT_WITH_COLLECTION", "1")
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: [{"id":"x1","title":"x","channel":"tagesschau","published_at":dt.datetime.now(dt.timezone.utc),"url":"https://www.youtube.com/watch?v=x1","description":""}])
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"success","text":"t"*1000,"source":"description"})())
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(ch), "--hours", "36"])
    main_mod.main()
    d = json.loads((tmp_path/"out"/"cyberlurch_weekly_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert d["digest_store_used_as_primary"] is True
    assert d["digest_store_collection_supplement_used"] is True
    assert d["digest_store_collection_skipped_due_to_primary"] is False


def test_workflow_has_no_pull_rebase_pattern():
    yml = pathlib.Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')
    assert 'pull --rebase' not in yml


def test_workflow_run_plan_log_formatting():
    yml = pathlib.Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')
    assert "printf 'Run plan: modes=[%s] which_report=%s\\n' \"$run_modes\" \"$run_which_report\"" in yml


def test_monthly_rollup_enrichment_contains_temporality_channels_themes(tmp_path, monkeypatch):
    ch = tmp_path / "channels.json"; _channels(ch)
    _digest_state(tmp_path/"state"/"cyberlurch_digests.json")
    _common(monkeypatch, tmp_path, "monthly")
    monkeypatch.setenv("ROLLUPS_STATE_PATH", str(tmp_path/"state"/"rollups.json"))
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not collect")))
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(ch), "--hours", "36"])
    main_mod.main()
    rollups = json.loads((tmp_path/"state"/"rollups.json").read_text(encoding="utf-8"))
    latest = rollups["reports"]["cyberlurch"][-1]
    assert isinstance(latest.get("top_channels"), list)
    assert isinstance(latest.get("top_themes"), list)
    assert isinstance(latest.get("items_by_temporality"), dict)
