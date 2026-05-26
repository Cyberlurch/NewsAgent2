import json
import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from newsagent2 import main


def _run_weekly(monkeypatch, tmp_path, *, report_key="cybermed", report_mode="weekly", event_name="workflow_dispatch", fixture_mode="1", fixture_payload=None, fixture_path=None):
    fixture = fixture_path or (tmp_path / "fixture.json")
    if fixture_payload is None and not fixture_path:
        fixture_payload = {"version": 1, "updated_at_utc": "", "digests": []}
    if fixture_payload is not None:
        fixture.write_text(json.dumps(fixture_payload), encoding="utf-8")
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
    diag_path = tmp_path / "out" / f"cybermed_{report_mode}_diagnostics.json"
    md_candidates = sorted((tmp_path / "out").glob(f"cybermed_{report_mode}_*.md"))
    md_text = md_candidates[-1].read_text(encoding="utf-8") if md_candidates else ""
    diag = json.loads(diag_path.read_text(encoding="utf-8")) if diag_path.exists() else None
    return diag, md_text


def test_scheduled_weekly_fixture_requested_but_not_enabled(monkeypatch, tmp_path, capsys):
    diag, _ = _run_weekly(monkeypatch, tmp_path, event_name="schedule", fixture_mode="1")
    out = capsys.readouterr().out
    assert "weekly loaded 0 digest item(s) for report" in out
    assert "tests/fixtures/cybermed_weekly_digest_store_nonempty.json" not in out
    if diag is not None:
        assert diag["cybermed_weekly_qa_fixture_requested"] is True
        assert diag["cybermed_weekly_qa_fixture_mode"] is False
        assert diag["cybermed_weekly_qa_fixture_safety_passed"] is False
        assert "event_not_manual" in diag["cybermed_weekly_qa_fixture_skipped_reason"]


def test_manual_weekly_fixture_enabled_with_safe_settings(monkeypatch, tmp_path):
    diag, _ = _run_weekly(monkeypatch, tmp_path, event_name="workflow_dispatch", fixture_mode="1")
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


def test_manual_weekly_fixture_renders_digest_items(monkeypatch, tmp_path):
    fixture_path = pathlib.Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "cybermed_weekly_digest_store_nonempty.json"
    diag, md = _run_weekly(monkeypatch, tmp_path, event_name="workflow_dispatch", fixture_mode="1", fixture_path=fixture_path)
    assert diag["cybermed_weekly_pubmed_items_selected_total"] == 2
    assert diag["cybermed_weekly_foamed_items_selected_total"] == 1
    assert diag["cybermed_weekly_deep_dives_selected_total"] == 1
    assert diag["cybermed_weekly_rendered_pubmed_items_total"] == 2
    assert diag["cybermed_weekly_rendered_foamed_items_total"] == 1
    assert diag["cybermed_weekly_rendered_deep_dives_total"] == 1
    assert diag["cybermed_weekly_rendered_top_picks_total"] == 2
    assert diag["cybermed_weekly_intro_pubmed_items_total"] == 2
    assert diag["cybermed_weekly_intro_foamed_items_total"] == 1
    assert diag["cybermed_weekly_intro_top_picks_total"] == 2
    assert diag["cybermed_weekly_intro_count_mismatch_total"] == 0
    assert diag["cybermed_weekly_intro_count_mismatch_fields"] == []
    assert diag["cybermed_weekly_report_matches_digest_inputs"] is True
    assert diag["cybermed_weekly_stored_bottom_lines_used_total"] == 3
    assert diag["cybermed_weekly_missing_bottom_lines_total"] == 0
    assert diag["cybermed_weekly_generated_or_fallback_bottom_lines_total"] == 0
    assert diag["cybermed_weekly_top_pick_source_counts"]["stored_true"] == 2
    assert diag["cybermed_weekly_top_pick_source_counts"]["stored_false"] == 1
    assert diag["cybermed_weekly_top_pick_source_counts"]["inferred"] == 0
    assert diag["cybermed_weekly_top_pick_inference_violations_total"] == 0
    assert diag["cybermed_weekly_bottom_line_preservation_violations_total"] == 0
    assert "Top Picks" in md
    assert "BL1" in md
    assert "BL2" in md
    assert "FBL1" in md
    assert "No abstract text was provided" not in md


def test_manual_weekly_fixture_intro_uses_rendered_top_picks_count(monkeypatch, tmp_path):
    pubmed = [{"item_id": f"p{i}", "source_type": "pubmed", "title": f"P{i}", "pmid": str(i), "published_at": f"2026-05-1{i%9}T10:00:00+00:00", "bottom_line": f"BL{i}", "top_pick": i <= 5} for i in range(1, 21)]
    foamed = [{"item_id": f"f{i}", "source_type": "foamed", "title": f"F{i}", "url": f"https://f{i}.example.com", "published_at": f"2026-05-1{i%9}T09:00:00+00:00", "bottom_line": f"FBL{i}", "top_pick": i <= 3} for i in range(1, 16)]
    top_picks = [{"item_id": f"p{i}"} for i in range(1, 6)] + [{"item_id": f"f{i}"} for i in range(1, 4)]
    fixture = {"version": 1, "updated_at_utc": "", "digests": [{"digest_id": "d1", "run_date": "2026-05-18", "items": {"pubmed": pubmed, "foamed": foamed}, "deep_dives": [], "top_picks": top_picks}]}
    diag, md = _run_weekly(monkeypatch, tmp_path, event_name="workflow_dispatch", fixture_mode="1", fixture_payload=fixture)
    assert diag["cybermed_weekly_loaded_top_picks_total"] == 8
    assert diag["cybermed_weekly_selected_top_picks_total"] == 5
    assert diag["cybermed_weekly_rendered_top_picks_total"] == 5
    assert diag["cybermed_weekly_intro_top_picks_total"] == 5
    assert diag["cybermed_weekly_intro_count_mismatch_total"] == 0
    assert diag["cybermed_weekly_intro_count_mismatch_fields"] == []
    assert isinstance(md, str)


def test_monthly_fixture_intro_and_diagnostics_top_pick_counts(monkeypatch, tmp_path):
    pubmed = [{"item_id": f"p{i}", "source_type": "pubmed", "title": f"P{i}", "pmid": str(i), "published_at": f"2026-05-1{i%9}T10:00:00+00:00", "bottom_line": f"BL{i}", "top_pick": i <= 5} for i in range(1, 21)]
    foamed = [{"item_id": f"f{i}", "source_type": "foamed", "title": f"F{i}", "url": f"https://f{i}.example.com", "published_at": f"2026-05-1{i%9}T09:00:00+00:00", "bottom_line": f"FBL{i}", "top_pick": i <= 3} for i in range(1, 16)]
    top_picks = [{"item_id": f"p{i}"} for i in range(1, 6)] + [{"item_id": f"f{i}"} for i in range(1, 4)]
    fixture = {"version": 1, "updated_at_utc": "", "digests": [{"digest_id": "d1", "run_date": "2026-05-18", "items": {"pubmed": pubmed, "foamed": foamed}, "deep_dives": [], "top_picks": top_picks}]}
    diag, md = _run_weekly(monkeypatch, tmp_path, report_mode="monthly", event_name="workflow_dispatch", fixture_mode="1", fixture_payload=fixture)
    assert diag["cybermed_monthly_from_daily_digests_enabled"] is True
    assert diag["cybermed_monthly_digest_only_mode"] is True
    assert diag["cybermed_monthly_collection_skipped"] is True
    assert diag["cybermed_monthly_collection_skipped_reason"] == "monthly_from_daily_digests"
    assert diag["cybermed_monthly_top_picks_loaded_total"] == 8
    assert diag["cybermed_monthly_loaded_top_picks_total"] == 8
    assert diag["cybermed_monthly_top_picks_selected_total"] == 5
    assert diag["cybermed_monthly_selected_top_picks_total"] == 5
    assert diag["cybermed_monthly_rendered_top_picks_total"] == 5
    assert diag["cybermed_monthly_intro_top_picks_total"] == 5
    assert diag["cybermed_monthly_top_picks_cap"] == 5
    assert diag["cybermed_monthly_top_picks_capped"] is True
    assert diag["cybermed_monthly_intro_count_mismatch_total"] == 0
    assert diag["cybermed_monthly_intro_count_mismatch_fields"] == []
    assert diag["cybermed_monthly_report_matches_digest_inputs"] is True
    assert diag["cybermed_monthly_stored_bottom_lines_used_total"] > 0
    assert diag["cybermed_monthly_empty_reason"] == ""
    assert isinstance(md, str)
