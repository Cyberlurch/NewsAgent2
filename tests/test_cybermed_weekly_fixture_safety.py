import json
import pathlib
import sys

import pytest

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
    monkeypatch.setenv("REPORT_TITLE", "Cybermed Monthly Report" if report_mode == "monthly" else "Cybermed Weekly Report")
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", event_name)
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_MODE", fixture_mode)
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_PATH", str(fixture))
    if event_name == "schedule":
        monkeypatch.setenv("CYBERMED_DAILY_DIGEST_STATE_PATH", str(tmp_path / "empty_cybermed_daily_digests.json"))
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
    monkeypatch.setattr(main, "search_recent_pubmed", lambda *a, **k: [])
    monkeypatch.setattr(main, "collect_foamed_items", lambda *a, **k: ([], {}))
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
    monkeypatch.setattr(main, "list_recent_videos", lambda *a, **k: [])
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
    assert diag["cybermed_weekly_rendered_deep_dives_total"] == 0
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
    assert diag["cybermed_weekly_deep_dives_suppressed_empty_total"] == 1
    assert diag["cybermed_weekly_deep_dive_placeholder_violations_total"] == 0
    assert "No stored deep-dive synopsis available." not in md
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
    assert diag["cybermed_monthly_top_picks_loaded_total"] >= 8
    assert diag["cybermed_monthly_loaded_top_picks_total"] >= 8
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
    assert "total included: 20 papers, 15 FOAMed, 5 top picks." in md
    assert "Cybermed Monthly Report – 2026-05" in md
    assert "## Executive editorial summary" in md
    assert "## This month’s clinical themes" in md
    assert "## Practice-impact section" in md
    assert md.count("**⭐ [") == 5
    assert isinstance(md, str)
    assert diag["cybermed_monthly_editorial_mode"] is True
    assert diag["cybermed_monthly_editorial_summary_generated_from_digest"] is True
    assert diag["cybermed_monthly_live_collection_used"] is False
    assert diag["cybermed_monthly_theme_count"] >= 1


def test_cybermed_digest_only_skips_overview_summarization(monkeypatch, tmp_path):
    fixture_path = pathlib.Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "cybermed_weekly_digest_store_nonempty.json"

    def _boom(*args, **kwargs):
        raise AssertionError("summarize() should not be called in Cybermed digest-only mode")

    monkeypatch.setattr(main, "summarize", _boom)

    weekly_diag, _ = _run_weekly(monkeypatch, tmp_path / "weekly", report_mode="weekly", event_name="workflow_dispatch", fixture_mode="1", fixture_path=fixture_path)
    monthly_diag, _ = _run_weekly(monkeypatch, tmp_path / "monthly", report_mode="monthly", event_name="workflow_dispatch", fixture_mode="1", fixture_path=fixture_path)

    assert weekly_diag["cybermed_digest_only_overview_summarization_skipped"] is True
    assert monthly_diag["cybermed_digest_only_overview_summarization_skipped"] is True
    assert weekly_diag["runtime_summarization_seconds"] == 0.0
    assert monthly_diag["runtime_summarization_seconds"] == 0.0


def test_monthly_digest_only_does_not_call_live_collectors(monkeypatch, tmp_path):
    fixture_path = pathlib.Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "cybermed_weekly_digest_store_nonempty.json"

    def _boom(*args, **kwargs):
        raise AssertionError("live collection should not be called in Cybermed monthly digest-only mode")

    monkeypatch.setattr(main, "search_recent_pubmed", _boom)
    monkeypatch.setattr(main, "collect_foamed_items", _boom)
    diag, _ = _run_weekly(monkeypatch, tmp_path, report_mode="monthly", event_name="workflow_dispatch", fixture_mode="1", fixture_path=fixture_path)
    assert diag["runtime_pubmed_collect_seconds"] == 0.0
    assert diag["runtime_foamed_collect_seconds"] == 0.0


def _run_digest_store_report(monkeypatch, tmp_path, *, report_mode="weekly", digests=None, send_email="1", email_mode="real", event_name="workflow_dispatch", send_func=None):
    store_path = tmp_path / "cybermed_daily_digests.json"
    store_path.write_text(json.dumps({"schema_version": 1, "digests": digests or []}), encoding="utf-8")
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setenv("REPORT_MODE", report_mode)
    monkeypatch.setenv("REPORT_TITLE", "Cybermed Monthly Report" if report_mode == "monthly" else "Cybermed Weekly Report")
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", send_email)
    monkeypatch.setenv("EMAIL_MODE", email_mode)
    monkeypatch.setenv("GITHUB_EVENT_NAME", event_name)
    monkeypatch.setenv("CYBERMED_WEEKLY_QA_FIXTURE_MODE", "0")
    monkeypatch.setenv("CYBERMED_DAILY_DIGEST_STATE_PATH", str(store_path))
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])
    calls = []
    if send_func is None:
        send_func = lambda subject, md: calls.append((subject, md))
    monkeypatch.setattr(main, "send_markdown", send_func)
    main.main()
    return calls


def test_cybermed_weekly_digest_only_empty_real_send_is_blocked(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(main, "send_markdown", lambda subject, md: calls.append((subject, md)))
    with pytest.raises(RuntimeError, match="Cybermed digest-only report was blocked because it had no digest content"):
        _run_digest_store_report(
            monkeypatch,
            tmp_path,
            report_mode="weekly",
            digests=[],
            send_email="1",
            email_mode="real",
            send_func=lambda subject, md: calls.append((subject, md)),
        )
    assert calls == []


def test_cybermed_weekly_digest_only_with_items_real_send_succeeds(monkeypatch, tmp_path):
    today = main.datetime.now(tz=main.STO).date().strftime("%Y-%m-%d")
    digests = [{
        "digest_id": "today",
        "run_date": today,
        "items": {
            "pubmed": [{"item_id": "p1", "source_type": "pubmed", "title": "Paper", "pmid": "1", "published_at": today, "bottom_line": "Useful paper"}],
            "foamed": [{"item_id": "f1", "source_type": "foamed", "title": "Post", "url": "https://example.com/post", "published_at": today, "bottom_line": "Useful post"}],
        },
        "deep_dives": [],
        "top_picks": [],
    }]
    calls = _run_digest_store_report(monkeypatch, tmp_path, report_mode="weekly", digests=digests)
    assert len(calls) == 1
    assert "Useful paper" in calls[0][1]
    assert "Useful post" in calls[0][1]


def test_cybermed_monthly_digest_only_empty_real_send_is_blocked(monkeypatch, tmp_path):
    calls = []
    with pytest.raises(RuntimeError, match="Cybermed digest-only report was blocked because it had no digest content"):
        _run_digest_store_report(
            monkeypatch,
            tmp_path,
            report_mode="monthly",
            digests=[],
            send_email="1",
            email_mode="real",
            send_func=lambda subject, md: calls.append((subject, md)),
        )
    assert calls == []


def test_cybermed_weekly_maps_stored_deep_dive_and_renders_structured_content(monkeypatch, tmp_path):
    fixture = {
        "version": 1,
        "updated_at_utc": "",
        "digests": [{
            "digest_id": "d1",
            "run_date": "2026-05-18",
            "items": {
                "pubmed": [{
                    "item_id": "paper-1",
                    "source_type": "pubmed",
                    "title": "Structured weekly paper",
                    "pmid": "12345",
                    "doi": "10.1000/weekly",
                    "url": "https://pubmed.ncbi.nlm.nih.gov/12345/",
                    "published_at": "2026-05-18T10:00:00+00:00",
                    "bottom_line": "Original bottom line",
                    "deep_dive_candidate": True,
                }],
                "foamed": [],
            },
            "deep_dives": [{
                "item_id": "paper-1",
                "pmid": "12345",
                "title": "Structured weekly paper",
                "study_type": "Randomized trial",
                "population_setting": "Adult ICU patients",
                "intervention_or_exposure": "Protocolized care",
                "comparator": "Usual care",
                "primary_endpoint": "Ventilator-free days",
                "primary_result_direction": "Improved",
                "primary_result_significance": "Statistically significant",
                "key_secondary_results": "Shorter ICU stay",
                "clinical_interpretation": "May change protocol design",
                "limitations": "Single-center study",
                "deep_dive_reasons": ["practice impact"],
                "bottom_line": "Stored deep-dive bottom line",
            }],
            "top_picks": [],
        }],
    }
    diag, md = _run_weekly(monkeypatch, tmp_path, fixture_payload=fixture)
    assert ("## Deep Dives" in md) or ("## Vertiefungen" in md)
    assert "**Study type:** Randomized trial" in md
    assert "**Population/setting:** Adult ICU patients" in md
    assert "**Intervention/exposure & comparator:** Protocolized care · Usual care" in md
    assert "**Primary endpoints:** Ventilator-free days" in md
    assert "Improved · Statistically significant · Shorter ICU stay" in md
    assert "**Limitations:** Single-center study" in md
    assert "**Why this matters:** May change protocol design" in md
    assert "practice impact" not in md
    assert "**BOTTOM LINE:** Stored deep-dive bottom line" in md
    assert "No stored deep-dive synopsis available." not in md
    assert diag["cybermed_weekly_deep_dives_loaded_total"] == 1
    assert diag["cybermed_weekly_deep_dives_selected_total"] == 1
    assert diag["cybermed_weekly_deep_dives_with_real_content_total"] == 1
    assert diag["cybermed_weekly_deep_dives_rendered_with_real_content_total"] == 1
    assert diag["cybermed_weekly_deep_dive_structured_fields_available_total"] == 1
    assert diag["cybermed_weekly_deep_dives_suppressed_thin_total"] == 0
    assert diag["cybermed_weekly_deep_dive_placeholder_violations_total"] == 0
    assert diag["cybermed_weekly_deep_dive_mapping_misses_total"] == 0
    assert diag["cybermed_weekly_deep_dive_mapping_keys_used_counts"]["item_id"] == 1


def test_cybermed_weekly_suppresses_deep_dive_with_only_bottom_line(monkeypatch, tmp_path):
    fixture = {
        "version": 1,
        "updated_at_utc": "",
        "digests": [{
            "digest_id": "d1",
            "run_date": "2026-05-18",
            "items": {"pubmed": [{
                "item_id": "paper-2",
                "source_type": "pubmed",
                "title": "Bottom line only paper",
                "pmid": "999",
                "published_at": "2026-05-18T10:00:00+00:00",
                "bottom_line": "Still useful as a paper",
                "deep_dive_candidate": True,
            }], "foamed": []},
            "deep_dives": [{"item_id": "paper-2", "pmid": "999", "bottom_line": "Only bottom line"}],
            "top_picks": [],
        }],
    }
    diag, md = _run_weekly(monkeypatch, tmp_path, fixture_payload=fixture)
    assert "Bottom line only paper" in md
    assert "## Papers" in md
    assert "## Deep Dives" not in md
    assert "No stored deep-dive synopsis available." not in md
    assert diag["cybermed_weekly_deep_dives_selected_total"] == 1
    assert diag["cybermed_weekly_deep_dives_with_real_content_total"] == 0
    assert diag["cybermed_weekly_deep_dives_rendered_with_real_content_total"] == 0
    assert diag["cybermed_weekly_deep_dives_suppressed_thin_total"] == 1
    assert diag["cybermed_weekly_deep_dive_placeholder_violations_total"] == 0


def test_cybermed_weekly_renders_stored_deep_dive_markdown(monkeypatch, tmp_path):
    markdown = "- **Study type:** Cohort study\n- **Population/setting:** ED patients with sepsis\n- **Key results:** Faster antibiotics were associated with lower mortality\n- **Limitations:** Residual confounding\n\n**BOTTOM LINE:** Operational timing may matter."
    fixture = {
        "version": 1,
        "updated_at_utc": "",
        "digests": [{
            "digest_id": "d1",
            "run_date": "2026-05-18",
            "items": {"pubmed": [{
                "item_id": "paper-md",
                "source_type": "pubmed",
                "title": "Markdown paper",
                "pmid": "777",
                "published_at": "2026-05-18T10:00:00+00:00",
                "bottom_line": "Operational timing may matter.",
                "deep_dive_candidate": True,
            }], "foamed": []},
            "deep_dives": [{"item_id": "paper-md", "pmid": "777", "deep_dive_markdown": markdown, "bottom_line": "Operational timing may matter."}],
            "top_picks": [],
        }],
    }
    diag, md = _run_weekly(monkeypatch, tmp_path, fixture_payload=fixture)
    assert ("## Deep Dives" in md) or ("## Vertiefungen" in md)
    assert "**Study type:** Cohort study" in md
    assert "**Population/setting:** ED patients with sepsis" in md
    assert "**Key results:** Faster antibiotics were associated with lower mortality" in md
    assert "**Limitations:** Residual confounding" in md
    assert "No stored deep-dive synopsis available." not in md
    assert diag["cybermed_weekly_deep_dive_markdown_available_total"] == 1
    assert diag["cybermed_weekly_deep_dives_rendered_with_real_content_total"] == 1


def test_cybermed_weekly_suppresses_reason_code_only_deep_dive(monkeypatch, tmp_path):
    fixture = {
        "version": 1,
        "updated_at_utc": "",
        "digests": [{
            "digest_id": "d1",
            "run_date": "2026-05-18",
            "items": {"pubmed": [{
                "item_id": "paper-reasons",
                "source_type": "pubmed",
                "title": "Reason codes paper",
                "pmid": "888",
                "published_at": "2026-05-18T10:00:00+00:00",
                "bottom_line": "Interesting but thin.",
                "deep_dive_candidate": True,
            }], "foamed": []},
            "deep_dives": [{
                "item_id": "paper-reasons",
                "pmid": "888",
                "bottom_line": "Interesting but thin.",
                "deep_dive_reasons": ["guideline", "RCT", "strong_anesthesia_relevance"],
            }],
            "top_picks": [],
        }],
    }
    diag, md = _run_weekly(monkeypatch, tmp_path, fixture_payload=fixture)
    assert "## Deep Dives" not in md
    assert "Why this matters" not in md
    assert "guideline" not in md
    assert "strong_anesthesia_relevance" not in md
    assert "No stored deep-dive synopsis available." not in md
    assert diag["cybermed_weekly_deep_dives_with_real_content_total"] == 0
    assert diag["cybermed_weekly_deep_dives_rendered_with_real_content_total"] == 0
    assert diag["cybermed_weekly_deep_dives_suppressed_thin_total"] == 1
    assert diag["cybermed_weekly_deep_dive_reason_codes_only_total"] == 1


def test_cybermed_weekly_foamed_bottom_line_bolds_label_only(monkeypatch, tmp_path):
    fixture = {
        "version": 1,
        "updated_at_utc": "",
        "digests": [{
            "digest_id": "d1",
            "run_date": "2026-05-18",
            "items": {"pubmed": [], "foamed": [{
                "item_id": "foam-1",
                "source_type": "foamed",
                "title": "FOAMed item",
                "url": "https://example.test/foam",
                "published_at": "2026-05-18T08:00:00+00:00",
                "bottom_line": "some text",
            }]},
            "deep_dives": [],
            "top_picks": [],
        }],
    }
    _, md = _run_weekly(monkeypatch, tmp_path, fixture_payload=fixture)
    assert "**BOTTOM LINE: some text**" not in md
    assert "**BOTTOM LINE:** some text" in md
    assert "No stored deep-dive synopsis available." not in md
