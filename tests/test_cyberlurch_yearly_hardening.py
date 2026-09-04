from __future__ import annotations

import json

import pytest

from newsagent2 import main, rollups


def _months(year=2025, count=12):
    return [{
        "month": f"{year}-{month:02d}", "generated_at": f"{year}-{month:02d}-02T00:00:00Z", "full_text_count": 1,
        "executive_summary": ["Persisted safe summary"],
        "top_themes": [{"theme": " Preparedness ", "count": 2}],
        "top_channels": [{"channel": "Channel A", "count": 3}],
        "topic_summaries": ["preparedness: 2 item(s)"],
        "topic_trajectories": ["Preparedness: sustained stream"],
        "representative_items": [{"video_id": f"item-{month}", "title": "Safe title", "channel": f"Channel {month % 2}",
                                  "date": f"{year}-{month:02d}-01", "bottom_line": "A persisted factual summary with useful substance.",
                                  "url": f"https://youtu.be/safe{month:02d}"}],
    } for month in range(1, count + 1)]


def _run(tmp_path, monkeypatch, entries, *, mode="none", event="workflow_dispatch"):
    path = tmp_path / "rollups.json"
    path.write_text(json.dumps({"reports": {"cyberlurch": entries}}))
    original = path.read_bytes()
    monkeypatch.setenv("YEAR_IN_REVIEW_YEAR", "2025")
    monkeypatch.setenv("EMAIL_MODE", mode)
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    sent = []
    monkeypatch.setattr(main, "send_markdown", lambda *args: sent.append(args))
    def provider(system, user):
        marker = "Authoritative source metadata (JSON):\n"
        sources = json.loads(user.split(marker, 1)[1].split("\nYour prior", 1)[0])
        refs = [row["ref_id"] for row in sources]
        pair = refs[:2]
        return json.dumps({
            "executive_summary": [{"heading": "Material shift", "synthesis": "Persisted evidence records a material shift.", "source_refs": pair}],
            "annual_trends": [{"heading": "Cross-month development", "synthesis": "The development persisted across the year.", "source_refs": pair}],
            "turning_points": [{"heading": "Important event", "synthesis": "One event changed the trajectory.", "source_refs": refs[:1]}],
            "notable_developments": [{"heading": "Standalone development", "synthesis": "A separate material development.", "source_refs": refs[-1:]}],
            "timeline": [{"period": "Early year", "synthesis": "The trajectory began in the early year.", "source_refs": refs[:1]}],
            "year_in_brief": "Persisted evidence shows a changing annual trajectory.",
            "source_refs_used": ["model compatibility value is ignored"],
        })
    monkeypatch.setattr(main, "synthesize_cyberlurch_yearly_json", provider)
    main._run_yearly_report(rollups_state_path=str(path), report_key="cyberlurch",
        base_report_title="Cyberlurch", base_report_subject="Cyberlurch", report_language="en",
        report_dir=str(tmp_path / "reports"))
    return path, original, sent, (tmp_path / "reports" / "cyberlurch_yearly_review_2025.md").read_text()


def test_prepare_unique_month_latest_and_sanitizes_without_mutation():
    entries = _months(count=1)
    entries += [{**entries[0], "generated_at": "2025-01-03T00:00:00Z",
                 "executive_summary": ["Error: Failed to create overview: RateLimitError(insufficient_quota; no credits remaining)"],
                 "top_items": [{"title": "Safe title", "url": "https://example.com", "bottom_line": "HTTP 429 provider error"}]}]
    original = json.dumps(entries, sort_keys=True)
    prepared, diag = rollups.prepare_yearly_rollups(entries, 2025)
    assert len(prepared) == 1 and diag["duplicate_monthly_rollups_suppressed_total"] == 1
    assert diag["found_months"] == ["2025-01"] and not diag["coverage_complete"]
    assert "RateLimitError" not in json.dumps(prepared)
    assert "Safe title" in prepared[0]["executive_summary"][0]
    assert json.dumps(entries, sort_keys=True) == original


@pytest.mark.parametrize("mode,event", [("real", "workflow_dispatch"), ("test", "schedule")])
def test_incomplete_production_blocks_before_email_and_is_readonly(tmp_path, monkeypatch, mode, event):
    with pytest.raises(RuntimeError, match="incomplete_coverage"):
        _run(tmp_path, monkeypatch, _months(count=11), mode=mode, event=event)
    # The read-only loader never rewrites the deliberately compact source file.
    path = tmp_path / "rollups.json"
    assert path.read_bytes() == json.dumps({"reports": {"cyberlurch": _months(count=11)}}).encode()


@pytest.mark.parametrize("mode,emails", [("none", 0), ("test", 1)])
def test_manual_partial_audit_visible_and_readonly(tmp_path, monkeypatch, mode, emails):
    path, original, sent, md = _run(tmp_path, monkeypatch, _months(count=11), mode=mode)
    assert path.read_bytes() == original
    assert len(sent) == emails and "This is a partial-year audit." in md


def test_complete_year_uses_semantic_renderer_and_one_provider_call(tmp_path, monkeypatch):
    path, original, sent, md = _run(tmp_path, monkeypatch, _months(), mode="real")
    assert path.read_bytes() == original and len(sent) == 1
    assert "Calendar coverage is complete." in md
    assert "## Major Trends" in md and "## Source Index" in md
    diag = json.loads((tmp_path / "reports" / "cyberlurch_yearly_diagnostics.json").read_text())
    assert diag["provider_operations"] == 1
    assert diag["collection_operations"] == diag["daily_digest_inputs"] == 0
