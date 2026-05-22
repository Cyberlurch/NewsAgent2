import json
import sys
from datetime import datetime, timezone

from newsagent2 import main


def test_cybermed_run_writes_daily_foundation_diagnostics_and_cyberlurch_unchanged(tmp_path, monkeypatch):
    report_dir = tmp_path / "out"
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(report_dir))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])

    def fake_channels(_path):
        return ([{"name": "PubMed: Test", "topic": "Critical Care", "source": "pubmed", "query": "test[Title]"}], {}, {})

    def fake_pubmed(*args, **kwargs):
        return [{
            "id": "123",
            "title": "A paper",
            "url": "https://pubmed.ncbi.nlm.nih.gov/123/",
            "published_at": datetime.now(timezone.utc),
            "journal": "J",
            "doi": "10.1/x",
            "text": "summary text",
            "abstract": "short abstract",
            "publication_types": ["Journal Article"],
        }]

    monkeypatch.setattr(main, "load_channels_config", fake_channels)
    monkeypatch.setattr(main, "search_recent_pubmed", fake_pubmed)
    monkeypatch.setattr(main, "load_foamed_sources_config", lambda _p: [{"name": "Src", "rss_url": "https://example.com/rss"}])
    monkeypatch.setattr(main, "collect_foamed_items", lambda *a, **k: ([], {"sources_total": 1, "sources_ok": 1, "sources_failed": 0, "items_raw": 0, "items_with_date": 0, "items_date_unknown": 0, "kept_last24h": 0, "per_source": {"Src": {"status": "ok", "rss_attempted": 1, "html_fallback_attempted": 0, "raw_count": 0, "kept_count": 0}}}))

    main.main()

    diag_path = report_dir / "cybermed_daily_diagnostics.json"
    assert diag_path.exists()
    diag = json.loads(diag_path.read_text(encoding="utf-8"))

    assert "pubmed_items_raw_total" in diag
    assert "foamed_sources_total" in diag
    assert "selection_counts" in diag
    assert diag["pubmed_items_skipped_by_state_total"] == 0
    assert diag["foamed_items_after_state_filter_total"] == 0
    assert "pubmed_window" in diag
    assert isinstance(diag.get("pubmed_journals"), list)

    as_text = json.dumps(diag)
    forbidden = ["SMTP_PASS", "OPENAI_API_KEY", "RECIPIENTS_CONFIG_JSON", "raw_html", "full_text", "abstract text", "@"]
    for key in forbidden:
        assert key not in as_text

    assert not (report_dir / "cyberlurch_daily_youtube_diagnostics.json").exists()
