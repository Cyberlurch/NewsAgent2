import json
import sys
from datetime import datetime, timezone

from newsagent2 import main


def test_cybermed_run_writes_daily_foundation_diagnostics_and_cyberlurch_unchanged(tmp_path, monkeypatch):
    report_dir = tmp_path / "out"
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(report_dir))
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({
        "foamed_source_health": {
            "Disabled One": {
                "last_health": "failed",
                "consecutive_failures": 5,
                "disabled_until_utc": "2026-05-30T00:00:00+00:00"
            }
        }
    }), encoding="utf-8")
    monkeypatch.setenv("STATE_PATH", str(state_path))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("FOAMED_AUDIT", "1")
    monkeypatch.setenv("CYBERMED_MAX_ITEMS_PER_CHANNEL", "25")
    monkeypatch.setattr(sys, "argv", ["newsagent2-main"])

    def fake_channels(_path):
        return ([{"name": "PubMed: Test", "topic": "Critical Care", "source": "pubmed", "query": "test[Title]"}], {}, {})

    seen_max_items = {}
    def fake_pubmed(*args, **kwargs):
        seen_max_items["value"] = kwargs.get("max_items")
        rows = [{
            "id": "123",
            "title": "A paper",
            "url": "https://pubmed.ncbi.nlm.nih.gov/123/",
            "published_at": datetime.now(timezone.utc),
            "journal": "J",
            "doi": "10.1/x",
            "text": "summary text",
            "abstract": "short abstract",
            "publication_types": ["Journal Article"],
            "mesh_headings": ["Intensive Care Units"],
            "keywords": ["resuscitation"],
            "abstract_sections": [{"label": "Background", "text": "x"}],
            "evidence_tags": ["intensive_care", "resuscitation"],
        }]
        if kwargs.get("return_metadata"):
            return rows, {"query_term": kwargs.get("term",""), "retmax": kwargs.get("max_items",0), "esearch_count_total": 40, "idlist_count": len(rows), "fetched_xml_count": len(rows), "parsed_article_count": len(rows), "possibly_truncated": True}
        return rows

    monkeypatch.setattr(main, "load_channels_config", fake_channels)
    monkeypatch.setattr(main, "search_recent_pubmed", fake_pubmed)
    monkeypatch.setattr(main, "load_foamed_sources_config", lambda _p: [{"name": "Src", "rss_url": "https://example.com/rss"}, {"name": "Disabled One", "rss_url": "https://example.com/rss2"}])
    foamed_stats = {"sources_total": 1, "sources_ok": 1, "sources_failed": 0, "items_raw": 0, "items_with_date": 0, "items_date_unknown": 0, "kept_last24h": 0, "newly_disabled_count": 0, "per_source": {"Src": {"health": "ok", "method": "feed", "why": "fresh", "feed_ok": 1, "feed_failed": 0, "html_fallback_used": 0, "entries_total": 5, "entries_with_date": 4, "items_raw": 4, "items_with_date": 4, "items_date_unknown": 0, "kept_last24h": 2, "feed_status_code": 200, "homepage_status_code": 200, "candidates_found": 2, "pages_fetched": 1, "error": ""}}, "audit": {"enabled": True, "sources": {"Src": {"rss_items_seen": 5, "rss_items_in_window": 2, "html_candidates_seen": 1, "html_items_in_window": 1, "html_not_in_rss_count": 0, "rss_not_in_html_count": 1, "audit_pages_fetched": 1, "content_mode": "rss_excerpt", "completeness_warning": ["rss_excerpt_only"]}}}}
    monkeypatch.setattr(main, "collect_foamed_items", lambda *a, **k: ([], foamed_stats))


    main.main()

    diag_path = report_dir / "cybermed_daily_diagnostics.json"
    assert diag_path.exists()
    diag = json.loads(diag_path.read_text(encoding="utf-8"))

    assert seen_max_items.get("value") == 25
    assert "pubmed_items_raw_total" in diag
    assert "foamed_sources_total" in diag
    assert "pubmed_items_with_publication_types_total" in diag
    assert "pubmed_items_with_mesh_headings_total" in diag
    assert "pubmed_items_with_keywords_total" in diag
    assert "pubmed_items_with_abstract_sections_total" in diag
    assert "pubmed_publication_type_counts" in diag
    assert "pubmed_raw_items_with_publication_types_total" in diag
    assert "pubmed_raw_items_with_mesh_headings_total" in diag
    assert "pubmed_raw_items_with_keywords_total" in diag
    assert "pubmed_raw_items_with_abstract_sections_total" in diag
    assert "pubmed_raw_publication_type_counts" in diag
    assert "pubmed_raw_evidence_tag_counts" in diag
    assert "pubmed_raw_mesh_heading_top_counts" in diag
    assert "pubmed_raw_keyword_top_counts" in diag
    assert "pubmed_evidence_tag_counts" in diag
    assert "pubmed_mesh_heading_top_counts" in diag
    assert "pubmed_keyword_top_counts" in diag
    assert len(diag["pubmed_publication_type_counts"]) <= 20
    assert len(diag["pubmed_evidence_tag_counts"]) <= 30
    assert len(diag["pubmed_raw_publication_type_counts"]) <= 20
    assert len(diag["pubmed_raw_evidence_tag_counts"]) <= 30
    assert len(diag["pubmed_raw_mesh_heading_top_counts"]) <= 30
    assert len(diag["pubmed_raw_keyword_top_counts"]) <= 30
    assert diag["pubmed_raw_items_with_publication_types_total"] >= diag["pubmed_items_with_publication_types_total"]
    assert diag["foamed_sources_config_total"] == 2
    assert diag["foamed_sources_processed_total"] == 1
    assert diag["foamed_sources_skipped_disabled_total"] == 1
    assert diag["foamed_auto_disable_disabled_active_count"] == 1
    assert diag["foamed_disabled_sources"][0]["name"] == "Disabled One"
    assert "selection_counts" in diag
    assert "selection_diagnostics" in diag
    sd = diag["selection_diagnostics"]
    for key in [
        "excluded_overview_offtopic",
        "below_threshold_overview",
        "excluded_by_allowlist",
        "publication_type_penalty_hits",
        "title_penalty_hits",
        "tier_counts",
        "domain_signal_counts",
        "clinical_intent_counts",
        "top_candidate_score_preview",
    ]:
        assert key in sd
    assert len(sd["top_candidate_score_preview"]) <= 10
    if sd["top_candidate_score_preview"]:
        preview_blob = json.dumps(sd["top_candidate_score_preview"])
        assert "title" not in preview_blob
        assert "abstract" not in preview_blob
        assert "url" not in preview_blob
        assert "doi" not in preview_blob
        assert "pmid" not in preview_blob
    assert diag["pubmed_items_skipped_by_state_total"] == 0
    assert diag["foamed_items_after_state_filter_total"] == 0
    assert "pubmed_window" in diag
    assert isinstance(diag.get("pubmed_journals"), list)


    assert diag["pubmed_channels_possibly_truncated_total"] >= 1
    assert "channel_hit_retmax_cap" in diag["pubmed_raw_completeness_warnings"]
    assert "pubmed_raw_items_missing_abstract_total" in diag
    assert "pubmed_raw_items_missing_publication_types_total" in diag
    pc = diag["pubmed_per_channel"][0]
    for k in ["esearch_count_total","retmax","idlist_count","parsed_article_count","possibly_truncated","publication_types_count","mesh_headings_count","keywords_count","abstract_sections_count","abstract_count","doi_count"]:
        assert k in pc
    assert diag.get("foamed_audit_enabled") is True
    assert "foamed_audit_summary" in diag

    fs = diag["foamed_per_source"][0]
    assert fs["health"] == "ok"
    assert fs["method"] == "feed"
    assert fs["entries_total"] == 5
    assert fs["kept_in_window_count"] == 2
    assert fs["html_fallback_used"] == 0

    as_text = json.dumps(diag)
    forbidden = ["SMTP_PASS", "OPENAI_API_KEY", "RECIPIENTS_CONFIG_JSON", "raw_xml", "raw_html", "full_text", "transcript", "abstract text", "@"]
    for key in forbidden:
        assert key not in as_text

    assert not (report_dir / "cyberlurch_daily_youtube_diagnostics.json").exists()
