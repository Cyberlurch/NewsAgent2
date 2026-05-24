from newsagent2.selector_medical import select_cybermed_pubmed_items, select_cybermed_foamed_items


def _cfg(tmp_path):
    p = tmp_path / 'sel.json'
    p.write_text('{"enabled": true, "selection": {"min_score_overview": 0.5, "min_score_deep_dive": 0.5, "overview_max_per_run": 25, "deep_dive_max_per_run": 5, "domain_keywords": {"icu_ccm": ["icu","critical care"], "emergency_resus":["resuscitation"], "anesthesia_periop":["anesthesia"]}, "clinical_intent_keywords": {"design": ["trial","review"], "clinical": ["mortality","patient"]}}, "scoring": {}, "classification_keywords": {}, "deep_dive_scoring": {}}')
    return str(p)


def test_low_evidence_news_excluded_and_radar(tmp_path):
    items = [{"title":"Ebola outbreak news", "text":"outbreak update", "publication_types":["News"], "content_length":120}]
    res = select_cybermed_pubmed_items(items, config_path=_cfg(tmp_path))
    assert len(res.overview_items) == 0
    diag = res.stats["selection_diagnostics"]
    assert diag["pubmed_low_evidence_radar_candidates_total"] >= 1


def test_low_priority_types_need_real_evidence_type(tmp_path):
    items = [
        {"title":"Editorial opinion", "text":"commentary", "publication_types":["Editorial"], "evidence_tags":["clinical_trial"], "content_length":100},
        {"title":"Guideline update", "text":"practice guideline ICU mortality recommendations", "publication_types":["Guideline"], "content_source":"pubmed_abstract", "content_length":300},
        {"title":"RCT ICU", "text":"randomized controlled trial ICU mortality", "publication_types":["Randomized Controlled Trial"], "content_source":"pubmed_abstract", "content_length":300},
        {"title":"Meta-analysis ICU", "text":"systematic review meta-analysis ICU mortality", "publication_types":["Meta-Analysis"], "content_source":"pubmed_abstract", "content_length":320},
    ]
    res = select_cybermed_pubmed_items(items, config_path=_cfg(tmp_path))
    titles = {it["title"] for it in res.overview_items}
    assert "Editorial opinion" not in titles
    assert "Guideline update" in titles
    assert "RCT ICU" in titles
    assert "Meta-analysis ICU" in titles


def test_top_pick_and_deep_dive_floor_reject_low_evidence(tmp_path):
    items = [
        {"title":"Comment", "text":"brief note", "publication_types":["Comment"], "content_length":80},
        {"title":"Strong guideline", "text":"practice guideline ICU mortality patient safety recommendation", "publication_types":["Practice Guideline"], "content_source":"pubmed_abstract", "content_length":420},
        {"title":"Metadata only", "text":"", "publication_types":["Randomized Controlled Trial"], "content_length":0},
    ]
    res = select_cybermed_pubmed_items(items, config_path=_cfg(tmp_path))
    assert all(it.get("publication_types") != ["Comment"] for it in res.deep_dive_items)
    assert all(not (it.get("publication_types") == ["Comment"] and it.get("top_pick")) for it in res.overview_items)


def test_diagnostics_preview_sanitized_and_audit_present(tmp_path):
    items = [{"title":"ICU trial", "text":"randomized controlled trial ICU mortality", "pmid":"123", "doi":"10/x", "url":"http://x", "publication_types":["Randomized Controlled Trial"], "content_source":"pubmed_abstract", "content_length":260}]
    res = select_cybermed_pubmed_items(items, config_path=_cfg(tmp_path))
    d = res.stats["selection_diagnostics"]
    for k in [
        "pubmed_raw_selection_audit_total",
        "pubmed_raw_selection_audit_overview_eligible_total",
        "pubmed_overview_eligible_after_type_floor_total",
        "pubmed_overview_excluded_by_type_floor_total",
        "pubmed_top_pick_floor_rejection_counts",
        "pubmed_deep_dive_floor_rejection_counts",
    ]:
        assert k in d
    preview = d["top_candidate_score_preview"][0]
    for bad in ["title", "abstract", "doi", "pmid", "url", "raw_html", "full_text", "article_body", "SMTP_PASS", "OPENAI_API_KEY", "RECIPIENTS_CONFIG_JSON"]:
        assert bad not in preview


def test_foamed_top_pick_prefers_strong_text():
    items=[
        {"title":"Resuscitation appraisal", "text":"critical care resuscitation ventilation sedation appraisal "*20, "foamed_source":"Core ICU"},
        {"title":"Short comment", "text":"nice post", "foamed_source":"Core ICU"},
    ]
    res=select_cybermed_foamed_items(items, max_overview=10, max_top_picks=1)
    assert len(res.overview_items) >= 1
    assert len(res.top_picks) == 1
