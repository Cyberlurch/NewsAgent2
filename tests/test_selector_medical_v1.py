from newsagent2.selector_medical import select_cybermed_pubmed_items, select_cybermed_foamed_items


def _cfg(tmp_path):
    p=tmp_path/'sel.json'
    p.write_text('{"enabled": true, "selection": {"min_score_overview": 1.0, "min_score_deep_dive": 1.0, "overview_max_per_run": 25, "deep_dive_max_per_run": 2, "domain_keywords": {"icu_ccm": ["icu","critical care"], "emergency_resus":["resuscitation"], "anesthesia_periop":["anesthesia"]}, "clinical_intent_keywords": {"design": ["trial","review"], "clinical": ["mortality","patient"]}}, "scoring": {}, "classification_keywords": {}, "deep_dive_scoring": {}}')
    return str(p)


def test_pubmed_rct_and_guideline_selection(tmp_path):
    items=[
        {"title":"ICU randomized trial mortality", "text":"Randomized controlled trial in ICU with mortality endpoint", "journal":"NEJM", "publication_types":["Randomized Controlled Trial"], "content_source":"pubmed_abstract", "content_length":320},
        {"title":"Guideline for sepsis", "text":"Practice guideline consensus for sepsis management", "journal":"JAMA", "publication_types":["Guideline"], "content_source":"pubmed_abstract", "content_length":280},
    ]
    res=select_cybermed_pubmed_items(items, config_path=_cfg(tmp_path))
    assert len(res.overview_items) == 2
    assert any(it.get("evidence_strength_score",0) >= 2 for it in res.overview_items)
    assert any("guideline_or_consensus" in (it.get("reason_labels") or []) for it in res.overview_items)


def test_pubmed_editorial_downranked_and_metadata_not_deep_dive(tmp_path):
    items=[
        {"title":"Editorial note", "text":"Opinion only", "publication_types":["Editorial"], "content_length":90},
        {"title":"Meta analysis outcomes", "text":"Systematic review meta-analysis mortality", "publication_types":["Meta-Analysis"], "content_source":"pubmed_abstract", "content_length":300},
    ]
    res=select_cybermed_pubmed_items(items, config_path=_cfg(tmp_path))
    all_labels = [lbl for it in (res.overview_items + items) for lbl in (it.get("reason_labels") or [])] if res.overview_items else []
    assert ("publication_type_low_priority" in all_labels) or (len(res.overview_items) < len(items))
    assert all((it.get("content_length_bucket") != "none") for it in res.deep_dive_items)


def test_foamed_top_pick_prefers_strong_text():
    items=[
        {"title":"Resuscitation appraisal", "text":"critical care resuscitation ventilation sedation appraisal "*20, "foamed_source":"Core ICU"},
        {"title":"Short comment", "text":"nice post", "foamed_source":"Core ICU"},
    ]
    res=select_cybermed_foamed_items(items, max_overview=10, max_top_picks=1)
    assert len(res.overview_items) >= 1
    assert len(res.top_picks) == 1
