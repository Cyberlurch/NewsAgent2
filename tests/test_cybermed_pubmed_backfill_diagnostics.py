from newsagent2.main import _pubmed_content_backfill_and_diagnostics


def test_pubmed_backfill_tags_and_counts():
    items = [
        {"id": "1", "abstract": "A" * 250, "abstract_sections": [{"label": "B", "text": "x"}], "publication_types": ["Trial"]},
        {"id": "2", "abstract": "", "full_text_excerpt": "F" * 500, "fulltext_source": "pmc_oa", "pmcid": "PMC1", "publication_types": ["Editorial"]},
        {"id": "3", "abstract": "", "doi": "10.1/x", "publication_types": ["Letter"]},
    ]
    d = _pubmed_content_backfill_and_diagnostics(items)
    assert d["pubmed_content_backfill_attempted_total"] == 2
    assert d["pubmed_items_metadata_only_total"] == 1
    assert items[0]["content_source"] == "pubmed_structured_abstract"
    assert items[1]["content_source"] == "pmc_oa_fulltext"
    assert items[2]["content_source"] == "metadata_only"
    assert items[2]["oa_status"] == "closed_or_unavailable"
    assert d["pubmed_metadata_only_publication_type_counts"]["Letter"] == 1


def test_diagnostics_no_raw_text_dump():
    items = [{"id": "x", "abstract": "short", "doi": "10.1/a"}]
    d = _pubmed_content_backfill_and_diagnostics(items)
    rendered = str(d)
    assert "API_KEY" not in rendered
    assert "@" not in rendered
    assert "<PubmedArticle" not in rendered
