from datetime import datetime, timezone

from src.newsagent2 import collectors_pubmed
from src.newsagent2.collectors_pubmed import _parse_pubmed_xml


def test_parse_pubmed_xml_enriches_metadata_and_keeps_doi_behavior():
    xml = """
    <PubmedArticleSet>
      <PubmedArticle>
        <MedlineCitation>
          <PMID>111</PMID>
          <Article>
            <ArticleTitle>Trial in intensive care</ArticleTitle>
            <Abstract>
              <AbstractText Label="Background">Line one.</AbstractText>
              <AbstractText Label="Methods">Randomized cohort in ICU.</AbstractText>
            </Abstract>
            <PublicationTypeList>
              <PublicationType>Journal Article</PublicationType>
              <PublicationType>Randomized Controlled Trial</PublicationType>
            </PublicationTypeList>
            <KeywordList>
              <Keyword>Sepsis</Keyword>
              <Keyword>Machine learning</Keyword>
            </KeywordList>
          </Article>
          <MeshHeadingList>
            <MeshHeading><DescriptorName>Intensive Care Units</DescriptorName></MeshHeading>
            <MeshHeading><DescriptorName>Resuscitation</DescriptorName></MeshHeading>
          </MeshHeadingList>
        </MedlineCitation>
        <PubmedData>
          <ArticleIdList>
            <ArticleId IdType="pubmed">111</ArticleId>
            <ArticleId IdType="doi">10.1000/test-doi</ArticleId>
            <ArticleId IdType="pmc">PMC1234</ArticleId>
            <ArticleId IdType="pii">S1234-5678(26)00001-2</ArticleId>
          </ArticleIdList>
        </PubmedData>
      </PubmedArticle>
    </PubmedArticleSet>
    """
    items = _parse_pubmed_xml(xml, max_items=5)
    assert len(items) == 1
    item = items[0]
    assert item["doi"] == "10.1000/test-doi"
    assert item["publication_types"] == ["Journal Article", "Randomized Controlled Trial"]
    assert item["mesh_headings"] == ["Intensive Care Units", "Resuscitation"]
    assert item["keywords"] == ["Sepsis", "Machine learning"]
    assert item["pmcid"] == "PMC1234"
    assert item["pii"] == "S1234-5678(26)00001-2"
    assert item["abstract_sections"][0]["label"] == "Background"
    assert item["abstract_sections"][1]["label"] == "Methods"
    assert "randomized_trial" in item["evidence_tags"]
    assert "intensive_care" in item["evidence_tags"]
    assert "ai_prediction_model" in item["evidence_tags"]


def test_parse_pubmed_xml_missing_optional_metadata_does_not_crash():
    xml = """
    <PubmedArticleSet>
      <PubmedArticle>
        <MedlineCitation>
          <PMID>222</PMID>
          <Article>
            <ArticleTitle>Short note</ArticleTitle>
          </Article>
        </MedlineCitation>
      </PubmedArticle>
    </PubmedArticleSet>
    """
    items = _parse_pubmed_xml(xml, max_items=5)
    assert len(items) == 1
    item = items[0]
    assert item["publication_types"] == []
    assert item["mesh_headings"] == []
    assert item["keywords"] == []
    assert item["abstract_sections"] == []
    assert item["evidence_tags"] == []
    assert item["doi"] == ""


def test_search_uses_explicit_historical_reference_time(monkeypatch):
    captured = {}

    def fake_request_json(_url, params, timeout_s=25):
        captured.update(params)
        return {"esearchresult": {"idlist": [], "count": "0"}}

    monkeypatch.setattr(collectors_pubmed, "_request_json", fake_request_json)

    collectors_pubmed.search_recent_pubmed(
        term="critical care",
        hours=72,
        max_items=25,
        reference_now_utc=datetime(2026, 8, 10, 5, 11, tzinfo=timezone.utc),
    )

    assert captured["mindate"] == "2026/08/07"
    assert captured["maxdate"] == "2026/08/10"
