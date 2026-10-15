"""Synthetic paraphrases, not copied abstracts; all generation is mocked."""
import copy
import json
from types import SimpleNamespace

import pytest

from newsagent2 import clinical_editorial as policy, main, reporter, summarizer, selector_medical
from newsagent2.cybermed_digest_store import aggregate_cybermed_digest_inputs


def paper(*, population="Adult ICU patients", title="ICU delirium prevention trial", journal="Anesth Analg", pmid="42854247", background=""):
    # Audit case paraphrased: incomplete ascertainment, no significant benefit.
    abstract = (f"Background: {background} Clinical uncertainty motivated evaluation.\n"
        "Methods: Randomized trial comparing a decision-support intervention with usual care. The primary outcome was delirium.\n"
        f"Population: {population}.\n"
        "Results: 7294 were randomized; delirium assessments existed for only 1255. Delirium occurred in 225/641 versus 201/614 (35.1% versus 32.7%), P=.37.\n"
        "Conclusions: No statistically significant difference was demonstrated; equivalence was not established.\n"
        "Limitations: Missing assessments restrict interpretation.")
    return {"id": pmid, "pmid": pmid, "source": "pubmed", "title": title, "journal": journal,
            "abstract": abstract, "text": abstract, "publication_types": ["Randomized Controlled Trial"],
            "content_source": "pubmed_abstract", "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"}


def ready(**kwargs):
    item = paper(**kwargs)
    selector_medical._attach_evidence_hint_labels(item)
    item.update(evidence_strength_label="A", clinical_relevance_1_5=4, practice_change_potential_1_5=3, top_pick=True)
    return item


def test_source_context_beats_pain_journal_and_long_background():
    item = paper(title="Life-sustaining treatment guidance in the ICU", journal="Reg Anesth Pain Med", pmid="42858614", background="Background context. " * 220)
    ctx = policy.context_for(item)
    assert ctx["track"] == "Critical Care" and ctx["subcategory"] != "Pain/Regional Anesthesia"
    assert ctx["population"] == "adult" and ctx["result_basis"] == "primary_reported"
    assert reporter._infer_track_and_subcategory(item)[0] == "Critical Care"
    assert "reg anesth pain med" not in selector_medical._text_haystack(item)


@pytest.mark.parametrize("population,expected", [
    ("Pediatric ICU children", "pediatric"), ("Neonatal intensive care newborns", "neonatal"),
    ("Adults and children in intensive care", "mixed"), ("ICU patients", "unknown"),
    ("Adult ICU patients, excluding children", "adult"),
])
def test_population_comes_from_subjects_not_journal(population, expected):
    item = paper(population=population, journal="Pediatric Critical Care Medicine")
    ctx = policy.context_for(item)
    assert ctx["population"] == expected
    assert f"Population {expected}" in policy.context_line(item)


@pytest.mark.parametrize("journal", ["N Engl J Med", "Nature", "Science", "JAMA", "Lancet", "BMJ"])
def test_general_journal_filter_is_local_and_content_based(journal):
    good = paper(journal=journal)
    bad = {"id": "999", "source": "pubmed", "title": "Prospective performance of a materials prediction model", "journal": journal,
           "text": "Methods: A randomized laboratory comparison of polymer manufacturing. Results: Material strength improved.", "publication_types": ["Journal Article"]}
    result = selector_medical.select_cybermed_pubmed_items([good, bad], config_path='data/cybermed_selection.json')
    assert good['id'] in {row['id'] for row in result.overview_items}
    assert bad['id'] not in {row['id'] for row in result.overview_items}


def test_long_incomplete_abstract_is_not_high_certainty():
    item = paper()
    item['abstract'] = item['text'] = 'Background: ' + 'Clinical context is discussed. ' * 250
    selector_medical._attach_evidence_hint_labels(item)
    assert item['clinical_context']['result_basis'] == 'unknown'
    assert item['text_confidence_label'] != 'high'
    line = reporter._pubmed_compact_line(item)
    assert 'Confidence High' not in line
    assert 'not GRADE' not in line and 'not effect certainty' not in line
    assert 'Primary results unknown' in item['clinical_context']['uncertainty']


@pytest.mark.parametrize('relevance,impact', [(2, 1), (5, 1), (1, 5), (None, None), (True, 4)])
def test_display_score_floors_prevent_unjustified_favorite(relevance, impact):
    item = ready(pmid='42855372')
    item.update(clinical_relevance_1_5=relevance, practice_change_potential_1_5=impact)
    policy.finalize_favorite(item, cadence='daily')
    assert not item['top_pick'] and item['top_pick_decision']['reason'] == 'display_score_floor'


def test_null_result_can_be_important_without_becoming_benefit():
    item = ready()
    policy.finalize_favorite(item, cadence='daily')
    assert item['top_pick']
    assert item['clinical_context']['result_direction'] == 'negative_or_null'
    assert item['top_pick_decision']['reason'] == 'clinical_priority_not_proven_benefit'
    assert 'no-effect/equivalence not established' in policy.context_line(item)


def test_unmapped_secondary_result_cannot_support_a_primary_benefit_favorite():
    item = ready()
    item.pop('clinical_context')
    item['abstract'] = item['text'] = 'Methods: Randomized trial of adult ICU patients.\nResults: A biomarker improved in the intervention group.\nConclusions: Further study is needed.'
    policy.attach_context(item)
    policy.finalize_favorite(item, cadence='daily')
    assert item['clinical_context']['result_basis'] == 'unknown'
    assert item['clinical_context']['result_direction'] == 'unknown'
    assert not item['top_pick']


def test_reported_conditional_guideline_is_not_an_automatic_strong_recommendation():
    item = ready(title='ICU assessment guideline')
    item.pop('clinical_context')
    item['publication_types'] = ['Practice Guideline']
    item['abstract'] = item['text'] = 'Population: Adult ICU patients.\nConclusions: We conditionally suggest a structured assessment process, with limited evidence.'
    policy.attach_context(item)
    policy.finalize_favorite(item, cadence='daily')
    assert item['clinical_context']['result_basis'] == 'recommendation_reported'
    assert item['clinical_context']['result_direction'] == 'unknown'
    assert item['top_pick_decision']['reason'] == 'clinical_priority_not_proven_benefit'


def test_observational_core_is_not_promoted_to_causal_result():
    item = paper()
    item['publication_types'] = ['Observational Study']
    item['abstract'] = item['text'] = 'Methods: Observational cohort of adult ICU patients. The primary outcome was mortality.\nResults: Exposure was associated with lower mortality.\nConclusions: Confounding limits interpretation.'
    from newsagent2.pubmed_facts import build_pubmed_facts
    facts = build_pubmed_facts(item)
    assert facts.observational
    assert 'associated' in facts.core().lower()
    assert 'caus' not in facts.core().lower() or 'not' in facts.core().lower()


def test_selector_main_renderer_and_stored_aggregate_share_decision_and_context():
    row = ready()
    policy.finalize_favorite(row, cadence='daily')
    daily = main._sanitize_cybermed_pubmed_item(row)
    daily['limitations'] = ['Missing assessments.', 'No full-text assessment.']
    snapshot = copy.deepcopy(daily)
    aggregate = aggregate_cybermed_digest_inputs([{'run_date': '2026-10-12', 'items': {'pubmed': [daily], 'foamed': []}}], pubmed_cap=20, foamed_cap=12, deep_dive_cap=8)
    weekly = aggregate['pubmed'][0]
    assert weekly['top_pick'] and weekly['top_pick_decision']['cadence'] == 'weekly'
    monthly = main._sanitize_cybermed_monthly_rollup_item(weekly)
    assert monthly['clinical_context'] == daily['clinical_context']
    assert monthly['limitations'] == daily['limitations']
    assert daily == snapshot
    md = reporter.to_markdown([row], '', {row['id']: 'Study type: Trial\nPopulation/setting: Adult ICU patients\nBOTTOM LINE: No benefit demonstrated.'}, report_title='Cybermed Daily', report_language='en', report_mode='daily')
    assert '⭐ Top pick' in md and 'Evidence A' in md
    assert 'not GRADE' not in md and 'not effect certainty' not in md
    assert 'Population adult ·' not in md
    from newsagent2.rollups import render_yearly_markdown
    year = render_yearly_markdown(report_title='Cybermed', report_language='en', year=2026, rollups=[{'month': '2026-10', 'cybermed_items': [monthly]}])
    assert 'Population adult ·' not in year and 'Missing assessments.' in year
    assert "['Missing assessments.'" not in year


def test_legacy_ranking_is_preserved_without_claiming_new_validation():
    legacy = {'source': 'pubmed', 'source_type': 'pubmed', 'item_id': '1', 'title': 'Stored paper', 'top_pick': True, 'bottom_line': 'Stored statement.'}
    before = copy.deepcopy(legacy)
    out = aggregate_cybermed_digest_inputs([{'run_date': '2026-08-01', 'items': {'pubmed': [legacy]}}], pubmed_cap=20, foamed_cap=12, deep_dive_cap=8)
    assert out['top_picks'] == ['1']  # Historical policy, not new clinical approval.
    row = out['pubmed'][0]
    assert row['stored_top_pick']['selected'] is True
    assert row['top_pick_decision']['reason'] == 'legacy_period_ranking_not_revalidated'
    md = reporter.to_markdown([row], '', {}, report_title='Cybermed Weekly', report_mode='weekly', report_language='en')
    assert '⭐ Top pick' in md and 'Stored ranking (not revalidated)' not in md
    assert row['top_pick_decision']['reason'] == 'legacy_period_ranking_not_revalidated'
    assert 'Unknown (legacy basis)' not in md and legacy == before


@pytest.mark.parametrize('value', [['First limitation.', 'Second limitation.'], 'First limitation; second limitation.'])
def test_limitation_types_remain_readable_in_saved_deep_dive(value):
    row = ready()
    row['limitations'] = value
    saved = main._sanitize_cybermed_pubmed_item(row)
    assert saved['limitations'] == value
    block = reporter._stored_cybermed_deep_dive_block({**saved, 'study_type': 'Randomized trial', 'population_setting': 'ICU patients'})
    assert 'First limitation' in block and "['" not in block


@pytest.mark.parametrize('mode', ['none', 'test'])
def test_main_deep_dive_cannot_override_low_display_scores(monkeypatch, tmp_path, mode):
    from test_cybermed_daily_digest_store import _base_env, _with_nonempty_selection
    _base_env(monkeypatch, tmp_path)
    _with_nonempty_selection(monkeypatch)
    monkeypatch.setenv('EMAIL_MODE', mode)
    item = paper(pmid='1')
    item.update(clinical_relevance_1_5=2, practice_change_potential_1_5=1, top_pick=False)
    monkeypatch.setattr(selector_medical, 'select_cybermed_pubmed_items', lambda rows: SimpleNamespace(overview_items=[item], deep_dive_items=[item], stats={'selection_diagnostics': {}}))
    monkeypatch.setattr(main, 'summarize_item_detail', lambda *a, **k: 'Study type: Randomized trial\nPopulation/setting: Adult ICU patients\nKey results: No significant difference, P=.37.\nBOTTOM LINE: No benefit demonstrated.')
    from newsagent2.run_context import PATHS, path_for
    before = {path_for(name): path_for(name).read_bytes() if path_for(name).exists() else None for name in PATHS}
    main.main()
    reports = list((tmp_path/'out').glob('*.md'))
    assert reports
    md = '\n'.join(p.read_text() for p in reports)
    assert '⭐ Top pick' not in md
    assert item.get('top_pick') is False
    for path, raw in before.items():
        assert (path.read_bytes() if path.exists() else None) == raw


def foamed_item():
    return {'source': 'foamed', 'title': 'A bedside approach to ICU delirium assessment', 'url': 'https://example.test/commentary',
        'source_role': 'commentary', 'primary_references': [{'kind': 'pmid', 'value': '123', 'url': 'https://pubmed.ncbi.nlm.nih.gov/123/'}],
        'text': 'The authors teach a structured bedside approach to delirium assessment in the ICU. They describe the timing of observations and the limits of incomplete documentation. This is educational commentary, with no independent intervention trial.'}


def fake_generation(monkeypatch, content, finish='stop'):
    calls = []
    monkeypatch.setattr(summarizer, '_is_cybermed_generation', lambda: True)
    monkeypatch.setattr(summarizer, '_get_client', lambda: object())
    def invoke(*a, **kw):
        calls.append(kw)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content), finish_reason=finish)])
    monkeypatch.setattr(summarizer, '_openai_chat_create', invoke)
    return calls


def test_foamed_grounded_learning_uses_existing_call_budget_and_provenance(monkeypatch):
    calls = fake_generation(monkeypatch, 'BOTTOM LINE: The authors explain a structured ICU delirium assessment approach and the limits of incomplete documentation.')
    result = summarizer.summarize_foamed_bottom_line(foamed_item())
    assert 'authors explain' in result
    assert len(calls) == 1 and calls[0]['max_tokens'] == 120
    request = json.loads(calls[0]['messages'][1]['content'].split('Item (JSON):\n')[1].split('\n\nNow write')[0])
    assert request['source_role'] == 'commentary'
    assert request['primary_references'][0]['value'] == '123'
    assert len(request['text']) + len(json.dumps(request['primary_references'])) + len(request['source_role']) <= 1500
    md = reporter.to_markdown([{**foamed_item(), 'bottom_line': result}], '', {}, report_title='Cybermed Daily', report_language='en', report_mode='daily')
    assert '[PMID 123](https://pubmed.ncbi.nlm.nih.gov/123/)' in md
    assert 'Commentary' in md


@pytest.mark.parametrize('content,finish', [
    ('BOTTOM LINE: An unfinished explanation', 'length'),
    ('BOTTOM LINE: A claim from https://invented.test/trial.', 'stop'),
    ('BOTTOM LINE: This is a commentary with no original research.', 'stop'),
    ('BOTTOM LINE: This ICU assessment prevents delirium in patients.', 'stop'),
    ('BOTTOM LINE: The authors recommend 99 assessment rounds per day in the ICU.', 'stop'),
])
def test_uncertain_foamed_output_is_omitted_without_extra_calls(monkeypatch, content, finish):
    calls = fake_generation(monkeypatch, content, finish)
    assert summarizer.summarize_foamed_bottom_line(foamed_item()) == ''
    assert len(calls) == 1


def test_thin_foamed_does_not_call_a_provider(monkeypatch):
    monkeypatch.setattr(summarizer, '_get_client', lambda: pytest.fail('thin source provider call'))
    assert summarizer.summarize_foamed_bottom_line({'title': 'A title', 'text': 'A short excerpt.'}) == ''


def test_missing_learning_summary_cannot_remain_a_new_foamed_favorite():
    item = foamed_item()
    selector_medical._attach_evidence_hint_labels(item, foamed=True)
    item.update(source_quality_label='core', text_confidence_label='moderate', top_pick=True,
                clinical_usefulness_1_5=4, practice_relevance_1_5=4)
    policy.finalize_favorite(item, cadence='daily')
    assert item['top_pick'] is False and item['top_pick_decision']['reason'] == 'learning_summary_unavailable'


def test_long_reference_is_not_truncated_into_a_new_model_url(monkeypatch):
    calls = fake_generation(monkeypatch, 'BOTTOM LINE: The authors explain ICU delirium assessment and incomplete documentation.')
    item = foamed_item()
    item['primary_references'][0]['url'] = 'https://example.test/' + 'x'*200
    assert summarizer.summarize_foamed_bottom_line(item)
    request = json.loads(calls[0]['messages'][1]['content'].split('Item (JSON):\n')[1].split('\n\nNow write')[0])
    assert request['primary_references'] == []
