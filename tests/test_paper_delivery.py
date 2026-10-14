"""Receipt-based paper decisions with invented metadata, never live providers."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
import copy
import json
import os

import pytest

from newsagent2 import paper_delivery as p, delivery, pubmed_ingestion as ingestion
from newsagent2.run_context import path_for, execution_context
from newsagent2.store_io import atomic_store, StoreError
from test_pubmed_ingestion import (REFERENCE, production, mock_source, configure_main, xml, response, http)


def paper(pmid='1',doi='10.1000/trial',*,result='Primary mortality was 18% versus 20%; P=.37.'):
    return {'id':pmid,'pmid':pmid,'source':'pubmed','title':'Trial in intensive care','doi':doi,
            'url':f'https://pubmed.ncbi.nlm.nih.gov/{pmid}/','publication_types':['Randomized Controlled Trial'],
            'abstract':'Methods: Adults were randomized to intervention versus control. Primary outcome was mortality.\nResults: '+result+'\nConclusions: Uncertainty remains.',
            'text':'Raw collector text', 'comments_corrections':[]}


def receipt(item=None,*,day='2026-10-01',status='accepted_by_smtp',report_key='cybermed',cadence='daily',mode='real',legacy=False):
    co=delivery.Coordinator(report_key,cadence,mode=mode,period_dates=(day,day))
    rows=[]
    if item is not None:
        row={k:item[k] for k in ('id','title','url')}
        row['source']=item.get('source','pubmed')
        if not legacy:row['paper_delivery']=p.paper_metadata(item)
        rows=[row]
    intents={'items_all_new':rows,'overview_items':rows,'detail_items':[],'foamed_overview_items':[],
             'report_key':report_key,'report_mode':cadence,'now_utc_iso':day+'T03:40:00+00:00'}
    result=co.generated('A validated mock report '+day,intents)
    result.status=status;result.state_persisted=True
    result.accepted=['a'*64] if status in {'accepted_by_smtp','partially_accepted'} else []
    result.rejected=['c'*64] if status=='partially_accepted' else []
    result.unknown=['a'*64] if status=='outcome_unknown' else []
    result.recipient_key_id='b'*64
    if status=='failed':result.category='all_recipients_rejected'
    co._persist(result)
    return result


def ledger(**kwargs):
    return p.PaperLedger(delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH')),**kwargs)


def test_fingerprint_ignores_models_metadata_html_and_whitespace():
    original=paper()
    changed=copy.deepcopy(original)
    changed.update(bottom_line='Changed model prose',query_provenance=['other'],channel='different',generated_at='later')
    changed['abstract']=changed['abstract'].replace('Adults','<b>Adults</b>').replace(' were ', '   were\n')
    assert p.paper_metadata(original)['source_version']==p.paper_metadata(changed)['source_version']
    assert p.metadata_valid(p.paper_metadata(original))


@pytest.mark.parametrize('doi',['https://doi.org/10.1000/TRIAL','http://dx.doi.org/10.1000/trial','doi: 10.1000/trial','10.1000%2Ftrial'])
def test_doi_normalization(doi):assert p.normalize_doi(doi)=='10.1000/trial'


@pytest.mark.parametrize('doi',['not a DOI','https://evil.test/10.1000/trial','10.1000/a b'])
def test_unproven_doi_is_not_identity(doi):assert p.normalize_doi(doi)==''


@pytest.mark.parametrize('days',[3,21,365])
def test_no_ttl_after_pruning_and_restart(monkeypatch,days):
    receipt(paper(),day=(REFERENCE-timedelta(days=days)).date().isoformat())
    decision=ledger().decide(p.paper_metadata(paper()))
    assert decision.action=='duplicate' and decision.category=='paper_already_accepted'
    monkeypatch.setattr(p,'build_pubmed_facts',lambda *a:pytest.fail('No model/facts reconstruction from receipt needed'))
    # The already captured metadata can be checked again after process restart.
    stored=delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions']
    metadata=next(iter(stored.values()))['finalization']['overview_items'][0]['paper_delivery']
    assert ledger().decide(metadata).action=='duplicate'


def test_same_doi_distinct_pmid_alias_requires_identical_source():
    receipt(paper())
    assert ledger().decide(p.paper_metadata(paper('2'))).action=='duplicate'
    changed=paper('2');changed['abstract']+='Additional unrelated findings.'
    assert ledger().decide(p.paper_metadata(changed)).category=='paper_alias_conflict'
    assert ledger().decide(p.paper_metadata(paper('1','10.1000/other'))).category=='paper_alias_conflict'


@pytest.mark.parametrize('status',['generated','failed'])
def test_generation_or_rejection_is_not_acceptance(status):
    receipt(paper(),status=status)
    decision=ledger().decide(p.paper_metadata(paper()))
    assert decision.action=='review' and decision.category=='saved_generation_requires_recovery'


@pytest.mark.parametrize('status',['partially_accepted','outcome_unknown'])
def test_uncertain_prior_edition_protects_new_day_and_version(status):
    old=receipt(paper(),status=status)
    for item in (paper(),paper(result='Primary mortality was 8% versus 20%; P=.01.')):
        decision=ledger().decide(p.paper_metadata(item))
        assert decision.action=='review' and old.edition_id in decision.edition_ids


@pytest.mark.parametrize('status',['partially_accepted','outcome_unknown'])
def test_unmapped_uncertainty_blocks_any_new_paper(status):
    receipt(status=status)
    assert ledger().decide(p.paper_metadata(paper('999'))).category=='unmapped_delivery_uncertain'


@pytest.mark.parametrize('mode',['none','test'])
def test_preview_receipts_cannot_create_production_acceptance(mode):
    receipt(paper(),mode=mode)
    assert ledger().decide(p.paper_metadata(paper())).category=='new_paper'


@pytest.mark.parametrize('cadence',['weekly','monthly','yearly'])
def test_aggregate_receipts_do_not_lock_daily(cadence):
    receipt(paper(),cadence=cadence)
    assert ledger().decide(p.paper_metadata(paper())).action=='include'


def test_newsletters_are_separate():
    receipt(paper(),report_key='cyberlurch')
    assert ledger().decide(p.paper_metadata(paper())).action=='include'
    assert ledger(report_key='cyberlurch').decide(p.paper_metadata(paper())).action=='duplicate'


def test_only_reported_papers_not_all_screened_items_count():
    r=receipt(paper())
    row=delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))['editions'][r.edition_id]
    row['finalization']['items_all_new'].append({'source':'pubmed','id':'2'})
    view=p.PaperLedger({'editions':{r.edition_id:row}})
    assert view.decide(p.paper_metadata(paper('2',doi=''))).action=='include'


def test_legacy_accepted_version_unknown_is_not_invented():
    receipt(paper(),legacy=True)
    assert ledger().decide(p.paper_metadata(paper())).category=='accepted_paper_version_unknown'


def test_old_sent_marker_and_digest_without_receipt_are_unknown():
    state={'reports':{'cybermed':{'pubmed':{'processed':{'1':{'sent_overview_at_utc':'2026-01-01'}}}}}}
    digests={'digests':[{'report_key':'cybermed','cadence':'daily','run_date':'2026-01-01','items':{'pubmed':[{'pmid':'2'}]}}]}
    view=ledger(state=state,legacy_digests=digests)
    assert view.decide(p.paper_metadata(paper())).category=='legacy_delivery_unknown'
    assert view.decide(p.paper_metadata(paper('2'))).category=='legacy_delivery_unknown'


@pytest.mark.parametrize('audience,key', [({'d'*64},'b'*64),({'a'*64},'e'*64)])
def test_audience_or_key_change_requires_review(audience,key):
    receipt(paper())
    assert ledger(audience=audience,key_id=key).decide(p.paper_metadata(paper())).category=='paper_audience_changed'


def test_explicit_primary_result_change_is_a_versioned_update():
    receipt(paper())
    updated=paper(result='Primary mortality was 8% versus 20%; P=.01.')
    assert ledger().decide(p.paper_metadata(updated)).category=='reported_results_update'
    receipt(updated,day='2026-10-02')
    assert ledger().decide(p.paper_metadata(updated)).action=='duplicate'
    assert ledger().decide(p.paper_metadata(paper())).action=='duplicate'  # no rollback resend


def test_secondary_change_and_ambiguous_text_are_review_not_new_benefit():
    original=paper(result='Primary mortality was 18% versus 20%; P=.37. Secondary pain was 3 versus 4.')
    receipt(original)
    secondary=paper(result='Primary mortality was 18% versus 20%; P=.37. Secondary pain was 2 versus 4.')
    assert ledger().decide(p.paper_metadata(secondary)).category=='source_change_requires_review'
    reword=copy.deepcopy(original);reword['abstract']+='Unstructured additional conclusions.'
    assert ledger().decide(p.paper_metadata(reword)).action=='review'


def test_changed_methods_or_endpoint_are_not_automatic_results_update():
    receipt(paper())
    changed=paper(result='Primary mortality was 8% versus 20%; P=.01.')
    changed['abstract']=changed['abstract'].replace('Adults','Children')
    assert ledger().decide(p.paper_metadata(changed)).action=='review'


@pytest.mark.parametrize('ref,typ,kind',[('RetractionOf','Retraction of Publication','retraction_notice'),
    ('ErratumFor','Published Erratum','correction_notice'),('ExpressionOfConcernFor','Expression of Concern','expression_of_concern_notice')])
def test_notices_are_independent_even_if_doi_matches_original(ref,typ,kind):
    receipt(paper())
    notice=paper('2');notice['publication_types']=[typ]
    notice['comments_corrections']=[{'ref_type':ref,'pmid':'1'}]
    meta=p.paper_metadata(notice)
    assert meta['kind']==kind and p.metadata_valid(meta)
    assert ledger().decide(meta).category==kind
    receipt(notice,day='2026-10-02')
    assert ledger().decide(meta).action=='duplicate'


def test_original_status_annotation_is_update_not_independent_retraction_claim():
    receipt(paper())
    updated=paper();updated['comments_corrections']=[{'ref_type':'RetractionIn','pmid':'2'}]
    assert ledger().decide(p.paper_metadata(updated)).category=='source_status_update'
    assert 'RetractionIn: PMID 2' in p.describe_update({'paper_delivery':p.paper_metadata(updated)},'source_status_update')


def test_relation_parser_keeps_official_types_and_ids():
    from newsagent2.collectors_pubmed import _parse_pubmed_xml
    raw=xml(['2']).replace('</MedlineCitation>','<CommentsCorrectionsList><CommentsCorrections RefType="ExpressionOfConcernFor"><PMID>1</PMID></CommentsCorrections></CommentsCorrectionsList></MedlineCitation>')
    record=_parse_pubmed_xml(raw,1)[0]
    assert record['comments_corrections']==[{'ref_type':'ExpressionOfConcernFor','pmid':'1'}]


def test_corrupt_paper_receipt_preserved_byte_for_byte():
    old=receipt(paper())
    path=path_for('DELIVERY_JOURNAL_PATH');data=json.loads(path.read_text())
    data['editions'][old.edition_id]['finalization']['overview_items'][0]['paper_delivery']['source_version']='0'*64
    raw=json.dumps(data).encode();path.write_bytes(raw)
    with pytest.raises(StoreError,match='invalid_existing_store'):delivery.load_journal(path)
    assert path.read_bytes()==raw


def test_receipt_identity_binding_is_strict():
    old=receipt(paper());data=delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))
    data['editions'][old.edition_id]['finalization']['overview_items'][0]['paper_delivery']['pmid']='2'
    assert not delivery.journal_valid(data)


def test_quarantine_keeps_work_without_false_processed_frontier(monkeypatch):
    mock_source(monkeypatch,['1','2','3'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);rows,_=run.collect('ICU','ICU',2)
        decision=p.Decision('review','legacy_delivery_unknown')
        run.quarantine('1',rows[0],decision)
        run.consumed={'2'};run.acknowledge_generation()
        resumed=ingestion.IngestionSession(REFERENCE);rows,meta=resumed.collect('ICU','ICU',2)
        assert [r['id'] for r in rows]==['3']
        assert meta['processed_through'] is None
        assert meta['review_required']==1
        resumed.consumed={'3'};resumed.acknowledge_generation()
        row=ingestion.load()['queries'][ingestion.fingerprint('ICU')]
        assert row['next_date']=='2026-10-12' and row['review']
        assert row['recent_done']['2026-10-11']==['2','3']
        assert resumed.processed_through(row) is None


def test_review_resolves_only_after_authoritative_acceptance(monkeypatch):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);rows,_=run.collect('ICU','ICU',1)
        run.quarantine('1',rows[0],p.Decision('review','paper_delivery_uncertain'))
        fake=p.PaperLedger({'editions':{}})
        run.refresh_reviews(fake) # merely new/unconfirmed never releases held work
        assert next(iter(run.data['queries'].values()))['review']
        receipt(rows[0])
        run.refresh_reviews(ledger())
        assert not next(iter(run.data['queries'].values()))['review']


def test_main_repeat_avoids_models_and_sender(monkeypatch,tmp_path):
    main=configure_main(monkeypatch,tmp_path,[{'source':'pubmed','name':'ICU','query':'ICU'}])
    mock_source(monkeypatch,['1'])
    raw=__import__('newsagent2.collectors_pubmed',fromlist=[''])._parse_pubmed_xml(xml(['1']),1)[0]
    raw['source']='pubmed';receipt(raw)
    for name in ('summarize','summarize_pubmed_bottom_line','summarize_item_detail','send_markdown'):
        monkeypatch.setattr(main,name,lambda *a,**k:pytest.fail('known accepted paper must not regenerate/send'))
    main.main()
    diag=json.loads((tmp_path/'out/cybermed_daily_ingestion_diagnostics.json').read_text())
    assert diag['paper_decisions']['1']['action']=='duplicate'


def test_main_notice_no_model_or_original_refetch(monkeypatch,tmp_path):
    main=configure_main(monkeypatch,tmp_path,[{'source':'pubmed','name':'ICU','query':'ICU'}])
    mock_source(monkeypatch,['2'])
    original=__import__('newsagent2.collectors_pubmed',fromlist=['']).fetch_pubmed_records
    from newsagent2 import collectors_pubmed as collector
    def notice(ids):
        rows=original(ids)
        for row in rows:
            row['publication_types']=['Retraction of Publication']
            row['comments_corrections']=[{'ref_type':'RetractionOf','pmid':'1'}]
        return rows
    monkeypatch.setattr(collector,'fetch_pubmed_records',notice)
    for name in ('summarize','summarize_pubmed_bottom_line','summarize_item_detail','fetch_pubmed_abstracts'):
        monkeypatch.setattr(main,name,lambda *a,**k:pytest.fail('notice must use source-only path'))
    main.main()
    report=next((tmp_path/'out').glob('cybermed_daily_summary*.md')).read_text()
    assert 'Retraction notice' in report and 'RetractionOf: PMID 1' in report
    stored=json.loads(next((tmp_path/'out').rglob('cybermed_daily_digests.json')).read_text())
    assert stored['digests'][0]['items']['pubmed'][0]['paper_delivery']['kind']=='retraction_notice'


def test_sealed_recovery_binds_source_metadata(monkeypatch):
    from newsagent2 import pre_smtp_recovery as recovery
    monkeypatch.setenv('REPORT_KEY','cybermed');monkeypatch.setenv('REPORT_MODE','daily')
    monkeypatch.setenv('EMAIL_MODE','real');monkeypatch.setenv('SEND_EMAIL','1')
    monkeypatch.setenv('DELIVERY_RECIPIENT_HMAC_KEY','k'*32)
    monkeypatch.setenv('RECIPIENTS_JSON_CYBERMED','["mock@example.test"]')
    co=delivery.Coordinator('cybermed','daily',mode='real',period_dates=('2026-10-01','2026-10-01'))
    item=paper();row={'source':'pubmed','id':'1','title':item['title'],'paper_delivery':p.paper_metadata(item)}
    intents={'items_all_new':[row],'overview_items':[row],'detail_items':[], 'foamed_overview_items':[],
             'report_key':'cybermed','report_mode':'daily','now_utc_iso':'2026-10-01T03:40:00+00:00'}
    from newsagent2.store_io import generated_store, digest_store
    with execution_context() as ctx:
        ctx.coordinator=co;ctx.defer_generation=True
        generated_store(path_for('CYBERMED_DAILY_DIGEST_STATE_PATH'),
            {'schema_version':1,'digests':[{'digest_id':'cybermed_daily_2026-10-01','run_date':'2026-10-01',
              'items':{'pubmed':[item],'foamed':[]}}]},kind='cybermed_daily',validate=digest_store)
        recovery.checkpoint(co,'Approved mock subject','Validated mock body.',intents,ctx.generated_candidates,markers_required=True)
    result=co.result;result.status='failed';result.category=recovery.FAILURE
    co._persist(result)
    assert recovery.replay(co.edition_id)['status']=='ready_for_explicit_replay'
    changed=paper(result='Primary mortality was 8% versus 20%; P=.01.')
    data=delivery.load_journal(co.path)
    data['editions'][co.edition_id]['finalization']['overview_items'][0]['paper_delivery']=p.paper_metadata(changed)
    assert not delivery.journal_valid(data)  # New paper metadata is bound to the sealed finalization.
    co.path.write_text(json.dumps(data))
    before=co.path.read_bytes()
    with pytest.raises(StoreError,match='invalid_existing_store'):
        recovery.replay(co.edition_id)
    assert co.path.read_bytes()==before


def test_acceptance_then_marker_persistence_failure_never_relicenses_send(monkeypatch):
    from newsagent2 import main, workflow_delivery
    result=receipt(paper());result.state_persisted=False
    co=delivery.Coordinator('cybermed','daily',mode='real',period_dates=('2026-10-01','2026-10-01'));co._persist(result)
    real_save=main.save_state
    monkeypatch.setattr(main,'save_state',lambda *a,**k:(_ for _ in ()).throw(StoreError('processed_items','persistence_failed')))
    with pytest.raises(StoreError):workflow_delivery.finalize_receipt(result.edition_id)
    assert ledger().decide(p.paper_metadata(paper())).action=='duplicate'
    monkeypatch.setattr(main,'save_state',real_save)
    workflow_delivery.finalize_receipt(result.edition_id)
    workflow_delivery.finalize_receipt(result.edition_id)
    assert ledger().decide(p.paper_metadata(paper())).action=='duplicate'


def test_push_failure_does_not_relicense_paper(monkeypatch,tmp_path):
    from newsagent2 import workflow_delivery
    result=receipt(paper())
    monkeypatch.setenv('EMAIL_MODE','real');monkeypatch.setenv('GITHUB_REF_NAME','mock')
    def git(args,**kwargs):
        op=args[1]
        return SimpleNamespace(returncode=1 if op in {'push','ls-remote'} else 0,
                               stdout='M journal' if op=='status' else 'd'*40,stderr='')
    monkeypatch.setattr(workflow_delivery.subprocess,'run',git)
    with pytest.raises(RuntimeError,match='git_push_failed'):
        workflow_delivery.publish_state([str(path_for('DELIVERY_JOURNAL_PATH'))],report_dir=tmp_path/'out')
    assert ledger().decide(p.paper_metadata(paper())).action=='duplicate'


def test_unproven_sent_identity_survives_processed_pruning(monkeypatch,tmp_path):
    main=configure_main(monkeypatch,tmp_path,[{'source':'pubmed','name':'ICU','query':'ICU'}],mode='real')
    state={'version':1,'reports':{'cybermed':{'pubmed':{'processed':{
        '1':{'processed_at_utc':'2025-01-01T00:00:00+00:00','sent_overview_at_utc':'2025-01-01T00:00:00+00:00'}}}}}}
    path_for('STATE_PATH').write_text(json.dumps(state))
    mock_source(monkeypatch,['1'])
    for name in ('summarize','summarize_pubmed_bottom_line','send_markdown'):
        monkeypatch.setattr(main,name,lambda *a,**k:pytest.fail('unknown legacy delivery must remain held'))
    main.main()
    saved=delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))
    assert saved['paper_legacy_unknown']['reports']['cybermed']==['1']
    assert p.PaperLedger(saved,state={}).decide(p.paper_metadata(paper())).category=='legacy_delivery_unknown'
    row=next(iter(ingestion.load()['queries'].values()))
    assert row['review'] and row['active'] is None
    assert '1' not in row['recent_done']['2026-10-11']


@pytest.mark.parametrize('status',['partially_accepted','outcome_unknown'])
def test_next_daily_uncertain_paper_is_held_without_models(monkeypatch,tmp_path,status):
    main=configure_main(monkeypatch,tmp_path,[{'source':'pubmed','name':'ICU','query':'ICU'}])
    mock_source(monkeypatch,['1'])
    from newsagent2 import collectors_pubmed as c
    record=c._parse_pubmed_xml(xml(['1']),1)[0]
    receipt(record,status=status)
    for name in ('summarize','summarize_pubmed_bottom_line','send_markdown'):
        monkeypatch.setattr(main,name,lambda *a,**k:pytest.fail('uncertain paper must not resend or regenerate'))
    main.main()
    data=json.loads(next((tmp_path/'out').rglob('cybermed_pubmed_ingestion.json')).read_text())
    held=next(iter(next(iter(data['queries'].values()))['review'].values()))
    assert held['category']=='paper_delivery_uncertain'
    assert held['edition_ids'][0].endswith(':production')


def test_unknown_identity_registry_recovery_preserves_unrelated_fields(tmp_path):
    from newsagent2.workflow_delivery import import_recovery
    old=receipt(paper())
    data=delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))
    data['paper_legacy_unknown']={'version':1,'reports':{'cybermed':['7']}}
    data['legacy_extension']={'untouched':True}
    directory=tmp_path/'artifact';directory.mkdir()
    atomic_store(directory/'journal.json',data,kind='delivery_journal',validate=delivery.journal_valid)
    import_recovery(directory)
    restored=delivery.load_journal(path_for('DELIVERY_JOURNAL_PATH'))
    assert restored['paper_legacy_unknown']['reports']['cybermed']==['7']
    assert ledger().decide(p.paper_metadata(paper('7',doi=''))).category=='legacy_delivery_unknown'


def test_bad_quarantine_metadata_stops_without_repair(monkeypatch):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);rows,_=run.collect('ICU','ICU',1)
        run.quarantine('1',rows[0],p.Decision('review','legacy_delivery_unknown'))
        data=json.loads(run.path.read_text());row=next(iter(data['queries'].values()))
        next(iter(row['review'].values()))['source_version']='0'*64
        raw=json.dumps(data).encode();run.path.write_bytes(raw)
        with pytest.raises(StoreError,match='invalid_existing_store'):ingestion.load()
        assert run.path.read_bytes()==raw


def test_retracted_original_is_not_an_ordinary_study():
    item=paper();item['publication_types'].append('Retracted Publication')
    assert ledger().decide(p.paper_metadata(item)).category=='retracted_original'


def test_conflicting_notice_types_require_review():
    item=paper('2');item['publication_types']=['Retraction of Publication','Expression of Concern']
    assert ledger().decide(p.paper_metadata(item)).category=='source_notice_ambiguous'


def test_readonly_review_audit_creates_nothing(monkeypatch,tmp_path):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);rows,_=run.collect('ICU','ICU',1)
        run.quarantine('1',rows[0],p.Decision('review','legacy_delivery_unknown'))
        before={str(f):f.read_bytes() for f in tmp_path.rglob('*') if f.is_file()}
        report=p.audit_review()
        after={str(f):f.read_bytes() for f in tmp_path.rglob('*') if f.is_file()}
        assert report['read_only'] and report['items'][0]['pmid']=='1'
        assert before==after


def test_notice_caps_keep_unrendered_work_pending(monkeypatch,tmp_path):
    main=configure_main(monkeypatch,tmp_path,[{'source':'pubmed','name':'ICU','query':'ICU'}])
    mock_source(monkeypatch,['1','2','3','4','5'])
    from newsagent2 import collectors_pubmed as collector
    original=collector.fetch_pubmed_records
    def notices(ids):
        rows=original(ids)
        for row in rows:row['publication_types']=['Published Erratum']
        return rows
    monkeypatch.setattr(collector,'fetch_pubmed_records',notices)
    main.main()
    saved=json.loads(next((tmp_path/'out').rglob('cybermed_pubmed_ingestion.json')).read_text())
    active=next(iter(saved['queries'].values()))['active']
    assert active['done']==['1','2','3'] and sorted(active['records'])==['4','5']
    digests=json.loads(next((tmp_path/'out').rglob('cybermed_daily_digests.json')).read_text())
    assert len(digests['digests'][0]['items']['pubmed'])==3


def test_decimal_formatting_is_not_a_new_result():
    original=paper();formatted=paper(result='Primary mortality was 18% versus 20%; P=0.370.')
    assert p.paper_metadata(original)['source_version']==p.paper_metadata(formatted)['source_version']


def test_clinically_sensitive_case_is_not_discarded():
    original=paper();changed=copy.deepcopy(original)
    original['abstract']+='Concentration was 5 mM.';changed['abstract']+='Concentration was 5 mm.'
    assert p.paper_metadata(original)['source_version']!=p.paper_metadata(changed)['source_version']
