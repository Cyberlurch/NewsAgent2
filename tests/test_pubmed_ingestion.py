"""Paraphrased metadata fixtures; all HTTP, provider and SMTP operations mocked."""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import copy
import hashlib
import json
import sys

import pytest
import requests

from newsagent2 import collectors_pubmed as c, pubmed_ingestion as ingestion
from newsagent2.run_context import execution_context, path_for, backfill_paths
from newsagent2.store_io import StoreError

REFERENCE = datetime(2026, 10, 14, 3, 40, tzinfo=timezone.utc)


def article(pmid, *, abstract=True):
    abstract_xml = '<Abstract><AbstractText>In a randomized study the main outcome was uncertain.</AbstractText></Abstract>' if abstract else ''
    return f'<PubmedArticle><MedlineCitation><PMID>{pmid}</PMID><Article><ArticleTitle>Intensive care trial {pmid}</ArticleTitle><Journal><Title>Intensive Care Med</Title><JournalIssue><PubDate><Year>2025</Year><Month>02</Month><Day>01</Day></PubDate></JournalIssue></Journal>{abstract_xml}<PublicationTypeList><PublicationType>Randomized Controlled Trial</PublicationType></PublicationTypeList></Article></MedlineCitation></PubmedArticle>'


def xml(ids, **kwargs):
    return '<PubmedArticleSet>' + ''.join(article(i, **kwargs) for i in ids) + '</PubmedArticleSet>'


def response(*, status=200, data=None, text='', headers=None):
    def decode():
        if isinstance(data, Exception):
            raise data
        return data
    return SimpleNamespace(status_code=status, json=decode, text=text, headers=headers or {})


@pytest.fixture
def no_wait(monkeypatch):
    monkeypatch.setenv('PUBMED_MIN_INTERVAL_S', '0')
    monkeypatch.setenv('PUBMED_BACKOFF_BASE_S', '0')
    monkeypatch.setenv('PUBMED_MAX_RETRIES', '2')


def http(monkeypatch, replies):
    seen = []
    def get(url, **kwargs):
        seen.append((url, kwargs))
        result = replies.pop(0)
        if isinstance(result, Exception):
            raise result
        return result
    monkeypatch.setattr(c._SESSION, 'get', get)
    return seen


@pytest.mark.parametrize('reply,category', [
    (response(status=429), 'rate_limited'), (response(status=503), 'server_error'),
    (response(status=403), 'http_error'), (requests.Timeout('secret@example.test key'), 'timeout'),
    (requests.ConnectionError('https://host.invalid/?api_key=secret'), 'transport_failed'),
    (response(data=ValueError('raw secret')), 'invalid_json'),
    (response(data=[]), 'invalid_json_structure')])
def test_http_failure_is_typed_and_redacted(monkeypatch, capsys, no_wait, reply, category):
    http(monkeypatch, [reply, reply])
    with pytest.raises(c.PubMedError, match=category) as err:
        c._request_json(c.PUBMED_ESEARCH_URL, {})
    assert 'secret' not in str(err.value)
    assert capsys.readouterr().out == ''


@pytest.mark.parametrize('data', [{}, {'esearchresult': {}}, {'esearchresult': {'count': 'invalid', 'idlist': []}},
    {'esearchresult': {'count': '1', 'idlist': []}}, {'esearchresult': {'count': '0', 'idlist': ['1']}},
    {'esearchresult': {'count': '2', 'idlist': ['1','1']}}, {'esearchresult': {'error': 'raw secret'}},
    {'esearchresult': {'count':'0','idlist':[], 'errorlist':{'fieldnotfound':['invalid']}}}])
def test_malformed_esearch_is_not_zero(data):
    with pytest.raises(c.PubMedError):
        c.validate_esearch(data)


def test_crdt_query_and_multiple_pages(monkeypatch, no_wait):
    seen = http(monkeypatch, [response(data={'esearchresult': {'count': '3', 'idlist': ['1','2'], 'webenv':'runtime-only','querykey':'1'}}),
                             response(data={'esearchresult': {'count': '3', 'idlist': ['3']}})])
    ids, meta = c.discover_pubmed_ids('ICU[Title] OR sepsis[Title]', date(2026,10,10), date(2026,10,11), page_size=2)
    assert ids == ['1','2','3'] and meta['pages'] == 2
    assert seen[0][1]['params']['term'] == '(ICU[Title] OR sepsis[Title]) AND ("2026/10/10"[crdt] : "2026/10/11"[crdt])'
    assert seen[1][1]['params']['retstart'] == '2'
    assert 'datetype' not in seen[0][1]['params']
    assert 'mindate' not in seen[0][1]['params']
    assert seen[1][1]['params']['term']=='#1' and seen[1][1]['params']['WebEnv']=='runtime-only'


def test_valid_zero(monkeypatch, no_wait):
    http(monkeypatch, [response(data={'esearchresult': {'count': '0', 'idlist': []}})])
    ids, meta = c.discover_pubmed_ids('ICU', REFERENCE.date(), REFERENCE.date())
    assert ids == [] and meta['esearch_count_total'] == 0


@pytest.mark.parametrize('replies,category', [
    ([{'count':'10001','idlist':['1']}], 'api_result_limit'),
    ([{'count':'2','idlist':['1']}, {'count':'3','idlist':['2']}], 'search_snapshot_changed'),
    ([{'count':'2','idlist':['1']}, {'count':'2','idlist':['1']}], 'search_snapshot_changed')])
def test_limit_and_mutable_snapshot_fail_closed(monkeypatch, no_wait, replies, category):
    http(monkeypatch, [response(data={'esearchresult': dict(r,webenv='runtime-only',querykey='1')}) for r in replies])
    with pytest.raises(c.PubMedError, match=category):
        c.discover_pubmed_ids('ICU', REFERENCE.date(), REFERENCE.date(), page_size=1)


@pytest.mark.parametrize('text,category', [('bad xml','invalid_xml'), ('<html/>','invalid_xml_structure'),
    ('<PubmedArticleSet/>','missing_fetch_ids'), (xml(['2']),'missing_fetch_ids'),
    (xml(['1','1']),'missing_fetch_ids')])
def test_fetch_validates_actual_ids(monkeypatch, no_wait, text, category):
    http(monkeypatch, [response(text=text)])
    with pytest.raises(c.PubMedError, match=category):
        c.fetch_pubmed_records(['1'])


def test_no_abstract_is_valid_metadata_but_absent_content(monkeypatch, no_wait):
    http(monkeypatch, [response(text=xml(['1'], abstract=False)), response(text=xml(['1'], abstract=False))])
    assert c.fetch_pubmed_records(['1'])[0]['abstract'] == ''
    assert c.fetch_pubmed_abstracts(['1']) == {}


def test_request_time_and_retry_after_are_bounded(monkeypatch, no_wait):
    clock = [0.0]
    monkeypatch.setattr(c.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(c.time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0]+seconds))
    monkeypatch.setattr(c, '_LAST_REQUEST_TS', 0)
    seen = http(monkeypatch, [response(status=429, headers={'Retry-After':'9999'})])
    with c.retrieval_budget(c.RetrievalBudget(seconds=2, max_requests=2)):
        with pytest.raises(c.PubMedError, match='rate_limited'):
            c._request_json(c.PUBMED_ESEARCH_URL, {}, timeout_s=25)
    assert len(seen) == 1 and seen[0][1]['timeout'] == 2
    assert clock[0] == 0  # Never sleep outside the remaining budget.


def test_request_cap_and_late_response(monkeypatch, no_wait):
    seen = http(monkeypatch, [response(data={})])
    with c.retrieval_budget(c.RetrievalBudget(max_requests=1)):
        c._request_json(c.PUBMED_ESEARCH_URL, {})
        with pytest.raises(c.PubMedError, match='request_budget'):
            c._request_json(c.PUBMED_ESEARCH_URL, {})
    assert len(seen) == 1
    clock = [0.0]
    monkeypatch.setattr(c.time, 'monotonic', lambda: clock[0])
    def late(*a, **k):
        clock[0] = 3.0
        return response(data={})
    monkeypatch.setattr(c._SESSION, 'get', late)
    with c.retrieval_budget(c.RetrievalBudget(seconds=2)):
        with pytest.raises(c.PubMedError, match='time_budget'):
            c._request_json(c.PUBMED_ESEARCH_URL, {})


@contextmanager
def production(monkeypatch):
    monkeypatch.setenv('EMAIL_MODE','real')
    monkeypatch.setenv('REPORT_KEY','cybermed')
    monkeypatch.setenv('REPORT_MODE','daily')
    with execution_context() as ctx:
        # Unit-level approved-generation proof; no sender or provider is called.
        ctx.delivery_approved = True
        ctx.coordinator = SimpleNamespace(edition_id="cybermed:daily:unit:production",
            result=SimpleNamespace(content_version="a" * 64, generation_validated=True))
        yield ctx


def mock_source(monkeypatch, ids):
    windows = []
    fetched = []
    def discover(term,start,end,**kwargs):
        windows.append((term,start,end))
        values = list(ids(term,start) if callable(ids) else ids)
        return values, {'esearch_count_total':len(values),'pages':1}
    def fetch(values):
        fetched.extend(values)
        return c._parse_pubmed_xml(xml(values),len(values))
    monkeypatch.setattr(c,'discover_pubmed_ids',discover)
    monkeypatch.setattr(c,'fetch_pubmed_records',fetch)
    return windows,fetched


def test_snapshot_record_cap_and_generation_cap_resume_without_starvation(monkeypatch):
    windows, fetched = mock_source(monkeypatch,['1','2','3','4','5'])
    with production(monkeypatch):
        first=ingestion.IngestionSession(REFERENCE,budget=c.RetrievalBudget(max_records=2))
        rows,meta=first.collect('ICU','ICU',2)
        assert [r['id'] for r in rows] == ['1','2']
        assert rows[0]['older_publication'] and rows[0]['published_at'].year == 2025
        assert meta['status']=='incomplete' and meta['processed_through'] is None
        first.consumed={'1'}  # Generation capacity omitted 2; preserve it.
        first.acknowledge_generation()
        second=ingestion.IngestionSession(REFERENCE)
        rows,meta=second.collect('ICU','ICU',2)
        assert [r['id'] for r in rows] == ['2','3']
        second.consumed={'2','3'};second.acknowledge_generation()
        third=ingestion.IngestionSession(REFERENCE)
        rows,_=third.collect('ICU','ICU',2)
        assert [r['id'] for r in rows] == ['4','5']
        third.consumed={'4','5'};third.acknowledge_generation()
        assert fetched == ['1','2','3','4','5']
        assert len(windows)==1  # Stable snapshot, no mutable offset resume.
        row=ingestion.load()['queries'][ingestion.fingerprint('ICU')]
        assert row['next_date']=='2026-10-12'
        assert row['active'] is None


@pytest.mark.parametrize('reference', [datetime(2026,10,12,3,40,tzinfo=timezone.utc), datetime(2026,10,14,3,40,tzinfo=timezone.utc)])
def test_outage_and_holiday_oldest_window_preserved_on_any_day(monkeypatch,reference):
    mock_source(monkeypatch,lambda term,day: [str(day.day)])
    with production(monkeypatch):
        old=ingestion.IngestionSession(datetime(2026,10,8,3,40,tzinfo=timezone.utc))
        old.collect('ICU','ICU',1)
        before=ingestion.load()['queries'][ingestion.fingerprint('ICU')]['active']['day']
        resumed=ingestion.IngestionSession(reference)
        rows,meta=resumed.collect('ICU','ICU',1)
        assert meta['window_start']==before and rows[0]['id']==str(date.fromisoformat(before).day)
        resumed.consumed={rows[0]['id']};resumed.acknowledge_generation()
        assert ingestion.load()['queries'][ingestion.fingerprint('ICU')]['next_date']==(date.fromisoformat(before)+timedelta(days=1)).isoformat()


def test_current_day_is_rescanned_before_certifying_closed_coverage(monkeypatch):
    mock_source(monkeypatch,['1'])
    monkeypatch.setenv('CYBERMED_PUBMED_INITIAL_DAYS','1')
    with production(monkeypatch):
        first=ingestion.IngestionSession(REFERENCE)
        # Explicitly start today to exercise its provisional snapshot.
        key=ingestion.fingerprint('ICU')
        first.data['queries'][key]={'name':'ICU','term':'ICU','initial_start':'2026-10-14','next_date':'2026-10-14','recent_done':{},'active':None}
        rows,_=first.collect('ICU','ICU',3);first.consumed={'1'};first.acknowledge_generation()
        assert ingestion.load()['queries'][key]['next_date']=='2026-10-14'
        mock_source(monkeypatch,['1','2'])
        resumed=ingestion.IngestionSession(REFERENCE+timedelta(days=1))
        rows,meta=resumed.collect('ICU','ICU',3)
        assert [r['id'] for r in rows]==['2']
        assert meta['window_start']=='2026-10-14'


def test_query_version_additive_and_zero_progress_not_delivery(monkeypatch):
    mock_source(monkeypatch,[])
    with production(monkeypatch):
        first=ingestion.IngestionSession(REFERENCE)
        first.data['legacy_extension']={'preserve':'yes'}
        first.collect('ICU','ICU',25)
        second=ingestion.IngestionSession(REFERENCE)
        second.collect('ICU','ICU OR sepsis',25)
        saved=ingestion.load()
        assert len(saved['queries'])==2 and saved['legacy_extension']=={'preserve':'yes'}
        assert not path_for('STATE_PATH').exists() and not path_for('DELIVERY_JOURNAL_PATH').exists()
        assert all(q['initial_start']=='2026-10-11' for q in saved['queries'].values())


def test_query_failure_retains_other_query_work(monkeypatch):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch):
        first=ingestion.IngestionSession(REFERENCE)
        first.collect('good','good',25)
        monkeypatch.setattr(c,'discover_pubmed_ids',lambda *a,**k: (_ for _ in ()).throw(c.PubMedError('server_error')))
        rows,meta=first.collect('bad','bad',25)
        assert first.failed and rows==[] and meta['status']=='failed'
        saved=ingestion.load()
        assert saved['queries'][ingestion.fingerprint('good')]['active']['records']['1']['title']
        artifact=Path(__import__('os').environ['REPORT_DIR'])/'recovery'/ingestion.RECOVERY
        assert json.loads(artifact.read_text())['sha256']==ingestion._seal(saved)


def test_missing_vs_corrupt_invalid_store_no_mutations(monkeypatch,tmp_path):
    target=path_for('CYBERMED_PUBMED_INGESTION_PATH')
    assert ingestion.load()==ingestion.EMPTY and not target.exists()
    for raw in (b'{bad json',b'{}',b'{"schema_version":1,"schema_version":1}'):
        target.write_bytes(raw)
        with pytest.raises(StoreError): ingestion.load()
        assert target.read_bytes()==raw


@pytest.mark.parametrize('mode',['none','test'])
@pytest.mark.parametrize('report',['cybermed','cyberlurch'])
def test_preview_paths_leave_all_production_bytes_unchanged(monkeypatch,mode,report):
    from newsagent2.run_context import PATHS
    paths={name:path_for(name) for name in PATHS}
    for p in paths.values(): p.write_bytes(b'{"existing":"unchanged"}')
    paths['CYBERMED_PUBMED_INGESTION_PATH'].write_text(json.dumps(ingestion.EMPTY))
    before={name:p.read_bytes() for name,p in paths.items()}
    mock_source(monkeypatch,['1'])
    monkeypatch.setenv('EMAIL_MODE',mode);monkeypatch.setenv('REPORT_KEY',report)
    with execution_context():
        run=ingestion.IngestionSession(REFERENCE);run.collect('ICU','ICU',2)
        assert run.path!=paths['CYBERMED_PUBMED_INGESTION_PATH']
    assert {name:p.read_bytes() for name,p in paths.items()}==before


def test_backfill_does_not_grant_ingestion_write(monkeypatch):
    monkeypatch.setenv('REPORT_KEY','cybermed');monkeypatch.setenv('REPORT_MODE','daily')
    monkeypatch.setenv('CYBERMED_DIGEST_BACKFILL_MODE','1');monkeypatch.setenv('CYBERMED_DIGEST_BACKFILL_APPLY','1')
    assert 'CYBERMED_PUBMED_INGESTION_PATH' not in backfill_paths()


def test_recovery_hash_conflict_idempotence_and_preview_rejection(monkeypatch,tmp_path):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);run.collect('ICU','ICU',2)
        artifact=Path(__import__('os').environ['REPORT_DIR'])/'recovery'/ingestion.RECOVERY
        envelope=json.loads(artifact.read_text());target=run.path
        target.unlink()  # Simulate push failure: the remote still has no store.
        ingestion.restore(artifact.parent)
        restored=target.read_bytes()
        ingestion.restore(artifact.parent)
        assert target.read_bytes()==restored
        changed=ingestion.load();changed['rotation']=99
        target.write_text(json.dumps(changed))
        ingestion.restore(artifact.parent)  # Previously restored artifact cannot undo later progress.
        assert ingestion.load()['rotation']==99
        envelope['base_sha256']='0'*64;envelope['data']['rotation']=100;envelope['sha256']=ingestion._seal(envelope['data'])
        artifact.write_text(json.dumps(envelope));before=target.read_bytes()
        with pytest.raises(StoreError,match='preimage_conflict'): ingestion.restore(artifact.parent)
        assert target.read_bytes()==before
    monkeypatch.setenv('EMAIL_MODE','none')
    with pytest.raises(StoreError,match='mutation_policy'): ingestion.restore(artifact.parent)


def test_persistence_failure_preserves_valid_recovery_before_target_write(monkeypatch):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE)
        real=ingestion.atomic_store
        def fail_target(path,*a,**k):
            if Path(path)==run.path: raise StoreError(ingestion.KIND,'persistence_failed')
            return real(path,*a,**k)
        monkeypatch.setattr(ingestion,'atomic_store',fail_target)
        with pytest.raises(StoreError): run.collect('ICU','ICU',2)
        assert not run.path.exists()
        artifact=Path(__import__('os').environ['REPORT_DIR'])/'recovery'/ingestion.RECOVERY
        assert ingestion.valid(json.loads(artifact.read_text())['data'])


def test_rotation_prevents_query_starvation(monkeypatch):
    channels=[{'source':'pubmed','name':str(i),'query':str(i)} for i in range(3)]
    with production(monkeypatch):
        assert ingestion.IngestionSession(REFERENCE).order(channels)[0]['name']=='0'
        assert ingestion.IngestionSession(REFERENCE).order(channels)[0]['name']=='1'
        assert ingestion.IngestionSession(REFERENCE).order(channels)[0]['name']=='2'


def test_dedup_merges_query_provenance():
    from newsagent2.main import _dedupe_items
    result=_dedupe_items([{'source':'pubmed','id':'1','query_provenance':['a']},
                          {'source':'pubmed','id':'1','query_provenance':['b']}])
    assert len(result)==1 and result[0]['query_provenance']==['a','b']


def configure_main(monkeypatch, tmp_path, channels, mode='none'):
    from newsagent2 import main, selector_medical
    monkeypatch.setenv('REPORT_KEY','cybermed');monkeypatch.setenv('REPORT_MODE','daily')
    monkeypatch.setenv('EMAIL_MODE',mode);monkeypatch.setenv('SEND_EMAIL','0')
    monkeypatch.setenv('REPORT_DIR',str(tmp_path/'out'))
    monkeypatch.setenv('REPORT_TITLE','The Cybermed Report')
    monkeypatch.setenv('REPORT_PROFILE','cybermed')
    monkeypatch.setenv('REPORT_LANGUAGE','de')
    monkeypatch.setenv('CYBERMED_MODEL_EVAL_MODE','0')
    monkeypatch.setenv('PUBMED_DEEPDIVE_USE_PMC_OA_FULLTEXT','false')
    monkeypatch.setenv('PUBMED_DEEPDIVE_USE_UNPAYWALL_FULLTEXT','false')
    monkeypatch.setattr(sys,'argv',['newsagent2'])
    monkeypatch.setattr(main,'_edition_now',lambda:REFERENCE)
    monkeypatch.setattr(main,'load_channels_config',lambda *a:(channels,{},{}))
    monkeypatch.setattr(main,'load_foamed_sources_config',lambda *a:[])
    monkeypatch.setattr(main,'get_pmcids_for_pmids',lambda *a,**k:{})
    monkeypatch.setattr(main,'_select_pubmed_deep_dives_with_content',lambda *a,**k:[])
    monkeypatch.setattr(main,'summarize_pubmed_bottom_line',lambda *a,**k:'The reported primary outcome was uncertain.')
    monkeypatch.setattr(main,'summarize',lambda *a,**k:'## Executive Summary\n\nThe reported primary outcome was uncertain.')
    monkeypatch.setattr(main,'summarize_item_detail',lambda *a,**k:'The reported primary outcome was uncertain.')
    monkeypatch.setattr(selector_medical,'select_cybermed_pubmed_items',lambda rows:SimpleNamespace(overview_items=rows,deep_dive_items=[],stats={}))
    return main


def test_daily_query_failure_blocks_sender_but_keeps_successful_ingestion(monkeypatch,tmp_path):
    channels=[{'name':'good','source':'pubmed','query':'good'},{'name':'bad','source':'pubmed','query':'bad'}]
    main=configure_main(monkeypatch,tmp_path,channels,mode='real')
    mock_source(monkeypatch,['1'])
    original=c.discover_pubmed_ids
    def discover(term,*args,**kwargs):
        if term=='bad':raise c.PubMedError('server_error')
        return original(term,*args,**kwargs)
    monkeypatch.setattr(c,'discover_pubmed_ids',discover)
    monkeypatch.setattr(main,'send_markdown',lambda *a,**k:pytest.fail('failed source must not send'))
    with pytest.raises(RuntimeError,match='pubmed_ingestion_failed'):
        main.main()
    assert ingestion.load()['queries'][ingestion.fingerprint('good')]['active']['records']['1']
    assert not path_for('CYBERMED_DAILY_DIGEST_STATE_PATH').exists()
    result=json.loads((tmp_path/'out/cybermed_daily_delivery_result.json').read_text())
    assert result['status']=='failed'


@pytest.mark.parametrize('mode',['none','test'])
def test_daily_older_publication_survives_and_preview_has_no_production_mutations(monkeypatch,tmp_path,mode):
    from conftest import accepted_sender_stub
    channels=[{'name':'ICU','source':'pubmed','query':'ICU'}]
    main=configure_main(monkeypatch,tmp_path,channels,mode=mode)
    mock_source(monkeypatch,['1'])
    if mode=='test':
        monkeypatch.setenv('SEND_EMAIL','1')
        monkeypatch.setattr(main,'send_markdown',accepted_sender_stub)
    from newsagent2.run_context import PATHS
    before={name:(path_for(name).read_bytes() if path_for(name).exists() else None) for name in PATHS}
    main.main()
    after={name:(path_for(name).read_bytes() if path_for(name).exists() else None) for name in PATHS}
    assert after==before
    report=next((tmp_path/'out').glob('cybermed_daily_*.md')).read_text()
    assert 'Intensive care trial 1' in report
    assert 'older publication' in report or 'ältere Publikation' in report
    assert 'Quellenabdeckung' not in report and 'PubMed capture is bounded' not in report
    saved=next((tmp_path/'out').rglob('cybermed_pubmed_ingestion.json'))
    data=json.loads(saved.read_text())
    active=data['queries'][ingestion.fingerprint('ICU')]['active']
    assert active is None  # Valid generation consumed it, with no SMTP success marker here.


def test_snapshot_with_one_request_budget_can_resume_fetch_next_run(monkeypatch,no_wait):
    monkeypatch.setenv('CYBERMED_PUBMED_INITIAL_DAYS','1')
    seen=http(monkeypatch,[response(data={'esearchresult':{'count':'2','idlist':['1','2']}})])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE,budget=c.RetrievalBudget(max_requests=1))
        rows,meta=run.collect('ICU','ICU',2)
        assert rows==[] and meta['status']=='incomplete'
        assert seen[0][1]['params']['retmax']=='10000'
        assert ingestion.load()['queries'][ingestion.fingerprint('ICU')]['active']['ids']==['1','2']
        http(monkeypatch,[response(text=xml(['1','2']))])
        resumed=ingestion.IngestionSession(REFERENCE,budget=c.RetrievalBudget(max_requests=1))
        rows,_=resumed.collect('ICU','ICU',2)
        assert {r['id'] for r in rows}=={'1','2'}


def test_publication_push_failure_is_visible_and_preserves_ingestion(monkeypatch,tmp_path):
    from newsagent2 import workflow_delivery
    mock_source(monkeypatch,['1'])
    monkeypatch.setenv('GITHUB_REF_NAME','test-branch')
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);run.collect('ICU','ICU',1)
        before=run.path.read_bytes()
        def git(args,**kwargs):
            operation=args[1]
            return SimpleNamespace(returncode=1 if operation in {'push','ls-remote'} else 0,
                                   stdout='M state\n' if operation=='status' else 'c'*40+'\n',stderr='')
        monkeypatch.setattr(workflow_delivery.subprocess,'run',git)
        with pytest.raises(RuntimeError,match='git_push_failed'):
            workflow_delivery.publish_state([str(run.path)],report_dir=tmp_path/'out')
        assert run.path.read_bytes()==before
        assert json.loads((tmp_path/'out/recovery/git_persistence.json').read_text())['category']=='git_push_failed_or_unknown'


def test_budget_configuration_cannot_expand_retrieval_limits(monkeypatch):
    with production(monkeypatch):
        monkeypatch.setenv('CYBERMED_PUBMED_REQUESTS_MAX','61')
        with pytest.raises(c.PubMedError,match='invalid_budget_configuration'):
            ingestion.IngestionSession(REFERENCE)


def test_generation_approval_is_required_before_consuming_candidates(monkeypatch):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch) as ctx:
        run=ingestion.IngestionSession(REFERENCE);run.collect('ICU','ICU',1)
        run.consumed={'1'};ctx.delivery_approved=False
        before=run.path.read_bytes()
        with pytest.raises(StoreError,match='generation_approval_required'): run.acknowledge_generation()
        assert run.path.read_bytes()==before


def test_equal_recovery_registered_before_later_progress(monkeypatch):
    mock_source(monkeypatch,['1'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);run.collect('ICU','ICU',1)
        directory=Path(__import__('os').environ['REPORT_DIR'])/'recovery'
        ingestion.restore(directory)  # Same data already exists on this checkout.
        advanced=ingestion.load();advanced['rotation']=42
        run.path.write_text(json.dumps(advanced))
        ingestion.restore(directory)
        assert ingestion.load()['rotation']==42


def test_processed_receipts_distinguish_generated_and_editorial_rejection(monkeypatch):
    mock_source(monkeypatch,['1','2','3'])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);run.collect('ICU','ICU',3)
        run.consumed={'1'};run.terminal={'2'};run.acknowledge_generation()
        row=ingestion.load()['queries'][ingestion.fingerprint('ICU')]
        receipt=row['processing_receipts']['2026-10-11'][0]
        assert receipt['generated']==['1'] and receipt['screened_out']==['2']
        assert receipt['delivery_claim'] is False and receipt['content_version']=='a'*64
        assert list(row['active']['records'])==['3']


def test_cyberlurch_cannot_publish_cybermed_ingestion(monkeypatch,tmp_path):
    from newsagent2.workflow_delivery import publish_state
    mock_source(monkeypatch,[])
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE);run.collect('ICU','ICU',1)
        monkeypatch.setenv('REPORT_KEY','cyberlurch')
        with pytest.raises(RuntimeError,match='newsletter_scope_mismatch'):
            publish_state([str(run.path)],report_dir=tmp_path/'out')


def test_unknown_publication_date_is_not_replaced_by_collection_time(monkeypatch,no_wait):
    raw='<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>1</PMID><Article><ArticleTitle>Metadata only</ArticleTitle></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>'
    http(monkeypatch,[response(text=raw)])
    record=c.fetch_pubmed_records(['1'])[0]
    assert record['published_at'] is None
    assert 'Published:' not in record['text']


def test_snapshot_cannot_exceed_declared_count(monkeypatch,no_wait):
    http(monkeypatch,[response(data={'esearchresult':{'count':'3','idlist':['1','2'],'webenv':'runtime-only','querykey':'1'}}),
                      response(data={'esearchresult':{'count':'3','idlist':['3','4']}})])
    with pytest.raises(c.PubMedError,match='invalid_esearch_count'):
        c.discover_pubmed_ids('ICU',REFERENCE.date(),REFERENCE.date(),page_size=2)


@pytest.mark.parametrize('reply,category',[(response(status=429),'rate_limited'),(requests.Timeout('secret'),'timeout')])
def test_source_failure_with_exhausted_retry_budget_still_blocks_delivery(monkeypatch,no_wait,reply,category):
    http(monkeypatch,[reply])
    with c.retrieval_budget(c.RetrievalBudget(max_requests=1)):
        with pytest.raises(c.PubMedError,match=category):
            c._request_json(c.PUBMED_ESEARCH_URL,{})


def test_unknown_or_naive_reference_is_rejected():
    with pytest.raises(c.PubMedError,match='invalid_reference_time'):
        ingestion.IngestionSession(datetime(2026,10,14,5,40))


@pytest.mark.parametrize('language', ['en', 'de', 'sv'])
@pytest.mark.parametrize('cadence', ['daily', 'weekly', 'monthly', 'yearly'])
def test_capture_progress_stays_internal_in_every_cadence_and_language(monkeypatch, language, cadence):
    from newsagent2 import main
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE)
        run.metadata={'ICU':{'status':'incomplete','category':'request_budget'}}
        monkeypatch.setenv('REPORT_LANGUAGE',language)
        monkeypatch.setenv('REPORT_MODE',cadence)
        before=copy.deepcopy((run.data, run.metadata))
        ctx=ingestion.current()
        ctx.pubmed_ingestion=run
        ctx.pubmed_generated_ids={'fixture-paper'}
        body='## Cybermed\n\nExisting reader layout and clinical content.\n'
        main._queue_report('Cybermed',body)
        assert run.reader_note()=='' and ctx.request[1]==body
        assert (run.data,run.metadata)==before
        assert 'fixture-paper' in run.consumed


def test_review_hold_stays_internal_without_masking_real_capture_incompleteness(monkeypatch):
    with production(monkeypatch):
        run=ingestion.IngestionSession(REFERENCE)
        run.data['queries']['held']={'review':{'1':{'category':'paper_delivery_uncertain'}}}
        before=copy.deepcopy(run.data)
        monkeypatch.setenv('REPORT_LANGUAGE','en')
        assert run.reader_note()==''
        run.metadata={'ICU':{'status':'incomplete','category':'request_budget'}}
        assert run.reader_note()=='' and run.data==before
        assert run.metadata['ICU']['status']=='incomplete'
