"""Offline bounded GET retries, schema/pagination and private-data guards."""
import json
from types import SimpleNamespace

import pytest
import requests

from newsagent2 import github_readonly as gh
from _github_fakes import response


class Clock:
    elapsed = 0
    def now(self): return self.elapsed
    def sleep(self, seconds): self.elapsed += seconds
    def wall(self): return 1700000000 + self.elapsed


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(gh, 'time', SimpleNamespace(monotonic=clock.now, sleep=clock.sleep, time=clock.wall))
    monkeypatch.setattr(gh.random, 'uniform', lambda low, high: 1)
    return clock


def reader(sequence, clock, *, budget=120):
    calls = []
    def get(url, **kwargs):
        calls.append((url,kwargs))
        value = sequence.pop(0)
        if isinstance(value,Exception): raise value
        return value
    return gh.GitHubReader(SimpleNamespace(get=get), 'offline/repo', budget_seconds=budget), calls


@pytest.mark.parametrize('first',[
    lambda: requests.Timeout('private.test secret@example.test token-secret'),
    lambda: requests.ConnectionError('private.test token-secret'),
    lambda: response(status=503), lambda: response(status=500),
    lambda: response(status=429,headers={'Retry-After':'2'}),
    lambda: response(status=403,headers={'X-RateLimit-Remaining':'0','X-RateLimit-Reset':'1700000002'}),
    lambda: response(status=403,headers={'X-RateLimit-Remaining':'0'}),
    lambda: response(status=403,headers={'Retry-After':'Tue, 14 Nov 2023 22:13:22 GMT'}),
])
def test_transient_get_recovers_with_timeouts_jitter_and_no_final_error(first,clock):
    api,calls=reader([first(),response({'created_at':'2026-10-23T08:50:09Z'})],clock)
    result=api.json('/actions/runs/71','run_created_at')
    assert result['created_at']=='2026-10-23T08:50:09Z' and len(calls)==2
    assert .5 <= clock.elapsed <= 2
    assert api.diagnostics==[dict(category='github_read_ok',operation='run_created_at',http_status=200,
        attempts=2,attempt_limit=4,budget_seconds=30,elapsed_seconds=clock.elapsed,budget_exhausted=False)]
    assert all(c[1]['timeout']==(3,5) and c[1]['allow_redirects'] is False for c in calls)


@pytest.mark.parametrize('status,headers',[(401,{}),(403,{}),(404,{}),(403,{'Retry-After':'secret'}),
    (403,{'X-RateLimit-Remaining':'1'}),(403,{'Retry-After':'NaN'})])
def test_permanent_status_is_not_retried_or_disclosed(status,headers,clock):
    api,calls=reader([response({'message':'private.test token-secret'},status=status,headers=headers)],clock)
    with pytest.raises(gh.GitHubReadError) as error: api.json('/actions/runs/71','run_created_at')
    assert error.value.diagnostic['category']=='github_http_permanent'
    assert error.value.diagnostic['attempts']==1 and error.value.diagnostic['http_status']==status
    assert len(calls)==1 and clock.elapsed==0
    assert 'private.test' not in str(error.value) and 'token-secret' not in json.dumps(error.value.diagnostic)


@pytest.mark.parametrize('first,expected_attempts',[(lambda:response(status=503),4),
    (lambda:requests.Timeout('private.test'),4),
    (lambda:response(status=429,headers={'Retry-After':'100000'}),1)])
def test_exhaustion_is_bounded_without_real_sleep(first,expected_attempts,clock):
    api,calls=reader([first() for _ in range(4)],clock)
    with pytest.raises(gh.GitHubReadError) as error: api.json('/actions/runs/71','run_created_at')
    assert len(calls)==expected_attempts and clock.elapsed<30
    assert error.value.diagnostic['budget_exhausted'] and error.value.diagnostic['attempts']==expected_attempts


def test_shared_budget_also_bounds_multiple_successful_operations(clock):
    api,calls=reader([response({}),response(status=503)],clock,budget=1)
    api.json('/actions/runs/71','run_created_at'); clock.elapsed=.99
    with pytest.raises(gh.GitHubReadError,match='budget_exhausted'):
        api.json('/actions/runs/72','run_created_at')
    assert len(calls)==1


def test_trickling_body_cannot_reset_the_total_read_deadline(clock):
    result=response()
    def read1(size,decode_content):
        clock.elapsed+=10
        return b' '
    result.raw=SimpleNamespace(read1=read1)
    api,calls=reader([result],clock)
    with pytest.raises(gh.GitHubReadError,match='github_timeout') as error:
        api.json('/actions/runs/71','run_created_at')
    assert len(calls)==1 and clock.elapsed==30 and error.value.diagnostic['budget_exhausted']


@pytest.mark.parametrize('data', [{}, {'jobs':[]}, {'total_count':0},
    {'total_count':1,'jobs':[{'id':1}]}, {'total_count':1,'jobs':[{'id':1,'steps':[{'name':'sender'}]}]},
    {'total_count':1,'jobs':[{'id':{},'steps':[]}]}])
def test_missing_schema_is_never_empty_success(data,clock):
    api,calls=reader([response(data)],clock)
    with pytest.raises(gh.GitHubReadError,match='schema_invalid'):
        api.collection('/actions/runs/71/jobs','jobs','run_jobs')
    assert len(calls)==1


def test_invalid_json_does_not_retry_or_echo_server_body(clock):
    api,calls=reader([response(content=b'private.test token-secret {not-json')],clock)
    with pytest.raises(gh.GitHubReadError,match='github_invalid_json'):
        api.json('/actions/runs/71','run_created_at')
    assert len(calls)==1 and all('private' not in str(d) for d in api.diagnostics)


def test_all_job_pages_are_checked_including_earlier_attempts(clock):
    jobs=[{'id':i,'steps':[]} for i in range(1,102)]
    api,calls=reader([response({'total_count':101,'jobs':jobs[:100]}),
                     response({'total_count':101,'jobs':jobs[100:]})],clock)
    assert api.collection('/actions/runs/71/jobs','jobs','run_jobs',params={'filter':'all'})==jobs
    assert [c[1]['params']['page'] for c in calls]==[1,2]
    assert all(c[1]['params']['filter']=='all' for c in calls)
    assert api.collections==[dict(operation='run_jobs',total_available=101,checked=101,complete_history=True,window_limit=None)]


@pytest.mark.parametrize('defect',['short','changed','duplicate','limit'])
def test_truncated_shifting_or_oversized_collections_are_explicit_failures(defect,clock):
    rows=[{'id':i,'steps':[]} for i in range(1,101)]
    first={'total_count':101,'jobs':rows}
    second={'total_count':101,'jobs':[{'id':101,'steps':[]}]}
    if defect=='short':first['jobs']=rows[:1]
    if defect=='changed':second['total_count']=102
    if defect=='duplicate':second['jobs'][0]['id']=1
    if defect=='limit':first['total_count']=1001
    api,calls=reader([response(first),response(second)],clock)
    with pytest.raises(gh.GitHubReadError): api.collection('/actions/runs/71/jobs','jobs','run_jobs')
    assert len(calls)==(1 if defect in {'short','limit'} else 2)


def test_existing_workflow_window_is_reported_without_claiming_archive_completeness(clock):
    rows=[{'id':i,'event':'schedule','conclusion':'success'} for i in range(1,101)]
    api,calls=reader([response({'total_count':876,'workflow_runs':rows})],clock)
    assert api.collection('/actions/workflows/newsagent.yml/runs','workflow_runs','workflow_runs',window=100)==rows
    assert api.collections[0]==dict(operation='workflow_runs',total_available=876,checked=100,complete_history=False,window_limit=100)
    assert len(calls)==1


def test_archive_redirect_never_forwards_authorization_or_signed_link(clock):
    target='https://results.blob.core.windows.net/private.zip?sig=secret'
    api,calls=reader([response(status=302,headers={'Location':target}),response(content=b'ZIP')],clock)
    assert api.archive('/actions/artifacts/7/zip')==b'ZIP'
    assert len(calls)==2 and calls[1][1]['headers']=={'Authorization':None}
    assert all('secret' not in str(d) and target not in str(d) for d in api.diagnostics)


@pytest.mark.parametrize('target',['http://results.blob.core.windows.net/a','https://evil.test/a',
    'https://user:password@results.blob.core.windows.net/a','https://results.blob.core.windows.net:4430/a'])
def test_unsafe_archive_redirect_is_blocked_before_following(target,clock):
    api,calls=reader([response(status=302,headers={'Location':target})],clock)
    with pytest.raises(gh.GitHubReadError,match='redirect_invalid'): api.archive('/actions/artifacts/7/zip')
    assert len(calls)==1


def test_archive_get_can_retry_but_never_retries_the_recovery_mutation(clock,monkeypatch):
    api,calls=reader([response(status=503),response(content=b'archive')],clock)
    assert api.archive('/actions/artifacts/7/zip')==b'archive' and len(calls)==2
    monkeypatch.setattr(gh,'MAX_ARCHIVE',2)
    api,calls=reader([response(content=b'too large')],clock)
    with pytest.raises(gh.GitHubReadError,match='oversized'): api.archive('/actions/artifacts/7/zip')
    assert len(calls)==1
