"""Bounded GET-only GitHub reads. Never retry a sender or a mutating operation.

Diagnostics contain fixed operation/category names and numeric metadata only.
The reader has a shared deadline, with a shorter budget for each GET (including
body reads). Requests timeouts additionally bound connection/read inactivity.
"""
from __future__ import annotations

from email.utils import parsedate_to_datetime
import json
import math
import random
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from urllib3.exceptions import HTTPError, ProtocolError, ReadTimeoutError

OPERATIONS = {'workflow_runs', 'run_jobs', 'run_artifacts', 'run_created_at', 'recovery_archive'}
RETRY_STATUS = {408, 429, 500, 502, 503, 504}
MAX_ARCHIVE = 100 * 1024 * 1024


class GitHubReadError(RuntimeError):
    def __init__(self, category, operation, status, attempts, exhausted=False, *, budget_seconds=30, elapsed_seconds=0):
        self.diagnostic = dict(category=category, operation=operation, http_status=status,
                               attempts=attempts, attempt_limit=4, budget_seconds=budget_seconds,
                               elapsed_seconds=elapsed_seconds, budget_exhausted=exhausted)
        super().__init__(category)  # Never include URLs, headers or response/exception text.


class GitHubReader:
    def __init__(self, session, repository, *, headers=None, budget_seconds=120,
                 clock=None, sleep=None, jitter=None, wall_time=None):
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
            raise RuntimeError('github_repository_invalid')
        self.session, self.headers = session, headers or {}
        self.base = 'https://api.github.com/repos/' + repository
        self.clock, self.sleep = clock or time.monotonic, sleep or time.sleep
        self.jitter, self.wall_time = jitter or random.uniform, wall_time or time.time
        self.deadline = self.clock() + budget_seconds
        self.diagnostics = []
        self.collections = []
        self.started, self.operation_budget = self.clock(), min(30,budget_seconds)

    def _fail(self, category, operation, status=None, attempts=None, exhausted=False):
        if attempts is None:
            attempts = self.diagnostics[-1]['attempts'] if self.diagnostics and self.diagnostics[-1]['operation']==operation else 1
        error = GitHubReadError(category, operation, status, attempts, exhausted,
            budget_seconds=round(self.operation_budget,3), elapsed_seconds=round(self.clock()-self.started,3))
        self.diagnostics.append(error.diagnostic)
        raise error from None

    def _timeout(self, deadline, operation, attempt):
        remaining = deadline - self.clock()
        if remaining <= .02:
            self._fail('github_read_budget_exhausted', operation, attempts=attempt, exhausted=True)
        return min(3, remaining / 2), min(5, remaining / 2)

    def _delay(self, response):
        """Return a server-directed delay, without logging header contents."""
        headers = response.headers
        raw = headers.get('Retry-After')
        if raw:
            try:
                seconds = float(raw)
                return max(0, seconds) if math.isfinite(seconds) else None
            except (ValueError, TypeError):
                try:
                    return max(0, parsedate_to_datetime(raw).timestamp() - self.wall_time())
                except (ValueError, TypeError, OverflowError):
                    pass
        if headers.get('X-RateLimit-Remaining') == '0':
            try:
                seconds = float(headers['X-RateLimit-Reset']) - self.wall_time()
                return max(0, seconds) if math.isfinite(seconds) else None
            except (KeyError, ValueError, TypeError):
                pass
        return None

    def _download(self, response, deadline, operation, attempt):
        # GitHub issues a signed storage redirect. It must stay HTTPS; never
        # forward the session's Authorization to storage, or log its URL.
        for _ in range(3):
            if response.status_code not in {301, 302, 303, 307, 308}:
                break
            target = urlparse(urljoin(self.base, response.headers.get('Location', '')))
            host = target.hostname or ''
            if (target.scheme != 'https' or target.username or target.password or target.port not in {None, 443}
                or not (host.endswith('.blob.core.windows.net') or host.endswith('.githubusercontent.com'))):
                self._fail('github_archive_redirect_invalid', operation, response.status_code, attempt)
            response.close()
            response = self.session.get(target.geturl(), headers={'Authorization': None},
                timeout=self._timeout(deadline, operation, attempt), allow_redirects=False, stream=True)
        return response

    def _read(self, path, operation, *, params=None, binary=False):
        if operation not in OPERATIONS or not path.startswith('/actions/'):
            raise ValueError('github_read_operation_invalid')
        deadline = min(self.deadline, self.clock() + 30)
        self.started, self.operation_budget = self.clock(), max(0,deadline-self.clock())
        for attempt in range(1, 5):
            response = None
            status = None
            try:
                response = self.session.get(self.base + path, params=params, headers=self.headers,
                    timeout=self._timeout(deadline, operation, attempt), allow_redirects=False, stream=True)
                if binary:
                    response = self._download(response, deadline, operation, attempt)
                status = response.status_code
                if status == 200:
                    body = bytearray()
                    for chunk in self._chunks(response):
                        if self.clock() >= deadline:
                            raise requests.Timeout()
                        body.extend(chunk)
                        if len(body) > (MAX_ARCHIVE if binary else 8 * 1024 * 1024):
                            self._fail('github_archive_oversized' if binary else 'github_response_oversized', operation, status, attempt)
                    if self.clock() >= deadline:
                        raise requests.Timeout()
                    if binary:
                        value = bytes(body)
                    else:
                        try:
                            value = json.loads(body)
                        except (ValueError, UnicodeError):
                            self._fail('github_invalid_json', operation, status, attempt)
                    if self.clock() >= deadline:
                        raise requests.Timeout()
                    self.diagnostics.append(dict(category='github_read_ok', operation=operation,
                        http_status=200, attempts=attempt, attempt_limit=4, budget_seconds=round(self.operation_budget,3),
                        elapsed_seconds=round(self.clock()-self.started,3), budget_exhausted=False))
                    return value
                delay = self._delay(response)
                retry = status in RETRY_STATUS or status == 403 and (
                    delay is not None or response.headers.get('X-RateLimit-Remaining') == '0')
                category = ('github_rate_limited' if status == 429 or status == 403 and retry else
                            'github_transient_http' if retry else 'github_http_permanent')
                if not retry:
                    self._fail(category, operation, status, attempt)
            except requests.Timeout:
                category, delay = 'github_timeout', None
            except requests.ConnectionError:
                category, delay = 'github_connection_failed', None
            except requests.RequestException:
                self._fail('github_request_failed', operation, status, attempt)
            finally:
                if response is not None:
                    response.close()
            remaining = deadline - self.clock()
            pause = max(delay or 0, min(4, .5 * 2 ** (attempt-1)) * self.jitter(.5, 1.5))
            if attempt == 4 or pause + .02 >= remaining:
                self._fail(category, operation, status, attempt, exhausted=True)
            self.sleep(pause)

    @staticmethod
    def _chunks(response):
        # read1 returns available data, rather than waiting to fill a large
        # buffer while a slow peer trickles bytes past the total deadline.
        read1 = getattr(response.raw, 'read1', None)
        if read1 is None:
            yield from response.iter_content(chunk_size=65536)
            return
        try:
            while chunk := read1(65536,decode_content=True):
                yield chunk
        except ReadTimeoutError:
            raise requests.Timeout() from None
        except ProtocolError:
            raise requests.ConnectionError() from None
        except HTTPError:
            raise requests.RequestException() from None

    def json(self, path, operation, *, params=None, validate=None):
        value = self._read(path, operation, params=params)
        if validate and not validate(value):
            self._fail('github_response_schema_invalid', operation, 200,
                       self.diagnostics[-1]['attempts'])
        return value

    def collection(self, path, field, operation, *, params=None, max_pages=10, window=None):
        """Require an explicitly complete counted result; never default missing
        fields to an empty success. Concurrently shifting pages fail closed.
        """
        collected, expected = [], None
        for page in range(1, max_pages+1):
            value = self.json(path, operation, params={**(params or {}), 'per_page': 100, 'page': page},
                validate=lambda v: isinstance(v, dict) and type(v.get('total_count')) is int
                    and v['total_count'] >= 0 and isinstance(v.get(field), list)
                    and len(v[field]) <= 100 and all(self._row_valid(field,row) for row in v[field]))
            count, rows = value['total_count'], value[field]
            if expected is not None and count != expected:
                self._fail('github_history_changed_during_read', operation, 200)
            expected = count
            target = min(count,window) if window is not None else count
            if target > max_pages * 100:
                self._fail('github_history_limit_exceeded', operation, 200)
            collected.extend(rows)
            if len(collected) >= target:
                if len(collected) == min(count,page*100) and len({r['id'] for r in collected}) == len(collected):
                    self.collections.append(dict(operation=operation, total_available=count, checked=target,
                        complete_history=target==count, window_limit=window))
                    return collected[:target]
                self._fail('github_response_schema_invalid', operation, 200)
            if len(rows) != 100:
                self._fail('github_history_incomplete', operation, 200)
        self._fail('github_history_limit_exceeded', operation, 200)

    def archive(self, path):
        return self._read(path, 'recovery_archive', binary=True)

    @staticmethod
    def _row_valid(field, row):
        if not isinstance(row, dict) or type(row.get('id')) is not int or row['id'] < 1:
            return False
        if field == 'workflow_runs':
            return isinstance(row.get('event'),str) and row.get('conclusion') in {
                'success','failure','cancelled','timed_out','action_required','neutral','skipped','stale','startup_failure'}
        if field == 'jobs':
            return isinstance(row.get('steps'),list) and all(isinstance(s,dict)
                and isinstance(s.get('name'),str) and {'started_at','conclusion'} <= s.keys() for s in row['steps'])
        if field == 'artifacts':
            return isinstance(row.get('name'),str) and type(row.get('expired')) is bool
        return False
