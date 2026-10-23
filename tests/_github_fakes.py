"""Real Response objects for GET mocks; explicit counted API fixture shapes."""
import json
from requests import Response


def response(data=None, *, content=None, status=200, headers=None):
    result = Response()
    result.status_code = status
    result.url = 'https://api.github.com/repos/offline/repo/actions/runs'
    result._content = content if content is not None else json.dumps(data).encode()
    result._content_consumed = True
    result.headers.update(headers or {})
    return result


def collection(data):
    """Supply the fields GitHub always sends, previously omitted by fixtures."""
    for field in ('workflow_runs', 'jobs', 'artifacts'):
        if field not in data:
            continue
        rows = data[field]
        data = {**data, 'total_count':len(rows)}
        if field == 'jobs':
            data[field] = [{'id':i+1, **row} for i,row in enumerate(rows)]
        if field == 'workflow_runs':
            data[field] = [{'event':'schedule', **row} for row in rows]
    return data
