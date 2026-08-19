"""
db/drata_client.py tests. DrataClient._push_one_record's retry budget is the critical
piece here: 429 (rate limit) and genuine errors (5xx/network) are counted separately
(2026-08-18 fix) so throttling under 10 concurrent push workers doesn't get reported as
a permanent per-record failure. time.sleep is patched out so these run instantly
regardless of configured retry delays.
"""

import pytest

from db import drata_client as dc


class _FakeResponse:
    def __init__(self, status_code, text='', json_data=None, headers=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data or {}
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def post(self, url, json=None, timeout=None):
        self.calls += 1
        return self._responses[min(self.calls - 1, len(self._responses) - 1)]

    def get(self, url, timeout=None):
        return self.post(url)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(dc.time, 'sleep', lambda seconds: None)


def _client_with_responses(responses):
    client = dc.DrataClient(api_key='k', connection_id='c')
    client._local.session = _FakeSession(responses)
    return client, client._local.session


def test_push_one_record_succeeds_first_try():
    client, session = _client_with_responses([_FakeResponse(200)])
    errors = []
    ok = client._push_one_record('http://x', {'personnelId': 'p'}, 1, 1, errors)
    assert ok is True
    assert errors == []
    assert session.calls == 1


def test_push_one_record_4xx_fails_immediately_no_retry():
    client, session = _client_with_responses([_FakeResponse(400, text='bad request')])
    errors = []
    ok = client._push_one_record('http://x', {'personnelId': 'p'}, 1, 1, errors)
    assert ok is False
    assert session.calls == 1  # no retry on a genuine 4xx
    assert errors[0]['error'] == 'HTTP 400: bad request'


def test_push_one_record_5xx_exhausts_max_retries():
    client, session = _client_with_responses([_FakeResponse(500, text='boom')] * 5)
    errors = []
    ok = client._push_one_record('http://x', {'personnelId': 'p'}, 1, 1, errors)
    assert ok is False
    assert session.calls == dc._MAX_RETRIES
    assert errors[0]['error'] == 'HTTP 500: boom'


def test_push_one_record_429_does_not_consume_genuine_error_budget():
    """More 429s than _MAX_RETRIES, followed by success -- proves rate-limit retries are
    counted separately from the small genuine-error budget."""
    responses = [_FakeResponse(429, headers={'Retry-After': '1'})] * (dc._MAX_RETRIES + 2) + [_FakeResponse(200)]
    client, session = _client_with_responses(responses)
    errors = []
    ok = client._push_one_record('http://x', {'personnelId': 'p'}, 1, 1, errors)
    assert ok is True
    assert errors == []
    assert session.calls == dc._MAX_RETRIES + 3


def test_push_one_record_exhausts_rate_limit_budget():
    responses = [_FakeResponse(429, headers={'Retry-After': '1'})] * (dc._MAX_RATE_LIMIT_RETRIES + 2)
    client, session = _client_with_responses(responses)
    errors = []
    ok = client._push_one_record('http://x', {'personnelId': 'p'}, 1, 1, errors)
    assert ok is False
    assert 'rate limited' in errors[0]['error']


def test_push_one_record_429_with_non_numeric_retry_after_falls_back():
    """Retry-After can be an HTTP-date string per spec -- a non-numeric value must not crash."""
    responses = [
        _FakeResponse(429, headers={'Retry-After': 'Wed, 21 Oct 2026 07:28:00 GMT'}),
        _FakeResponse(200),
    ]
    client, _ = _client_with_responses(responses)
    errors = []
    ok = client._push_one_record('http://x', {'personnelId': 'p'}, 1, 1, errors)
    assert ok is True


def test_get_person_status_404_returns_none():
    client, _ = _client_with_responses([_FakeResponse(404)])
    assert client.get_person_status('user@example.com') is None


def test_get_person_status_returns_employment_status():
    client, _ = _client_with_responses([_FakeResponse(200, json_data={'employmentStatus': 'CURRENT_EMPLOYEE'})])
    assert client.get_person_status('user@example.com') == 'CURRENT_EMPLOYEE'


def test_get_person_status_429_with_non_numeric_retry_after_falls_back():
    responses = [
        _FakeResponse(429, headers={'Retry-After': 'not-a-number'}),
        _FakeResponse(200, json_data={'employmentStatus': 'CURRENT_EMPLOYEE'}),
    ]
    client, _ = _client_with_responses(responses)
    assert client.get_person_status('user@example.com') == 'CURRENT_EMPLOYEE'
