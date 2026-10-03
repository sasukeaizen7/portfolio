from datetime import date

import pytest
import requests

from etl.extract import CITIES, ExtractError, fetch_daily_weather


class FakeResponse:
    def __init__(self, status: int, body: dict | None = None):
        self.status_code, self._body, self.text = status, body or {}, str(body)

    def json(self):
        return self._body


class FakeSession:
    """Replays a scripted list of responses (or exceptions) and records the requests."""

    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


PARIS = CITIES["paris"]
JAN = (date(2024, 1, 1), date(2024, 1, 3))


def test_success_sends_expected_parameters(paris_payload):
    s = FakeSession(FakeResponse(200, paris_payload))
    assert fetch_daily_weather(PARIS, *JAN, session=s) == paris_payload
    assert s.calls[0]["start_date"] == "2024-01-01" and "precipitation_sum" in s.calls[0]["daily"]


def test_retries_transient_errors_then_succeeds(paris_payload):
    s = FakeSession(FakeResponse(503), requests.ConnectionError("reset"), FakeResponse(200, paris_payload))
    assert fetch_daily_weather(PARIS, *JAN, session=s, backoff_s=0) == paris_payload
    assert len(s.calls) == 3


def test_gives_up_after_max_attempts():
    s = FakeSession(*[FakeResponse(429)] * 3)
    with pytest.raises(ExtractError, match="giving up after 3 attempts"):
        fetch_daily_weather(PARIS, *JAN, session=s, attempts=3, backoff_s=0)


def test_client_errors_are_not_retried():
    s = FakeSession(FakeResponse(400, {"reason": "bad date"}))
    with pytest.raises(ExtractError, match="HTTP 400"):
        fetch_daily_weather(PARIS, *JAN, session=s, backoff_s=0)
    assert len(s.calls) == 1


def test_rejects_inverted_date_range():
    with pytest.raises(ValueError):
        fetch_daily_weather(PARIS, date(2024, 2, 1), date(2024, 1, 1), session=FakeSession())
