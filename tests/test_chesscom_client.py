"""Client-level tests: retry/backoff, 404-as-None, and Retry-After handling.

These patch the *session* (not _get) so the real _get logic runs over fake
HTTP responses.
"""

from __future__ import annotations

import pytest

from chess_analytics.chesscom_client import ChessComClient, ChessComError

_CFG = {
    "api": {
        "base_url": "https://api.chess.com/pub",
        "user_agent": "chess-analytics-test/0.1",
        "timeout_sec": 5,
        "min_interval_sec": 0.0,   # no real sleeping in tests
        "max_retries": 4,
        "use_os_trust_store": False,
    }
}


class _FakeResp:
    def __init__(self, status_code, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class _FakeSession:
    """Returns queued responses in order; records how many GETs happened."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.headers = {}
        self.calls = 0

    def get(self, url, timeout=None):
        self.calls += 1
        return self._responses.pop(0)


def _client_with(responses):
    client = ChessComClient(_CFG)
    client._session = _FakeSession(responses)
    return client


def test_200_returns_parsed_json():
    client = _client_with([_FakeResp(200, {"players": ["hikaru"]})])
    assert client.get_titled_players("GM") == ["hikaru"]


def test_404_returns_none_not_error():
    client = _client_with([_FakeResp(404, text="not found")])
    assert client.get_player_profile("nobody-xyz") is None


def test_retries_on_429_then_succeeds(monkeypatch):
    # Avoid real backoff sleeps from tenacity + Retry-After.
    monkeypatch.setattr("chess_analytics.chesscom_client.time.sleep", lambda *_: None)
    responses = [
        _FakeResp(429, headers={"Retry-After": "1"}),
        _FakeResp(200, {"players": ["magnuscarlsen"]}),
    ]
    client = _client_with(responses)
    assert client.get_titled_players("GM") == ["magnuscarlsen"]
    assert client._session.calls == 2


def test_exhausted_retries_raises(monkeypatch):
    monkeypatch.setattr("chess_analytics.chesscom_client.time.sleep", lambda *_: None)
    responses = [_FakeResp(503) for _ in range(4)]
    client = _client_with(responses)
    with pytest.raises(ChessComError):
        client.get_titled_players("GM")


def test_unexpected_4xx_raises():
    client = _client_with([_FakeResp(418, text="teapot")])
    with pytest.raises(ChessComError):
        client.get_player_profile("hikaru")
