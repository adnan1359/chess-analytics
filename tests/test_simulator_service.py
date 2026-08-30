"""Tests for the Cloud Run simulator service (simulator/main.py).

Driven through the WSGI interface with a fake ``environ``, so no server, no
network, and no GCP. That is the whole benefit of keeping it a plain WSGI app.
"""

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def service():
    path = REPO / "simulator" / "main.py"
    spec = importlib.util.spec_from_file_location("simulator_main", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def games_dir(tmp_path) -> Path:
    """A landing-zone-shaped directory of real fixture games."""
    games = json.loads(
        (REPO / "tests" / "fixtures" / "api" / "games_2026_08.json").read_text(encoding="utf-8")
    )["games"]
    part = tmp_path / "year=2026" / "month=08"
    part.mkdir(parents=True)
    (part / "player=hikaru.json").write_text(
        "\n".join(json.dumps(g) for g in games), encoding="utf-8"
    )
    return tmp_path


def _call(service, path: str, method: str = "GET", body: dict | None = None):
    """Invoke the WSGI app and return (status, parsed_json)."""
    raw = json.dumps(body or {}).encode() if body is not None else b""
    environ = {
        "PATH_INFO": path,
        "REQUEST_METHOD": method,
        "CONTENT_LENGTH": str(len(raw)) if raw else "",
        "wsgi.input": io.BytesIO(raw),
    }
    captured: dict = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    chunks = service.app(environ, start_response)
    return captured["status"], json.loads(b"".join(chunks))


def test_health_endpoint(service):
    status, payload = _call(service, "/health")
    assert status.startswith("200")
    assert payload == {"status": "ok"}


def test_unknown_path_is_404(service):
    status, payload = _call(service, "/nope")
    assert status.startswith("404")
    assert payload["error"] == "not found"


def test_replay_requires_post(service):
    status, _ = _call(service, "/replay", method="GET")
    assert status.startswith("404")


def test_replay_runs_and_reports_a_summary(service, games_dir, monkeypatch):
    # 'memory' publisher: no Pub/Sub, and a huge speed multiplier so the paced
    # replay finishes instantly.
    monkeypatch.setenv("CHESS_STREAMING__PUBLISHER", "memory")
    from chess_analytics import config as config_mod

    config_mod.load_config.cache_clear()

    status, payload = _call(
        service, "/replay", method="POST",
        body={"games_root": str(games_dir), "limit": 2, "speed": 100000, "seed": 1},
    )
    config_mod.load_config.cache_clear()

    assert status.startswith("200"), payload
    assert payload["games"] == 2
    assert payload["games_completed"] == 2
    assert payload["events_published"] > 0


def test_replay_missing_games_returns_500_with_reason(service, tmp_path, monkeypatch):
    monkeypatch.setenv("CHESS_STREAMING__PUBLISHER", "memory")
    from chess_analytics import config as config_mod

    config_mod.load_config.cache_clear()
    status, payload = _call(
        service, "/replay", method="POST",
        body={"games_root": str(tmp_path / "empty"), "limit": 1},
    )
    config_mod.load_config.cache_clear()

    assert status.startswith("500")
    assert payload["error"] == "FileNotFoundError"


def test_malformed_body_is_reported_not_crashed(service, monkeypatch):
    monkeypatch.setenv("CHESS_STREAMING__PUBLISHER", "memory")
    environ = {
        "PATH_INFO": "/replay",
        "REQUEST_METHOD": "POST",
        "CONTENT_LENGTH": "7",
        "wsgi.input": io.BytesIO(b"{bad{{{"),
    }
    captured: dict = {}
    chunks = service.app(environ, lambda s, h: captured.setdefault("status", s))
    payload = json.loads(b"".join(chunks))
    assert captured["status"].startswith("500")
    assert "JSON" in payload["error"] or payload["error"] == "JSONDecodeError"


def test_concurrent_replay_is_rejected_with_409(service, games_dir, monkeypatch):
    """Two overlapping replays would double-publish the same game ids.

    A 409 (not a 500) is correct: the request is legitimate, just mistimed.
    """
    monkeypatch.setenv("CHESS_STREAMING__PUBLISHER", "memory")
    from chess_analytics import config as config_mod

    config_mod.load_config.cache_clear()

    # Hold the lock as an in-flight replay would.
    assert service._replay_lock.acquire(blocking=False)
    try:
        status, payload = _call(
            service, "/replay", method="POST",
            body={"games_root": str(games_dir), "limit": 1},
        )
    finally:
        service._replay_lock.release()
    config_mod.load_config.cache_clear()

    assert status.startswith("409")
    assert "already running" in payload["error"]


def test_lock_is_released_after_a_failed_replay(service, tmp_path, monkeypatch):
    """A failure must not wedge the service into permanent 409s."""
    monkeypatch.setenv("CHESS_STREAMING__PUBLISHER", "memory")
    from chess_analytics import config as config_mod

    config_mod.load_config.cache_clear()
    _call(service, "/replay", method="POST",
          body={"games_root": str(tmp_path / "gone"), "limit": 1})
    config_mod.load_config.cache_clear()

    acquired = service._replay_lock.acquire(blocking=False)
    assert acquired, "lock leaked after a failed replay"
    service._replay_lock.release()
