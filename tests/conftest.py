"""Shared test fixtures.

We monkeypatch ``ChessComClient._get`` to read canned JSON from
tests/fixtures/api instead of hitting the network. Everything above ``_get``
(endpoint methods, pagination, lineage stamping, partitioned landing) runs for
real — so the tests validate our logic, not requests'.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chess_analytics import config as config_mod
from chess_analytics.chesscom_client import ChessComClient

FIXTURES = Path(__file__).parent / "fixtures" / "api"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# Maps an API path (as passed to _get) to a fixture file. Missing -> 404/None.
_ROUTES = {
    "titled/GM": "titled_GM.json",
    "player/hikaru": "player_hikaru.json",
    "player/hikaru/stats": "player_hikaru_stats.json",
    "player/hikaru/games/2026/08": "games_2026_08.json",
}


@pytest.fixture
def local_cfg(tmp_path, monkeypatch):
    """Config with storage pointed at a temp dir; trust-store off (no net)."""
    monkeypatch.setenv("CHESS_STORAGE__LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("CHESS_API__USE_OS_TRUST_STORE", "false")
    config_mod.load_config.cache_clear()
    cfg = config_mod.load_config()
    yield cfg
    config_mod.load_config.cache_clear()


@pytest.fixture
def fake_client(monkeypatch):
    """Patch _get to serve fixtures; archives index points at 2026/08."""
    def _fake_get(self, path: str):
        path = path.strip("/")
        if path == "player/hikaru/games/archives":
            return {"archives": ["https://api.chess.com/pub/player/hikaru/games/2026/08"]}
        name = _ROUTES.get(path)
        return _load(name) if name else None

    monkeypatch.setattr(ChessComClient, "_get", _fake_get)
