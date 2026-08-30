"""End-to-end ingestion tests (fixtures + temp local landing zone).

Proves the landing contract: correct partition paths, NDJSON, lineage fields,
and re-run idempotency — the same behaviour that will run on GCP.
"""

from __future__ import annotations

import json
from pathlib import Path

from chess_analytics.ingestion import pull_game_archives, pull_player_profiles, pull_titled_players


def _read_ndjson(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_pull_titled_lands_ndjson_with_lineage(local_cfg, fake_client):
    counts = pull_titled_players.run(["GM"])
    assert counts["GM"] == 5

    root = Path(local_cfg["storage"]["local_root"]) / "raw" / "titled" / "GM.json"
    rows = _read_ndjson(root)
    assert {r["username"] for r in rows} >= {"hikaru", "magnuscarlsen"}
    assert all(r["title"] == "GM" for r in rows)
    assert all("_ingested_at" in r and "_source_endpoint" in r for r in rows)


def test_pull_profiles_writes_profile_and_stats(local_cfg, fake_client):
    summary = pull_player_profiles.run(["Hikaru"])   # mixed case -> lowercased
    assert summary == {"profiles": 1, "stats": 1, "missing": 0}

    base = Path(local_cfg["storage"]["local_root"]) / "raw" / "players"
    profile = json.loads((base / "profiles" / "hikaru.json").read_text(encoding="utf-8"))
    assert profile["username"] == "hikaru"
    assert profile["_source_endpoint"] == "player/hikaru"
    assert (base / "stats" / "hikaru.json").exists()


def test_missing_player_counted_not_fatal(local_cfg, fake_client):
    summary = pull_player_profiles.run(["ghost-user-404"])
    assert summary == {"profiles": 0, "stats": 0, "missing": 1}


def test_pull_archives_partitions_and_is_idempotent(local_cfg, fake_client):
    first = pull_game_archives.run(["hikaru"], months=1)
    assert first == {"players": 1, "partitions": 1, "games": 2}

    part = (
        Path(local_cfg["storage"]["local_root"])
        / "raw" / "games" / "year=2026" / "month=08" / "player=hikaru.json"
    )
    games = _read_ndjson(part)
    assert len(games) == 2
    assert games[0]["url"].endswith("98234571")
    assert "_ingested_at" in games[0]

    # Re-run overwrites the same partition — no duplication.
    pull_game_archives.run(["hikaru"], months=1)
    assert len(_read_ndjson(part)) == 2
