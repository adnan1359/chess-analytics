"""Pull monthly game archives and land them partitioned by year/month/player.

Output (Hive-style partitioning, one game per NDJSON line):
    raw/games/year=YYYY/month=MM/player=<username>.json

Idempotency: each (year, month, player) is a single file. Re-running a pull
overwrites exactly that partition file and nothing else — safe to re-run and
safe to backfill a specific month without touching others.

Usage:
    python -m chess_analytics.ingestion.pull_game_archives --usernames hikaru --months 2
    python -m chess_analytics.ingestion.pull_game_archives --from-titled GM --limit 200
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..chesscom_client import ChessComClient
from ..config import load_config
from ..logging_setup import get_logger
from ..storage import get_storage
from . import stamp_lineage
from .pull_player_profiles import _usernames_from_titled

log = get_logger(__name__)


def _partition_key(year: int, month: int, username: str) -> str:
    return f"games/year={year:04d}/month={month:02d}/player={username}.json"


def run(usernames: list[str], months: int) -> dict[str, int]:
    cfg = load_config()
    client = ChessComClient(cfg)
    storage = get_storage(cfg)

    total_games = 0
    partitions = 0
    for username in usernames:
        u = username.lower()
        for year, month, games in client.iter_recent_months(u, months):
            if not games:
                continue
            endpoint = f"player/{u}/games/{year:04d}/{month:02d}"
            rows = (stamp_lineage(g, endpoint) for g in games)
            n = storage.write_ndjson(_partition_key(year, month, u), rows)
            total_games += n
            partitions += 1
    summary = {"players": len(usernames), "partitions": partitions, "games": total_games}
    log.info("archive pull complete: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull Chess.com monthly game archives.")
    parser.add_argument("--usernames", nargs="*", help="Explicit usernames to pull.")
    parser.add_argument("--from-titled", help="Read usernames from a landed titled list, e.g. GM.")
    parser.add_argument("--limit", type=int, help="Cap the number of players.")
    parser.add_argument("--months", type=int, help="Most recent N months (default: config).")
    args = parser.parse_args()

    cfg = load_config()
    months = args.months or cfg["ingestion"]["archive_months"]
    if args.usernames:
        names = args.usernames
    elif args.from_titled:
        names = _usernames_from_titled(cfg, args.from_titled, args.limit or cfg["ingestion"]["top_players"])
    else:
        parser.error("provide --usernames or --from-titled")
    run(names, months)


if __name__ == "__main__":
    main()
