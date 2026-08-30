"""Pull titled-player username lists and land them as the player universe.

Output: raw/titled/<TITLE>.json   (NDJSON, one row per player)
    {"username": "...", "title": "GM", "_ingested_at": ..., "_source_endpoint": ...}

Usage:
    python -m chess_analytics.ingestion.pull_titled_players
    python -m chess_analytics.ingestion.pull_titled_players --titles GM IM
"""

from __future__ import annotations

import argparse

from ..chesscom_client import ChessComClient
from ..config import load_config
from ..logging_setup import get_logger
from ..storage import get_storage
from . import stamp_lineage

log = get_logger(__name__)


def run(titles: list[str] | None = None) -> dict[str, int]:
    cfg = load_config()
    titles = titles or cfg["ingestion"]["titled_categories"]
    client = ChessComClient(cfg)
    storage = get_storage(cfg)

    counts: dict[str, int] = {}
    for title in titles:
        endpoint = f"titled/{title.upper()}"
        usernames = client.get_titled_players(title)
        rows = (
            stamp_lineage({"username": u, "title": title.upper()}, endpoint)
            for u in usernames
        )
        counts[title.upper()] = storage.write_ndjson(f"titled/{title.upper()}.json", rows)
    log.info("titled player pull complete: %s", counts)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull Chess.com titled players.")
    parser.add_argument("--titles", nargs="*", help="Override configured titles, e.g. GM IM")
    args = parser.parse_args()
    run(args.titles)


if __name__ == "__main__":
    main()
