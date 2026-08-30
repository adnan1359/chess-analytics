"""Sprint 1 batch entrypoint: titled players -> profiles/stats -> game archives.

This is the single command Airflow's `pull_new_games` task (or a Cloud Run job)
invokes. Each step is idempotent, so the whole thing is safe to re-run.

    python scripts/run_ingestion.py --titles GM --players 200 --months 6

Run it from a network that can reach api.chess.com (home / GCP). On the
Cognizant corporate network, Zscaler blocks chess.com — see README.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running as a plain script (python scripts/run_ingestion.py) without install.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chess_analytics.config import load_config          # noqa: E402
from chess_analytics.logging_setup import get_logger      # noqa: E402
from chess_analytics.ingestion import (                    # noqa: E402
    pull_game_archives,
    pull_player_profiles,
    pull_titled_players,
)
from chess_analytics.ingestion.pull_player_profiles import _usernames_from_titled  # noqa: E402

log = get_logger("run_ingestion")


def main() -> None:
    cfg = load_config()
    ap = argparse.ArgumentParser(description="Run the Sprint 1 ingestion pipeline.")
    ap.add_argument("--titles", nargs="*", default=cfg["ingestion"]["titled_categories"])
    ap.add_argument("--players", type=int, default=cfg["ingestion"]["top_players"],
                    help="How many players (per first title) to pull profiles+archives for.")
    ap.add_argument("--months", type=int, default=cfg["ingestion"]["archive_months"])
    args = ap.parse_args()

    log.info("STEP 1/3 titled players: %s", args.titles)
    pull_titled_players.run(args.titles)

    seed_title = args.titles[0]
    usernames = _usernames_from_titled(cfg, seed_title, args.players)
    log.info("STEP 2/3 profiles+stats for %d players (seed=%s)", len(usernames), seed_title)
    pull_player_profiles.run(usernames)

    log.info("STEP 3/3 game archives (%d months)", args.months)
    pull_game_archives.run(usernames, args.months)

    log.info("ingestion pipeline complete")


if __name__ == "__main__":
    main()
