"""Run the game-event simulator locally.

    # Watch the feed as NDJSON, no GCP needed (publisher: stdout in config)
    python scripts/run_simulator.py --limit 3 --speed 20

    # Publish to Pub/Sub for real
    CHESS_STREAMING__PUBLISHER=pubsub \
    CHESS_PROJECT__GCP_PROJECT_ID=my-project \
    python scripts/run_simulator.py --limit 50

Reads games from the Sprint 1 landing zone, so the streaming path replays the
same real data the batch path models.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chess_analytics.config import load_config              # noqa: E402
from chess_analytics.logging_setup import get_logger        # noqa: E402
from chess_analytics.streaming.publisher import get_publisher  # noqa: E402
from chess_analytics.streaming.simulator import (            # noqa: E402
    Simulator,
    SimulatorConfig,
    load_game_contexts,
)

log = get_logger("run_simulator")


def main() -> int:
    cfg = load_config()
    default_root = (
        Path(cfg["storage"]["local_root"]) / cfg["storage"]["raw_prefix"] / "games"
    )

    ap = argparse.ArgumentParser(description="Replay archived games as a live event stream.")
    ap.add_argument("--games-root", default=str(default_root),
                    help="Directory of landed game NDJSON (default: local landing zone).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Max games to replay (0 = config's max_games).")
    ap.add_argument("--concurrent", type=int,
                    help="Games in flight at once (default: config).")
    ap.add_argument("--speed", type=float,
                    help="Speed multiplier; >1 compresses think time.")
    ap.add_argument("--seed", type=int, help="Seed the jitter for a reproducible run.")
    args = ap.parse_args()

    sim_config = SimulatorConfig.from_config(cfg)
    if args.concurrent is not None:
        sim_config.concurrent_games = args.concurrent
    if args.speed is not None:
        sim_config.speed_multiplier = args.speed
    if args.seed is not None:
        sim_config.seed = args.seed
    if args.limit:
        sim_config.max_games = args.limit

    contexts = load_game_contexts(args.games_root, sim_config.max_games)
    if not contexts:
        log.error("no replayable games found under %s", args.games_root)
        return 1

    publisher = get_publisher(cfg)
    log.info(
        "replaying %d games, %d concurrent, speed x%s, publisher=%s",
        len(contexts), sim_config.concurrent_games, sim_config.speed_multiplier,
        type(publisher).__name__,
    )
    with publisher:
        Simulator(publisher, sim_config).run(contexts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
