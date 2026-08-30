"""Pull player profiles + stats for a set of usernames.

Output:
    raw/players/profiles/<username>.json   (single JSON object)
    raw/players/stats/<username>.json      (single JSON object)

Usernames come from (in priority order): the --usernames flag, else the
titled-player lists already landed by pull_titled_players.

Usage:
    python -m chess_analytics.ingestion.pull_player_profiles --usernames magnuscarlsen hikaru
    python -m chess_analytics.ingestion.pull_player_profiles --from-titled GM --limit 50
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

log = get_logger(__name__)


def _usernames_from_titled(cfg: dict, title: str, limit: int | None) -> list[str]:
    """Read usernames back from a previously-landed local titled file."""
    path = (
        Path(cfg["storage"]["local_root"])
        / cfg["storage"]["raw_prefix"]
        / "titled"
        / f"{title.upper()}.json"
    )
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — run pull_titled_players first (local backend only)."
        )
    names = [json.loads(line)["username"] for line in path.read_text(encoding="utf-8").splitlines() if line]
    return names[:limit] if limit else names


def run(usernames: list[str]) -> dict[str, int]:
    cfg = load_config()
    client = ChessComClient(cfg)
    storage = get_storage(cfg)

    ok_profiles = ok_stats = missing = 0
    for username in usernames:
        u = username.lower()
        profile = client.get_player_profile(u)
        if profile is None:
            log.warning("no profile for %s (404) — skipping", u)
            missing += 1
            continue
        storage.write_json(f"players/profiles/{u}.json", stamp_lineage(profile, f"player/{u}"))
        ok_profiles += 1

        stats = client.get_player_stats(u)
        if stats is not None:
            storage.write_json(f"players/stats/{u}.json", stamp_lineage(stats, f"player/{u}/stats"))
            ok_stats += 1

    summary = {"profiles": ok_profiles, "stats": ok_stats, "missing": missing}
    log.info("profile pull complete: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull Chess.com player profiles + stats.")
    parser.add_argument("--usernames", nargs="*", help="Explicit usernames to pull.")
    parser.add_argument("--from-titled", help="Read usernames from a landed titled list, e.g. GM.")
    parser.add_argument("--limit", type=int, help="Cap the number pulled.")
    args = parser.parse_args()

    if args.usernames:
        names = args.usernames
    elif args.from_titled:
        names = _usernames_from_titled(load_config(), args.from_titled, args.limit)
    else:
        parser.error("provide --usernames or --from-titled")
    run(names)


if __name__ == "__main__":
    main()
