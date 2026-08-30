"""Run the batch pipeline without Airflow/Composer.

Cloud Composer costs roughly $300/month, which is hard to justify for a portfolio
project. This script executes the same SQL files in the same dependency order as
``airflow/dags/chess_daily_batch.py``, so the pipeline is fully runnable on the
free tier. The DAG remains the production artefact; this is the cheap path.

    # validate every statement against BigQuery without running it (free)
    python scripts/run_batch.py --start 2026-08-01 --end 2026-08-31 --dry-run

    # execute
    python scripts/run_batch.py --start 2026-08-01 --end 2026-08-31

    # Silver only
    python scripts/run_batch.py --start 2026-08-01 --end 2026-08-31 --layers silver
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chess_analytics.config import load_config          # noqa: E402
from chess_analytics.logging_setup import get_logger    # noqa: E402
from chess_analytics.sql_runner import run              # noqa: E402

log = get_logger("run_batch")

# (layer, sql file, needs the date window) in dependency order — mirrors the DAG.
PIPELINE: list[tuple[str, str, bool]] = [
    ("ops",    "ops/pipeline_runs.sql",            False),
    ("bronze", "bronze/raw_games.sql",             False),
    ("bronze", "bronze/raw_players.sql",           False),
    ("silver", "silver/clean_games.sql",           True),
    ("silver", "silver/player_game_results.sql",   False),
    ("silver", "silver/openings_dim.sql",          False),
    ("silver", "silver/dim_players.sql",           False),
    ("gold",   "gold/daily_player_kpis.sql",       True),
    ("gold",   "gold/opening_win_rates.sql",       False),
    ("gold",   "gold/elo_trend_weekly.sql",        False),
    ("gold",   "gold/time_control_meta.sql",       False),
    ("gold",   "gold/player_cohorts.sql",          False),
]

DQ_STEP = ("dq", "dq_checks/silver_clean_games.sql", True)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the chess batch pipeline on BigQuery.")
    ap.add_argument("--start", required=True, type=_parse_date, help="Window start (YYYY-MM-DD).")
    ap.add_argument("--end", required=True, type=_parse_date, help="Window end, inclusive.")
    ap.add_argument("--layers", nargs="*", default=["ops", "bronze", "silver", "gold", "dq"],
                    help="Subset of layers to run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Validate against BigQuery and report bytes scanned; execute nothing.")
    args = ap.parse_args()

    if args.start > args.end:
        ap.error("--start must not be after --end")

    cfg = load_config()
    run_id = f"manual-{uuid.uuid4().hex[:12]}"
    window = {"start_date": args.start, "end_date": args.end}
    log.info("run_id=%s window=%s..%s layers=%s dry_run=%s",
             run_id, args.start, args.end, args.layers, args.dry_run)

    steps = [s for s in PIPELINE if s[0] in args.layers]
    if "dq" in args.layers:
        steps.append(DQ_STEP)

    failures = 0
    for layer, sql_file, needs_window in steps:
        params = dict(window) if needs_window else {}
        if sql_file == DQ_STEP[1]:
            params["run_id"] = run_id
        log.info("[%s] %s", layer, sql_file)
        try:
            run(sql_file, params=params, config=cfg, dry_run=args.dry_run)
        except Exception as exc:                      # noqa: BLE001 - report and continue
            failures += 1
            log.error("[%s] %s FAILED: %s", layer, sql_file, exc)
            # Later layers read earlier ones, so continuing past a failure would
            # just produce confusing downstream errors.
            break

    if failures:
        log.error("pipeline aborted after %d failure(s)", failures)
        return 1
    log.info("pipeline complete (run_id=%s)", run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
