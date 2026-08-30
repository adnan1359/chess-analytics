"""Daily batch ETL: Chess.com API -> Bronze -> Silver -> Gold, with DQ gating.

One DAG handles both the daily schedule and backfills. There is deliberately no
separate backfill DAG: every task is parameterised by the run's data interval and
every write is window-scoped and idempotent, so a historical range is just

    airflow dags backfill chess_daily_batch -s 2026-03-01 -e 2026-06-30

A second DAG would duplicate the whole graph and then drift from it.

Deployment note: this file imports ``chess_analytics``, so the repo's ``src/``
must be on the Composer worker PYTHONPATH (or the package pip-installed into the
environment). ``sql/`` must be deployed too — see docs/architecture.md.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup

from chess_analytics.config import load_config
from chess_analytics.sql_runner import render, template_vars

CONFIG = load_config()
GCP_PROJECT = template_vars(CONFIG)["GCP_PROJECT"]
BQ_LOCATION = CONFIG["project"].get("bq_location", "US")

# Airflow renders these Jinja macros per run; for a backfill they follow the
# interval being replayed, which is exactly what makes backfill correct.
WINDOW_START = "{{ dag_run.conf.get('start_date', data_interval_start | ds) }}"
WINDOW_END = "{{ dag_run.conf.get('end_date', data_interval_end | ds) }}"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    # The Chess.com API is the only external dependency and it is rate-limited,
    # so a failed ingest should back off rather than hammer it.
    "execution_timeout": timedelta(hours=2),
}


def _date_params(*names: str) -> list[dict]:
    """Named BigQuery DATE query parameters bound to the run window."""
    values = {"start_date": WINDOW_START, "end_date": WINDOW_END}
    return [
        {
            "name": name,
            "parameterType": {"type": "DATE"},
            "parameterValue": {"value": values[name]},
        }
        for name in names
    ]


def bq_task(task_id: str, sql_file: str, params: list[dict] | None = None) -> BigQueryInsertJobOperator:
    """A BigQuery job task from one of our ``sql/`` files.

    SQL is rendered (``${VAR}`` substitution) at DAG-parse time; the date window
    stays a real BigQuery query *parameter* rather than string interpolation, so
    the SQL is injection-safe and BigQuery can reuse the query plan.
    """
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": render(sql_file, CONFIG),
                "useLegacySql": False,
                **({"queryParameters": params} if params else {}),
            }
        },
        location=BQ_LOCATION,
    )


with DAG(
    dag_id="chess_daily_batch",
    description="Chess.com medallion batch pipeline (Bronze -> Silver -> Gold) with DQ gate.",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",          # 04:00 UTC: after the prior UTC day is closed
    catchup=False,                  # backfills are explicit, not accidental
    max_active_runs=1,              # MERGE targets overlap; serialise runs
    default_args=DEFAULT_ARGS,
    tags=["chess", "batch", "medallion"],
    doc_md=__doc__,
    params={
        "titles": Param(["GM"], type="array",
                        description="Titled categories to seed the player universe."),
        "players": Param(200, type="integer", description="Players to pull archives for."),
        "months": Param(1, type="integer",
                        description="Recent months of archives to pull. 1 for a daily run."),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    # ---------------------------------------------------------------- #
    # Extract: pull from the Chess.com API into the GCS landing zone.
    # ---------------------------------------------------------------- #
    @task(task_id="ingest_chesscom")
    def ingest_chesscom(**context) -> dict:
        """Land raw API payloads in GCS. Idempotent: overwrites its partitions."""
        from chess_analytics.ingestion import (
            pull_game_archives,
            pull_player_profiles,
            pull_titled_players,
        )
        from chess_analytics.ingestion.pull_player_profiles import _usernames_from_titled

        conf = context["params"]
        pull_titled_players.run(conf["titles"])
        usernames = _usernames_from_titled(CONFIG, conf["titles"][0], conf["players"])
        pull_player_profiles.run(usernames)
        return pull_game_archives.run(usernames, conf["months"])

    ingest = ingest_chesscom()

    # ---------------------------------------------------------------- #
    # Bronze: external tables over the landing zone (no data movement).
    # ---------------------------------------------------------------- #
    with TaskGroup(group_id="bronze") as bronze:
        bq_task("raw_games", "bronze/raw_games.sql")
        bq_task("raw_players", "bronze/raw_players.sql")

    ops_ddl = bq_task("create_ops_tables", "ops/pipeline_runs.sql")

    # ---------------------------------------------------------------- #
    # Silver: conform, dedupe, type.
    # ---------------------------------------------------------------- #
    silver_clean_games = bq_task(
        "silver_clean_games", "silver/clean_games.sql",
        _date_params("start_date", "end_date"),
    )
    silver_player_game_results = bq_task(
        "silver_player_game_results", "silver/player_game_results.sql"
    )
    silver_openings_dim = bq_task("silver_openings_dim", "silver/openings_dim.sql")
    silver_dim_players = bq_task("silver_dim_players", "silver/dim_players.sql")

    # ---------------------------------------------------------------- #
    # Gold: KPI marts. Independent of each other, so they run in parallel.
    # ---------------------------------------------------------------- #
    with TaskGroup(group_id="gold") as gold:
        gold_daily = bq_task(
            "daily_player_kpis", "gold/daily_player_kpis.sql",
            _date_params("start_date", "end_date"),
        )
        gold_openings = bq_task("opening_win_rates", "gold/opening_win_rates.sql")
        gold_elo = bq_task("elo_trend_weekly", "gold/elo_trend_weekly.sql")
        gold_tc = bq_task("time_control_meta", "gold/time_control_meta.sql")
        gold_cohorts = bq_task("player_cohorts", "gold/player_cohorts.sql")

    # ---------------------------------------------------------------- #
    # Data quality: measure, then decide. Two tasks on purpose — the write
    # always completes so a failing run still leaves a full diagnostic record.
    # ---------------------------------------------------------------- #
    @task(task_id="run_dq_checks")
    def run_dq_checks(**context) -> str:
        from google.cloud import bigquery

        from chess_analytics.sql_runner import render as _render

        run_id = context["run_id"] or str(uuid.uuid4())
        client = bigquery.Client(project=GCP_PROJECT)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter(
                    "start_date", "DATE", context["data_interval_start"].date()
                ),
                bigquery.ScalarQueryParameter(
                    "end_date", "DATE", context["data_interval_end"].date()
                ),
            ]
        )
        client.query(
            _render("dq_checks/silver_clean_games.sql", CONFIG), job_config=job_config
        ).result()
        return run_id

    @task(task_id="dq_gate")
    def dq_gate(run_id: str) -> None:
        """Fail the DAG if any error-severity assertion failed.

        Warnings are left in ops.dq_results for the operational dashboard; only
        `error` blocks the pipeline, so a new API enum value does not take down
        a run that is otherwise correct.
        """
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT)
        rows = list(
            client.query(
                f"""
                SELECT check_name, failed_rows, total_rows, failed_pct, detail
                FROM `{GCP_PROJECT}.ops.dq_results`
                WHERE run_id = @run_id
                  AND severity = 'error'
                  AND NOT passed
                ORDER BY failed_pct DESC
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
                    ]
                ),
            ).result()
        )
        if rows:
            summary = "\n".join(
                f"  - {r.check_name}: {r.failed_rows}/{r.total_rows} "
                f"({r.failed_pct}%) — {r.detail}"
                for r in rows
            )
            raise AirflowFailException(f"{len(rows)} DQ check(s) failed:\n{summary}")

    dq_run = run_dq_checks()
    gate = dq_gate(dq_run)

    end = EmptyOperator(task_id="end")

    # ---------------------------------------------------------------- #
    # Graph
    # ---------------------------------------------------------------- #
    start >> ingest >> bronze >> ops_ddl

    # dim_players reads Bronze only, so it does not wait on clean_games.
    ops_ddl >> [silver_clean_games, silver_dim_players]

    silver_clean_games >> [silver_player_game_results, silver_openings_dim]

    # Gold dependencies, stated per-model rather than as one coarse barrier.
    silver_player_game_results >> [gold_daily, gold_elo, gold_cohorts]
    silver_openings_dim >> gold_openings
    silver_clean_games >> [gold_openings, gold_tc]
    silver_dim_players >> gold_cohorts

    gold >> dq_run >> gate >> end
