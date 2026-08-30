"""Render ``${VAR}`` SQL templates and (optionally) run them on BigQuery.

The SQL under ``sql/`` is the single definition of the batch pipeline; it is
deliberately plain ``.sql`` with ``${VAR}`` placeholders rather than a
framework-specific dialect, so the same files are used by:

* the Airflow DAG (``BigQueryInsertJobOperator``),
* ``scripts/run_batch.py`` for a Composer-free local run, and
* ``tests/test_sql_syntax.py``, which renders and parses them offline.

Query *parameters* (``@start_date``) are intentionally NOT string-substituted —
they go to BigQuery as real query parameters, so the SQL is injection-safe and
BigQuery can cache plans.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from string import Template
from typing import Any

from .config import load_config
from .logging_setup import get_logger

log = get_logger(__name__)

SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"

_PLACEHOLDER_RE = re.compile(r"\$\{(\w+)\}")


def template_vars(config: dict[str, Any] | None = None) -> dict[str, str]:
    """Values substituted into ``${VAR}`` placeholders."""
    cfg = config or load_config()
    return {
        "GCP_PROJECT": os.environ.get("CHESS_GCP_PROJECT", cfg["project"]["gcp_project_id"]),
        "BUCKET": cfg["storage"]["gcs_bucket"],
        "BQ_LOCATION": cfg["project"].get("bq_location", "US"),
    }


def render(sql_path: str | Path, config: dict[str, Any] | None = None) -> str:
    """Read a .sql file and substitute ``${VAR}`` placeholders.

    Raises if the file references a placeholder we have no value for — better a
    loud failure here than a BigQuery error about a table called
    ``${GCP_PROJECT}.silver.clean_games``.
    """
    path = Path(sql_path)
    if not path.is_absolute():
        path = SQL_ROOT / path
    raw = path.read_text(encoding="utf-8")

    variables = template_vars(config)
    missing = {name for name in _PLACEHOLDER_RE.findall(raw)} - variables.keys()
    if missing:
        raise KeyError(f"{path.name}: no value for placeholder(s) {sorted(missing)}")

    return Template(raw).safe_substitute(variables)


def run(
    sql_path: str | Path,
    params: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    dry_run: bool = False,
) -> Any:
    """Execute a rendered SQL file on BigQuery.

    ``params`` become typed BigQuery query parameters. ``dry_run=True`` asks
    BigQuery to validate the SQL and report bytes scanned without running it —
    the cheapest possible CI check against the real engine.
    """
    from google.cloud import bigquery  # lazy: keeps local/test runs GCP-free

    sql = render(sql_path, config)
    client = bigquery.Client(project=template_vars(config)["GCP_PROJECT"])

    job_config = bigquery.QueryJobConfig(
        dry_run=dry_run,
        use_query_cache=False,
        query_parameters=_to_bq_params(params or {}),
    )
    job = client.query(sql, job_config=job_config)

    if dry_run:
        log.info("%s: dry run OK, %s bytes would be scanned",
                 Path(sql_path).name, f"{job.total_bytes_processed:,}")
        return job

    job.result()  # block until finished
    log.info("%s: done (%s ms, %s bytes)",
             Path(sql_path).name,
             job.slot_millis if hasattr(job, "slot_millis") else "n/a",
             f"{job.total_bytes_processed or 0:,}")
    return job


def bq_param_type(value: Any) -> str:
    """BigQuery scalar type for a Python value.

    Order matters: ``bool`` is a subclass of ``int`` and ``datetime`` is a
    subclass of ``date``, so the narrower type must be tested first.
    """
    from datetime import date, datetime

    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, date):
        return "DATE"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, str):
        return "STRING"
    raise TypeError(f"unsupported query-parameter type: {type(value).__name__}")


def _to_bq_params(params: dict[str, Any]) -> list[Any]:
    """Map Python values to BigQuery ScalarQueryParameters."""
    from google.cloud import bigquery

    return [
        bigquery.ScalarQueryParameter(name, bq_param_type(value), value)
        for name, value in params.items()
    ]
