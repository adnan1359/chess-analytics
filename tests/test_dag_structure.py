"""Static checks on the Airflow DAG.

Airflow cannot be installed in this environment (no Windows/py3.14 wheel), so we
cannot import the DAG and walk the real graph. Instead we parse the file with
``ast`` and assert the things that actually break in review:

* it is syntactically valid Python (a broken DAG file silently vanishes from the
  Airflow UI rather than erroring loudly);
* every SQL file it references exists on disk — a typo here fails at runtime,
  minutes into a scheduled run;
* the window-scoped tasks are the ones that receive date parameters;
* the operational conventions we chose (max_active_runs=1, catchup=False,
  retries) are still in place.

The real import-time check belongs in CI where Airflow is installed:
``pytest --dag-import`` or ``airflow dags list-import-errors``. Noted in docs.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
DAG_FILE = REPO / "airflow" / "dags" / "chess_daily_batch.py"
SQL_ROOT = REPO / "sql"


@pytest.fixture(scope="module")
def tree() -> ast.Module:
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"), filename=str(DAG_FILE))


@pytest.fixture(scope="module")
def source() -> str:
    return DAG_FILE.read_text(encoding="utf-8")


def test_dag_file_is_valid_python(tree):
    assert isinstance(tree, ast.Module)


def test_every_referenced_sql_file_exists(source):
    """Collect the "layer/file.sql" string literals and check them on disk."""
    referenced = {
        node.value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.endswith(".sql")
    }
    assert referenced, "the DAG should reference SQL files"
    missing = sorted(rel for rel in referenced if not (SQL_ROOT / rel).is_file())
    assert not missing, f"DAG references SQL that does not exist: {missing}"


def test_all_pipeline_sql_is_referenced_by_the_dag(source):
    """Nothing in sql/ is orphaned — every model is actually orchestrated."""
    referenced = {
        node.value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.endswith(".sql")
    }
    on_disk = {str(p.relative_to(SQL_ROOT)).replace("\\", "/") for p in SQL_ROOT.rglob("*.sql")}
    assert on_disk - referenced == set(), f"SQL not orchestrated by the DAG: {sorted(on_disk - referenced)}"


def test_window_scoped_models_receive_date_params(source):
    """clean_games and daily_player_kpis are the two window-scoped writes."""
    for task in ["silver_clean_games", "gold/daily_player_kpis.sql"]:
        assert task in source
    # Both bq_task calls that pass _date_params must be exactly these two.
    assert source.count('_date_params("start_date", "end_date")') == 2


def test_operational_conventions_present(source):
    # Serialised runs: the MERGE and the Gold DELETE+INSERT target overlapping
    # windows, so concurrent runs could interleave.
    assert "max_active_runs=1" in source
    # Backfills should be deliberate, not triggered by a paused-DAG catch-up.
    assert "catchup=False" in source
    assert '"retries": 2' in source
    assert "retry_delay" in source


def test_dq_is_measure_then_gate(source):
    """The gate must be a separate task from the measurement write."""
    assert "run_dq_checks" in source
    assert "dq_gate" in source
    assert "AirflowFailException" in source
    assert source.index("def run_dq_checks") < source.index("def dq_gate")


def test_no_hardcoded_project_id(source):
    """Project comes from config/env, never a literal in the DAG."""
    assert "chess-analytics-dev" not in source
    assert "template_vars" in source
