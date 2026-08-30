"""Offline verification of the batch SQL.

No BigQuery connection is available in CI here, so these tests do what *can* be
done offline, and are explicit about the limit:

* every file parses in the BigQuery dialect (catches typos, unbalanced parens,
  wrong-dialect syntax);
* every ``${VAR}`` placeholder has a value, and an unknown one fails loudly;
* the MERGE's UPDATE SET covers every non-key column in its own DDL;
* the vocabulary in mappings.yaml and the SQL CASE branches agree.

What they do NOT check: that referenced columns exist in BigQuery. That needs a
real dry-run against the live schema — ``scripts/run_batch.py --dry-run``, which
is free and is the intended pre-deploy gate.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot
import yaml

from chess_analytics import sql_runner

SQL_ROOT = Path(__file__).resolve().parents[1] / "sql"
MAPPINGS = yaml.safe_load(
    (Path(__file__).resolve().parents[1] / "config" / "mappings.yaml").read_text(encoding="utf-8")
)
ALL_SQL = sorted(SQL_ROOT.rglob("*.sql"))


def _render(path: Path) -> str:
    return sql_runner.render(path)


def test_sql_files_exist():
    assert len(ALL_SQL) >= 13, "expected the full pipeline's SQL to be present"


@pytest.mark.parametrize("sql_path", ALL_SQL, ids=lambda p: str(p.relative_to(SQL_ROOT)))
def test_sql_parses_as_bigquery(sql_path):
    """Each file is valid BigQuery per sqlglot.

    Note: sqlglot does not model `CREATE EXTERNAL TABLE ... WITH PARTITION
    COLUMNS`, so it degrades those to a generic Command node instead of raising.
    Statement-level structure is still validated.
    """
    statements = sqlglot.parse(_render(sql_path), dialect="bigquery")
    assert statements, f"{sql_path.name} produced no statements"
    assert all(s is not None for s in statements)


@pytest.mark.parametrize("sql_path", ALL_SQL, ids=lambda p: str(p.relative_to(SQL_ROOT)))
def test_no_unsubstituted_placeholders(sql_path):
    assert "${" not in _render(sql_path), f"{sql_path.name} has an unsubstituted placeholder"


def test_render_rejects_unknown_placeholder(tmp_path):
    bad = tmp_path / "bad.sql"
    bad.write_text("SELECT 1 FROM `${NOT_A_REAL_VAR}.t`", encoding="utf-8")
    with pytest.raises(KeyError, match="NOT_A_REAL_VAR"):
        sql_runner.render(bad)


# --------------------------------------------------------------------------- #
# MERGE completeness — forgetting a column in UPDATE SET is a silent staleness
# bug: re-running the transform would leave that column at its original value.
# --------------------------------------------------------------------------- #
def test_merge_update_covers_every_non_key_column():
    sql = (SQL_ROOT / "silver" / "clean_games.sql").read_text(encoding="utf-8")

    ddl = re.search(
        r"CREATE TABLE IF NOT EXISTS[^(]*\((.*?)\n\)\nPARTITION BY", sql, re.DOTALL
    )
    assert ddl, "could not locate the clean_games DDL column block"
    ddl_columns = {
        m.group(1)
        for m in re.finditer(
            r"^\s{2}(\w+)\s+(?:STRING|INT64|FLOAT64|BOOL|DATE|TIMESTAMP)",
            ddl.group(1),
            re.MULTILINE,
        )
    }
    assert "game_id" in ddl_columns and len(ddl_columns) > 25, ddl_columns

    update_block = re.search(r"WHEN MATCHED THEN UPDATE SET(.*?)WHEN NOT MATCHED", sql, re.DOTALL)
    assert update_block, "MERGE has no UPDATE SET block"
    updated = set(re.findall(r"^\s{2}(\w+)\s*=", update_block.group(1), re.MULTILINE))

    # game_id is the join key and must not be reassigned.
    expected = ddl_columns - {"game_id"}
    assert updated == expected, (
        f"missing from UPDATE SET: {sorted(expected - updated)}; "
        f"unexpected: {sorted(updated - expected)}"
    )


# --------------------------------------------------------------------------- #
# Drift guard: config/mappings.yaml is the source of truth, but the batch path
# re-expresses it as SQL CASE branches. If someone adds a code to the YAML and
# not the SQL, Silver would silently classify it as 'unknown'.
# --------------------------------------------------------------------------- #
def test_every_termination_code_is_mapped_in_silver_sql():
    sql = (SQL_ROOT / "silver" / "clean_games.sql").read_text(encoding="utf-8")
    for code, category in MAPPINGS["result_reason_to_termination"].items():
        assert f"'{code}'" in sql, f"result code {code!r} from mappings.yaml is not handled in clean_games.sql"
        assert f"'{category}'" in sql, f"termination category {category!r} is missing from clean_games.sql"


def test_is_draw_is_derived_from_outcome_not_termination():
    """is_draw must not depend on the termination lookup.

    If it did, a draw reason the API adds tomorrow would fall to 'unknown',
    make is_draw false, and silently corrupt every draw_rate in Gold.
    """
    sql = (SQL_ROOT / "silver" / "clean_games.sql").read_text(encoding="utf-8")
    assert re.search(r"\(outcome = '1/2-1/2'\)\s*AS is_draw", sql), (
        "is_draw should be derived from outcome"
    )
    assert not re.search(r"termination IN \(.*?\)\s*AS is_draw", sql, re.DOTALL), (
        "is_draw must not be derived from the termination CASE"
    )


def test_draw_terminations_match_dq_check():
    """The draw-category list lives in the DQ check that flags unmapped codes."""
    sql = (SQL_ROOT / "dq_checks" / "silver_clean_games.sql").read_text(encoding="utf-8")
    block = re.search(
        r"is_draw AND termination NOT IN \((.*?)\)", sql, re.DOTALL
    )
    assert block, "could not find the unmapped-draw-reason check"
    in_sql = set(re.findall(r"'([\w_]+)'", block.group(1)))
    assert in_sql == set(MAPPINGS["draw_terminations"]), (
        f"DQ draw list disagrees with mappings.yaml draw_terminations: "
        f"only in SQL {sorted(in_sql - set(MAPPINGS['draw_terminations']))}, "
        f"only in YAML {sorted(set(MAPPINGS['draw_terminations']) - in_sql)}"
    )


def test_elo_bracket_labels_match_mappings():
    sql = (SQL_ROOT / "silver" / "clean_games.sql").read_text(encoding="utf-8")
    for bracket in MAPPINGS["elo_brackets"]:
        assert f"'{bracket['label']}'" in sql, f"Elo bracket {bracket['label']!r} missing from clean_games.sql"


def test_time_classes_match_mappings():
    sql = (SQL_ROOT / "silver" / "clean_games.sql").read_text(encoding="utf-8")
    for tc in MAPPINGS["time_classes"]:
        assert f"'{tc}'" in sql, f"time_class {tc!r} missing from clean_games.sql"


def test_eco_volumes_match_mappings():
    """Volume names appear in three files; all must agree with the YAML."""
    files = ["silver/clean_games.sql", "silver/openings_dim.sql"]
    for rel in files:
        sql = (SQL_ROOT / rel).read_text(encoding="utf-8")
        for name in MAPPINGS["eco_volumes"].values():
            assert name in sql, f"ECO volume {name!r} missing from {rel}"


# --------------------------------------------------------------------------- #
# Cost / correctness conventions we want to keep holding
# --------------------------------------------------------------------------- #
def test_window_scoped_sql_uses_query_parameters_not_interpolation():
    """Date windows must be BigQuery parameters, never string-substituted."""
    for rel in ["silver/clean_games.sql", "gold/daily_player_kpis.sql",
                "dq_checks/silver_clean_games.sql"]:
        sql = (SQL_ROOT / rel).read_text(encoding="utf-8")
        assert "@start_date" in sql and "@end_date" in sql, f"{rel} should use date parameters"


def test_partitioned_tables_declare_partitioning():
    """Every large fact/mart table is partitioned, so daily reads prune."""
    expected = {
        "silver/clean_games.sql": "PARTITION BY game_date",
        "gold/daily_player_kpis.sql": "PARTITION BY game_date",
        "gold/elo_trend_weekly.sql": "PARTITION BY week_start_date",
        "ops/pipeline_runs.sql": "PARTITION BY DATE(logged_at)",
    }
    for rel, clause in expected.items():
        sql = (SQL_ROOT / rel).read_text(encoding="utf-8")
        assert clause in sql, f"{rel} is missing `{clause}`"


def test_gold_window_rebuild_is_transactional():
    """DELETE+INSERT must be wrapped so a failure cannot leave a hole."""
    sql = (SQL_ROOT / "gold" / "daily_player_kpis.sql").read_text(encoding="utf-8")
    assert "BEGIN TRANSACTION" in sql and "COMMIT TRANSACTION" in sql
    assert sql.index("BEGIN TRANSACTION") < sql.index("DELETE FROM")
    assert sql.index("INSERT INTO") < sql.index("COMMIT TRANSACTION")
