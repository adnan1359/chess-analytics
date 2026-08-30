"""Drift guards between the event contract, the BigQuery DDL, and the pipeline.

Three components must agree on what an event looks like: the simulator that
produces it, the Dataflow job that consumes it, and the table it lands in. Two of
those are Python and one is SQL, so nothing but a test keeps them aligned.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot

from chess_analytics import sql_runner
from chess_analytics.streaming import events as ev

SQL_ROOT = Path(__file__).resolve().parents[1] / "sql"
STREAMING_SQL = SQL_ROOT / "streaming" / "live_game_events.sql"


@pytest.fixture(scope="module")
def rendered() -> str:
    return sql_runner.render(STREAMING_SQL)


def _table_columns(sql: str, table: str) -> list[str]:
    """Ordered column names from a `CREATE TABLE ... (` block."""
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS `[^`]*\.{table}`\s*\((.*?)\n\)\s*\nPARTITION BY",
        sql,
        re.DOTALL,
    )
    assert match, f"could not locate DDL for {table}"
    return re.findall(
        r"^\s{2}(\w+)\s+(?:STRING|INT64|FLOAT64|BOOL|DATE|TIMESTAMP)\b",
        match.group(1),
        re.MULTILINE,
    )


def test_streaming_sql_parses_as_bigquery(rendered):
    statements = sqlglot.parse(rendered, dialect="bigquery")
    assert len(statements) >= 5    # schema + 4 tables


def test_live_game_events_columns_match_event_contract(rendered):
    """The table must have exactly the contract's fields, in the same order.

    Order matters because the producer builds rows from EVENT_FIELDS; a mismatch
    would surface as confusing BigQuery insert errors at runtime rather than here.
    """
    columns = _table_columns(rendered, "live_game_events")
    assert columns == list(ev.EVENT_FIELDS), (
        f"missing from DDL: {sorted(set(ev.EVENT_FIELDS) - set(columns))}; "
        f"extra in DDL: {sorted(set(columns) - set(ev.EVENT_FIELDS))}"
    )


def test_all_streaming_tables_exist(rendered):
    for table in ("live_game_events", "live_game_stats", "late_events", "dlq_events"):
        assert f".{table}`" in rendered, f"{table} DDL is missing"


def test_streaming_tables_are_partitioned_and_expire(rendered):
    """A streaming sink without an expiry grows without bound."""
    assert rendered.count("PARTITION BY DATE(") == 4
    assert rendered.count("partition_expiration_days") == 4


def test_is_simulated_is_not_nullable(rendered):
    """The honesty flag must never be absent — that is the whole point of it."""
    assert re.search(r"is_simulated\s+BOOL\s+NOT NULL", rendered)


def test_event_types_are_documented_in_ddl(rendered):
    for event_type in sorted(ev.EVENT_TYPES):
        assert event_type in rendered, f"{event_type} not mentioned in the DDL"


def test_no_per_move_fen_column(rendered):
    """Per-move FEN is not derivable from the source; it must not be a column.

    Chess.com archives give one final FEN per game. A `fen` column on the event
    table would invite someone to populate it with something invented.
    """
    columns = _table_columns(rendered, "live_game_events")
    assert "fen" not in columns
    assert "final_fen" in columns
    assert "fen" not in ev.EVENT_FIELDS
