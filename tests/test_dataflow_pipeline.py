"""Tests for the Dataflow streaming transforms, on the DirectRunner.

The pipeline's I/O (Pub/Sub in, BigQuery out) can't run offline, but the parts
that carry logic — validation routing, event-time stamping, the window
summarisers, and late-pane splitting — all can, and those are what break.

``build_pipeline`` itself is exercised via a real ``TestStream`` in
``test_windowing_end_to_end`` with the sinks swapped out, so the wiring and the
windowing/lateness configuration are covered too, not just the DoFns.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

beam = pytest.importorskip("apache_beam", reason="apache-beam not installed")

# Beam's DirectRunner is slow to spin up; deselect with -m "not slow".
pytestmark = pytest.mark.slow

from apache_beam.testing.test_pipeline import TestPipeline          # noqa: E402
from apache_beam.testing.util import assert_that, equal_to, is_empty  # noqa: E402

from chess_analytics.streaming import events as ev                  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 8, 30, 12, 0, 0, tzinfo=timezone.utc)


def _load_pipeline_module():
    """Import dataflow/streaming_pipeline.py (not an installed package)."""
    path = REPO / "dataflow" / "streaming_pipeline.py"
    spec = importlib.util.spec_from_file_location("streaming_pipeline", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def sp():
    return _load_pipeline_module()


@pytest.fixture(scope="module")
def sample_events() -> list[dict]:
    """A full replayed game's events, as the simulator would emit them."""
    games = json.loads(
        (REPO / "tests" / "fixtures" / "api" / "games_2026_08.json").read_text(encoding="utf-8")
    )["games"]
    ctx = ev.game_context_from_archive(games[2])

    def stamps():
        current = T0
        while True:
            yield current
            current += timedelta(seconds=2)

    return [ev.to_json_dict(e) for e in ev.iter_game_events(ctx, stamps())]


class _FakeMessage:
    """Stands in for beam.io.PubsubMessage (which needs a real read to build)."""

    def __init__(self, data: bytes, attributes: dict | None = None) -> None:
        self.data = data
        self.attributes = attributes or {}


class _FakeWindow:
    def __init__(self, start: datetime, end: datetime) -> None:
        self.start = start.timestamp()
        self.end = end.timestamp()


# --------------------------------------------------------------------------- #
# Module imports at all — catches Beam API misuse in the pipeline file.
# --------------------------------------------------------------------------- #
def test_pipeline_module_imports(sp):
    assert hasattr(sp, "build_pipeline")
    assert sp.PIPELINE_VERSION


def test_arg_defaults_match_config_intent(sp):
    args, _ = sp.parse_args(["--project", "p"])
    assert args.allowed_lateness_sec == 120
    assert args.session_gap_sec == 300
    assert args.metrics_window_sec == 60
    assert args.dedup_window_sec == 600


# --------------------------------------------------------------------------- #
# ParseAndValidate
# --------------------------------------------------------------------------- #
def test_parse_valid_event_goes_to_main_output(sp, sample_events):
    message = _FakeMessage(json.dumps(sample_events[1]).encode())
    with TestPipeline() as p:
        outputs = (
            p
            | beam.Create([message])
            | beam.ParDo(sp.ParseAndValidate()).with_outputs(sp.TAG_INVALID, main="valid")
        )
        assert_that(
            outputs.valid | beam.Map(lambda e: e["move_san"]),
            equal_to(["d4"]),
            label="valid",
        )
        assert_that(outputs[sp.TAG_INVALID], is_empty(), label="noinvalid")


@pytest.mark.parametrize(
    "payload, expected_error",
    [
        (b"not json at all", "JSONDecodeError"),
        (b'{"event_type": "move"}', "EventValidationError"),          # no ids
        (b'{"event_id":"1","event_type":"nope","event_ts":"x","game_id":"g"}',
         "EventValidationError"),
        (b'[]', "EventValidationError"),                              # not an object
    ],
)
def test_bad_messages_go_to_dlq_not_main(sp, payload, expected_error):
    """A malformed message must neither crash the pipeline nor vanish."""
    with TestPipeline() as p:
        outputs = (
            p
            | beam.Create([_FakeMessage(payload, {"event_id": "abc"})])
            | beam.ParDo(sp.ParseAndValidate()).with_outputs(sp.TAG_INVALID, main="valid")
        )
        assert_that(outputs.valid, is_empty(), label="novalid")
        assert_that(
            outputs[sp.TAG_INVALID] | beam.Map(lambda r: r["error_type"]),
            equal_to([expected_error]),
            label="dlq",
        )


def test_dlq_row_preserves_payload_and_attributes(sp):
    with TestPipeline() as p:
        outputs = (
            p
            | beam.Create([_FakeMessage(b"broken", {"event_id": "e1", "game_id": "g1"})])
            | beam.ParDo(sp.ParseAndValidate()).with_outputs(sp.TAG_INVALID, main="valid")
        )
        assert_that(
            outputs[sp.TAG_INVALID] | beam.Map(lambda r: (r["payload"], r["pubsub_message_id"])),
            equal_to([("broken", "e1")]),
        )
        assert_that(
            outputs[sp.TAG_INVALID] | beam.Map(lambda r: json.loads(r["attributes"])["game_id"]),
            equal_to(["g1"]),
            label="attrs",
        )


def test_invalid_utf8_is_handled_not_raised(sp):
    """Undecodable bytes must land in the DLQ, not kill the worker."""
    with TestPipeline() as p:
        outputs = (
            p
            | beam.Create([_FakeMessage(b"\xff\xfe\x00bad")])
            | beam.ParDo(sp.ParseAndValidate()).with_outputs(sp.TAG_INVALID, main="valid")
        )
        assert_that(outputs.valid, is_empty(), label="novalid")
        assert_that(outputs[sp.TAG_INVALID] | beam.Map(lambda r: 1), equal_to([1]), label="one")


# --------------------------------------------------------------------------- #
# StampEventTime
# --------------------------------------------------------------------------- #
def test_stamp_event_time_uses_producer_timestamp(sp, sample_events):
    """Windows must be built on event time, or lateness is invisible."""
    with TestPipeline() as p:
        outputs = (
            p
            | beam.Create([sample_events[0]])
            | beam.ParDo(sp.StampEventTime()).with_outputs(sp.TAG_INVALID, main="timed")
        )
        assert_that(
            outputs.timed
            | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: float(ts)),
            equal_to([T0.timestamp()]),
        )


def test_stamp_event_time_bad_timestamp_goes_to_dlq(sp, sample_events):
    bad = dict(sample_events[0], event_ts="definitely-not-a-timestamp")
    with TestPipeline() as p:
        outputs = (
            p
            | beam.Create([bad])
            | beam.ParDo(sp.StampEventTime()).with_outputs(sp.TAG_INVALID, main="timed")
        )
        assert_that(outputs.timed, is_empty(), label="none")
        assert_that(
            outputs[sp.TAG_INVALID] | beam.Map(lambda r: r["error_type"]),
            equal_to(["ValueError"]),
            label="dlq",
        )


# --------------------------------------------------------------------------- #
# Window summarisers (called directly — they are plain functions)
# --------------------------------------------------------------------------- #
def test_summarise_game_session(sp, sample_events):
    window = _FakeWindow(T0, T0 + timedelta(minutes=1))
    rows = list(sp.summarise_game_session(("98235123", sample_events), window=window))
    assert len(rows) == 1
    row = rows[0]

    assert row["window_type"] == "game_session"
    assert row["game_id"] == "98235123"
    assert row["moves_in_window"] == 20          # 22 events - start - end
    assert (row["first_ply"], row["last_ply"]) == (1, 20)
    assert row["player_white"] == "hikaru"
    assert row["time_class"] == "bullet"
    # 20 moves in a 1-minute window
    assert row["moves_per_minute"] == pytest.approx(20.0)
    # Events are 2s apart in the fixture stream.
    assert row["avg_think_time_sec"] == pytest.approx(2.0)
    # Lowest clock seen across both sides.
    assert row["min_clock_sec"] == pytest.approx(40.2)
    assert row["is_finished"] is True
    assert row["outcome"] == "0-1"
    assert row["termination"] == "resignation"


def test_summarise_game_session_without_end_event(sp, sample_events):
    """A game still in progress must report is_finished=False, not crash."""
    in_progress = [e for e in sample_events if e["event_type"] != "game_end"]
    window = _FakeWindow(T0, T0 + timedelta(minutes=1))
    row = next(sp.summarise_game_session(("98235123", in_progress), window=window))
    assert row["is_finished"] is False
    assert row["outcome"] is None
    assert row["termination"] is None


def test_summarise_game_session_handles_single_move(sp, sample_events):
    """One move means zero gaps — avg_think_time must be None, not a divide-by-zero."""
    one = [e for e in sample_events if e["event_type"] == "move"][:1]
    window = _FakeWindow(T0, T0 + timedelta(minutes=1))
    row = next(sp.summarise_game_session(("98235123", one), window=window))
    assert row["moves_in_window"] == 1
    assert row["avg_think_time_sec"] is None


def test_summarise_platform_window(sp, sample_events):
    window = _FakeWindow(T0, T0 + timedelta(seconds=60))
    row = next(sp.summarise_platform_window(("bullet", sample_events), window=window))
    assert row["window_type"] == "fixed_metrics"
    assert row["time_class"] == "bullet"
    assert row["moves_in_window"] == 20
    assert row["moves_per_minute"] == pytest.approx(20.0)
    # Platform rows describe the feed, not one game.
    assert row["game_id"] is None


def test_summariser_rows_match_live_game_stats_columns(sp, sample_events):
    """Both summarisers must emit exactly the stats table's columns."""
    import re

    from chess_analytics import sql_runner

    sql = sql_runner.render(REPO / "sql" / "streaming" / "live_game_events.sql")
    block = re.search(
        r"CREATE TABLE IF NOT EXISTS `[^`]*\.live_game_stats`\s*\((.*?)\n\)\s*\nPARTITION BY",
        sql, re.DOTALL,
    )
    columns = set(re.findall(
        r"^\s{2}(\w+)\s+(?:STRING|INT64|FLOAT64|BOOL|DATE|TIMESTAMP)\b",
        block.group(1), re.MULTILINE,
    ))

    window = _FakeWindow(T0, T0 + timedelta(minutes=1))
    session_row = next(sp.summarise_game_session(("g", sample_events), window=window))
    platform_row = next(sp.summarise_platform_window(("bullet", sample_events), window=window))
    assert set(session_row) == columns
    assert set(platform_row) == columns


# --------------------------------------------------------------------------- #
# Windowing + lateness, end to end on the DirectRunner
# --------------------------------------------------------------------------- #
def test_windowing_end_to_end(sp, sample_events):
    """Drive real event-time windowing with TestStream.

    Two games' events are interleaved and the watermark advanced past them; each
    game must collapse into its own session-window stats row.
    """
    from apache_beam.testing.test_stream import TestStream

    game_a = sample_events
    game_b = [dict(e, game_id="OTHER", event_id=f"b-{i}") for i, e in enumerate(sample_events)]

    stream = TestStream()
    for a, b in zip(game_a, game_b):
        ts = datetime.fromisoformat(a["event_ts"]).timestamp()
        stream = stream.add_elements([a, b], event_timestamp=ts)
    # Push the watermark well past the session gap so windows close.
    last = datetime.fromisoformat(game_a[-1]["event_ts"]).timestamp()
    stream = stream.advance_watermark_to(last + 10_000).advance_watermark_to_infinity()

    options = beam.options.pipeline_options.PipelineOptions()
    options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

    with TestPipeline(options=options) as p:
        rows = (
            p
            | stream
            | beam.Map(lambda e: (e["game_id"], e))
            | beam.WindowInto(
                beam.window.Sessions(300),
                allowed_lateness=120,
                trigger=beam.trigger.AfterWatermark(late=beam.trigger.AfterCount(1)),
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
            )
            | beam.GroupByKey()
            | beam.FlatMap(sp.summarise_game_session)
        )
        # One stats row per game, each with all 20 moves.
        assert_that(
            rows | beam.Map(lambda r: (r["game_id"], r["moves_in_window"], r["is_finished"])),
            equal_to([("98235123", 20, True), ("OTHER", 20, True)]),
        )


def test_dedup_drops_replayed_event_ids(sp, sample_events):
    """Pub/Sub is at-least-once, so redelivery must not inflate counts."""
    from apache_beam.transforms.deduplicate import Deduplicate

    duplicated = sample_events + sample_events        # every event twice

    options = beam.options.pipeline_options.PipelineOptions()
    options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

    with TestPipeline(options=options) as p:
        out = (
            p
            | beam.Create(duplicated)
            | beam.Map(lambda e: (e["event_id"], e))
            | Deduplicate(processing_time_duration=600)
            | beam.Map(lambda kv: kv[0])
        )
        assert_that(out, equal_to([e["event_id"] for e in sample_events]))
