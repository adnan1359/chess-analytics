"""Dataflow streaming job: Pub/Sub -> validate -> dedup -> window -> BigQuery.

    python dataflow/streaming_pipeline.py \
        --project my-proj --region us-central1 \
        --subscription chess-game-events-dataflow \
        --runner DataflowRunner \
        --temp_location gs://chess-lakehouse/tmp

Run with ``--runner DirectRunner`` to exercise it locally against a real
subscription.

## Shape of the pipeline

    Pub/Sub
      |-> decode+validate --(invalid)--> streaming.dlq_events
      |
      +-- valid
           |-> Deduplicate(event_id)
                |-> streaming.live_game_events        (every event)
                |-> session windows by game_id  -----> streaming.live_game_stats
                |-> fixed 1-min windows (platform) --> streaming.live_game_stats
                |-> late panes ----------------------> streaming.late_events

## Why the business logic is not in here

Every non-trivial decision — event shape, validation rules, clock parsing — lives
in ``chess_analytics.streaming.events``, which is pure Python and unit-tested
without Beam. This file is deliberately thin glue: DoFns that call those
functions and wire up windowing. Beam pipelines are awkward to test and slow to
run; the logic they carry should not be trapped inside them.

## Two different failure sinks, on purpose

* ``dlq_events`` — the message arrived fine but is not a valid event (bad JSON,
  unknown ``event_type``). A pipeline concern, handled here via a tagged output.
* the Pub/Sub **DLQ topic** — the message could not be *delivered* (nacked
  repeatedly). A subscription concern, configured in Terraform, not here.

Conflating them hides real problems: a poison message that crashes the worker
never reaches a tagged output, so the DLQ topic is the only thing that catches it.

## Late data

``allowed_lateness`` keeps a window open past the watermark. Anything later is
routed to ``late_events`` rather than dropped — silently discarding late data is
the classic streaming mistake, because the totals still look plausible.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms.deduplicate import Deduplicate
from apache_beam.transforms.window import FixedWindows, Sessions

from chess_analytics.streaming import events as ev

PIPELINE_VERSION = "streaming_pipeline/0.1"

TAG_INVALID = "invalid"
TAG_LATE = "late"


# --------------------------------------------------------------------------- #
# DoFns — each one delegates to the tested pure functions.
# --------------------------------------------------------------------------- #
class ParseAndValidate(beam.DoFn):
    """Decode a Pub/Sub message into a validated event row.

    Bad messages are emitted on the ``invalid`` tag instead of raising: one
    malformed message must not stall the pipeline, but it must not vanish either.
    """

    def process(self, message: beam.io.PubsubMessage) -> Iterable[Any]:
        raw = message.data.decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
            event = ev.validate(payload)
            yield ev.to_json_dict(event)
        except Exception as exc:                                    # noqa: BLE001
            yield beam.pvalue.TaggedOutput(
                TAG_INVALID,
                {
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:1000],
                    "pubsub_message_id": (message.attributes or {}).get("event_id"),
                    "attributes": json.dumps(dict(message.attributes or {})),
                    "payload": raw[:8000],
                    "pipeline_version": PIPELINE_VERSION,
                },
            )


class StampEventTime(beam.DoFn):
    """Re-time each element to its producer ``event_ts``.

    Without this, windows would be built on *arrival* time, which makes late and
    out-of-order data invisible — and therefore makes the whole late-data story
    a fiction. Events with an unparseable timestamp go to the DLQ.
    """

    def process(self, event: dict[str, Any]) -> Iterable[Any]:
        try:
            ts = datetime.fromisoformat(event["event_ts"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            yield beam.window.TimestampedValue(event, ts.timestamp())
        except (KeyError, ValueError, TypeError) as exc:
            yield beam.pvalue.TaggedOutput(
                TAG_INVALID,
                {
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "error_type": type(exc).__name__,
                    "error_message": f"bad event_ts: {exc}"[:1000],
                    "pubsub_message_id": event.get("event_id"),
                    "attributes": None,
                    "payload": json.dumps(event)[:8000],
                    "pipeline_version": PIPELINE_VERSION,
                },
            )


class SplitLatePanes(beam.DoFn):
    """Route late panes to the late-events sink, on-time panes onward.

    ``PaneInfoParam`` is how Beam exposes whether the current firing is EARLY,
    ON_TIME or LATE, which is the only reliable way to detect lateness after
    windowing.
    """

    def process(
        self,
        element: tuple[str, dict[str, Any]],
        pane_info=beam.DoFn.PaneInfoParam,
        window=beam.DoFn.WindowParam,
        timestamp=beam.DoFn.TimestampParam,
    ) -> Iterable[Any]:
        _, event = element
        if pane_info.timing == beam.utils.windowed_value.PaneInfoTiming.LATE:
            received = datetime.now(timezone.utc)
            event_ts = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
            yield beam.pvalue.TaggedOutput(
                TAG_LATE,
                {
                    "event_id": event.get("event_id"),
                    "game_id": event.get("game_id"),
                    "event_type": event.get("event_type"),
                    "event_ts": event.get("event_ts"),
                    "received_at": received.isoformat(),
                    "lateness_sec": (received - event_ts).total_seconds(),
                    "window_start": datetime.fromtimestamp(
                        float(window.start), tz=timezone.utc
                    ).isoformat(),
                    "pane_info": str(pane_info.timing),
                    "payload": json.dumps(event)[:8000],
                },
            )
        else:
            yield element


def summarise_game_session(
    keyed: tuple[str, Iterable[dict[str, Any]]],
    window=beam.DoFn.WindowParam,
) -> Iterable[dict[str, Any]]:
    """Collapse one game's events in a session window into a single stats row.

    Session windows are the right primitive for a game: activity separated by a
    gap longer than ``session_gap`` genuinely is a different playing session,
    which no fixed window can express.
    """
    game_id, group = keyed
    rows = sorted(group, key=lambda e: (e.get("ply") or 0, e.get("event_ts") or ""))
    moves = [r for r in rows if r.get("event_type") == ev.EVENT_MOVE]
    end = next((r for r in rows if r.get("event_type") == ev.EVENT_GAME_END), None)

    window_start = datetime.fromtimestamp(float(window.start), tz=timezone.utc)
    window_end = datetime.fromtimestamp(float(window.end), tz=timezone.utc)
    duration_min = max((window_end - window_start).total_seconds() / 60.0, 1e-9)

    stamps = sorted(
        datetime.fromisoformat(r["event_ts"]) for r in moves if r.get("event_ts")
    )
    gaps = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:])]
    clocks = [
        c for r in moves for c in (r.get("white_clock_sec"), r.get("black_clock_sec"))
        if c is not None
    ]
    first = rows[0] if rows else {}

    yield {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "window_type": "game_session",
        "game_id": game_id,
        "player_white": first.get("player_white"),
        "player_black": first.get("player_black"),
        "time_class": first.get("time_class"),
        "eco_code": first.get("eco_code"),
        "moves_in_window": len(moves),
        "first_ply": min((r["ply"] for r in moves if r.get("ply")), default=None),
        "last_ply": max((r["ply"] for r in moves if r.get("ply")), default=None),
        "moves_per_minute": round(len(moves) / duration_min, 3),
        "avg_think_time_sec": round(sum(gaps) / len(gaps), 3) if gaps else None,
        "min_clock_sec": min(clocks) if clocks else None,
        "is_finished": end is not None,
        "outcome": (end or {}).get("outcome"),
        "termination": (end or {}).get("termination"),
        "emitted_at": datetime.now(timezone.utc).isoformat(),
    }


def summarise_platform_window(
    keyed: tuple[str, Iterable[dict[str, Any]]],
    window=beam.DoFn.WindowParam,
) -> Iterable[dict[str, Any]]:
    """Platform-wide throughput for one fixed window, keyed by time_class.

    This is the "is the feed alive and how fast" metric, which needs a fixed
    window: session windows never close while play continues, so they cannot
    answer it.
    """
    time_class, group = keyed
    rows = list(group)
    moves = [r for r in rows if r.get("event_type") == ev.EVENT_MOVE]

    window_start = datetime.fromtimestamp(float(window.start), tz=timezone.utc)
    window_end = datetime.fromtimestamp(float(window.end), tz=timezone.utc)
    duration_min = max((window_end - window_start).total_seconds() / 60.0, 1e-9)
    clocks = [
        c for r in moves for c in (r.get("white_clock_sec"), r.get("black_clock_sec"))
        if c is not None
    ]

    yield {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "window_type": "fixed_metrics",
        # game_id is intentionally NULL: this row describes the platform, not a
        # game. active_games would need its own column; moves_in_window and
        # moves_per_minute are the throughput signals.
        "game_id": None,
        "player_white": None,
        "player_black": None,
        "time_class": time_class,
        "eco_code": None,
        "moves_in_window": len(moves),
        "first_ply": None,
        "last_ply": None,
        "moves_per_minute": round(len(moves) / duration_min, 3),
        "avg_think_time_sec": None,
        "min_clock_sec": min(clocks) if clocks else None,
        "is_finished": any(r.get("event_type") == ev.EVENT_GAME_END for r in rows),
        "outcome": None,
        "termination": None,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
def build_pipeline(pipeline: beam.Pipeline, args: argparse.Namespace) -> None:
    project = args.project
    subscription = f"projects/{project}/subscriptions/{args.subscription}"

    def table(name: str) -> str:
        return f"{project}:streaming.{name}"

    # with_attributes=True so the DLQ rows can record Pub/Sub attributes.
    raw = (
        pipeline
        | "ReadPubSub" >> beam.io.ReadFromPubSub(
            subscription=subscription, with_attributes=True
        )
    )

    parsed = raw | "ParseValidate" >> beam.ParDo(ParseAndValidate()).with_outputs(
        TAG_INVALID, main="valid"
    )

    stamped = parsed.valid | "StampEventTime" >> beam.ParDo(
        StampEventTime()
    ).with_outputs(TAG_INVALID, main="timed")

    # Dedup on event_id. Pub/Sub is at-least-once, so redelivery is expected
    # rather than exceptional; without this, a retried publish becomes a
    # duplicate row and every count is subtly inflated.
    deduped = stamped.timed | "DedupByEventId" >> Deduplicate(
        processing_time_duration=args.dedup_window_sec
    )

    # Sink 1: raw event stream.
    _ = deduped | "WriteEvents" >> beam.io.WriteToBigQuery(
        table("live_game_events"),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )

    # Sink 2: per-game session windows, with late panes split off.
    session_windowed = (
        deduped
        | "KeyByGame" >> beam.Map(lambda e: (e["game_id"], e))
        | "SessionWindow" >> beam.WindowInto(
            Sessions(args.session_gap_sec),
            allowed_lateness=args.allowed_lateness_sec,
            trigger=beam.trigger.AfterWatermark(late=beam.trigger.AfterCount(1)),
            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
        )
        | "SplitLate" >> beam.ParDo(SplitLatePanes()).with_outputs(
            TAG_LATE, main="ontime"
        )
    )

    _ = (
        session_windowed.ontime
        | "GroupGame" >> beam.GroupByKey()
        | "SummariseSession" >> beam.FlatMap(summarise_game_session)
        | "WriteSessionStats" >> beam.io.WriteToBigQuery(
            table("live_game_stats"),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )

    # Sink 3: platform throughput on fixed windows.
    _ = (
        deduped
        | "FixedWindow" >> beam.WindowInto(FixedWindows(args.metrics_window_sec))
        | "KeyByTimeClass" >> beam.Map(lambda e: (e.get("time_class") or "unknown", e))
        | "GroupTimeClass" >> beam.GroupByKey()
        | "SummarisePlatform" >> beam.FlatMap(summarise_platform_window)
        | "WriteMetrics" >> beam.io.WriteToBigQuery(
            table("live_game_stats"),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )

    # Sink 4: late events.
    _ = session_windowed[TAG_LATE] | "WriteLate" >> beam.io.WriteToBigQuery(
        table("late_events"),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )

    # Sink 5: DLQ from both failure points, merged into one table.
    _ = (
        (parsed[TAG_INVALID], stamped[TAG_INVALID])
        | "FlattenInvalid" >> beam.Flatten()
        | "WriteDlq" >> beam.io.WriteToBigQuery(
            table("dlq_events"),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )


def parse_args(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--subscription", default="chess-game-events-dataflow")
    parser.add_argument("--session_gap_sec", type=int, default=300)
    parser.add_argument("--metrics_window_sec", type=int, default=60)
    parser.add_argument("--allowed_lateness_sec", type=int, default=120)
    parser.add_argument("--dedup_window_sec", type=int, default=600)
    return parser.parse_known_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.getLogger().setLevel(logging.INFO)
    args, beam_argv = parse_args(argv)

    options = PipelineOptions(beam_argv)
    options.view_as(SetupOptions).save_main_session = True
    # Streaming is required for Pub/Sub, and Dataflow needs the project set even
    # when it is also passed to us directly.
    options.view_as(StandardOptions).streaming = True
    options.view_as(GoogleCloudOptions).project = args.project

    with beam.Pipeline(options=options) as pipeline:
        build_pipeline(pipeline, args)


if __name__ == "__main__":
    main()
