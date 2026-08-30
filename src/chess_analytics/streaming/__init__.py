"""Streaming path: game-event simulator -> Pub/Sub -> Dataflow -> BigQuery.

Chess.com publishes no real-time API, so the "live" feed is a **replay** of real
archived games, emitted move-by-move. That is a standard pattern for building and
demonstrating a streaming system against real data, but it must never be passed
off as live: every event carries ``is_simulated = TRUE`` and keeps the original
game's date in ``source_game_date``, so event time and historical time can never
be silently conflated downstream.

Layout, arranged so the interesting logic is testable without Beam or GCP:

* :mod:`~chess_analytics.streaming.events` — the event contract: builders,
  validation, and the field list the BigQuery DDL is checked against. Pure
  Python.
* :mod:`~chess_analytics.streaming.publisher` — ``pubsub`` / ``memory`` /
  ``stdout`` backends behind one interface, mirroring ``storage.py``.
* :mod:`~chess_analytics.streaming.simulator` — the replay engine. Pure Python;
  takes a publisher and a clock, so tests run instantly with no sleeping.
* ``dataflow/streaming_pipeline.py`` — thin Beam glue that imports the above.
"""
