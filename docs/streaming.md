# Streaming Pipeline Design

```
Landed archives (GCS)
      |
      v
Game-event simulator (Cloud Run)          <- replays real games move-by-move
      |  ordering_key = game_id
      v
Pub/Sub topic  chess-game-events ----(undeliverable)----> DLQ topic
      |
      v
Dataflow streaming job (Beam)
      |-- decode + validate --(invalid)--> streaming.dlq_events
      |-- Deduplicate(event_id)
      |     |--> streaming.live_game_events         every valid event
      |     |--> session windows (game_id) -------> streaming.live_game_stats
      |     |--> fixed 1-min windows (time_class) -> streaming.live_game_stats
      |     \--> late panes ---------------------> streaming.late_events
```

## The honest bit: this is a replay, not a live feed

Chess.com publishes **no** real-time API. The "live" stream is real archived
games replayed move-by-move. That is a legitimate and common way to build and
demonstrate a streaming system against real data — but it would be misleading to
present it as live play, so the design refuses to let that confusion happen:

- every event carries **`is_simulated = TRUE`** (`NOT NULL` in the DDL);
- **`source_game_date`** keeps the date the game was *actually* played, separate
  from `event_ts`, which is replay wall-clock;
- `producer_version` records which simulator build emitted the row.

A dashboard built on this can therefore say "simulated feed" truthfully, and no
join can silently mix replay time with historical time.

## Simulator

[`src/chess_analytics/streaming/simulator.py`](../src/chess_analytics/streaming/simulator.py)

- **Interleaved.** Several games are in flight at once, sequenced through a
  min-heap keyed on each game's next due time. A simulator that finished game 1
  before starting game 2 would produce a stream whose windowed aggregates are
  trivially clean, and would prove nothing about the pipeline.
- **Injectable clock and sleeper.** Nothing calls `time.sleep` directly, so the
  tests replay thousands of events deterministically in milliseconds.
- **Seeded.** Think-time jitter comes from a seeded `Random`, so any demo is
  exactly reproducible.
- **Paced.** `speed_multiplier` compresses think time so a demo produces useful
  volume quickly.

Run it with no GCP at all (`publisher: stdout` emits NDJSON):

```bash
python scripts/run_simulator.py --limit 3 --speed 500 --seed 42
```

### Clocks: only what was actually measured

Many archived PGNs carry no `[%clk]` annotations. The running clocks therefore
start as `NULL` and only ever hold values read from the PGN. Seeding them with
the base time would emit `white_clock_sec = 600.0` on move 40 of a 10-minute
game — a number never measured, which would quietly poison `min_clock_sec` and
every clock-pressure metric. `game_start` is the one exception: at move 0 both
clocks genuinely *are* the base time.

Daily games get `NULL` clocks throughout, because their `base_time_sec` is
seconds **per move**, not a starting clock.

### No per-move FEN

The original plan sketched a `fen` field on every move event. It is not
implemented, because it is not derivable from the source: Chess.com archives give
one *final* FEN per game, not a position per ply. Producing it would mean
replaying moves through a real board implementation (`python-chess`). No metric
needs it, so rather than emit a plausible-but-wrong value the field is absent —
and a test asserts the column does not exist, so nobody adds it and fills it with
something invented. `final_fen` is populated on `game_end`, where the source
genuinely provides it.

## Pub/Sub

- **Ordering.** `ordering_key = game_id`, so a game's plies cannot be processed
  out of order, while different games still parallelise.
- **Attributes.** `event_id`, `game_id`, `event_type` are duplicated into message
  attributes so a consumer or Pub/Sub filter can route without parsing the body.
- **Flush on exit.** The publisher blocks on every in-flight future; a process
  that exited immediately after publishing would drop messages still sitting in
  the client's batch buffer.

## Dataflow job

[`dataflow/streaming_pipeline.py`](../dataflow/streaming_pipeline.py)

The Beam file is deliberately **thin glue**. Every real decision — event shape,
validation rules, clock parsing — lives in
[`streaming/events.py`](../src/chess_analytics/streaming/events.py), which is pure
Python and unit-tested without Beam. Logic trapped inside a Beam pipeline is slow
and awkward to test, so it does not live there.

### Event time, not arrival time

`StampEventTime` re-times every element to the producer's `event_ts`. Windowing
on *arrival* time would make out-of-order and late data invisible — and would
make the entire late-data story below a fiction.

### Deduplication

Pub/Sub is at-least-once, so redelivery is expected, not exceptional. Without
dedup a retried publish becomes a duplicate row and every count is quietly
inflated. `Deduplicate(processing_time_duration=600)` keyed on `event_id` handles
it; `event_id` is a per-event UUID, which is why the simulator generates a fresh
one per event rather than deriving it from `(game_id, ply)`.

### Two windows, because they answer different questions

| Window | Key | Answers |
|---|---|---|
| `Sessions(300s)` | `game_id` | per-game summary: moves, think time, clock pressure |
| `FixedWindows(60s)` | `time_class` | platform throughput: is the feed alive, moves/min |

Session windows are the right primitive for a game — activity separated by a long
gap genuinely is a different session, which no fixed window expresses. But a
session window never closes while play continues, so it cannot answer "how fast
is the feed right now"; that needs the fixed window.

### Late data is kept, not dropped

`allowed_lateness = 120s` keeps a window open past the watermark. Anything later
is routed to `streaming.late_events` via a tagged output, detected using Beam's
`PaneInfoParam` (`LATE` panes). Silently discarding late data is the classic
streaming mistake: the totals still look plausible and are quietly wrong.
Retaining the rows makes lateness *measurable*, so `allowed_lateness` can be
tuned against evidence rather than guesswork.

### Two failure sinks, on purpose

|  | What it catches | Where it is configured |
|---|---|---|
| `streaming.dlq_events` | delivered fine, but **not a valid event** — bad JSON, unknown `event_type`, missing `ply`, undecodable UTF-8 | this pipeline, via a tagged output |
| Pub/Sub **DLQ topic** | could not be **delivered** — nacked repeatedly | the subscription (Terraform, Sprint 6) |

Both are needed because they fail in different places. A poison message that
crashes the worker never reaches a tagged output, so only the DLQ topic catches
it. Conflating the two would leave one class of failure invisible.

## Sinks

| Table | Grain | Notes |
|---|---|---|
| `streaming.live_game_events` | one row per valid event | columns are asserted to match `EVENT_FIELDS` exactly |
| `streaming.live_game_stats` | one row per (window, key) | both window types, distinguished by `window_type` |
| `streaming.late_events` | one row per late event | includes `lateness_sec` and the raw payload |
| `streaming.dlq_events` | one row per rejected message | includes the raw payload, so nothing is lost |

All four are partitioned on date with `partition_expiration_days = 30`: a
streaming sink without an expiry grows forever, and this one is fed by a
simulator that can be left running.

## Deploy

```bash
# 1. Create the BigQuery sinks (one-time)
python scripts/run_batch.py --start 2026-08-01 --end 2026-08-01 --layers streaming

# 2. Pub/Sub topic + DLQ + subscription with ordering
gcloud pubsub topics create chess-game-events
gcloud pubsub topics create chess-game-events-dlq
gcloud pubsub subscriptions create chess-game-events-dataflow \
  --topic chess-game-events \
  --enable-message-ordering \
  --dead-letter-topic chess-game-events-dlq \
  --max-delivery-attempts 5

# 3. Simulator on Cloud Run (concurrency 1 — see below)
docker build -f simulator/Dockerfile -t chess-simulator .
gcloud run deploy chess-simulator --source . \
  --set-env-vars CHESS_STREAMING__PUBLISHER=pubsub \
  --max-instances 1 --concurrency 1 --timeout 3600

# 4. Dataflow job
python dataflow/streaming_pipeline.py \
  --project $PROJECT --region us-central1 \
  --subscription chess-game-events-dataflow \
  --runner DataflowRunner \
  --temp_location gs://chess-lakehouse/tmp \
  --max_num_workers 2
```

Terraform for all of the above lands in Sprint 6.

### Why concurrency 1 on Cloud Run

A replay is long-running and holds a publisher. Two overlapping replays would
double-publish the same game ids, so `/replay` takes a non-blocking lock and
returns **409** if one is already running — a 409 rather than a 500, because the
request is legitimate, just mistimed. A test asserts the lock is released even
when a replay fails, so a failure cannot wedge the service into permanent 409s.

### Cost

Dataflow is the one component with no free tier. Keep `--max_num_workers 2`, run
the job only while demonstrating, and drain it afterwards:

```bash
gcloud dataflow jobs drain $JOB_ID --region us-central1
```

Pub/Sub (10 GB/month) and Cloud Run (2M requests/month) stay inside the free
tier at demo volumes.

## Testing

| What | How |
|---|---|
| event contract, validation, clocks | pure unit tests, no Beam/GCP |
| replay engine (interleaving, ordering, pacing, determinism) | fake clock + in-memory publisher |
| Beam DoFns, DLQ routing, summarisers | `TestPipeline` on the DirectRunner |
| real event-time windowing + lateness | `TestStream` with a driven watermark |
| dedup | duplicate feed through `Deduplicate` |
| Cloud Run service | WSGI called directly with a fake `environ` |
| contract vs BigQuery DDL | test parses the DDL and compares to `EVENT_FIELDS` |

Beam tests are marked `slow` (~85s for DirectRunner startup). Skip them in tight
loops with `-m "not slow"`; CI runs everything.

What is **not** covered offline: an actual Pub/Sub round trip and actual
BigQuery streaming inserts. Those need a project — run the simulator with
`--runner DirectRunner` against a real subscription to verify them.
