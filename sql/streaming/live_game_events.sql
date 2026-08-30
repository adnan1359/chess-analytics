-- =========================================================================
-- Streaming: the four sinks the Dataflow job writes to.
--
-- Field list on live_game_events is checked against EVENT_FIELDS in
-- src/chess_analytics/streaming/events.py by tests/test_streaming_sql.py, so
-- the producer, the pipeline, and the warehouse cannot drift apart.
--
-- All four are partitioned on ingestion/event date and expire old data: a
-- streaming table without an expiry grows forever, and this one is fed by a
-- simulator that can be left running.
-- =========================================================================

CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT}.streaming` OPTIONS (location = '${BQ_LOCATION}');

-- -------------------------------------------------------------------------
-- 1. live_game_events — every valid, deduplicated event.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.streaming.live_game_events`
(
  event_id          STRING    NOT NULL OPTIONS (description = 'UUID. Dedup key.'),
  event_type        STRING    NOT NULL OPTIONS (description = 'game_start | move | game_end'),
  event_ts          TIMESTAMP NOT NULL OPTIONS (description = 'Replay wall-clock. NOT the original game time.'),
  game_id           STRING    NOT NULL OPTIONS (description = 'Also the Pub/Sub ordering key.'),
  game_url          STRING,
  player_white      STRING,
  player_black      STRING,
  white_elo         INT64,
  black_elo         INT64,
  time_class        STRING,
  time_control_raw  STRING,
  eco_code          STRING,
  opening_name      STRING,
  ply               INT64     OPTIONS (description = '1-based half-move index. NULL on start/end.'),
  move_number       INT64     OPTIONS (description = '1-based full-move number.'),
  color             STRING    OPTIONS (description = 'white | black. NULL on start/end.'),
  move_san          STRING,
  mover_clock_sec   FLOAT64   OPTIONS (description = 'Clock of the side that just moved.'),
  white_clock_sec   FLOAT64   OPTIONS (description = 'Last known White clock. NULL for daily games.'),
  black_clock_sec   FLOAT64,
  outcome           STRING    OPTIONS (description = 'game_end only.'),
  winner_color      STRING,
  termination       STRING,
  total_moves       INT64,
  final_fen         STRING    OPTIONS (description = 'game_end only. No per-move FEN — see events.py.'),
  is_simulated      BOOL      NOT NULL OPTIONS (description = 'Always TRUE: these are replays, not live play.'),
  source_game_date  DATE      OPTIONS (description = 'Date the game was ACTUALLY played.'),
  producer_version  STRING    OPTIONS (description = 'Which simulator build emitted this.')
)
PARTITION BY DATE(event_ts)
CLUSTER BY game_id, event_type
OPTIONS (
  description = 'Move-level event stream from the game simulator. All rows are simulated replays.',
  partition_expiration_days = 30
);


-- -------------------------------------------------------------------------
-- 2. live_game_stats — windowed aggregates, one row per (window, game).
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.streaming.live_game_stats`
(
  window_start      TIMESTAMP NOT NULL,
  window_end        TIMESTAMP NOT NULL,
  window_type       STRING    NOT NULL OPTIONS (description = 'game_session | fixed_metrics'),
  game_id           STRING,
  player_white      STRING,
  player_black      STRING,
  time_class        STRING,
  eco_code          STRING,
  moves_in_window   INT64,
  first_ply         INT64,
  last_ply          INT64,
  moves_per_minute  FLOAT64,
  avg_think_time_sec FLOAT64  OPTIONS (description = 'Mean gap between consecutive move events.'),
  min_clock_sec     FLOAT64   OPTIONS (description = 'Lowest clock seen: the clock-pressure signal.'),
  is_finished       BOOL      OPTIONS (description = 'A game_end event fell in this window.'),
  outcome           STRING,
  termination       STRING,
  emitted_at        TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY window_type, game_id
OPTIONS (
  description = 'Session- and fixed-window aggregates emitted by the Dataflow streaming job.',
  partition_expiration_days = 30
);


-- -------------------------------------------------------------------------
-- 3. late_events — arrived past the allowed lateness.
--
-- Dropping late data silently is the classic streaming mistake: the numbers
-- look fine and are quietly wrong. These rows are kept so lateness is
-- measurable and the watermark config can be tuned against evidence.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.streaming.late_events`
(
  event_id          STRING    NOT NULL,
  game_id           STRING,
  event_type        STRING,
  event_ts          TIMESTAMP OPTIONS (description = 'Event time claimed by the producer.'),
  received_at       TIMESTAMP NOT NULL OPTIONS (description = 'When the pipeline actually saw it.'),
  lateness_sec      FLOAT64   OPTIONS (description = 'received_at - event_ts.'),
  window_start      TIMESTAMP OPTIONS (description = 'The window it would have belonged to.'),
  pane_info         STRING    OPTIONS (description = 'Beam pane timing, e.g. LATE.'),
  payload           STRING    OPTIONS (description = 'Raw JSON, so nothing is lost.')
)
PARTITION BY DATE(received_at)
CLUSTER BY game_id
OPTIONS (
  description = 'Events that missed their window. Retained to measure lateness, not discarded.',
  partition_expiration_days = 30
);


-- -------------------------------------------------------------------------
-- 4. dlq_events — decoded but failed validation.
--
-- Distinct from the Pub/Sub DLQ *topic*, which handles delivery failures
-- (a message nacked N times). This table handles messages that were delivered
-- fine but are not valid events — bad JSON, unknown event_type, missing ply.
-- Both are needed; they fail in different places. See docs/streaming.md.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.streaming.dlq_events`
(
  received_at       TIMESTAMP NOT NULL,
  error_type        STRING    OPTIONS (description = 'JSONDecodeError | EventValidationError | ...'),
  error_message     STRING,
  pubsub_message_id STRING,
  attributes        STRING    OPTIONS (description = 'Pub/Sub attributes as JSON.'),
  payload           STRING    OPTIONS (description = 'Raw undecoded message body.'),
  pipeline_version  STRING
)
PARTITION BY DATE(received_at)
OPTIONS (
  description = 'Messages that failed parsing or validation in the streaming pipeline.',
  partition_expiration_days = 30
);
