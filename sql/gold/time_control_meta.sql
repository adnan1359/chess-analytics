-- =========================================================================
-- Gold: time_control_meta  —  one row per (time_class, elo_bracket).
--
-- Answers: "how does the game itself change with the clock?" Faster formats
-- should show fewer draws, more timeouts, and shorter games.
--
-- Deliberately NOT included: average game DURATION. We only receive end_time,
-- not start_time, and the clock data in the PGN is per-move rather than a total.
-- A duration column derived from the base time control would be an assumption
-- dressed up as a measurement, so it is left out rather than faked.
-- =========================================================================

CREATE OR REPLACE TABLE `${GCP_PROJECT}.gold.time_control_meta`
OPTIONS (
  description = 'Game characteristics by time control and Elo bracket. No duration column — see note in SQL.'
)
AS
WITH games AS (
  SELECT
    time_class,
    elo_bracket_white AS elo_bracket,
    base_time_sec,
    increment_sec,
    is_daily,
    total_moves,
    is_draw,
    termination,
    ABS(white_elo - black_elo) AS elo_gap
  FROM `${GCP_PROJECT}.silver.clean_games`
  WHERE rated
    AND elo_bracket_white IS NOT NULL
)

SELECT
  time_class,
  elo_bracket,
  COUNT(*)                                                        AS games,

  AVG(total_moves)                                                AS avg_total_moves,
  APPROX_QUANTILES(total_moves, 100)[SAFE_OFFSET(50)]             AS median_total_moves,
  APPROX_QUANTILES(total_moves, 100)[SAFE_OFFSET(90)]             AS p90_total_moves,

  SAFE_DIVIDE(COUNTIF(is_draw), COUNT(*))                         AS draw_rate,
  SAFE_DIVIDE(COUNTIF(NOT is_draw), COUNT(*))                     AS decisive_rate,

  -- Termination mix: the shape of HOW games end is the most format-sensitive
  -- signal here (bullet is dominated by timeouts, daily by resignations).
  SAFE_DIVIDE(COUNTIF(termination = 'checkmate'),   COUNT(*))     AS checkmate_rate,
  SAFE_DIVIDE(COUNTIF(termination = 'resignation'), COUNT(*))     AS resignation_rate,
  SAFE_DIVIDE(COUNTIF(termination = 'timeout'),     COUNT(*))     AS timeout_rate,
  SAFE_DIVIDE(COUNTIF(termination = 'abandonment'), COUNT(*))     AS abandonment_rate,
  SAFE_DIVIDE(COUNTIF(termination = 'unknown'),     COUNT(*))     AS unknown_termination_rate,

  AVG(elo_gap)                                                    AS avg_elo_gap,
  -- Daily games express base_time_sec as seconds PER MOVE, so mixing them into
  -- one average would be nonsense. Report them separately.
  AVG(IF(NOT is_daily, base_time_sec, NULL))                      AS avg_base_time_sec,
  AVG(IF(NOT is_daily, increment_sec, NULL))                      AS avg_increment_sec,
  COUNTIF(is_daily)                                               AS daily_games,

  CURRENT_TIMESTAMP()                                             AS _transformed_at
FROM games
GROUP BY time_class, elo_bracket;
