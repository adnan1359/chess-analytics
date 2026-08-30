-- =========================================================================
-- DQ: assertions over silver.clean_games for the run window.
--
-- Params: @run_id STRING, @start_date DATE, @end_date DATE
--
-- Contract: this script only WRITES results to ops.dq_results. It never fails.
-- The Airflow task queries the results afterwards and fails the DAG if any
-- severity='error' check did not pass. Separating "measure" from "decide" means
-- a failed run still leaves a full diagnostic record instead of dying on the
-- first bad assertion.
--
-- severity:
--   error -> blocks the DAG. A violation means the data is wrong.
--   warn  -> recorded and dashboarded. Usually means the API showed us
--            something new that our mappings do not cover yet.
-- =========================================================================

INSERT INTO `${GCP_PROJECT}.ops.dq_results`
  (run_id, checked_at, check_name, target_table, severity,
   failed_rows, total_rows, failed_pct, threshold_pct, passed, detail,
   window_start, window_end)

WITH scoped AS (
  SELECT *
  FROM `${GCP_PROJECT}.silver.clean_games`
  WHERE game_date BETWEEN @start_date AND @end_date
),

-- One pass over the window computes every row-level metric.
metrics AS (
  SELECT
    COUNT(*)                                                     AS total_rows,
    COUNTIF(game_id IS NULL)                                     AS null_game_id,
    COUNTIF(game_date IS NULL)                                   AS null_game_date,
    COUNTIF(player_white IS NULL OR player_black IS NULL)         AS null_player,
    COUNTIF(white_elo NOT BETWEEN 100 AND 3600
         OR black_elo NOT BETWEEN 100 AND 3600)                  AS elo_out_of_range,
    COUNTIF(outcome NOT IN ('1-0', '0-1', '1/2-1/2'))            AS invalid_outcome,
    COUNTIF(termination = 'unknown')                             AS unknown_termination,
    COUNTIF(time_class = 'unknown')                              AS unknown_time_class,
    COUNTIF(game_date > CURRENT_DATE())                          AS future_dated,
    COUNTIF(total_moves IS NULL OR total_moves <= 0)             AS non_positive_moves,
    -- The longest known competitive chess game is 269 moves; 500 is a generous
    -- ceiling that still catches a parsing failure inflating the count.
    COUNTIF(total_moves > 500)                                   AS implausible_moves,
    COUNTIF(player_white = player_black)                         AS self_play,
    COUNTIF(eco_code IS NULL)                                    AS null_eco,
    COUNTIF(winner_color IS NULL AND outcome != '1/2-1/2')       AS missing_winner,
    -- A drawn game whose termination is not a draw_* category means the API
    -- used a draw reason our mappings do not cover yet. is_draw is derived from
    -- outcome so the metrics stay correct; this is a WARN telling us to add the
    -- code to config/mappings.yaml, not a reason to block the pipeline.
    COUNTIF(
      is_draw AND termination NOT IN (
        'draw_agreed', 'draw_repetition', 'draw_stalemate',
        'draw_insufficient_material', 'draw_50_move', 'draw_timeout_vs_insufficient'
      )
    )                                                            AS unmapped_draw_reason,
    -- The mirror case: a decisive game tagged with a draw termination.
    COUNTIF(NOT is_draw AND STARTS_WITH(termination, 'draw_'))   AS decisive_with_draw_termination
  FROM scoped
),

dupes AS (
  SELECT COALESCE(SUM(extra), 0) AS duplicate_rows
  FROM (
    SELECT COUNT(*) - 1 AS extra
    FROM scoped
    GROUP BY game_id
    HAVING COUNT(*) > 1
  )
),

checks AS (
  -- Column names come from the first branch of the UNION ALL.
  SELECT 'game_id_not_null' AS name,
         'error'            AS severity,
         m.null_game_id     AS failed,
         m.total_rows       AS total,
         0.0                AS threshold_pct,
         'game_id is the natural key; NULL breaks dedup' AS detail
    FROM metrics m
  UNION ALL SELECT 'game_id_unique', 'error', d.duplicate_rows, m.total_rows, 0.0,
         'MERGE + ROW_NUMBER should guarantee one row per game_id'
    FROM metrics m CROSS JOIN dupes d
  UNION ALL SELECT 'game_date_not_null', 'error', m.null_game_date, m.total_rows, 0.0,
         'partition column' FROM metrics m
  UNION ALL SELECT 'players_not_null', 'error', m.null_player, m.total_rows, 0.0,
         'both sides must be identified' FROM metrics m
  UNION ALL SELECT 'elo_in_range', 'error', m.elo_out_of_range, m.total_rows, 0.1,
         'Elo outside 100-3600 indicates a parse or source problem' FROM metrics m
  UNION ALL SELECT 'outcome_valid', 'error', m.invalid_outcome, m.total_rows, 0.0,
         'outcome must be 1-0, 0-1 or 1/2-1/2' FROM metrics m
  UNION ALL SELECT 'no_future_dates', 'error', m.future_dated, m.total_rows, 0.0,
         'game_date after today means bad end_time' FROM metrics m
  UNION ALL SELECT 'total_moves_positive', 'error', m.non_positive_moves, m.total_rows, 0.5,
         'movetext parsing produced no moves' FROM metrics m
  UNION ALL SELECT 'winner_present_when_decisive', 'error', m.missing_winner, m.total_rows, 0.0,
         'decisive game with no winner_color' FROM metrics m
  UNION ALL SELECT 'decisive_not_draw_terminated', 'error', m.decisive_with_draw_termination,
         m.total_rows, 0.0,
         'decisive game carries a draw_* termination: result mapping is wrong' FROM metrics m
  UNION ALL SELECT 'no_self_play', 'error', m.self_play, m.total_rows, 0.0,
         'white and black are the same account' FROM metrics m
  UNION ALL SELECT 'rows_present', 'error',
         IF(m.total_rows = 0, 1, 0), GREATEST(m.total_rows, 1), 0.0,
         'window produced zero rows: upstream ingestion may have failed' FROM metrics m
  -- Warnings: unexpected but not corrupting.
  UNION ALL SELECT 'termination_mapped', 'warn', m.unknown_termination, m.total_rows, 1.0,
         'unmapped result code; add it to config/mappings.yaml' FROM metrics m
  UNION ALL SELECT 'draw_reason_mapped', 'warn', m.unmapped_draw_reason, m.total_rows, 1.0,
         'drawn game with a non-draw_* termination; add the code to config/mappings.yaml'
    FROM metrics m
  UNION ALL SELECT 'time_class_known', 'warn', m.unknown_time_class, m.total_rows, 1.0,
         'unexpected time_class from the API' FROM metrics m
  UNION ALL SELECT 'total_moves_plausible', 'warn', m.implausible_moves, m.total_rows, 0.1,
         'move count above 500; check clock-comment stripping' FROM metrics m
  UNION ALL SELECT 'eco_code_present', 'warn', m.null_eco, m.total_rows, 5.0,
         'games without an ECO header drop out of opening analytics' FROM metrics m
)

SELECT
  @run_id                                                        AS run_id,
  CURRENT_TIMESTAMP()                                            AS checked_at,
  c.name                                                         AS check_name,
  '${GCP_PROJECT}.silver.clean_games'                            AS target_table,
  c.severity,
  c.failed                                                       AS failed_rows,
  c.total                                                        AS total_rows,
  ROUND(COALESCE(SAFE_DIVIDE(c.failed, c.total) * 100, 0), 4)     AS failed_pct,
  c.threshold_pct,
  -- Passes when the failure rate is at or below the allowed threshold. A
  -- threshold of 0.0 means "not a single row may violate this".
  COALESCE(SAFE_DIVIDE(c.failed, c.total) * 100, 0) <= c.threshold_pct AS passed,
  c.detail,
  @start_date                                                    AS window_start,
  @end_date                                                      AS window_end
FROM checks AS c;
