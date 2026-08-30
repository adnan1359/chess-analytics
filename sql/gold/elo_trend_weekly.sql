-- =========================================================================
-- Gold: elo_trend_weekly  —  one row per (player, week, time_class).
--
-- Answers: "is this player climbing or sliding, and how fast?"
--
-- Weekly rather than daily because Elo is noisy game-to-game; a week is long
-- enough to smooth a bad session but short enough to show real trajectory.
--
-- The interesting column is elo_change_wow, which needs the PREVIOUS week's
-- value — a LAG over the player's own history. That is why this model is a full
-- rebuild rather than an incremental window: computing week N's delta requires
-- week N-1, so an incremental run would need to re-read history anyway.
-- =========================================================================

CREATE OR REPLACE TABLE `${GCP_PROJECT}.gold.elo_trend_weekly`
PARTITION BY week_start_date
CLUSTER BY username, time_class
OPTIONS (
  description = 'Weekly Elo trajectory per player and format, with week-over-week deltas.'
)
AS
WITH weekly AS (
  SELECT
    DATE_TRUNC(game_date, WEEK(MONDAY))  AS week_start_date,
    username,
    time_class,
    COUNT(*)                             AS games_played,
    AVG(score)                           AS score_rate,
    AVG(CAST(elo AS FLOAT64))            AS avg_elo,
    MIN(elo)                             AS min_elo,
    MAX(elo)                             AS max_elo,
    -- Closing Elo for the week: the rating attached to the chronologically last
    -- game. ARRAY_AGG ... LIMIT 1 is the idiomatic BigQuery "argmax" and is
    -- cheaper than a self-join or a window + filter.
    ARRAY_AGG(elo ORDER BY ended_at DESC, game_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS closing_elo,
    ARRAY_AGG(elo ORDER BY ended_at ASC,  game_id ASC  LIMIT 1)[SAFE_OFFSET(0)] AS opening_elo
  FROM `${GCP_PROJECT}.silver.player_game_results`
  WHERE elo IS NOT NULL
    AND rated
  GROUP BY week_start_date, username, time_class
),

with_lag AS (
  SELECT
    *,
    LAG(closing_elo) OVER w      AS prev_week_closing_elo,
    LAG(week_start_date) OVER w  AS prev_week_start_date
  FROM weekly
  WINDOW w AS (PARTITION BY username, time_class ORDER BY week_start_date)
)

SELECT
  week_start_date,
  username,
  time_class,
  games_played,
  score_rate,
  avg_elo,
  min_elo,
  max_elo,
  opening_elo,
  closing_elo,
  closing_elo - opening_elo                        AS elo_change_in_week,
  prev_week_closing_elo,
  closing_elo - prev_week_closing_elo              AS elo_change_wow,
  -- Weeks are not necessarily contiguous: a player may not play for a month.
  -- Exposing the gap lets the dashboard avoid drawing a misleading straight
  -- line across an inactive stretch.
  DATE_DIFF(week_start_date, prev_week_start_date, WEEK) AS weeks_since_prev_active,
  CURRENT_TIMESTAMP()                              AS _transformed_at
FROM with_lag;
