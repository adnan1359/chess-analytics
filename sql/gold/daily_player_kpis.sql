-- =========================================================================
-- Gold: daily_player_kpis  —  one row per (player, date, time_class).
--
-- Answers: "how did this player do on this day in this format?"
--
-- Grain note: time_class is part of the key on purpose. A blitz Elo and a rapid
-- Elo are different scales, so averaging them into one daily number would be
-- meaningless. Looker rolls up across formats when asked; it cannot un-mix a
-- pre-blended average.
--
-- Rebuild strategy: the run window is replaced, not appended. DELETE + INSERT
-- inside one transaction keeps the table correct if a day is reprocessed after
-- late-arriving games land, and makes the whole task idempotent.
-- Params: @start_date DATE, @end_date DATE (inclusive)
-- =========================================================================

CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT}.gold` OPTIONS (location = '${BQ_LOCATION}');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.gold.daily_player_kpis`
(
  game_date         DATE   NOT NULL,
  username          STRING NOT NULL,
  time_class        STRING NOT NULL,
  games_played      INT64,
  wins              INT64,
  losses            INT64,
  draws             INT64,
  win_rate          FLOAT64 OPTIONS (description = 'wins / games_played. Draws count as losses here.'),
  score_rate        FLOAT64 OPTIONS (description = 'AVG(score): draws count half. The standard chess performance measure.'),
  games_as_white    INT64,
  games_as_black    INT64,
  avg_total_moves   FLOAT64,
  elo_first         INT64  OPTIONS (description = 'Elo in the first game of the day (chronological).'),
  elo_last          INT64  OPTIONS (description = 'Elo in the last game of the day.'),
  elo_delta         INT64  OPTIONS (description = 'elo_last - elo_first. Intra-day rating movement.'),
  avg_opponent_elo  FLOAT64,
  distinct_openings INT64,
  _transformed_at   TIMESTAMP
)
PARTITION BY game_date
CLUSTER BY username, time_class
OPTIONS (description = 'Daily per-player performance KPIs. From silver.player_game_results.');


BEGIN TRANSACTION;

-- Replace only the window being processed; other partitions are untouched.
DELETE FROM `${GCP_PROJECT}.gold.daily_player_kpis`
WHERE game_date BETWEEN @start_date AND @end_date;

INSERT INTO `${GCP_PROJECT}.gold.daily_player_kpis`
WITH windowed AS (
  SELECT *
  FROM `${GCP_PROJECT}.silver.player_game_results`
  WHERE game_date BETWEEN @start_date AND @end_date
),

-- Rank each player's games within the day so we can read the day's opening and
-- closing Elo. ended_at is the true chronological order; game_id breaks ties
-- for games that finished in the same second, keeping the result deterministic.
ordered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY username, game_date, time_class ORDER BY ended_at ASC, game_id ASC
    ) AS seq_asc,
    ROW_NUMBER() OVER (
      PARTITION BY username, game_date, time_class ORDER BY ended_at DESC, game_id DESC
    ) AS seq_desc
  FROM windowed
)

SELECT
  game_date,
  username,
  time_class,
  COUNT(*)                                                          AS games_played,
  COUNTIF(result = 'win')                                           AS wins,
  COUNTIF(result = 'loss')                                          AS losses,
  COUNTIF(result = 'draw')                                          AS draws,
  SAFE_DIVIDE(COUNTIF(result = 'win'), COUNT(*))                    AS win_rate,
  AVG(score)                                                        AS score_rate,
  COUNTIF(color = 'white')                                          AS games_as_white,
  COUNTIF(color = 'black')                                          AS games_as_black,
  AVG(total_moves)                                                  AS avg_total_moves,
  MAX(IF(seq_asc  = 1, elo, NULL))                                  AS elo_first,
  MAX(IF(seq_desc = 1, elo, NULL))                                  AS elo_last,
  MAX(IF(seq_desc = 1, elo, NULL)) - MAX(IF(seq_asc = 1, elo, NULL)) AS elo_delta,
  AVG(opponent_elo)                                                 AS avg_opponent_elo,
  COUNT(DISTINCT eco_code)                                          AS distinct_openings,
  CURRENT_TIMESTAMP()                                               AS _transformed_at
FROM ordered
GROUP BY game_date, username, time_class;

COMMIT TRANSACTION;
