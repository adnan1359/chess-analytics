-- =========================================================================
-- Gold: player_cohorts  —  one row per (join_cohort_year, time_class).
--
-- Answers: "do players who joined Chess.com in 2014 look different from the
-- 2022 intake?" Classic cohort analysis: group by signup year, compare
-- behaviour and strength.
--
-- Honest caveat, also stated in the docs: our player universe is seeded from
-- TITLED players, so these cohorts describe titled players by signup year — not
-- the Chess.com population. Cohort sizes are small for early years, so
-- cohort_size is exposed and tiny cohorts are flagged rather than silently
-- averaged into a misleading trend line.
-- =========================================================================

CREATE OR REPLACE TABLE `${GCP_PROJECT}.gold.player_cohorts`
OPTIONS (
  description = 'Cohort analysis by Chess.com signup year. Universe is titled players — see SQL caveat.'
)
AS
WITH player_activity AS (
  SELECT
    r.username,
    r.time_class,
    COUNT(*)                       AS games_played,
    AVG(r.score)                   AS score_rate,
    AVG(CAST(r.elo AS FLOAT64))    AS avg_elo,
    MAX(r.elo)                     AS peak_elo,
    MIN(r.game_date)               AS first_game_date,
    MAX(r.game_date)               AS last_game_date,
    AVG(r.total_moves)             AS avg_total_moves
  FROM `${GCP_PROJECT}.silver.player_game_results` AS r
  WHERE r.rated
    AND r.elo IS NOT NULL
  GROUP BY r.username, r.time_class
),

joined AS (
  SELECT
    d.join_cohort_year,
    a.*,
    d.country_code,
    d.title,
    d.followers
  FROM player_activity AS a
  JOIN `${GCP_PROJECT}.silver.dim_players` AS d
    USING (username)
  WHERE d.join_cohort_year IS NOT NULL
)

SELECT
  join_cohort_year,
  time_class,
  COUNT(DISTINCT username)                                     AS cohort_size,
  SUM(games_played)                                            AS total_games,
  -- Per-player average, not a raw mean over games: without this, a single
  -- hyper-active account would dominate its cohort's numbers.
  AVG(games_played)                                            AS avg_games_per_player,
  APPROX_QUANTILES(games_played, 100)[SAFE_OFFSET(50)]         AS median_games_per_player,

  AVG(avg_elo)                                                 AS avg_elo,
  AVG(peak_elo)                                                AS avg_peak_elo,
  APPROX_QUANTILES(CAST(avg_elo AS INT64), 100)[SAFE_OFFSET(50)] AS median_avg_elo,
  AVG(score_rate)                                              AS avg_score_rate,
  AVG(avg_total_moves)                                         AS avg_total_moves,

  -- Tenure: how long the cohort has been on the platform, and whether they are
  -- still around in our data window.
  DATE_DIFF(CURRENT_DATE(), DATE(join_cohort_year, 1, 1), YEAR) AS cohort_age_years,
  AVG(DATE_DIFF(last_game_date, first_game_date, DAY))          AS avg_active_span_days,
  COUNTIF(last_game_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS players_active_last_30d,
  SAFE_DIVIDE(
    COUNTIF(last_game_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)),
    COUNT(DISTINCT username)
  )                                                            AS active_last_30d_rate,

  COUNT(DISTINCT username) >= 10                               AS is_significant_sample,
  CURRENT_TIMESTAMP()                                          AS _transformed_at
FROM joined
GROUP BY join_cohort_year, time_class;
