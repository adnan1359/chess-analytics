-- =========================================================================
-- Gold: opening_win_rates  —  one row per (eco_code, elo_bracket, time_class).
--
-- Answers: "does the Najdorf actually score well, and does that change with
-- strength and time control?"
--
-- Measured from WHITE's perspective, which is the convention in opening theory
-- (an opening is "good for White" or "good for Black"). white_score_rate is the
-- headline number: 0.5 means balanced, >0.5 favours White.
--
-- Bracketing uses the WHITE player's bracket, with a games_in_bracket guard so
-- tiny samples don't produce 100%-win-rate noise in the dashboard.
--
-- Full rebuild: this is a small aggregate over all history and trends only make
-- sense against the full corpus, so CREATE OR REPLACE is both correct and
-- idempotent. It is the largest scan in the DAG — see docs for the cost note.
-- =========================================================================

CREATE OR REPLACE TABLE `${GCP_PROJECT}.gold.opening_win_rates`
CLUSTER BY eco_code, time_class
OPTIONS (
  description = 'Opening performance by Elo bracket and time control, from White’s perspective.'
)
AS
WITH games AS (
  SELECT
    g.eco_code,
    g.elo_bracket_white AS elo_bracket,
    g.time_class,
    g.outcome,
    g.is_draw,
    g.total_moves,
    g.termination
  FROM `${GCP_PROJECT}.silver.clean_games` AS g
  WHERE g.eco_code IS NOT NULL
    AND g.elo_bracket_white IS NOT NULL
    AND g.rated                       -- unrated games carry no competitive signal
),

aggregated AS (
  SELECT
    eco_code,
    elo_bracket,
    time_class,
    COUNT(*)                                                   AS games,
    COUNTIF(outcome = '1-0')                                   AS white_wins,
    COUNTIF(outcome = '0-1')                                   AS black_wins,
    COUNTIF(is_draw)                                           AS draws,
    SAFE_DIVIDE(COUNTIF(outcome = '1-0'), COUNT(*))            AS white_win_rate,
    SAFE_DIVIDE(COUNTIF(outcome = '0-1'), COUNT(*))            AS black_win_rate,
    SAFE_DIVIDE(COUNTIF(is_draw), COUNT(*))                    AS draw_rate,
    -- Draws count half, so this is comparable across openings with very
    -- different draw tendencies (a solid line and a sharp line can share a
    -- win rate while scoring quite differently).
    SAFE_DIVIDE(COUNTIF(outcome = '1-0') + 0.5 * COUNTIF(is_draw), COUNT(*)) AS white_score_rate,
    AVG(total_moves)                                           AS avg_total_moves,
    SAFE_DIVIDE(COUNTIF(termination = 'checkmate'), COUNT(*))  AS checkmate_rate
  FROM games
  GROUP BY eco_code, elo_bracket, time_class
)

SELECT
  a.eco_code,
  COALESCE(o.opening_name, 'Unknown')  AS opening_name,
  o.eco_volume,
  a.elo_bracket,
  a.time_class,
  a.games,
  a.white_wins,
  a.black_wins,
  a.draws,
  a.white_win_rate,
  a.black_win_rate,
  a.draw_rate,
  a.white_score_rate,
  a.avg_total_moves,
  a.checkmate_rate,
  -- Sample-size flag for the dashboard: rates below this threshold are noise.
  a.games >= 30                        AS is_significant_sample,
  CURRENT_TIMESTAMP()                  AS _transformed_at
FROM aggregated AS a
LEFT JOIN `${GCP_PROJECT}.silver.openings_dim` AS o
  USING (eco_code);
