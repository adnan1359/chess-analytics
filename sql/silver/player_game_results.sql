-- =========================================================================
-- Silver: player_game_results  —  a VIEW that re-grains games to one row per
-- (game, player).
--
-- clean_games is one row per game with white/black as separate columns. Almost
-- every player-centric metric ("my win rate", "my Elo trend") needs the other
-- grain: one row per player per game. Doing that UNION ALL inside each Gold
-- model would repeat the win/loss mapping four times and invite drift, so it
-- lives here once.
--
-- A VIEW (not a table) because it is a pure reshape of clean_games: zero
-- storage, always consistent, and the optimizer pushes Gold's partition
-- filters down into it.
-- =========================================================================

CREATE OR REPLACE VIEW `${GCP_PROJECT}.silver.player_game_results`
OPTIONS (
  description = 'clean_games re-grained to one row per (game, player). View over silver.clean_games.'
)
AS
SELECT
  game_id,
  game_date,
  ended_at,
  time_class,
  rated,
  eco_code,
  eco_volume,
  opening_name,
  total_moves,
  termination,
  is_draw,
  player_white                          AS username,
  'white'                               AS color,
  white_elo                             AS elo,
  elo_bracket_white                     AS elo_bracket,
  player_black                          AS opponent_username,
  black_elo                             AS opponent_elo,
  -- Score follows chess convention: 1 / 0.5 / 0. Summing it gives points,
  -- and AVG(score) is the standard "performance" measure (draws count half) —
  -- which is why we keep it alongside the plain win flag.
  CASE outcome WHEN '1-0' THEN 1.0 WHEN '0-1' THEN 0.0 ELSE 0.5 END AS score,
  CASE outcome WHEN '1-0' THEN 'win' WHEN '0-1' THEN 'loss' ELSE 'draw' END AS result
FROM `${GCP_PROJECT}.silver.clean_games`

UNION ALL

SELECT
  game_id,
  game_date,
  ended_at,
  time_class,
  rated,
  eco_code,
  eco_volume,
  opening_name,
  total_moves,
  termination,
  is_draw,
  player_black                          AS username,
  'black'                               AS color,
  black_elo                             AS elo,
  elo_bracket_black                     AS elo_bracket,
  player_white                          AS opponent_username,
  white_elo                             AS opponent_elo,
  CASE outcome WHEN '0-1' THEN 1.0 WHEN '1-0' THEN 0.0 ELSE 0.5 END AS score,
  CASE outcome WHEN '0-1' THEN 'win' WHEN '1-0' THEN 'loss' ELSE 'draw' END AS result
FROM `${GCP_PROJECT}.silver.clean_games`;
