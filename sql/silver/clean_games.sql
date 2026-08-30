-- =========================================================================
-- Silver: clean_games  —  one row per *distinct* game, typed and conformed.
--
-- Grain:        one row per Chess.com game id
-- Idempotency:  MERGE on game_id, scoped to the run's date window
-- Params:       @start_date DATE, @end_date DATE  (inclusive)
--
-- Why MERGE and not INSERT: a game played between two tracked players lands in
-- BOTH players' monthly archives, so Bronze legitimately contains it twice.
-- game_id (parsed from the game URL) is the natural key; we dedupe within the
-- batch with ROW_NUMBER and then MERGE so re-runs never double-insert.
--
-- Vocabulary (result codes, terminations, Elo brackets) is defined once in
-- config/mappings.yaml. tests/test_mapping_drift.py fails if this file and that
-- file disagree.
-- =========================================================================

CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT}.silver` OPTIONS (location = '${BQ_LOCATION}');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.silver.clean_games`
(
  game_id            STRING  NOT NULL OPTIONS (description = 'Numeric id parsed from the game URL. Natural key.'),
  game_uuid          STRING,
  game_url           STRING,
  game_date          DATE    NOT NULL OPTIONS (description = 'UTC date the game ended. Partition key.'),
  ended_at           TIMESTAMP,
  rated              BOOL,
  rules              STRING,
  time_class         STRING  OPTIONS (description = 'bullet | blitz | rapid | daily | unknown'),
  time_control_raw   STRING,
  base_time_sec      INT64   OPTIONS (description = 'For daily games this is seconds PER MOVE, not a base clock.'),
  increment_sec      INT64,
  is_daily           BOOL,
  player_white       STRING,
  player_black       STRING,
  white_elo          INT64,
  black_elo          INT64,
  elo_bracket_white  STRING,
  elo_bracket_black  STRING,
  outcome            STRING  OPTIONS (description = "'1-0' | '0-1' | '1/2-1/2'"),
  winner_color       STRING  OPTIONS (description = "'white' | 'black' | NULL when drawn"),
  winner_username    STRING,
  termination        STRING  OPTIONS (description = 'checkmate | resignation | timeout | abandonment | draw_* | unknown'),
  is_draw            BOOL,
  eco_code           STRING  OPTIONS (description = 'From the PGN [ECO] header, e.g. B90.'),
  eco_volume         STRING,
  opening_name       STRING  OPTIONS (description = 'Derived from the ECOUrl slug.'),
  total_moves        INT64   OPTIONS (description = 'Full moves (a white+black pair counts as one).'),
  final_fen          STRING,
  _ingested_at       TIMESTAMP,
  _transformed_at    TIMESTAMP,
  _source_endpoint   STRING
)
PARTITION BY game_date
CLUSTER BY time_class, eco_code, player_white
OPTIONS (
  -- Deliberately NOT require_partition_filter: the Gold trend and cohort
  -- models legitimately aggregate full history. Pruning is enforced where it
  -- pays off (the MERGE below, and the daily Gold rebuilds) rather than by a
  -- table-level rule that those models would have to work around.
  description = 'Conformed, deduplicated standard-chess games. Built from bronze.raw_games.'
);


MERGE `${GCP_PROJECT}.silver.clean_games` AS T
USING (
  WITH bronze_window AS (
    SELECT *
    FROM `${GCP_PROJECT}.bronze.raw_games`
    -- Prune Bronze at the month grain first (hive partition columns), then
    -- filter precisely on the derived game_date below. Pruning here is what
    -- keeps a daily run from scanning the whole lake.
    WHERE (year * 100 + month) BETWEEN
            (EXTRACT(YEAR FROM @start_date) * 100 + EXTRACT(MONTH FROM @start_date))
        AND (EXTRACT(YEAR FROM @end_date)   * 100 + EXTRACT(MONTH FROM @end_date))
  ),

  parsed AS (
    SELECT
      REGEXP_EXTRACT(url, r'/(\d+)/?$')                       AS game_id,
      uuid                                                     AS game_uuid,
      url                                                      AS game_url,
      DATE(TIMESTAMP_SECONDS(end_time))                        AS game_date,
      TIMESTAMP_SECONDS(end_time)                              AS ended_at,
      rated,
      rules,
      LOWER(time_class)                                        AS time_class,
      time_control                                             AS time_control_raw,
      LOWER(white.username)                                    AS player_white,
      LOWER(black.username)                                    AS player_black,
      CAST(white.rating AS INT64)                              AS white_elo,
      CAST(black.rating AS INT64)                              AS black_elo,
      LOWER(white.result)                                      AS white_result,
      LOWER(black.result)                                      AS black_result,
      fen                                                      AS final_fen,
      eco                                                      AS eco_url,
      pgn,
      -- Movetext only: the header block holds [Date "2026.08.15"], whose
      -- digit-dot pattern would otherwise be read as a move number.
      REGEXP_EXTRACT(pgn, r'(?s)\n\s*\n(.*)$')                 AS movetext,
      REGEXP_EXTRACT(pgn, r'\[ECO "([^"]+)"\]')                AS eco_code,
      _ingested_at,
      _source_endpoint
    FROM bronze_window
  ),

  derived AS (
    SELECT
      * EXCEPT (movetext, pgn, eco_url),

      -- Time control: "600" -> 600/0 ; "180+2" -> 180/2 ; "1/259200" -> daily.
      CAST(REGEXP_EXTRACT(time_control_raw, r'^(?:1/)?(\d+)') AS INT64)          AS base_time_sec,
      CAST(COALESCE(REGEXP_EXTRACT(time_control_raw, r'\+(\d+)$'), '0') AS INT64) AS increment_sec,
      STARTS_WITH(time_control_raw, '1/')                                         AS is_daily,

      -- Total FULL moves = the highest move number in the movetext.
      -- Clock comments must be stripped first: "{[%clk 0:09:57.5]}" contains
      -- "57." which a bare (\d+)\. would read as move 57 and silently inflate
      -- the count on every clocked game.
      (
        SELECT MAX(CAST(n AS INT64))
        FROM UNNEST(
          REGEXP_EXTRACT_ALL(
            REGEXP_REPLACE(movetext, r'\{[^}]*\}', ' '),
            r'(\d+)\.'
          )
        ) AS n
      )                                                                          AS total_moves,

      -- Opening name from the ECOUrl slug:
      -- ".../Sicilian-Defense-Najdorf-Variation" -> "Sicilian Defense Najdorf Variation"
      NULLIF(REPLACE(REGEXP_EXTRACT(eco_url, r'([^/]+)/?$'), '-', ' '), '')      AS opening_name,

      CASE UPPER(SUBSTR(eco_code, 1, 1))
        WHEN 'A' THEN 'Flank openings'
        WHEN 'B' THEN 'Semi-open games (excl. French)'
        WHEN 'C' THEN 'Open games and French'
        WHEN 'D' THEN 'Closed and semi-closed games'
        WHEN 'E' THEN 'Indian defences'
      END                                                                        AS eco_volume
    FROM parsed
  ),

  conformed AS (
    SELECT
      d.* EXCEPT (white_result, black_result),

      -- Constrain to the known vocabulary so an unexpected new value from the
      -- API shows up as 'unknown' in Gold instead of silently adding a category.
      CASE
        WHEN d.time_class IN ('bullet', 'blitz', 'rapid', 'daily') THEN d.time_class
        ELSE 'unknown'
      END                                                                        AS time_class_clean,

      -- Outcome: exactly one side's result is 'win'; otherwise it is a draw.
      CASE
        WHEN d.white_result = 'win' THEN '1-0'
        WHEN d.black_result = 'win' THEN '0-1'
        ELSE '1/2-1/2'
      END                                                                        AS outcome,
      CASE
        WHEN d.white_result = 'win' THEN 'white'
        WHEN d.black_result = 'win' THEN 'black'
      END                                                                        AS winner_color,

      -- Termination reason always sits on the side that did NOT win; for draws
      -- both sides carry the same reason, so White's is correct in all cases.
      CASE COALESCE(IF(d.white_result = 'win', d.black_result, d.white_result), '')
        WHEN 'checkmated'          THEN 'checkmate'
        WHEN 'resigned'            THEN 'resignation'
        WHEN 'timeout'             THEN 'timeout'
        WHEN 'abandoned'           THEN 'abandonment'
        WHEN 'agreed'              THEN 'draw_agreed'
        WHEN 'repetition'          THEN 'draw_repetition'
        WHEN 'stalemate'           THEN 'draw_stalemate'
        WHEN 'insufficient'        THEN 'draw_insufficient_material'
        WHEN '50move'              THEN 'draw_50_move'
        WHEN 'timevsinsufficient'  THEN 'draw_timeout_vs_insufficient'
        WHEN 'kingofthehill'       THEN 'variant_objective'
        WHEN 'threecheck'          THEN 'variant_objective'
        WHEN 'bughousepartnerlose' THEN 'variant_partner_lost'
        ELSE 'unknown'
      END                                                                        AS termination
    FROM derived AS d
  ),

  final AS (
    SELECT
      game_id, game_uuid, game_url, game_date, ended_at, rated, rules,
      time_class_clean AS time_class,
      time_control_raw, base_time_sec, increment_sec, is_daily,
      player_white, player_black, white_elo, black_elo,

      CASE
        WHEN white_elo IS NULL      THEN NULL
        WHEN white_elo < 2000       THEN 'u2000'
        WHEN white_elo < 2200       THEN '2000_2199'
        WHEN white_elo < 2400       THEN '2200_2399'
        WHEN white_elo < 2600       THEN '2400_2599'
        WHEN white_elo < 2800       THEN '2600_2799'
        ELSE '2800_plus'
      END                                                                        AS elo_bracket_white,
      CASE
        WHEN black_elo IS NULL      THEN NULL
        WHEN black_elo < 2000       THEN 'u2000'
        WHEN black_elo < 2200       THEN '2000_2199'
        WHEN black_elo < 2400       THEN '2200_2399'
        WHEN black_elo < 2600       THEN '2400_2599'
        WHEN black_elo < 2800       THEN '2600_2799'
        ELSE '2800_plus'
      END                                                                        AS elo_bracket_black,

      outcome,
      winner_color,
      CASE winner_color
        WHEN 'white' THEN player_white
        WHEN 'black' THEN player_black
      END                                                                        AS winner_username,
      termination,
      -- Derived from outcome, NOT from termination. outcome comes from the
      -- win/no-win logic and needs no lookup table, so a draw reason the API
      -- adds tomorrow still yields a correct is_draw. Deriving it from the
      -- termination CASE would mark such a game not-a-draw and corrupt every
      -- draw_rate in Gold. The DQ checks flag the unmapped code as a warning.
      (outcome = '1/2-1/2')                                                      AS is_draw,
      eco_code, eco_volume, opening_name, total_moves, final_fen,
      _ingested_at,
      CURRENT_TIMESTAMP()                                                        AS _transformed_at,
      _source_endpoint,

      -- Dedupe the same game arriving from both players' archives. Newest
      -- ingest wins; _source_endpoint breaks ties so the pick is deterministic.
      ROW_NUMBER() OVER (
        PARTITION BY game_id
        ORDER BY _ingested_at DESC, _source_endpoint
      )                                                                          AS _rn
    FROM conformed
    WHERE game_id IS NOT NULL      -- malformed/absent URL; counted by the DQ checks
      AND game_date IS NOT NULL    -- NOT NULL partition column; needs end_time
      AND rules = 'chess'          -- standard chess only; variants would distort
                                   -- opening + result analytics in Gold
  )

  SELECT * EXCEPT (_rn)
  FROM final
  WHERE _rn = 1
    -- Restrict to the requested window. Bronze was pruned at month grain above;
    -- this narrows to the exact days so a daily run touches one partition.
    AND game_date BETWEEN @start_date AND @end_date
) AS S
ON  T.game_id = S.game_id
-- Constant-ish predicate on the partition column so the MERGE prunes the target
-- instead of scanning every partition ever written.
AND T.game_date BETWEEN @start_date AND @end_date

WHEN MATCHED THEN UPDATE SET
  game_uuid = S.game_uuid,
  game_url = S.game_url,
  game_date = S.game_date,
  ended_at = S.ended_at,
  rated = S.rated,
  rules = S.rules,
  time_class = S.time_class,
  time_control_raw = S.time_control_raw,
  base_time_sec = S.base_time_sec,
  increment_sec = S.increment_sec,
  is_daily = S.is_daily,
  player_white = S.player_white,
  player_black = S.player_black,
  white_elo = S.white_elo,
  black_elo = S.black_elo,
  elo_bracket_white = S.elo_bracket_white,
  elo_bracket_black = S.elo_bracket_black,
  outcome = S.outcome,
  winner_color = S.winner_color,
  winner_username = S.winner_username,
  termination = S.termination,
  is_draw = S.is_draw,
  eco_code = S.eco_code,
  eco_volume = S.eco_volume,
  opening_name = S.opening_name,
  total_moves = S.total_moves,
  final_fen = S.final_fen,
  _ingested_at = S._ingested_at,
  _transformed_at = S._transformed_at,
  _source_endpoint = S._source_endpoint

WHEN NOT MATCHED THEN INSERT ROW;
