-- =========================================================================
-- Silver: dim_players  —  current-state player dimension (SCD Type 1).
--
-- Sprint 2 keeps this a Type 1 snapshot: latest profile + latest ratings, one
-- row per player. Sprint 4 adds the Type 2 history table (dim_players_history)
-- with valid_from / valid_to / is_current for rating-over-time analysis. Type 1
-- stays because most Gold joins want "who is this player now", and making every
-- lookup filter is_current would be needless friction.
--
-- Full rebuild: the dimension is small (thousands of rows) and always derived
-- from the current Bronze snapshot, so CREATE OR REPLACE is idempotent by
-- construction — no MERGE bookkeeping needed.
-- =========================================================================

CREATE OR REPLACE TABLE `${GCP_PROJECT}.silver.dim_players`
CLUSTER BY username
OPTIONS (
  description = 'Current-state player dimension (SCD1). From bronze.raw_players / raw_player_stats / raw_titled.'
)
AS
WITH profiles AS (
  -- Bronze holds one file per player, but guard against the same player being
  -- landed twice (e.g. a partial re-run) by keeping the newest ingest.
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      LOWER(username)                                      AS username,
      CAST(player_id AS INT64)                             AS player_id,
      name                                                 AS display_name,
      title,
      status                                               AS membership_status,
      -- country arrives as ".../pub/country/US"; the ISO code is the last segment.
      REGEXP_EXTRACT(country, r'([^/]+)/?$')               AS country_code,
      CAST(followers AS INT64)                             AS followers,
      DATE(TIMESTAMP_SECONDS(CAST(joined AS INT64)))       AS joined_date,
      TIMESTAMP_SECONDS(CAST(last_online AS INT64))        AS last_online_at,
      COALESCE(is_streamer, FALSE)                         AS is_streamer,
      url                                                  AS profile_url,
      _ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(username) ORDER BY _ingested_at DESC
      )                                                    AS rn
    FROM `${GCP_PROJECT}.bronze.raw_players`
    WHERE username IS NOT NULL
  )
  WHERE rn = 1
),

stats AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      -- The /stats API response carries no username; ingestion stamps one in
      -- (see ingestion/pull_player_profiles.py) so this stays a plain join key.
      LOWER(username) AS username,

      CAST(chess_rapid.last.rating  AS INT64) AS rapid_rating,
      CAST(chess_blitz.last.rating  AS INT64) AS blitz_rating,
      CAST(chess_bullet.last.rating AS INT64) AS bullet_rating,
      CAST(chess_rapid.best.rating  AS INT64) AS rapid_best_rating,
      CAST(chess_blitz.best.rating  AS INT64) AS blitz_best_rating,
      CAST(chess_bullet.best.rating AS INT64) AS bullet_best_rating,

      -- Career record summed across the three live formats. COALESCE because a
      -- player with no bullet games simply has no chess_bullet object.
      COALESCE(CAST(chess_rapid.record.win  AS INT64), 0)
        + COALESCE(CAST(chess_blitz.record.win  AS INT64), 0)
        + COALESCE(CAST(chess_bullet.record.win AS INT64), 0) AS career_wins,
      COALESCE(CAST(chess_rapid.record.loss AS INT64), 0)
        + COALESCE(CAST(chess_blitz.record.loss AS INT64), 0)
        + COALESCE(CAST(chess_bullet.record.loss AS INT64), 0) AS career_losses,
      COALESCE(CAST(chess_rapid.record.draw AS INT64), 0)
        + COALESCE(CAST(chess_blitz.record.draw AS INT64), 0)
        + COALESCE(CAST(chess_bullet.record.draw AS INT64), 0) AS career_draws,

      CAST(fide AS INT64) AS fide_rating,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(username) ORDER BY _ingested_at DESC
      ) AS rn
    FROM `${GCP_PROJECT}.bronze.raw_player_stats`
    WHERE username IS NOT NULL
  )
  WHERE rn = 1
),

titles AS (
  -- A player can appear under only one title list, but dedupe defensively.
  SELECT
    LOWER(username) AS username,
    ANY_VALUE(title) AS titled_as
  FROM `${GCP_PROJECT}.bronze.raw_titled`
  GROUP BY LOWER(username)
)

SELECT
  p.username,
  p.player_id,
  p.display_name,
  COALESCE(p.title, t.titled_as)                      AS title,
  t.titled_as IS NOT NULL                             AS is_titled,
  p.membership_status,
  p.country_code,
  p.followers,
  p.joined_date,
  EXTRACT(YEAR FROM p.joined_date)                    AS join_cohort_year,
  p.last_online_at,
  p.is_streamer,
  p.profile_url,

  s.rapid_rating,
  s.blitz_rating,
  s.bullet_rating,
  s.rapid_best_rating,
  s.blitz_best_rating,
  s.bullet_best_rating,
  -- "Headline" rating: the strongest live format we have a number for.
  GREATEST(
    COALESCE(s.rapid_rating,  0),
    COALESCE(s.blitz_rating,  0),
    COALESCE(s.bullet_rating, 0)
  )                                                   AS peak_current_rating,
  s.fide_rating,
  s.career_wins,
  s.career_losses,
  s.career_draws,
  SAFE_DIVIDE(
    s.career_wins,
    s.career_wins + s.career_losses + s.career_draws
  )                                                   AS career_win_rate,

  p._ingested_at,
  CURRENT_TIMESTAMP()                                 AS _transformed_at
FROM profiles AS p
LEFT JOIN stats  AS s USING (username)
LEFT JOIN titles AS t USING (username);
