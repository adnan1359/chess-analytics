-- =========================================================================
-- Silver: openings_dim  —  canonical opening per ECO code.
--
-- clean_games already carries eco_code / opening_name denormalized (cheap, and
-- it keeps Gold joins off the critical path). This dimension exists for a
-- different reason: Chess.com derives ECOUrl from the actual move order, so the
-- SAME eco_code legitimately appears with several different opening names
-- (transpositions, differently-named sub-variations).
--
-- Collapsing to the most frequent name per code gives Gold a stable label, and
-- name_variant_count surfaces how ambiguous each code is — a real data-quality
-- signal rather than a hidden inconsistency.
--
-- Full rebuild: the dimension is tiny (a few thousand rows) and deriving it
-- from all history is both cheaper and simpler than maintaining it incrementally.
-- =========================================================================

CREATE OR REPLACE TABLE `${GCP_PROJECT}.silver.openings_dim`
OPTIONS (
  description = 'One row per ECO code with its most common opening name. Built from silver.clean_games.'
)
AS
WITH name_counts AS (
  SELECT
    eco_code,
    opening_name,
    COUNT(*) AS games
  FROM `${GCP_PROJECT}.silver.clean_games`
  WHERE eco_code IS NOT NULL
    AND opening_name IS NOT NULL
  GROUP BY eco_code, opening_name
),

ranked AS (
  SELECT
    eco_code,
    opening_name,
    games,
    SUM(games)        OVER (PARTITION BY eco_code) AS total_games,
    COUNT(*)          OVER (PARTITION BY eco_code) AS name_variant_count,
    ROW_NUMBER()      OVER (PARTITION BY eco_code ORDER BY games DESC, opening_name) AS rn
  FROM name_counts
)

SELECT
  eco_code,
  opening_name                                   AS opening_name,
  CASE UPPER(SUBSTR(eco_code, 1, 1))
    WHEN 'A' THEN 'Flank openings'
    WHEN 'B' THEN 'Semi-open games (excl. French)'
    WHEN 'C' THEN 'Open games and French'
    WHEN 'D' THEN 'Closed and semi-closed games'
    WHEN 'E' THEN 'Indian defences'
  END                                            AS eco_volume,
  UPPER(SUBSTR(eco_code, 1, 1))                  AS eco_volume_code,
  total_games                                    AS games_observed,
  name_variant_count,
  CURRENT_TIMESTAMP()                            AS _transformed_at
FROM ranked
WHERE rn = 1;
