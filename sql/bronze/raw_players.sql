-- Bronze external tables for player profiles, stats, and the titled universe.
-- One object per file for profiles/stats; NDJSON row-per-player for titled.

CREATE OR REPLACE EXTERNAL TABLE `${GCP_PROJECT}.bronze.raw_players`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://${BUCKET}/raw/players/profiles/*'],
  max_staleness = INTERVAL 1 HOUR,
  metadata_cache_mode = 'AUTOMATIC'
);

CREATE OR REPLACE EXTERNAL TABLE `${GCP_PROJECT}.bronze.raw_player_stats`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://${BUCKET}/raw/players/stats/*'],
  max_staleness = INTERVAL 1 HOUR,
  metadata_cache_mode = 'AUTOMATIC'
);

CREATE OR REPLACE EXTERNAL TABLE `${GCP_PROJECT}.bronze.raw_titled`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://${BUCKET}/raw/titled/*'],
  max_staleness = INTERVAL 1 HOUR,
  metadata_cache_mode = 'AUTOMATIC'
);
