-- Bronze external table over the partitioned game landing zone.
-- Schema is auto-detected from the NDJSON; hive partitioning exposes
-- year/month as pruneable INT columns so single-month queries scan one month.
--
-- Replace ${GCP_PROJECT} / ${BUCKET} before running (or use `bq mk --table
-- --external_table_definition`). External => no storage cost, always fresh.

CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT}.bronze`
  OPTIONS (location = 'US');

CREATE OR REPLACE EXTERNAL TABLE `${GCP_PROJECT}.bronze.raw_games`
WITH PARTITION COLUMNS (
  year  INT64,
  month INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  hive_partition_uri_prefix = 'gs://${BUCKET}/raw/games',
  uris = ['gs://${BUCKET}/raw/games/*'],
  max_staleness = INTERVAL 1 HOUR,
  metadata_cache_mode = 'AUTOMATIC'
);
