-- =========================================================================
-- Ops: pipeline_runs + dq_results  —  the pipeline's own observability tables.
--
-- Airflow's metadata DB knows whether a task succeeded; it does not know how
-- many rows the task wrote, or which DQ assertion failed. Keeping that in
-- BigQuery means run history is queryable next to the data it describes, and
-- the operational dashboard is just another Looker source.
-- =========================================================================

CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT}.ops` OPTIONS (location = '${BQ_LOCATION}');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops.pipeline_runs`
(
  run_id          STRING    NOT NULL OPTIONS (description = 'Airflow run_id, or a uuid for manual runs.'),
  dag_id          STRING,
  task_id         STRING,
  layer           STRING    OPTIONS (description = 'bronze | silver | gold | dq'),
  target_table    STRING,
  status          STRING    OPTIONS (description = 'success | failed'),
  window_start    DATE,
  window_end      DATE,
  rows_written    INT64,
  bytes_processed INT64,
  duration_sec    FLOAT64,
  error_message   STRING,
  started_at      TIMESTAMP,
  finished_at     TIMESTAMP,
  logged_at       TIMESTAMP NOT NULL
)
PARTITION BY DATE(logged_at)
CLUSTER BY dag_id, task_id
OPTIONS (description = 'Row-level audit of every pipeline task execution.');


CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops.dq_results`
(
  run_id        STRING    NOT NULL,
  checked_at    TIMESTAMP NOT NULL,
  check_name    STRING    NOT NULL,
  target_table  STRING,
  severity      STRING    OPTIONS (description = 'error blocks the DAG; warn is recorded only.'),
  failed_rows   INT64,
  total_rows    INT64,
  failed_pct    FLOAT64,
  threshold_pct FLOAT64,
  passed        BOOL,
  detail        STRING,
  window_start  DATE,
  window_end    DATE
)
PARTITION BY DATE(checked_at)
CLUSTER BY check_name
OPTIONS (description = 'One row per data-quality assertion per run.');
