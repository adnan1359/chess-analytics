# ♟️ Chess.com Real-Time & Batch Analytics Platform

A data-engineering platform that ingests Chess.com game data, models it through a
medallion (Bronze → Silver → Gold) lakehouse in BigQuery, and serves both
**batch** analytics (openings, Elo trends, cohorts) and a **streaming** live-game
pipeline (Pub/Sub → Dataflow). Built on GCP with idempotent pipelines, data
quality checks, SCD-2 dimensions, and IaC.

> Full design & sprint plan: [`docs/architecture.md`](docs/architecture.md).

## Status

| Sprint | Scope | State |
|--------|-------|-------|
| **1 — Foundation & ingestion** | API client, storage abstraction, batch ingestion, Bronze schemas, tests | ✅ code complete & tested |
| **2 — Batch pipeline (Silver/Gold)** | PGN parsing, `clean_games` MERGE, 5 Gold marts, DQ framework, Airflow DAG | ✅ code complete, SQL verified offline |
| 3 — Streaming | game-event simulator, Dataflow job | ⬜ next |
| 4 — DQ, monitoring, SCD-2 | assertions, dim_players, DLQ | ⬜ |
| 5 — Dashboards | Looker Studio | ⬜ |
| 6 — Hardening | Terraform, CI/CD, docs | ⬜ |

## ⚠️ Network note (Cognizant corporate network)

The Cognizant network's **Zscaler** proxy blocks `chess.com` and `api.chess.com`
(returns an HTTP 403 block page for both `www` and `api`). **Live ingestion will
not run from a corporate machine.** This is a network policy, not a code issue —
the same code was verified end-to-end offline against fixtures, and is designed
to run where the API is reachable:

- from a **home / personal network**, or
- from **GCP itself** (Cloud Run job / Composer worker) — the intended runtime.

Behind any TLS-inspecting proxy, install `truststore` (in requirements) and keep
`api.use_os_trust_store: true` so Python trusts the corporate root CA. That part
is already wired and verified; only the final 403 policy block remains.

## Quickstart

```bash
python -m venv .venv && source .venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
pytest                                                   # 92 tests, all offline

# 1. Ingest (from a network that can reach api.chess.com — see note above)
python scripts/run_ingestion.py --titles GM --players 50 --months 3
# -> lands NDJSON under ./data/raw/ (local backend, default)

# 2. Transform on BigQuery. --dry-run validates every statement for free.
python scripts/run_batch.py --start 2026-08-01 --end 2026-08-31 --dry-run
python scripts/run_batch.py --start 2026-08-01 --end 2026-08-31
```

Switch to GCS with zero code changes:

```bash
export CHESS_STORAGE__BACKEND=gcs
export CHESS_STORAGE__GCS_BUCKET=chess-lakehouse
```

## Layout

```
chess_com/
├── config/
│   ├── config.yaml               # single config; env-overridable (CHESS_*)
│   └── mappings.yaml             # business vocabulary shared by SQL + Python
├── src/chess_analytics/
│   ├── config.py                 # YAML + env-override loader
│   ├── logging_setup.py          # JSON logs in cloud, plain locally
│   ├── chesscom_client.py        # rate-limited, retrying API client
│   ├── storage.py                # local | GCS landing writer
│   ├── sql_runner.py             # ${VAR} rendering + BQ execution / dry-run
│   ├── ingestion/                # pull_titled_players / _profiles / _archives
│   └── transforms/pgn.py         # PGN parser (feeds the Sprint 3 simulator)
├── sql/
│   ├── bronze/                   # external-table DDL over the landing zone
│   ├── silver/                   # clean_games (MERGE), views, dimensions
│   ├── gold/                     # 5 KPI marts
│   ├── dq_checks/                # assertions -> ops.dq_results
│   └── ops/                      # pipeline_runs, dq_results
├── airflow/dags/chess_daily_batch.py   # one DAG; backfill via data interval
├── scripts/
│   ├── run_ingestion.py          # extract entrypoint
│   └── run_batch.py              # Composer-free transform runner
├── docs/schemas/                 # bronze + silver/gold design notes
├── tests/                        # 92 tests, no network or GCP needed
└── requirements.txt
```

## Design decisions

- **Immutable Bronze.** Raw payload landed verbatim + additive `_ingested_at` /
  `_source_endpoint` lineage. All modelling deferred to Silver.
- **NDJSON, Hive-partitioned.** `raw/games/year=YYYY/month=MM/player=<u>.json`
  → one file per `(month, player)` = the idempotency unit; re-runs and
  single-month backfills overwrite exactly one partition.
- **External Bronze tables.** No load step, no duplicate storage; new files are
  queryable immediately with `max_staleness` metadata caching.
- **Config once, run anywhere.** One YAML, per-key env overrides
  (`CHESS_API__MIN_INTERVAL_SEC=0.5`) — same code local / Airflow / Cloud Run.
- **Well-behaved client.** Descriptive User-Agent (Chess.com requires it),
  serial pacing, exponential backoff, `Retry-After` respected, 404 → `None`.
- **Pure ELT.** All batch modelling is SQL in `sql/`; Python does not transform
  batch rows. The one Python parser (`transforms/pgn.py`) exists for the
  streaming simulator, which needs a move *sequence* rather than aggregates.
- **Idempotent everywhere.** Silver `MERGE`s on `game_id` (games appear in both
  players' archives); windowed Gold does `DELETE`+`INSERT` in a transaction;
  small marts are `CREATE OR REPLACE`. Re-running any window is safe.
- **One DAG, not two.** Backfill is the same graph driven by a different data
  interval (`airflow dags backfill`), so there's no second copy to drift.
- **Measure, then gate.** DQ writes every assertion to `ops.dq_results` and a
  separate task fails the run — a failed run still leaves full diagnostics.
- **Vocabulary in one place.** `config/mappings.yaml` is the source of truth for
  result/termination/bracket codes, and a test fails if the SQL drifts from it.

### Verification honesty

There is no BigQuery connection in this environment, so the SQL is verified
offline: all 13 files parse in the **BigQuery dialect** (`sqlglot`), the MERGE's
`UPDATE SET` is asserted to cover every non-key column, and the mapping drift
guards run in CI. Column-level resolution against live schemas needs
`scripts/run_batch.py --dry-run`, which is free and is the intended pre-deploy
gate. Airflow can't be installed here either (no Windows/py3.14 wheel), so the
DAG is checked statically via `ast` — real import validation belongs in CI.
