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
| 2 — Batch pipeline (Silver/Gold) | dbt/SQL transforms, Airflow DAG | ⬜ next |
| 3 — Streaming | game-event simulator, Dataflow job | ⬜ |
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
pytest                                                   # 11 tests, all offline

# Real ingestion (from a network that can reach api.chess.com):
python scripts/run_ingestion.py --titles GM --players 50 --months 3
# -> lands NDJSON under ./data/raw/ (local backend, default)
```

Switch to GCS with zero code changes:

```bash
export CHESS_STORAGE__BACKEND=gcs
export CHESS_STORAGE__GCS_BUCKET=chess-lakehouse
```

## Layout

```
chess_com/
├── config/config.yaml            # single config; env-overridable (CHESS_*)
├── src/chess_analytics/
│   ├── config.py                 # YAML + env-override loader
│   ├── logging_setup.py          # JSON logs in cloud, plain locally
│   ├── chesscom_client.py        # rate-limited, retrying API client
│   ├── storage.py                # local | GCS landing writer
│   └── ingestion/                # pull_titled_players / _profiles / _archives
├── scripts/run_ingestion.py      # Sprint 1 batch entrypoint
├── sql/bronze/                   # BigQuery external-table DDL
├── docs/schemas/bronze_schemas.md
├── tests/                        # pytest + fixtures (no network needed)
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
