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
| **3 — Streaming** | game-event simulator (Cloud Run), Pub/Sub + DLQ, Dataflow/Beam job, 4 streaming sinks | ✅ complete, Beam tested on DirectRunner |
| 4 — Monitoring & SCD-2 | `dim_players_history`, Cloud Monitoring alerts, backfill hardening | ⬜ next |
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
pytest                                                   # 175 tests, all offline

# 1. Ingest (from a network that can reach api.chess.com — see note above)
python scripts/run_ingestion.py --titles GM --players 50 --months 3
# -> lands NDJSON under ./data/raw/ (local backend, default)

# 2. Transform on BigQuery. --dry-run validates every statement for free.
python scripts/run_batch.py --start 2026-08-01 --end 2026-08-31 --dry-run
python scripts/run_batch.py --start 2026-08-01 --end 2026-08-31

# 3. Watch the streaming feed — no GCP needed (publisher: stdout emits NDJSON)
python scripts/run_simulator.py --limit 3 --speed 500 --seed 42
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
│   ├── transforms/pgn.py         # PGN parser (shared by batch docs + streaming)
│   └── streaming/                # events contract, publishers, replay engine
├── sql/
│   ├── bronze/                   # external-table DDL over the landing zone
│   ├── silver/                   # clean_games (MERGE), views, dimensions
│   ├── gold/                     # 5 KPI marts
│   ├── streaming/                # live events, windowed stats, late, DLQ
│   ├── dq_checks/                # assertions -> ops.dq_results
│   └── ops/                      # pipeline_runs, dq_results
├── airflow/dags/chess_daily_batch.py   # one DAG; backfill via data interval
├── dataflow/streaming_pipeline.py      # Beam job (thin glue over streaming/)
├── simulator/                    # Cloud Run service + slim Dockerfile
├── scripts/
│   ├── run_ingestion.py          # extract entrypoint
│   ├── run_batch.py              # Composer-free transform runner
│   └── run_simulator.py          # local event feed
├── docs/                         # architecture, streaming, layer schemas
├── tests/                        # 175 tests, no network or GCP needed
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
- **The streaming feed is labelled as simulated.** Chess.com has no real-time
  API, so the "live" stream replays real archived games. Every event carries
  `is_simulated = TRUE` (`NOT NULL`) and keeps `source_game_date` separate from
  replay `event_ts`, so replay time can never be mistaken for real game time.
  See [docs/streaming.md](docs/streaming.md).
- **Never emit a number we didn't measure.** Unclocked PGNs report `NULL` clocks
  rather than the base time, and there is no per-move `fen` column because the
  source doesn't provide one. Tests enforce both.
- **Late data is kept, not dropped.** Events past the watermark go to
  `streaming.late_events` so lateness is measurable — silently discarding it is
  the classic streaming mistake, because the totals still look plausible.

### Verification honesty

What **is** verified here (175 tests, all offline):

- The streaming path is genuinely exercised: Beam DoFns, DLQ routing, and the
  window summarisers run on the **DirectRunner**, real event-time windowing and
  lateness are driven with `TestStream`, dedup is proven against a duplicated
  feed, and the Cloud Run service is called through WSGI. The simulator was run
  end-to-end producing 96 interleaved events from 3 real games.
- All 14 SQL files parse in the **BigQuery dialect** (`sqlglot`); the MERGE's
  `UPDATE SET` is asserted to cover every non-key column; the event contract is
  compared field-by-field against the streaming DDL.

What is **not** verified: a real Pub/Sub round trip, real BigQuery streaming
inserts, and column resolution against live schemas. Those need a GCP project —
`scripts/run_batch.py --dry-run` is the free pre-deploy gate for the SQL.
Airflow has no Windows/py3.14 wheel, so the DAG is checked statically via `ast`;
real import validation belongs in CI.

Beam tests take ~85s. Use `pytest -m "not slow"` in tight loops; CI runs all.
