# Architecture & Sprint Plan

## Problem statement

Build a real-time **and** batch analytics platform for Chess.com game data:
ingest live-style game events, enrich with player profiles, compute live
dashboards (game stats, Elo tracking), and produce daily batch analytics
(opening analysis, win-rate trends, cohorts) — on GCP, with data-engineering
rigor.

## Data source strategy

Chess.com exposes a **free, unauthenticated** public API (`/pub`):

| Endpoint | Use |
|---|---|
| `/pub/player/{u}` | profile |
| `/pub/player/{u}/games/{YYYY}/{MM}` | monthly game archive (PGN + metadata) |
| `/pub/player/{u}/stats` | ratings, W/L/D by format |
| `/pub/titled/{TITLE}` | all players of a title |
| `/pub/leaderboards` | top players per format |

There is **no public streaming/WebSocket API**, so the streaming layer (Sprint 3)
is a **game-event simulator** that replays real archived games move-by-move into
Pub/Sub — the standard "replay real data as live events" pattern. Every emitted
event is explicitly flagged `is_simulated = TRUE` and retains the original game
date, so replayed traffic can never be presented as live play. Detail:
[`streaming.md`](streaming.md).

## Architecture

```
 Chess.com API ──(batch pull)──▶ GCS raw landing ──▶ BigQuery Bronze (external)
        │                                                     │
        └──(replay)──▶ Game Event Simulator (Cloud Run)       ▼
                              │                          Silver (clean, dedup)
                              ▼                                │
                       Pub/Sub topic ──▶ Dataflow ──▶ BQ Streaming    ▼
                              │            (Beam)                  Gold (KPIs)
                              ▼                                       │
                         Pub/Sub DLQ                            Looker Studio
 Orchestration: Cloud Composer (Airflow) — daily batch ETL, backfill, DQ.
```

## Medallion layers (BigQuery)

- **Bronze** — external tables over raw NDJSON in GCS; verbatim payload +
  lineage. See [`schemas/bronze_schemas.md`](schemas/bronze_schemas.md).
- **Silver** — `clean_games` (typed, deduped on game id via MERGE),
  `player_game_results` (view, player grain), `openings_dim`, `dim_players`.
- **Gold** — `daily_player_kpis`, `opening_win_rates` (by Elo bracket),
  `elo_trend_weekly`, `player_cohorts`, `time_control_meta`.

Design detail for both: [`schemas/silver_gold_schemas.md`](schemas/silver_gold_schemas.md).

## Orchestration

One DAG, [`airflow/dags/chess_daily_batch.py`](../airflow/dags/chess_daily_batch.py):

```
start -> ingest_chesscom -> bronze -> create_ops_tables
                                        |-> silver_clean_games -> player_game_results -> gold(daily, elo, cohorts)
                                        |                      \-> openings_dim ------> gold(opening_win_rates)
                                        \-> silver_dim_players -----------------------> gold(cohorts)
                                                                                        gold -> run_dq_checks -> dq_gate -> end
```

There is **no separate backfill DAG**. Every task is parameterised by the run's
data interval and every write is window-scoped and idempotent, so a range is just
`airflow dags backfill chess_daily_batch -s ... -e ...`. A second DAG would
duplicate the graph and then drift from it.

Composer costs ~$300/month, so [`scripts/run_batch.py`](../scripts/run_batch.py)
runs the identical SQL in the identical order without Airflow — the free-tier
path. `--dry-run` validates every statement against BigQuery at no cost and is
the intended pre-deploy gate.

## Engineering practices (the point of the project)

| Concern | Approach |
|---|---|
| Idempotency | partition-level overwrite (batch); `event_id` dedup (stream) |
| Late data | Dataflow watermark + side-output to `late_events` |
| SCD | Type-2 `dim_players` for Elo history |
| Data quality | SQL assertions (null rate, Elo range, valid results, no future dates) |
| Backfill | Airflow DAG parameterized by `(start_date, end_date)` |
| Dead-letter | Pub/Sub DLQ for poison messages |
| Observability | `ops.pipeline_runs`, DQ dashboards, Cloud Monitoring alerts |
| Cost | external Bronze, partition pruning, Dataflow autoscaling, budget alerts |
| IaC / CI-CD | Terraform for all resources; Cloud Build on push to `main` |

## Sprint plan

1. **Foundation & ingestion** — GCP/APIs, pull titled players + profiles +
   archives, land in GCS, Bronze schemas. ✅
2. **Batch pipeline** — Bronze→Silver→Gold SQL, Airflow DAG, idempotent MERGE,
   DQ assertion framework, PGN parser. ✅
3. **Streaming** — simulator on Cloud Run, Pub/Sub + DLQ, Dataflow job with
   dedup, session + fixed windows, and late-data capture. ✅
   Detail: [`streaming.md`](streaming.md).
4. **Monitoring & SCD-2** — `dim_players_history` (Type 2), Cloud Monitoring
   alerts, backfill hardening. (The DQ assertion framework and DLQ landed early,
   in Sprints 2 and 3.)
5. **Analytics & dashboards** — 4 Looker Studio dashboards.
6. **Hardening** — Terraform, CI/CD, cost controls, docs, portfolio polish.

## Cost notes

BigQuery (1 TB query / 10 GB storage free), GCS (5 GB), Pub/Sub (10 GB/mo), and
Cloud Run (2M req/mo) fit the free tier. Dataflow and Composer are **not** free —
use minimal workers / smallest Composer env, or run Airflow locally in Docker and
drive GCP via client libraries. The architecture is identical either way.
