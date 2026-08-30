# Silver & Gold Layer Design

Bronze is raw and unmodelled ([`bronze_schemas.md`](bronze_schemas.md)). Silver
conforms it; Gold answers questions.

## Silver

### `silver.clean_games` — the core fact table

One row per **distinct game**. Partitioned by `game_date`, clustered by
`time_class, eco_code, player_white`.

Key derivations (all in [`sql/silver/clean_games.sql`](../../sql/silver/clean_games.sql)):

| Output | Derived from | Notes |
|---|---|---|
| `game_id` | `url` | Trailing integer. **The natural key.** |
| `game_date` | `end_time` epoch | Partition column, `NOT NULL` |
| `outcome` | `white.result` / `black.result` | `1-0` / `0-1` / `1/2-1/2` |
| `winner_color` | same | `NULL` when drawn |
| `termination` | the **non-winning** side's result code | see below |
| `total_moves` | PGN movetext | highest full-move number |
| `eco_code` | PGN `[ECO]` header | the `eco` *field* is a URL, not the code |
| `opening_name` | `eco` URL slug | `Sicilian-Defense-…` → `Sicilian Defense …` |
| `base_time_sec`, `increment_sec`, `is_daily` | `time_control` | `"180+2"`, `"1/259200"` |
| `elo_bracket_*` | ratings | see *Elo brackets* below |

#### Result encoding — the one genuinely tricky bit

Chess.com does **not** provide a single result field. Each side carries its own
code; exactly one is `win`, and the other holds the *reason* the game ended. For
a draw, **both** sides carry the same draw reason. So:

```
reason = IF(white.result = 'win', black.result, white.result)
```

is correct in all three cases, and that reason maps 1:1 to our `termination`
category (`checkmated` → `checkmate`, `agreed` → `draw_agreed`, …). The full map
lives in [`config/mappings.yaml`](../../config/mappings.yaml).

#### Two traps worth knowing

1. **Clock comments corrupt move counting.** Real PGNs annotate every move:
   `1. e4 {[%clk 0:09:57.5]}`. A naive `(\d+)\.` move-number regex reads the
   `57.` inside the clock and reports move 57 for a 20-move game. Comments are
   stripped *before* counting.
2. **The `[Date "2026.08.15"]` header does the same thing.** Headers are split
   off on the blank line before the movetext is tokenized.

Both are covered by tests in [`tests/test_pgn.py`](../../tests/test_pgn.py).

#### Deduplication & idempotency

A game between two tracked players lands in **both** players' archives, so
Bronze legitimately holds it twice. Silver dedupes with
`ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY _ingested_at DESC)` and then
`MERGE`s on `game_id`, so re-running a window never double-inserts. The MERGE's
`ON` clause also constrains `game_date` to the window so the target prunes
instead of scanning all history.

#### Scope

Filtered to `rules = 'chess'`. Variants (bughouse, king-of-the-hill, 3-check)
would distort opening and result analytics; they are excluded rather than
silently blended in.

### `silver.player_game_results` — a VIEW

`clean_games` re-grained to **one row per (game, player)** via `UNION ALL`.
Player-centric metrics need this grain; putting it in a view means the win/loss
mapping is written once instead of in each of three Gold models. A view (not a
table) costs no storage and lets Gold's partition filters push down.

Carries `score` (1 / 0.5 / 0) alongside `result`, because `AVG(score)` — draws
counting half — is the standard chess performance measure and is not recoverable
from a win rate alone.

### `silver.openings_dim`

One row per ECO code with its most common `opening_name`. Exists because
Chess.com derives the opening from actual move order, so one ECO code
legitimately appears under several names (transpositions). `name_variant_count`
exposes that ambiguity instead of hiding it.

### `silver.dim_players`

Current-state player dimension (**SCD Type 1**). Type 2 history arrives in
Sprint 4 as a separate `dim_players_history`; Type 1 stays because most joins
want "who is this player now" and shouldn't have to filter `is_current`.

## Gold

| Table | Grain | Answers |
|---|---|---|
| `daily_player_kpis` | player × date × time_class | daily form, intra-day Elo movement |
| `opening_win_rates` | eco × elo_bracket × time_class | which openings actually score |
| `elo_trend_weekly` | player × week × time_class | trajectory, week-over-week delta |
| `time_control_meta` | time_class × elo_bracket | how the clock changes the game |
| `player_cohorts` | signup year × time_class | cohort comparison |

### Design choices

- **`time_class` is in almost every grain.** Blitz and rapid Elo are different
  scales; blending them yields a meaningless average. Looker can roll up; it
  cannot un-blend.
- **Full rebuild vs windowed.** `daily_player_kpis` replaces only the run window
  (`DELETE`+`INSERT` inside a transaction, so a failure can't leave a hole). The
  other four are `CREATE OR REPLACE` — they're small aggregates whose trend and
  `LAG` logic needs full history anyway, which makes them idempotent by
  construction.
- **Sample-size flags, not silent noise.** `opening_win_rates` and
  `player_cohorts` expose `is_significant_sample` rather than letting a 2-game
  100% win rate look like a finding.
- **No fabricated metrics.** `time_control_meta` deliberately has **no average
  game duration**: the API gives `end_time` but no start time, and PGN clocks are
  per-move. A duration derived from the base time control would be an assumption
  presented as a measurement.

### Elo brackets — a deviation from the original plan

The plan specified `<1500 / 1500-2000 / 2000+`. That is wrong *for this dataset*:
the player universe is seeded from **titled** players, so essentially every row
lands in `2000+` and the dimension does no work. Replaced with six brackets from
`u2000` to `2800_plus` (see `config/mappings.yaml`), which discriminate within a
titled population.

**Standing caveat:** every Gold table describes *titled players*, not the
Chess.com population. `player_cohorts` in particular is "titled players by
signup year". Widening the seed (e.g. from leaderboards or a random sample) is
the fix if population-level claims are ever needed.

## Data quality

[`sql/dq_checks/silver_clean_games.sql`](../../sql/dq_checks/silver_clean_games.sql)
writes one row per assertion to `ops.dq_results`, then a separate Airflow task
decides. Splitting *measure* from *decide* means a failing run still leaves a
complete diagnostic record instead of dying on the first bad assertion.

- `severity = 'error'` → blocks the DAG (null keys, duplicate `game_id`,
  invalid outcome, future dates, draw-flag inconsistency, self-play, zero rows).
- `severity = 'warn'` → recorded and dashboarded (unmapped termination, unknown
  `time_class`, implausible move counts, missing ECO). A new API enum value
  should show up as a warning, not take down a correct run.

Each check has a `threshold_pct`; `0.0` means not a single row may violate it.

## Keeping two implementations honest

The batch path is pure ELT (SQL), while the Sprint 3 streaming simulator needs
the same vocabulary in Python. Both read `config/mappings.yaml`, and
[`tests/test_sql.py`](../../tests/test_sql.py) fails if a code exists in the YAML
but isn't handled in the SQL — so adding a termination code can't silently leave
one path behind.
