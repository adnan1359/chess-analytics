# Bronze Layer — Raw Landing Schemas

The Bronze layer is the raw API payload, landed **verbatim** as NDJSON, with
only additive lineage fields. No cleaning happens here — that's Silver's job.
Bronze tables are BigQuery **external tables** over the GCS landing zone, so a
new file appears in the table the moment it's written (no load step).

## Landing-zone layout (GCS and local mirror)

```
gs://chess-lakehouse/raw/
├── titled/<TITLE>.json                                  # one row per player
├── players/
│   ├── profiles/<username>.json                         # one object
│   └── stats/<username>.json                            # one object
└── games/
    └── year=YYYY/month=MM/player=<username>.json        # one game per line
```

`year=`/`month=` are Hive-style partition keys. BigQuery external tables with
`hive_partition_uri_prefix` expose them as pruneable columns, so a query for one
month scans one month's files.

## Lineage fields (added to every record)

| field               | type      | meaning                                  |
|---------------------|-----------|------------------------------------------|
| `_ingested_at`      | TIMESTAMP | UTC time the record was landed           |
| `_source_endpoint`  | STRING    | API path it came from (traceability)     |

## `bronze.raw_games`

Selected columns (full raw object retained; nested `white`/`black` are STRUCTs).

| column         | type      | notes                                                |
|----------------|-----------|------------------------------------------------------|
| `url`          | STRING    | canonical game URL; game id is the trailing integer  |
| `uuid`         | STRING    | Chess.com game uuid                                  |
| `pgn`          | STRING    | full PGN incl. headers (`ECO`, `Termination`, clocks)|
| `time_control` | STRING    | e.g. `"600"`, `"180+2"`, `"1/259200"` (daily)        |
| `time_class`   | STRING    | `bullet` / `blitz` / `rapid` / `daily`               |
| `rated`        | BOOL      |                                                      |
| `end_time`     | INT64     | epoch seconds → Silver derives `game_date`           |
| `fen`          | STRING    | final position                                       |
| `eco`          | STRING    | opening URL (ECO *code* is in the PGN `[ECO]` header)|
| `white`        | STRUCT    | `{username, rating, result, uuid, @id}`              |
| `black`        | STRUCT    | `{username, rating, result, uuid, @id}`              |

**Result codes** live per-side in `white.result` / `black.result`. Exactly one
side is `win`; the other carries the *reason*: `checkmated`, `resigned`,
`timeout`, `abandoned`, or a draw reason (`agreed`, `repetition`, `stalemate`,
`insufficient`, `50move`, `timevsinsufficient`). Silver maps this pair to a
single `result` (`1-0` / `0-1` / `1/2-1/2`) plus a `termination` category.

**Deduplication:** a game between two tracked players lands in *both* players'
archives. The natural key is the game id parsed from `url`. Silver dedupes with
a `MERGE` on that key.

## `bronze.raw_players` (profiles) and `bronze.raw_player_stats`

Profile: `username`, `player_id`, `title`, `name`, `country`, `followers`,
`joined` (epoch), `last_online` (epoch), `status`.

Stats: per-format objects (`chess_rapid`, `chess_blitz`, `chess_bullet`, …)
each with `last`/`best`/`record{win,loss,draw}`. Feeds the SCD-2 `dim_players`
rating history in Sprint 4.

## `bronze.raw_titled`

`username`, `title`, plus lineage. The player universe seed.
