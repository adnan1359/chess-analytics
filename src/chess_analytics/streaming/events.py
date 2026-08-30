"""The game-event contract: builders, validation, and the schema field list.

This module is the single definition of what a chess game event *is*. Both the
simulator (producer) and the Dataflow pipeline (consumer) import it, and
``tests/test_streaming.py`` checks the field list against the BigQuery DDL in
``sql/streaming/`` — so the producer, consumer, and warehouse cannot drift apart.

Deliberately pure Python: no Beam, no GCP, no I/O. That keeps the validation and
shaping logic — the part that actually has bugs — unit-testable in milliseconds.

### On honesty about simulated data

Every event carries ``is_simulated = TRUE`` and ``source_game_date`` (the date
the game was really played). ``event_ts`` is replay wall-clock. Keeping both
means nobody downstream can mistake a 2026 replay of a 2024 game for live 2026
play, and the dashboards can say so out loud.

### On per-move FEN

The plan sketched a ``fen`` field on every move event. It is not included,
because it is **not derivable from the source data**: Chess.com archives give one
final FEN per game, not a position per ply. Producing per-move FEN would mean
replaying the moves through a real board implementation (e.g. ``python-chess``).
No current Gold or streaming metric needs it, so rather than emit a plausible
but wrong value, the field is omitted. ``final_fen`` is populated on
``game_end``, where the source actually provides it.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterator

from ..transforms import pgn

# Event types. game_start is emitted before the first move so a consumer knows a
# session opened (and gets the metadata) without having to infer it from move 1.
EVENT_GAME_START = "game_start"
EVENT_MOVE = "move"
EVENT_GAME_END = "game_end"
EVENT_TYPES = frozenset({EVENT_GAME_START, EVENT_MOVE, EVENT_GAME_END})

# Field order is the contract checked against sql/streaming/live_game_events.sql.
EVENT_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "event_ts",
    "game_id",
    "game_url",
    "player_white",
    "player_black",
    "white_elo",
    "black_elo",
    "time_class",
    "time_control_raw",
    "eco_code",
    "opening_name",
    "ply",
    "move_number",
    "color",
    "move_san",
    "mover_clock_sec",
    "white_clock_sec",
    "black_clock_sec",
    "outcome",
    "winner_color",
    "termination",
    "total_moves",
    "final_fen",
    "is_simulated",
    "source_game_date",
    "producer_version",
)

PRODUCER_VERSION = "simulator/0.1"


class EventValidationError(ValueError):
    """Event failed schema validation. Routed to the pipeline's DLQ output."""


@dataclass(frozen=True)
class GameContext:
    """Immutable per-game metadata, stamped onto every event for that game.

    Events are self-contained on purpose: a consumer can compute per-game stats
    without joining back to a dimension or holding cross-event state. The small
    duplication is worth the statelessness in a streaming context.
    """

    game_id: str
    game_url: str | None
    player_white: str
    player_black: str
    white_elo: int | None
    black_elo: int | None
    time_class: str | None
    time_control_raw: str | None
    eco_code: str | None
    opening_name: str | None
    outcome: str
    winner_color: str | None
    termination: str
    total_moves: int
    final_fen: str | None
    source_game_date: date | None
    base_time_sec: int | None = None
    moves: list[pgn.Move] = field(default_factory=list)


def game_context_from_archive(game: dict[str, Any]) -> GameContext | None:
    """Build a :class:`GameContext` from one raw archive game record.

    Returns ``None`` for records the simulator cannot replay — no game id, no
    moves, or a non-standard variant. Skipping is correct here: an unreplayable
    game is not an error, it is just not a source of events.
    """
    game_id = pgn.game_id_from_url(game.get("url"))
    if not game_id or game.get("rules") not in (None, "chess"):
        return None

    parsed = pgn.parse_pgn(game.get("pgn") or "")
    if not parsed.moves:
        return None

    white = game.get("white") or {}
    black = game.get("black") or {}
    white_result = (white.get("result") or "").lower()
    black_result = (black.get("result") or "").lower()
    outcome, winner_color = pgn.derive_outcome(white_result, black_result)
    tc = pgn.parse_time_control(game.get("time_control"))

    end_time = game.get("end_time")
    source_game_date = (
        datetime.fromtimestamp(end_time, tz=timezone.utc).date() if end_time else None
    )

    return GameContext(
        game_id=game_id,
        game_url=game.get("url"),
        player_white=(white.get("username") or "").lower() or None,
        player_black=(black.get("username") or "").lower() or None,
        white_elo=white.get("rating"),
        black_elo=black.get("rating"),
        time_class=(game.get("time_class") or "").lower() or None,
        time_control_raw=game.get("time_control"),
        eco_code=parsed.headers.get("ECO"),
        opening_name=pgn.opening_name_from_eco_url(game.get("eco")),
        outcome=outcome,
        winner_color=winner_color,
        termination=pgn.derive_termination(white_result, black_result),
        total_moves=parsed.total_moves,
        final_fen=game.get("fen"),
        source_game_date=source_game_date,
        base_time_sec=tc.base_sec,
        moves=parsed.moves,
    )


def _clock_to_seconds(clock: str | None) -> float | None:
    """Parse a PGN clock (``'0:09:57.5'`` / ``'1:02:03'``) into seconds."""
    if not clock:
        return None
    parts = clock.strip().split(":")
    try:
        values = [float(p) for p in parts]
    except ValueError:
        return None
    seconds = 0.0
    for value in values:            # most-significant first
        seconds = seconds * 60 + value
    return round(seconds, 1)


def _base(ctx: GameContext, event_type: str, event_ts: datetime) -> dict[str, Any]:
    """The metadata every event carries, regardless of type."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": event_ts.astimezone(timezone.utc).isoformat(),
        "game_id": ctx.game_id,
        "game_url": ctx.game_url,
        "player_white": ctx.player_white,
        "player_black": ctx.player_black,
        "white_elo": ctx.white_elo,
        "black_elo": ctx.black_elo,
        "time_class": ctx.time_class,
        "time_control_raw": ctx.time_control_raw,
        "eco_code": ctx.eco_code,
        "opening_name": ctx.opening_name,
        "ply": None,
        "move_number": None,
        "color": None,
        "move_san": None,
        "mover_clock_sec": None,
        "white_clock_sec": None,
        "black_clock_sec": None,
        "outcome": None,
        "winner_color": None,
        "termination": None,
        "total_moves": None,
        "final_fen": None,
        "is_simulated": True,
        "source_game_date": ctx.source_game_date.isoformat() if ctx.source_game_date else None,
        "producer_version": PRODUCER_VERSION,
    }


def starting_clocks(ctx: GameContext) -> tuple[float | None, float | None]:
    """Both sides' clocks at move 0.

    Daily games express ``base_time_sec`` as seconds *per move* rather than a
    starting clock, so they get NULL instead of a number that would silently
    mean something different from every other row.
    """
    if ctx.base_time_sec is None or (ctx.time_control_raw or "").startswith("1/"):
        return None, None
    return float(ctx.base_time_sec), float(ctx.base_time_sec)


def build_game_start(ctx: GameContext, event_ts: datetime,
                     white_clock_sec: float | None = None,
                     black_clock_sec: float | None = None) -> dict[str, Any]:
    """Session-opening event. Carries metadata but no result."""
    event = _base(ctx, EVENT_GAME_START, event_ts)
    event.update(white_clock_sec=white_clock_sec, black_clock_sec=black_clock_sec)
    return event


def build_move(
    ctx: GameContext,
    move: pgn.Move,
    event_ts: datetime,
    white_clock_sec: float | None = None,
    black_clock_sec: float | None = None,
) -> dict[str, Any]:
    """One half-move. ``mover_clock_sec`` is the clock of the side that moved."""
    event = _base(ctx, EVENT_MOVE, event_ts)
    event.update(
        ply=move.ply,
        move_number=move.move_number,
        color=move.color,
        move_san=move.san,
        mover_clock_sec=_clock_to_seconds(move.clock),
        white_clock_sec=white_clock_sec,
        black_clock_sec=black_clock_sec,
    )
    return event


def build_game_end(ctx: GameContext, event_ts: datetime,
                   white_clock_sec: float | None = None,
                   black_clock_sec: float | None = None) -> dict[str, Any]:
    """Terminal event, carrying the result and the final position."""
    event = _base(ctx, EVENT_GAME_END, event_ts)
    event.update(
        outcome=ctx.outcome,
        winner_color=ctx.winner_color,
        termination=ctx.termination,
        total_moves=ctx.total_moves,
        final_fen=ctx.final_fen,
        white_clock_sec=white_clock_sec,
        black_clock_sec=black_clock_sec,
    )
    return event


@dataclass(frozen=True)
class EventPlan:
    """One event's *content*, with its timestamp deliberately absent.

    Separating "what happens" from "when it happens" is what lets the simulator
    interleave several games on one timeline: it builds each game's plan once,
    then stamps entries with real times as it paces them out. It also makes the
    clock-tracking logic testable without any notion of time at all.
    """

    event_type: str
    move: pgn.Move | None = None
    white_clock_sec: float | None = None
    black_clock_sec: float | None = None


def plan_game_events(ctx: GameContext) -> list[EventPlan]:
    """The ordered event plan for one game: start, every half-move, end.

    Clocks are carried forward per side — a move updates only the mover's clock,
    so each entry holds the last *observed* value for both sides.

    "Observed" is load-bearing. Many archived PGNs carry no ``[%clk]``
    annotations at all. Seeding the running clocks with the base time would then
    emit ``white_clock_sec = 600.0`` on move 40 of a 10-minute game — a number we
    never measured, which would silently poison ``min_clock_sec`` and every
    clock-pressure metric downstream. So the running clocks start as NULL and
    only ever hold values actually read from the PGN. ``game_start`` is the one
    exception: at move 0 both clocks genuinely *are* the base time.
    """
    plans = [EventPlan(EVENT_GAME_START, None, *starting_clocks(ctx))]

    white_clock: float | None = None
    black_clock: float | None = None
    for move in ctx.moves:
        mover_clock = _clock_to_seconds(move.clock)
        if mover_clock is not None:
            if move.color == "white":
                white_clock = mover_clock
            else:
                black_clock = mover_clock
        plans.append(EventPlan(EVENT_MOVE, move, white_clock, black_clock))

    plans.append(EventPlan(EVENT_GAME_END, None, white_clock, black_clock))
    return plans


def build_event(ctx: GameContext, plan: EventPlan, event_ts: datetime) -> dict[str, Any]:
    """Materialise a planned event at a concrete timestamp."""
    if plan.event_type == EVENT_GAME_START:
        return build_game_start(ctx, event_ts, plan.white_clock_sec, plan.black_clock_sec)
    if plan.event_type == EVENT_GAME_END:
        return build_game_end(ctx, event_ts, plan.white_clock_sec, plan.black_clock_sec)
    if plan.move is None:
        raise ValueError("move event plan has no move")
    return build_move(ctx, plan.move, event_ts, plan.white_clock_sec, plan.black_clock_sec)


def iter_game_events(ctx: GameContext, timestamps: Iterator[datetime]) -> Iterator[dict[str, Any]]:
    """Full event sequence for one game, pulling one timestamp per event."""
    for plan in plan_game_events(ctx):
        yield build_event(ctx, plan, next(timestamps))


# --------------------------------------------------------------------------- #
# Validation — the consumer side. Invalid events go to the DLQ, never to the
# fact table, and never silently dropped.
# --------------------------------------------------------------------------- #
_REQUIRED_ALWAYS = ("event_id", "event_type", "event_ts", "game_id")


def validate(event: dict[str, Any]) -> dict[str, Any]:
    """Return the event if valid, else raise :class:`EventValidationError`.

    Checks the invariants a consumer genuinely depends on, rather than
    re-checking every field: identity (for dedup), type, and the per-type
    required fields.
    """
    if not isinstance(event, dict):
        raise EventValidationError(f"event must be an object, got {type(event).__name__}")

    missing = [k for k in _REQUIRED_ALWAYS if not event.get(k)]
    if missing:
        raise EventValidationError(f"missing required field(s): {missing}")

    event_type = event["event_type"]
    if event_type not in EVENT_TYPES:
        raise EventValidationError(f"unknown event_type: {event_type!r}")

    unknown = set(event) - set(EVENT_FIELDS)
    if unknown:
        raise EventValidationError(f"unknown field(s): {sorted(unknown)}")

    if event_type == EVENT_MOVE:
        for key in ("ply", "move_number", "color", "move_san"):
            if event.get(key) in (None, ""):
                raise EventValidationError(f"move event missing {key}")
        if event["color"] not in ("white", "black"):
            raise EventValidationError(f"invalid color: {event['color']!r}")
        if not isinstance(event["ply"], int) or event["ply"] < 1:
            raise EventValidationError(f"invalid ply: {event['ply']!r}")

    if event_type == EVENT_GAME_END:
        if event.get("outcome") not in ("1-0", "0-1", "1/2-1/2"):
            raise EventValidationError(f"invalid outcome: {event.get('outcome')!r}")
        if not event.get("termination"):
            raise EventValidationError("game_end missing termination")

    return event


def to_json_dict(event: dict[str, Any]) -> dict[str, Any]:
    """Normalise an event to a JSON/BigQuery-safe dict with every field present.

    Absent keys are filled with ``None`` so the row shape is stable — BigQuery
    streaming inserts are far happier with a consistent schema than with
    per-row optional keys.
    """
    return {name: event.get(name) for name in EVENT_FIELDS}


def context_as_dict(ctx: GameContext) -> dict[str, Any]:
    """Debug helper: the context without the (large) move list."""
    data = asdict(ctx)
    data.pop("moves", None)
    return data
