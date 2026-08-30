"""Tests for the event contract and the replay engine.

Everything here runs with no Beam, no GCP, and no sleeping: the simulator takes
an injectable clock and sleeper, and the publisher is an in-memory list.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from chess_analytics.streaming import events as ev
from chess_analytics.streaming.publisher import MemoryPublisher, StdoutPublisher
from chess_analytics.streaming.simulator import (
    GameReplay,
    Simulator,
    SimulatorConfig,
    load_game_contexts,
)

FIXTURES = Path(__file__).parent / "fixtures" / "api"
T0 = datetime(2026, 8, 30, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def raw_games() -> list[dict]:
    return json.loads((FIXTURES / "games_2026_08.json").read_text(encoding="utf-8"))["games"]


@pytest.fixture
def ctx(raw_games) -> ev.GameContext:
    """Game 3: bullet, 60+1, clock comments on every move, Black wins by resignation."""
    context = ev.game_context_from_archive(raw_games[2])
    assert context is not None
    return context


def _timestamps(start: datetime = T0, step_sec: float = 1.0):
    current = start
    while True:
        yield current
        current += timedelta(seconds=step_sec)


# --------------------------------------------------------------------------- #
# GameContext construction
# --------------------------------------------------------------------------- #
def test_context_from_archive(ctx):
    assert ctx.game_id == "98235123"
    assert ctx.player_white == "hikaru"
    assert ctx.player_black == "fabianocaruana"
    assert ctx.white_elo == 3401
    assert ctx.outcome == "0-1"
    assert ctx.winner_color == "black"
    assert ctx.termination == "resignation"
    assert ctx.total_moves == 10
    assert ctx.time_class == "bullet"
    assert ctx.base_time_sec == 60
    assert ctx.eco_code == "D37"
    assert ctx.opening_name == "Queens Gambit Declined Four Move Variation"
    # source_game_date is the date the game was REALLY played, kept distinct
    # from replay event time so the two can never be conflated. It is derived
    # from end_time, which must agree with the PGN [Date] header.
    assert ctx.source_game_date.isoformat() == "2026-08-22"


def test_unreplayable_games_are_skipped():
    assert ev.game_context_from_archive({"url": "https://x/game/live/1"}) is None      # no pgn
    assert ev.game_context_from_archive({"pgn": "[Event \"x\"]\n\n1. e4"}) is None      # no url
    assert ev.game_context_from_archive(
        {"url": "https://x/game/live/1", "pgn": "[E \"x\"]\n\n1. e4", "rules": "bughouse"}
    ) is None                                                                          # variant


# --------------------------------------------------------------------------- #
# Event planning and building
# --------------------------------------------------------------------------- #
def test_plan_covers_start_all_plies_and_end(ctx):
    plans = ev.plan_game_events(ctx)
    # 1 start + 20 plies + 1 end
    assert len(plans) == 22
    assert plans[0].event_type == ev.EVENT_GAME_START
    assert plans[-1].event_type == ev.EVENT_GAME_END
    assert all(p.event_type == ev.EVENT_MOVE for p in plans[1:-1])


def test_clocks_carry_forward_per_side(ctx):
    plans = ev.plan_game_events(ctx)
    # game_start: at move 0 both clocks genuinely ARE the base time.
    assert plans[0].white_clock_sec == 60.0
    assert plans[0].black_clock_sec == 60.0

    # After White's 1. d4 {0:01:00.9}: White observed, Black not yet seen.
    assert plans[1].white_clock_sec == 60.9
    assert plans[1].black_clock_sec is None

    # After Black's 1... Nf6 {0:01:00.8}: both now observed.
    assert plans[2].white_clock_sec == 60.9
    assert plans[2].black_clock_sec == 60.8

    # Final plan entry holds both sides' last observed clocks.
    assert plans[-1].white_clock_sec == pytest.approx(40.2)
    assert plans[-1].black_clock_sec == pytest.approx(48.8)


def test_unclocked_pgn_reports_null_clocks_not_the_base_time(raw_games):
    """Games with no [%clk] annotations must not claim a clock we never read.

    Seeding the running clocks with the base time would emit
    white_clock_sec=600.0 on move 40 of a 10-minute game — fabricated data that
    would corrupt min_clock_sec and every clock-pressure metric.
    """
    ctx = ev.game_context_from_archive(raw_games[0])   # rapid 600, no clocks
    assert all(m.clock is None for m in ctx.moves)

    plans = ev.plan_game_events(ctx)
    # game_start still reports the base time — that part is genuinely known.
    assert plans[0].white_clock_sec == 600.0

    # Every move and the end event report NULL, because nothing was observed.
    for plan in plans[1:]:
        assert plan.white_clock_sec is None, "invented a White clock"
        assert plan.black_clock_sec is None, "invented a Black clock"


def test_daily_games_get_null_clocks():
    """Daily base_time_sec is seconds PER MOVE, not a starting clock."""
    ctx = ev.GameContext(
        game_id="1", game_url=None, player_white="a", player_black="b",
        white_elo=2000, black_elo=2000, time_class="daily",
        time_control_raw="1/259200", eco_code=None, opening_name=None,
        outcome="1-0", winner_color="white", termination="resignation",
        total_moves=1, final_fen=None, source_game_date=None,
        base_time_sec=259200, moves=[],
    )
    assert ev.starting_clocks(ctx) == (None, None)
    assert ev.plan_game_events(ctx)[0].white_clock_sec is None


def test_events_are_fully_shaped_and_valid(ctx):
    for event in ev.iter_game_events(ctx, _timestamps()):
        assert set(event) == set(ev.EVENT_FIELDS), "event shape must match the contract"
        ev.validate(event)                       # must not raise
        assert event["is_simulated"] is True
        assert event["game_id"] == "98235123"
        assert event["producer_version"] == ev.PRODUCER_VERSION


def test_event_ids_are_unique(ctx):
    events = list(ev.iter_game_events(ctx, _timestamps()))
    ids = [e["event_id"] for e in events]
    assert len(set(ids)) == len(ids), "event_id is the dedup key; must be unique"


def test_move_event_content(ctx):
    events = list(ev.iter_game_events(ctx, _timestamps()))
    first_move = events[1]
    assert first_move["event_type"] == "move"
    assert (first_move["ply"], first_move["move_number"]) == (1, 1)
    assert first_move["color"] == "white"
    assert first_move["move_san"] == "d4"
    assert first_move["mover_clock_sec"] == 60.9
    # Result fields belong only on game_end.
    assert first_move["outcome"] is None
    assert first_move["total_moves"] is None


def test_game_end_event_content(ctx):
    end = list(ev.iter_game_events(ctx, _timestamps()))[-1]
    assert end["event_type"] == "game_end"
    assert end["outcome"] == "0-1"
    assert end["winner_color"] == "black"
    assert end["termination"] == "resignation"
    assert end["total_moves"] == 10
    assert end["final_fen"].startswith("r1b2rk1")


def test_event_ts_is_iso_utc(ctx):
    event = next(iter(ev.iter_game_events(ctx, _timestamps())))
    parsed = datetime.fromisoformat(event["event_ts"])
    assert parsed.tzinfo is not None
    assert parsed.astimezone(timezone.utc) == T0


@pytest.mark.parametrize(
    "clock, expected",
    [
        ("0:09:57.5", 597.5),
        ("0:01:00.9", 60.9),
        ("1:02:03", 3723.0),
        ("59.9", 59.9),
        (None, None),
        ("garbage", None),
    ],
)
def test_clock_parsing(clock, expected):
    assert ev._clock_to_seconds(clock) == expected


# --------------------------------------------------------------------------- #
# Validation — the consumer contract
# --------------------------------------------------------------------------- #
def _valid_move_event(ctx) -> dict:
    return list(ev.iter_game_events(ctx, _timestamps()))[1]


def test_validate_accepts_good_event(ctx):
    assert ev.validate(_valid_move_event(ctx))


@pytest.mark.parametrize("field", ["event_id", "event_type", "event_ts", "game_id"])
def test_validate_rejects_missing_identity(ctx, field):
    event = _valid_move_event(ctx)
    event[field] = None
    with pytest.raises(ev.EventValidationError, match="missing required"):
        ev.validate(event)


def test_validate_rejects_unknown_event_type(ctx):
    event = _valid_move_event(ctx)
    event["event_type"] = "teleport"
    with pytest.raises(ev.EventValidationError, match="unknown event_type"):
        ev.validate(event)


def test_validate_rejects_unknown_field(ctx):
    """Schema drift must fail loudly, not get silently dropped at insert time."""
    event = _valid_move_event(ctx)
    event["surprise_column"] = 1
    with pytest.raises(ev.EventValidationError, match="unknown field"):
        ev.validate(event)


@pytest.mark.parametrize(
    "field, value",
    [("ply", None), ("move_san", None), ("color", "green"), ("ply", 0), ("ply", "1")],
)
def test_validate_rejects_bad_move_fields(ctx, field, value):
    event = _valid_move_event(ctx)
    event[field] = value
    with pytest.raises(ev.EventValidationError):
        ev.validate(event)


def test_validate_rejects_bad_game_end(ctx):
    end = list(ev.iter_game_events(ctx, _timestamps()))[-1]
    bad = dict(end, outcome="win")
    with pytest.raises(ev.EventValidationError, match="invalid outcome"):
        ev.validate(bad)
    with pytest.raises(ev.EventValidationError, match="missing termination"):
        ev.validate(dict(end, termination=None))


def test_validate_rejects_non_dict():
    with pytest.raises(ev.EventValidationError, match="must be an object"):
        ev.validate(["not", "a", "dict"])


def test_to_json_dict_fills_all_fields():
    row = ev.to_json_dict({"event_id": "x"})
    assert set(row) == set(ev.EVENT_FIELDS)
    assert row["event_id"] == "x"
    assert row["move_san"] is None


# --------------------------------------------------------------------------- #
# Replay engine
# --------------------------------------------------------------------------- #
def test_game_replay_walks_the_plan_then_stops(ctx):
    replay = GameReplay(ctx)
    produced = []
    ts = T0
    while not replay.finished:
        event = replay.next_event(ts)
        if event:
            produced.append(event)
        ts += timedelta(seconds=1)
    assert len(produced) == 22
    assert replay.next_event(ts) is None          # exhausted, stays exhausted


class _FakeClock:
    """Monotonic fake clock advanced only by the fake sleeper."""

    def __init__(self, start: datetime = T0) -> None:
        self.now = start
        self.slept = 0.0

    def __call__(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept += seconds
        self.now += timedelta(seconds=seconds)


def test_simulator_publishes_every_event_of_every_game(raw_games):
    contexts = [ev.game_context_from_archive(g) for g in raw_games]
    contexts = [c for c in contexts if c]
    assert len(contexts) == 3

    pub = MemoryPublisher()
    clock = _FakeClock()
    sim = Simulator(
        pub,
        SimulatorConfig(concurrent_games=3, max_games=0, seed=42),
        clock=clock,
        sleeper=clock.sleep,
    )
    summary = sim.run(contexts)

    expected = sum(len(ev.plan_game_events(c)) for c in contexts)
    assert summary["events_published"] == expected == len(pub)
    assert summary["games_completed"] == 3


def test_simulator_interleaves_concurrent_games(raw_games):
    """Concurrency is the point: a sequential stream would prove nothing."""
    contexts = [c for c in (ev.game_context_from_archive(g) for g in raw_games) if c]
    pub = MemoryPublisher()
    clock = _FakeClock()
    Simulator(
        pub,
        SimulatorConfig(concurrent_games=3, max_games=0, seed=7),
        clock=clock,
        sleeper=clock.sleep,
    ).run(contexts)

    game_sequence = [e["game_id"] for e in pub.events]
    # More runs of alternating game ids than there are games => interleaved.
    switches = sum(1 for a, b in zip(game_sequence, game_sequence[1:]) if a != b)
    assert switches > len(contexts), f"stream looks sequential: {switches} switches"


def test_simulator_preserves_per_game_ply_order(raw_games):
    """Ordering key is game_id, so each game's plies must stay ascending."""
    contexts = [c for c in (ev.game_context_from_archive(g) for g in raw_games) if c]
    pub = MemoryPublisher()
    clock = _FakeClock()
    Simulator(
        pub,
        SimulatorConfig(concurrent_games=3, max_games=0, seed=1),
        clock=clock,
        sleeper=clock.sleep,
    ).run(contexts)

    per_game: dict[str, list[int]] = {}
    for event, key in zip(pub.events, pub.ordering_keys):
        assert key == event["game_id"], "ordering key must be game_id"
        if event["event_type"] == "move":
            per_game.setdefault(event["game_id"], []).append(event["ply"])

    for game_id, plies in per_game.items():
        assert plies == sorted(plies), f"{game_id} plies out of order"
        assert plies == list(range(1, len(plies) + 1)), f"{game_id} has gaps"


def test_simulator_event_timestamps_are_monotonic(raw_games):
    contexts = [c for c in (ev.game_context_from_archive(g) for g in raw_games) if c]
    pub = MemoryPublisher()
    clock = _FakeClock()
    Simulator(
        pub, SimulatorConfig(concurrent_games=2, max_games=0, seed=3),
        clock=clock, sleeper=clock.sleep,
    ).run(contexts)
    stamps = [datetime.fromisoformat(e["event_ts"]) for e in pub.events]
    assert stamps == sorted(stamps), "the feed must advance in event time"


def test_speed_multiplier_compresses_elapsed_time(raw_games):
    contexts = [c for c in (ev.game_context_from_archive(g) for g in raw_games) if c][:1]

    def elapsed(multiplier: float) -> float:
        clock = _FakeClock()
        Simulator(
            MemoryPublisher(),
            SimulatorConfig(concurrent_games=1, max_games=0, seed=99,
                            speed_multiplier=multiplier),
            clock=clock, sleeper=clock.sleep,
        ).run(contexts)
        return clock.slept

    slow, fast = elapsed(1.0), elapsed(10.0)
    assert fast < slow
    assert fast == pytest.approx(slow / 10, rel=0.05)


def test_max_games_bounds_the_run(raw_games):
    contexts = [c for c in (ev.game_context_from_archive(g) for g in raw_games) if c]
    pub = MemoryPublisher()
    clock = _FakeClock()
    summary = Simulator(
        pub, SimulatorConfig(concurrent_games=5, max_games=1, seed=5),
        clock=clock, sleeper=clock.sleep,
    ).run(contexts)
    assert summary["games"] == 1
    assert len({e["game_id"] for e in pub.events}) == 1


def test_seed_makes_runs_reproducible(raw_games):
    contexts = [c for c in (ev.game_context_from_archive(g) for g in raw_games) if c]

    def run_once() -> list[str]:
        pub, clock = MemoryPublisher(), _FakeClock()
        Simulator(
            pub, SimulatorConfig(concurrent_games=3, max_games=0, seed=1234),
            clock=clock, sleeper=clock.sleep,
        ).run(contexts)
        return [e["game_id"] for e in pub.events]

    assert run_once() == run_once()


# --------------------------------------------------------------------------- #
# Loading from the landing zone
# --------------------------------------------------------------------------- #
def test_load_game_contexts_reads_landed_ndjson(tmp_path, raw_games):
    part = tmp_path / "year=2026" / "month=08"
    part.mkdir(parents=True)
    (part / "player=hikaru.json").write_text(
        "\n".join(json.dumps(g) for g in raw_games), encoding="utf-8"
    )
    contexts = load_game_contexts(tmp_path)
    assert len(contexts) == 3
    assert {c.game_id for c in contexts} == {"98234571", "98234999", "98235123"}


def test_load_game_contexts_missing_dir_is_actionable(tmp_path):
    with pytest.raises(FileNotFoundError, match="run_ingestion"):
        load_game_contexts(tmp_path / "nope")


def test_stdout_publisher_emits_ndjson(capsys, ctx):
    pub = StdoutPublisher()
    for event in ev.iter_game_events(ctx, _timestamps()):
        pub.publish(event, ordering_key=event["game_id"])
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 22
    assert json.loads(lines[0])["event_type"] == "game_start"
