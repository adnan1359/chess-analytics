"""Replay engine: turns archived games into a live-looking event stream.

Design points that matter:

* **Interleaved, not sequential.** Several games are "in progress" at once and
  their events interleave, because that is what real traffic looks like. A
  simulator that finishes game 1 before starting game 2 produces a stream whose
  windowed aggregates are trivially clean and therefore prove nothing.
* **Injectable clock and sleeper.** Time and sleeping are parameters, so tests
  drive thousands of events deterministically in milliseconds. Nothing here
  calls ``time.sleep`` directly.
* **Deterministic with a seed.** Think-time jitter uses a seeded ``Random``, so a
  demo can be reproduced exactly.
"""

from __future__ import annotations

import heapq
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from ..config import load_config
from ..logging_setup import get_logger
from . import events as ev
from .publisher import Publisher, get_publisher

log = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SimulatorConfig:
    concurrent_games: int = 5
    move_delay_min_sec: float = 0.5
    move_delay_max_sec: float = 3.0
    speed_multiplier: float = 1.0
    max_games: int = 20
    seed: int | None = None

    @classmethod
    def from_config(cls, config: dict[str, Any] | None = None) -> "SimulatorConfig":
        cfg = (config or load_config())["streaming"]["simulator"]
        return cls(
            concurrent_games=cfg["concurrent_games"],
            move_delay_min_sec=cfg["move_delay_min_sec"],
            move_delay_max_sec=cfg["move_delay_max_sec"],
            speed_multiplier=cfg["speed_multiplier"],
            max_games=cfg["max_games"],
        )


def iter_archive_games(root: str | Path) -> Iterator[dict[str, Any]]:
    """Yield raw game records from the NDJSON landing zone.

    Reads ``raw/games/**/*.json`` written by the Sprint 1 ingestion, so the
    streaming path replays the same real data the batch path models.
    """
    import json

    games_root = Path(root)
    if not games_root.exists():
        raise FileNotFoundError(
            f"{games_root} not found — run scripts/run_ingestion.py first, "
            "or point --games-root at a landed archive directory."
        )
    for path in sorted(games_root.rglob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                yield json.loads(line)


def load_game_contexts(root: str | Path, limit: int = 0) -> list[ev.GameContext]:
    """Build replayable contexts from landed archives, skipping unusable games."""
    contexts: list[ev.GameContext] = []
    skipped = 0
    for game in iter_archive_games(root):
        ctx = ev.game_context_from_archive(game)
        if ctx is None:
            skipped += 1
            continue
        contexts.append(ctx)
        if limit and len(contexts) >= limit:
            break
    log.info("loaded %d replayable games (skipped %d unusable)", len(contexts), skipped)
    return contexts


class GameReplay:
    """Tracks one game's position in its own event plan."""

    def __init__(self, ctx: ev.GameContext) -> None:
        self.ctx = ctx
        self._plans = ev.plan_game_events(ctx)
        self._index = 0

    @property
    def finished(self) -> bool:
        return self._index >= len(self._plans)

    def next_event(self, event_ts: datetime) -> dict[str, Any] | None:
        """Materialise the next event at ``event_ts``, or None when exhausted."""
        if self.finished:
            return None
        plan = self._plans[self._index]
        self._index += 1
        return ev.build_event(self.ctx, plan, event_ts)


class Simulator:
    """Interleaves several :class:`GameReplay` streams into one paced feed."""

    def __init__(
        self,
        publisher: Publisher,
        config: SimulatorConfig | None = None,
        clock: Callable[[], datetime] = _utcnow,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.publisher = publisher
        self.config = config or SimulatorConfig()
        self.clock = clock
        self.sleeper = sleeper
        self._random = random.Random(self.config.seed)
        self.published = 0
        self.games_completed = 0

    def _delay(self) -> float:
        """Randomised think time, compressed by the speed multiplier."""
        base = self._random.uniform(
            self.config.move_delay_min_sec, self.config.move_delay_max_sec
        )
        return base / max(self.config.speed_multiplier, 1e-9)

    def run(self, contexts: Iterable[ev.GameContext]) -> dict[str, int]:
        """Replay ``contexts``, keeping up to ``concurrent_games`` in flight.

        A min-heap keyed on each game's next due time is the natural structure
        here: it yields events in true chronological order across all in-flight
        games, which is exactly the interleaving a real feed produces.
        """
        queue = list(contexts)
        if self.config.max_games:
            queue = queue[: self.config.max_games]
        pending = iter(queue)

        now = self.clock()
        # heap entries: (due_at, tiebreak, GameReplay)
        heap: list[tuple[datetime, int, GameReplay]] = []
        counter = 0

        def admit(count: int) -> None:
            nonlocal counter
            for _ in range(count):
                ctx = next(pending, None)
                if ctx is None:
                    return
                counter += 1
                # Stagger starts so games don't all open on the same tick.
                due = now + timedelta(seconds=self._random.uniform(0, self._delay()))
                heapq.heappush(heap, (due, counter, GameReplay(ctx)))

        admit(self.config.concurrent_games)

        while heap:
            due_at, tiebreak, replay = heapq.heappop(heap)

            # Advance wall-clock to the event's due time. With a test sleeper
            # this is instant; in production it paces the feed.
            wait = (due_at - self.clock()).total_seconds()
            if wait > 0:
                self.sleeper(wait)

            event_ts = self.clock()
            event = replay.next_event(event_ts)

            if event is None:
                self.games_completed += 1
                admit(1)            # keep the in-flight count topped up
                continue

            self.publisher.publish(event, ordering_key=event["game_id"])
            self.published += 1

            next_due = event_ts + timedelta(seconds=self._delay())
            heapq.heappush(heap, (next_due, tiebreak, replay))

        self.publisher.flush()
        summary = {
            "games": len(queue),
            "games_completed": self.games_completed,
            "events_published": self.published,
        }
        log.info("simulation complete: %s", summary)
        return summary


def run_simulation(
    games_root: str | Path,
    config: dict[str, Any] | None = None,
    limit: int = 0,
    publisher: Publisher | None = None,
) -> dict[str, int]:
    """Convenience entrypoint used by the CLI and the Cloud Run service."""
    cfg = config or load_config()
    sim_config = SimulatorConfig.from_config(cfg)
    contexts = load_game_contexts(games_root, limit or sim_config.max_games)
    pub = publisher or get_publisher(cfg)
    with pub:
        return Simulator(pub, sim_config).run(contexts)
