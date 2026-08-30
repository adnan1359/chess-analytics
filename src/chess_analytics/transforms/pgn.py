"""PGN parsing + the scalar derivations that mirror the Silver SQL.

Real Chess.com PGNs are standard PGN with two quirks worth knowing:

1. Every move carries a clock comment: ``1. e4 {[%clk 0:09:57.5]}``.
2. When those comments are present, Chess.com repeats the move number before
   Black's move (``1... c5``). Without them, Black's move has no number at all.

Both shapes are handled. Everything here is pure-functional and dependency-free
so it runs identically in a test, in Dataflow, and in the Cloud Run simulator.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, NamedTuple

import yaml

_MAPPINGS_PATH = Path(__file__).resolve().parents[3] / "config" / "mappings.yaml"

# ``[Key "value"]`` — one per line in the header block.
_HEADER_RE = re.compile(r'^\[(\w+)\s+"(.*)"\]\s*$', re.MULTILINE)
# Header block and movetext are separated by a blank line.
_SPLIT_RE = re.compile(r"\n\s*\n", re.MULTILINE)
_COMMENT_RE = re.compile(r"\{[^}]*\}")
_CLOCK_RE = re.compile(r"\[%clk\s+([^\]]+)\]")
# Leading move number on a token: "12." or "12..." (also handles glued "12.e4").
_MOVENUM_PREFIX_RE = re.compile(r"^\d+\.(?:\.\.)?")
_RESULT_TOKENS = frozenset({"1-0", "0-1", "1/2-1/2", "*"})


@lru_cache(maxsize=1)
def _mappings() -> dict[str, Any]:
    with _MAPPINGS_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


class Move(NamedTuple):
    ply: int              # 1-based half-move index
    move_number: int      # 1-based full-move number (ply 1 and 2 -> move 1)
    color: str            # 'white' | 'black'
    san: str              # e.g. 'Nf3', 'O-O', 'exd5+', 'e8=Q#'
    clock: str | None     # remaining clock for the mover, e.g. '0:09:57.5'


@dataclass(frozen=True)
class ParsedPgn:
    headers: dict[str, str]
    moves: list[Move]

    @property
    def total_plies(self) -> int:
        return len(self.moves)

    @property
    def total_moves(self) -> int:
        """Full moves (a white+black pair counts as one)."""
        return (len(self.moves) + 1) // 2


def split_pgn(pgn: str) -> tuple[str, str]:
    """Split a PGN into ``(header_block, movetext)``.

    Splitting on the blank line matters: the header block contains
    ``[Date "2026.08.15"]``, whose digit-dot pattern would otherwise be
    mistaken for a move number.
    """
    if not pgn:
        return "", ""
    parts = _SPLIT_RE.split(pgn.strip(), maxsplit=1)
    return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], "")


def parse_headers(pgn: str) -> dict[str, str]:
    header_block, _ = split_pgn(pgn)
    return {m.group(1): m.group(2) for m in _HEADER_RE.finditer(header_block)}


def parse_moves(movetext: str) -> list[Move]:
    """Tokenize movetext into an ordered list of :class:`Move`.

    Clocks are collected in document order and paired positionally with moves:
    Chess.com emits exactly one ``[%clk ...]`` per move, so index *i* of the
    clock list belongs to move *i*. If clocks are absent (or partial), the
    remaining moves simply get ``clock=None``.
    """
    if not movetext:
        return []

    clocks = _CLOCK_RE.findall(movetext)
    body = _COMMENT_RE.sub(" ", movetext)

    sans: list[str] = []
    for raw in body.split():
        if raw in _RESULT_TOKENS or raw.startswith("$"):
            continue
        token = _MOVENUM_PREFIX_RE.sub("", raw)
        if not token or token in _RESULT_TOKENS:
            continue  # token was a bare move number, or a glued result
        sans.append(token)

    return [
        Move(
            ply=i + 1,
            move_number=i // 2 + 1,
            color="white" if i % 2 == 0 else "black",
            san=san,
            clock=clocks[i] if i < len(clocks) else None,
        )
        for i, san in enumerate(sans)
    ]


def parse_pgn(pgn: str) -> ParsedPgn:
    header_block, movetext = split_pgn(pgn)
    return ParsedPgn(
        headers={m.group(1): m.group(2) for m in _HEADER_RE.finditer(header_block)},
        moves=parse_moves(movetext),
    )


# --------------------------------------------------------------------------- #
# Scalar derivations — mirrored by CASE expressions in sql/silver/clean_games.sql
# --------------------------------------------------------------------------- #
def derive_outcome(white_result: str, black_result: str) -> tuple[str, str | None]:
    """Map per-side result codes to ``(outcome, winner_color)``.

    ``outcome`` is ``'1-0'`` / ``'0-1'`` / ``'1/2-1/2'``; ``winner_color`` is
    ``None`` for a draw.
    """
    if white_result == "win":
        return "1-0", "white"
    if black_result == "win":
        return "0-1", "black"
    return "1/2-1/2", None


def derive_termination(white_result: str, black_result: str) -> str:
    """Category for *why* the game ended, from the losing/draw reason code.

    The reason always lives on the side that did not win; for a draw both sides
    carry the same reason, so taking White's is correct in all three cases.
    """
    reason = black_result if white_result == "win" else white_result
    return _mappings()["result_reason_to_termination"].get(reason, "unknown")


def is_draw(outcome: str) -> bool:
    """Authoritative draw test — mirrors ``is_draw`` in the Silver SQL.

    Deliberately keyed on ``outcome``, not ``termination``: outcome needs no
    lookup table, so a draw reason the API adds tomorrow is still classified
    correctly instead of falling to 'unknown' and reading as decisive.
    """
    return outcome == "1/2-1/2"


def is_draw_termination(termination: str) -> bool:
    """Whether a termination category is one of the known draw reasons.

    Not a substitute for :func:`is_draw` — this is the check the DQ layer uses
    to spot draw reasons missing from ``config/mappings.yaml``.
    """
    return termination in set(_mappings()["draw_terminations"])


class TimeControl(NamedTuple):
    base_sec: int | None
    increment_sec: int
    is_daily: bool


def parse_time_control(time_control: str | None) -> TimeControl:
    """Parse Chess.com ``time_control`` into components.

    Formats: ``"600"`` (10 min), ``"180+2"`` (3 min + 2 s), ``"1/259200"``
    (daily — seconds *per move*, so it is not comparable to a base clock).
    """
    if not time_control:
        return TimeControl(None, 0, False)
    daily = time_control.startswith("1/")
    base_match = re.match(r"^(?:1/)?(\d+)", time_control)
    inc_match = re.search(r"\+(\d+)$", time_control)
    return TimeControl(
        base_sec=int(base_match.group(1)) if base_match else None,
        increment_sec=int(inc_match.group(1)) if inc_match else 0,
        is_daily=daily,
    )


def elo_bracket(elo: int | None) -> str | None:
    if elo is None:
        return None
    for bracket in _mappings()["elo_brackets"]:
        if bracket["min"] <= elo <= bracket["max"]:
            return bracket["label"]
    return None


def eco_volume(eco_code: str | None) -> str | None:
    """Map an ECO code (e.g. ``B90``) to its Encyclopaedia volume name."""
    if not eco_code:
        return None
    return _mappings()["eco_volumes"].get(eco_code[0].upper())


def opening_name_from_eco_url(eco_url: str | None) -> str | None:
    """Turn an ECOUrl into a readable opening name.

    ``.../openings/Sicilian-Defense-Najdorf-Variation`` -> ``Sicilian Defense
    Najdorf Variation``.
    """
    if not eco_url:
        return None
    slug = eco_url.rstrip("/").rsplit("/", 1)[-1]
    return slug.replace("-", " ").strip() or None


def game_id_from_url(url: str | None) -> str | None:
    """Extract the numeric game id from a Chess.com game URL.

    This is the natural key used to dedupe: a game between two tracked players
    appears in *both* players' monthly archives.
    """
    if not url:
        return None
    match = re.search(r"/(\d+)/?$", url)
    return match.group(1) if match else None
