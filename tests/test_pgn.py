"""Tests for PGN parsing and the scalar derivations.

The fixtures are real-shaped Chess.com PGNs: game 1 has no clock comments,
game 3 has one per move plus ``1...`` numbering for Black. Both must parse.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chess_analytics.transforms import pgn as P

FIXTURES = Path(__file__).parent / "fixtures" / "api"


@pytest.fixture(scope="module")
def games() -> list[dict]:
    return json.loads((FIXTURES / "games_2026_08.json").read_text(encoding="utf-8"))["games"]


# --------------------------------------------------------------------------- #
# Header / movetext splitting
# --------------------------------------------------------------------------- #
def test_headers_parsed(games):
    headers = P.parse_headers(games[0]["pgn"])
    assert headers["ECO"] == "B90"
    assert headers["White"] == "Hikaru"
    assert headers["Result"] == "1-0"
    assert headers["Termination"] == "Hikaru won by checkmate"


def test_date_header_is_not_mistaken_for_a_move_number(games):
    """`[Date "2026.08.15"]` matches a naive `\\d+\\.` move-number regex.

    Splitting headers off before tokenizing is what prevents that, so assert
    the movetext really excludes the header block.
    """
    _, movetext = P.split_pgn(games[0]["pgn"])
    assert "[Date" not in movetext
    assert movetext.startswith("1. e4")


def test_pgn_without_movetext_is_safe():
    assert P.parse_pgn('[Event "x"]\n').moves == []
    assert P.parse_pgn("").moves == []
    assert P.parse_moves("") == []


# --------------------------------------------------------------------------- #
# Move tokenizing
# --------------------------------------------------------------------------- #
def test_moves_without_clocks(games):
    """Game 1: standard PGN, Black's move carries no number."""
    parsed = P.parse_pgn(games[0]["pgn"])
    assert parsed.total_plies == 39          # 20. Qd3 is White's 20th, then 1-0
    assert parsed.total_moves == 20
    assert parsed.moves[0] == P.Move(1, 1, "white", "e4", None)
    assert parsed.moves[1] == P.Move(2, 1, "black", "c5", None)
    assert parsed.moves[-1].san == "Qd3"
    assert parsed.moves[-1].color == "white"
    # The result token must not become a move.
    assert all(m.san not in ("1-0", "0-1", "1/2-1/2") for m in parsed.moves)


def test_moves_with_clocks_and_black_numbering(games):
    """Game 3: clock comments + `1...` numbering for Black."""
    parsed = P.parse_pgn(games[2]["pgn"])
    assert parsed.total_plies == 20
    assert parsed.total_moves == 10
    assert parsed.moves[0] == P.Move(1, 1, "white", "d4", "0:01:00.9")
    assert parsed.moves[1] == P.Move(2, 1, "black", "Nf6", "0:01:00.8")
    # Castling and check annotations survive tokenizing.
    assert parsed.moves[18].san == "O-O-O"
    assert parsed.moves[19].san == "Bxa3+"
    assert parsed.moves[19].clock == "0:00:48.8"


def test_draw_result_token_not_counted_as_move(games):
    parsed = P.parse_pgn(games[1]["pgn"])
    assert parsed.moves[-1].san == "Qe2"
    assert parsed.total_moves == 16


@pytest.mark.parametrize(
    "movetext, expected",
    [
        ("1. e4 c5 2. Nf3", ["e4", "c5", "Nf3"]),
        ("1.e4 c5", ["e4", "c5"]),                       # glued move number
        ("1. e4 $1 c5", ["e4", "c5"]),                    # NAG dropped
        ("1. e4 {comment} c5 1/2-1/2", ["e4", "c5"]),     # comment + result
        ("1. e8=Q# 1-0", ["e8=Q#"]),                      # promotion + mate
    ],
)
def test_tokenizer_edge_cases(movetext, expected):
    assert [m.san for m in P.parse_moves(movetext)] == expected


# --------------------------------------------------------------------------- #
# Scalar derivations
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "white, black, outcome, winner",
    [
        ("win", "checkmated", "1-0", "white"),
        ("resigned", "win", "0-1", "black"),
        ("agreed", "agreed", "1/2-1/2", None),
        ("insufficient", "insufficient", "1/2-1/2", None),
    ],
)
def test_derive_outcome(white, black, outcome, winner):
    assert P.derive_outcome(white, black) == (outcome, winner)


@pytest.mark.parametrize(
    "white, black, expected",
    [
        ("win", "checkmated", "checkmate"),
        ("win", "resigned", "resignation"),
        ("win", "timeout", "timeout"),
        ("resigned", "win", "resignation"),      # reason sits on the loser
        ("agreed", "agreed", "draw_agreed"),
        ("repetition", "repetition", "draw_repetition"),
        ("50move", "50move", "draw_50_move"),
        ("win", "somethingnew", "unknown"),      # unmapped code degrades safely
    ],
)
def test_derive_termination(white, black, expected):
    assert P.derive_termination(white, black) == expected


def test_is_draw_keys_on_outcome():
    """Draw detection must not depend on the termination lookup table."""
    assert P.is_draw("1/2-1/2") is True
    assert P.is_draw("1-0") is False
    assert P.is_draw("0-1") is False


def test_is_draw_termination_is_the_mapping_membership_check():
    assert P.is_draw_termination("draw_agreed") is True
    assert P.is_draw_termination("draw_50_move") is True
    assert P.is_draw_termination("checkmate") is False
    assert P.is_draw_termination("unknown") is False


def test_unmapped_draw_reason_still_reads_as_a_draw():
    """The scenario the outcome-keyed design protects: a brand-new draw code.

    termination degrades to 'unknown', but the game is still correctly a draw.
    """
    outcome, winner = P.derive_outcome("somenewdrawcode", "somenewdrawcode")
    assert (outcome, winner) == ("1/2-1/2", None)
    assert P.derive_termination("somenewdrawcode", "somenewdrawcode") == "unknown"
    assert P.is_draw(outcome) is True
    assert P.is_draw_termination("unknown") is False   # correctly flagged for DQ


@pytest.mark.parametrize(
    "raw, base, inc, daily",
    [
        ("600", 600, 0, False),
        ("180+2", 180, 2, False),
        ("60+1", 60, 1, False),
        ("1/259200", 259200, 0, True),   # daily: seconds PER MOVE
        (None, None, 0, False),
    ],
)
def test_parse_time_control(raw, base, inc, daily):
    assert P.parse_time_control(raw) == P.TimeControl(base, inc, daily)


@pytest.mark.parametrize(
    "elo, expected",
    [
        (1850, "u2000"),
        (2000, "2000_2199"),
        (2199, "2000_2199"),
        (2832, "2800_plus"),
        (None, None),
    ],
)
def test_elo_bracket(elo, expected):
    assert P.elo_bracket(elo) == expected


def test_eco_volume():
    assert P.eco_volume("B90") == "Semi-open games (excl. French)"
    assert P.eco_volume("e60") == "Indian defences"   # case-insensitive
    assert P.eco_volume(None) is None


def test_opening_name_from_eco_url():
    url = "https://www.chess.com/openings/Sicilian-Defense-Najdorf-Variation"
    assert P.opening_name_from_eco_url(url) == "Sicilian Defense Najdorf Variation"
    assert P.opening_name_from_eco_url(None) is None


def test_game_id_from_url(games):
    assert P.game_id_from_url(games[0]["url"]) == "98234571"
    assert P.game_id_from_url("https://www.chess.com/game/live/123/") == "123"
    assert P.game_id_from_url("https://www.chess.com/game/live/abc") is None
    assert P.game_id_from_url(None) is None
