"""Cloud Run service wrapping the game-event simulator.

Deployed as a **Cloud Run job/service** invoked on demand or by Cloud Scheduler.
Games are read from GCS (the same landing zone the batch path uses) and events
are published to Pub/Sub.

Endpoints:
    GET  /health   liveness probe
    POST /replay   start a replay; body: {"limit": 20, "speed": 10, "seed": 1}

Why a tiny WSGI app instead of Flask/FastAPI: the service has one real endpoint,
and Cloud Run only needs something that speaks WSGI on $PORT. Avoiding a web
framework keeps the container small and the dependency surface honest.

Concurrency note: a replay is long-running and holds a publisher, so the service
is designed for ``--max-instances 1 --concurrency 1``. Two overlapping replays
would double-publish the same game ids; the in-flight guard below rejects that
rather than silently corrupting the stream.
"""

from __future__ import annotations

import json
import os
import threading
import traceback
from pathlib import Path
from typing import Any, Callable, Iterable
from wsgiref.simple_server import make_server

from chess_analytics.config import load_config
from chess_analytics.logging_setup import get_logger
from chess_analytics.streaming.publisher import get_publisher
from chess_analytics.streaming.simulator import Simulator, SimulatorConfig, load_game_contexts

log = get_logger("simulator-service")

# One replay at a time — see the concurrency note above.
_replay_lock = threading.Lock()


def _json_response(start_response: Callable, status: str, payload: dict[str, Any]) -> Iterable[bytes]:
    body = json.dumps(payload).encode("utf-8")
    start_response(status, [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(body))),
    ])
    return [body]


def _games_root(cfg: dict[str, Any]) -> str:
    """Where to read games from.

    On Cloud Run this is a GCS path; the simulator's reader works on local paths,
    so GCS is surfaced via Cloud Storage FUSE (mounted at /gcs) when available.
    Falling back to the local landing zone keeps the container runnable locally.
    """
    mounted = Path("/gcs") / cfg["storage"]["gcs_bucket"] / cfg["storage"]["raw_prefix"] / "games"
    if mounted.exists():
        return str(mounted)
    return str(Path(cfg["storage"]["local_root"]) / cfg["storage"]["raw_prefix"] / "games")


def _run_replay(body: dict[str, Any]) -> dict[str, Any]:
    cfg = load_config()
    sim_config = SimulatorConfig.from_config(cfg)
    if "limit" in body:
        sim_config.max_games = int(body["limit"])
    if "speed" in body:
        sim_config.speed_multiplier = float(body["speed"])
    if "seed" in body:
        sim_config.seed = int(body["seed"])
    if "concurrent" in body:
        sim_config.concurrent_games = int(body["concurrent"])

    root = body.get("games_root") or _games_root(cfg)
    contexts = load_game_contexts(root, sim_config.max_games)
    if not contexts:
        raise FileNotFoundError(f"no replayable games under {root}")

    publisher = get_publisher(cfg)
    with publisher:
        return Simulator(publisher, sim_config).run(contexts)


def app(environ: dict[str, Any], start_response: Callable) -> Iterable[bytes]:
    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET")

    if path == "/health":
        return _json_response(start_response, "200 OK", {"status": "ok"})

    if path == "/replay" and method == "POST":
        if not _replay_lock.acquire(blocking=False):
            # 409, not 500: the caller asked for something legitimate at the
            # wrong time, and retrying later will work.
            return _json_response(
                start_response, "409 Conflict",
                {"error": "a replay is already running"},
            )
        try:
            length = int(environ.get("CONTENT_LENGTH") or 0)
            raw = environ["wsgi.input"].read(length) if length else b"{}"
            body = json.loads(raw or b"{}")
            summary = _run_replay(body)
            log.info("replay complete: %s", summary)
            return _json_response(start_response, "200 OK", summary)
        except Exception as exc:                                    # noqa: BLE001
            log.error("replay failed: %s\n%s", exc, traceback.format_exc())
            return _json_response(
                start_response, "500 Internal Server Error",
                {"error": type(exc).__name__, "message": str(exc)},
            )
        finally:
            _replay_lock.release()

    return _json_response(start_response, "404 Not Found", {"error": "not found"})


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    log.info("simulator service listening on :%d", port)
    with make_server("", port, app) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
