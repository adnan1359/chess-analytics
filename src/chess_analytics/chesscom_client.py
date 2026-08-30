"""Thin, well-behaved client for the Chess.com public data API (`/pub`).

Design notes (why this isn't just `requests.get`):

* Chess.com sits behind Cloudflare and returns **403** to requests without a
  descriptive ``User-Agent``. We send one that includes a contact address, as
  the maintainers ask.
* There is no published rate limit, but bursty/parallel calls get **429**. We
  therefore (a) keep a single serial ``Session`` with a minimum inter-request
  gap and (b) retry with exponential backoff, honouring ``Retry-After``.
* A missing player/archive returns **404** — that's data, not an error, so
  those surface as ``None`` rather than raising.

Only GETs, and only against the read-only public API. No auth involved.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Iterator

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import load_config
from .logging_setup import get_logger

log = get_logger(__name__)

_TRUST_STORE_INJECTED = False


def _maybe_use_os_trust_store() -> None:
    """Route TLS validation through the OS trust store (idempotent).

    Needed only behind a TLS-inspecting corporate proxy whose root CA lives in
    the OS store but not in certifi. A no-op if ``truststore`` isn't installed.
    """
    global _TRUST_STORE_INJECTED
    if _TRUST_STORE_INJECTED:
        return
    try:
        import truststore

        truststore.inject_into_ssl()
        _TRUST_STORE_INJECTED = True
        log.debug("OS trust store injected for TLS validation")
    except ImportError:
        log.warning("use_os_trust_store=true but 'truststore' not installed; "
                    "using certifi. `pip install truststore` if behind a proxy.")


class ChessComError(RuntimeError):
    """Non-retryable API error (e.g. exhausted retries, unexpected status)."""


class _RetryableStatus(Exception):
    """Internal signal that a status code is worth retrying (429/5xx)."""


class ChessComClient:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or load_config())["api"]
        if cfg.get("use_os_trust_store"):
            _maybe_use_os_trust_store()
        self.base_url: str = cfg["base_url"].rstrip("/")
        self.timeout: int = cfg["timeout_sec"]
        self.min_interval: float = cfg["min_interval_sec"]
        self.max_retries: int = cfg["max_retries"]

        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": cfg["user_agent"], "Accept": "application/json"}
        )
        # Serialize requests + enforce the minimum gap across threads.
        self._lock = threading.Lock()
        self._last_request_ts = 0.0

    # ------------------------------------------------------------------ #
    # Low-level GET with pacing + retry
    # ------------------------------------------------------------------ #
    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        wait = self.min_interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.monotonic()

    def _get(self, path: str) -> dict[str, Any] | None:
        """GET ``{base}/{path}``. Returns parsed JSON, or ``None`` on 404."""
        url = f"{self.base_url}/{path.lstrip('/')}"

        @retry(
            retry=retry_if_exception_type((_RetryableStatus, requests.RequestException)),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            stop=stop_after_attempt(self.max_retries),
            reraise=True,
        )
        def _do() -> dict[str, Any] | None:
            with self._lock:
                self._throttle()
                resp = self._session.get(url, timeout=self.timeout)

            if resp.status_code == 404:
                log.info("404 (not found): %s", url)
                return None
            if resp.status_code == 429 or resp.status_code >= 500:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(min(float(retry_after), 60))
                    except ValueError:
                        pass
                raise _RetryableStatus(f"{resp.status_code} for {url}")
            if resp.status_code != 200:
                raise ChessComError(f"{resp.status_code} for {url}: {resp.text[:200]}")
            return resp.json()

        try:
            return _do()
        except _RetryableStatus as exc:
            raise ChessComError(f"exhausted retries: {exc}") from exc

    # ------------------------------------------------------------------ #
    # Public endpoints
    # ------------------------------------------------------------------ #
    def get_titled_players(self, title: str) -> list[str]:
        """Usernames for a title, e.g. ``GM`` / ``IM`` / ``WGM``."""
        data = self._get(f"titled/{title.upper()}")
        return data.get("players", []) if data else []

    def get_leaderboards(self) -> dict[str, Any]:
        """Top players per format (daily/rapid/blitz/bullet, etc.).

        A single cheap call that returns genuinely strong, active players —
        the right seed for archive pulls, versus slicing a huge alphabetical
        titled list.
        """
        return self._get("leaderboards") or {}

    def get_player_profile(self, username: str) -> dict[str, Any] | None:
        return self._get(f"player/{username.lower()}")

    def get_player_stats(self, username: str) -> dict[str, Any] | None:
        return self._get(f"player/{username.lower()}/stats")

    def list_archive_urls(self, username: str) -> list[str]:
        """Monthly archive URLs, oldest first, e.g. .../games/2026/08."""
        data = self._get(f"player/{username.lower()}/games/archives")
        return data.get("archives", []) if data else []

    def get_month_games(self, username: str, year: int, month: int) -> list[dict[str, Any]]:
        data = self._get(f"player/{username.lower()}/games/{year:04d}/{month:02d}")
        return data.get("games", []) if data else []

    def iter_recent_months(
        self, username: str, months: int
    ) -> Iterator[tuple[int, int, list[dict[str, Any]]]]:
        """Yield ``(year, month, games)`` for the most recent ``months`` archives.

        Uses the archives index so we never guess at months that don't exist.
        """
        urls = self.list_archive_urls(username)[-months:]
        for url in urls:
            year, month = int(url[-7:-3]), int(url[-2:])
            yield year, month, self.get_month_games(username, year, month)
