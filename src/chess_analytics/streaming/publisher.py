"""Event publishers: Pub/Sub, in-memory (tests), stdout (local demo).

Same shape as ``storage.py`` — one interface, swappable backends — so the
simulator never knows whether it is talking to Pub/Sub or a list. That is what
lets the replay engine be tested exhaustively with no GCP project.

Ordering: moves within a game are only meaningful in sequence, so Pub/Sub
messages are published with ``ordering_key = game_id``. Pub/Sub then guarantees
per-key ordered delivery, which keeps ply 5 from being processed before ply 4
while still allowing different games to be handled in parallel.
"""

from __future__ import annotations

import json
from typing import Any

from ..config import load_config
from ..logging_setup import get_logger

log = get_logger(__name__)


class Publisher:
    """Base interface."""

    def publish(self, event: dict[str, Any], ordering_key: str | None = None) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        """Block until every queued message is acknowledged by the broker."""

    def __enter__(self) -> "Publisher":
        return self

    def __exit__(self, *exc: object) -> None:
        self.flush()


class MemoryPublisher(Publisher):
    """Collects events in a list. Used by tests and by the Beam DirectRunner demo."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.ordering_keys: list[str | None] = []

    def publish(self, event: dict[str, Any], ordering_key: str | None = None) -> None:
        self.events.append(event)
        self.ordering_keys.append(ordering_key)

    def __len__(self) -> int:
        return len(self.events)


class StdoutPublisher(Publisher):
    """Prints NDJSON. Lets the simulator be watched end-to-end with no GCP."""

    def __init__(self) -> None:
        self.count = 0

    def publish(self, event: dict[str, Any], ordering_key: str | None = None) -> None:
        print(json.dumps(event, separators=(",", ":"), default=str), flush=True)
        self.count += 1


class PubSubPublisher(Publisher):
    """Real Pub/Sub. Imports the client lazily so local runs stay GCP-free."""

    def __init__(self, project_id: str, topic: str, enable_ordering: bool = True) -> None:
        from google.cloud import pubsub_v1

        # Batching amortises the per-request overhead of high-rate publishing.
        # With ordering enabled the client preserves per-key order across batches.
        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=enable_ordering
        )
        self._client = pubsub_v1.PublisherClient(publisher_options=publisher_options)
        self._topic_path = self._client.topic_path(project_id, topic)
        self._enable_ordering = enable_ordering
        self._futures: list[Any] = []
        log.info("publishing to %s (ordering=%s)", self._topic_path, enable_ordering)

    def publish(self, event: dict[str, Any], ordering_key: str | None = None) -> None:
        payload = json.dumps(event, separators=(",", ":"), default=str).encode("utf-8")
        kwargs: dict[str, Any] = {}
        if self._enable_ordering and ordering_key:
            kwargs["ordering_key"] = ordering_key
        # event_id and game_id are duplicated into message attributes so a
        # consumer (or a Pub/Sub filter) can route without parsing the payload.
        future = self._client.publish(
            self._topic_path,
            payload,
            event_id=str(event.get("event_id", "")),
            game_id=str(event.get("game_id", "")),
            event_type=str(event.get("event_type", "")),
            **kwargs,
        )
        self._futures.append(future)

    def flush(self) -> None:
        """Wait for all in-flight publishes and surface any failure.

        Without this, a process that exits immediately after publishing can drop
        messages still sitting in the client's batch buffer.
        """
        failures = 0
        for future in self._futures:
            try:
                future.result(timeout=60)
            except Exception as exc:                       # noqa: BLE001
                failures += 1
                log.error("publish failed: %s", exc)
        if failures:
            raise RuntimeError(f"{failures}/{len(self._futures)} publishes failed")
        log.info("flushed %d messages", len(self._futures))
        self._futures.clear()


def get_publisher(config: dict[str, Any] | None = None) -> Publisher:
    cfg = config or load_config()
    backend = cfg["streaming"]["publisher"].lower()
    if backend == "memory":
        return MemoryPublisher()
    if backend == "stdout":
        return StdoutPublisher()
    if backend == "pubsub":
        return PubSubPublisher(
            project_id=cfg["project"]["gcp_project_id"],
            topic=cfg["streaming"]["topic"],
            enable_ordering=cfg["streaming"]["enable_ordering"],
        )
    raise ValueError(f"unknown publisher backend: {backend!r}")
