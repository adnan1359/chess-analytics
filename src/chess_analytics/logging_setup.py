"""Structured-ish logging. JSON in the cloud, human-readable locally.

Cloud Logging / Dataflow parse JSON lines into structured fields; a plain
formatter is nicer at a local terminal. Toggle with CHESS_LOG_JSON=true.
"""

from __future__ import annotations

import json
import logging
import os
import sys


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # already configured (e.g. re-import)
        return logger

    level = os.environ.get("CHESS_LOG_LEVEL", "INFO").upper()
    # stderr, not stdout: the StdoutPublisher emits the event feed as NDJSON on
    # stdout, so log lines there would corrupt it and break piping. Cloud
    # Logging and Dataflow capture both streams, so nothing is lost in the cloud.
    handler = logging.StreamHandler(sys.stderr)
    if os.environ.get("CHESS_LOG_JSON", "").lower() in ("1", "true", "yes"):
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-7s %(name)s | %(message)s")
        )
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
