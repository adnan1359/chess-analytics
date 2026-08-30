"""Landing-zone writer: one interface, two backends (local disk / GCS).

Sprint 1 runs entirely on ``local`` so there's zero GCP cost or setup. Flip
``storage.backend`` to ``gcs`` (or ``CHESS_STORAGE__BACKEND=gcs``) later and the
ingestion code is unchanged — only the object created here differs.

We land **NDJSON** (newline-delimited JSON), one record per line, because that's
what BigQuery external tables and ``bq load`` expect for JSON. A single JSON
array would force whole-file reads; NDJSON streams and splits cleanly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .config import load_config
from .logging_setup import get_logger

log = get_logger(__name__)


class Storage:
    """Base interface. Keys are POSIX-style paths under the raw prefix."""

    def write_ndjson(self, key: str, records: Iterable[dict[str, Any]]) -> int:
        raise NotImplementedError

    def write_json(self, key: str, record: dict[str, Any]) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError


class LocalStorage(Storage):
    def __init__(self, root: str, raw_prefix: str) -> None:
        self.root = Path(root) / raw_prefix
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def write_ndjson(self, key: str, records: Iterable[dict[str, Any]]) -> int:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
                fh.write("\n")
                count += 1
        log.info("wrote %d records -> %s", count, path)
        return count

    def write_json(self, key: str, record: dict[str, Any]) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(record, fh, ensure_ascii=False)

    def exists(self, key: str) -> bool:
        return self._path(key).exists()


class GCSStorage(Storage):
    """GCS-backed. Imports google-cloud-storage lazily so Sprint 1 stays dep-light."""

    def __init__(self, bucket: str, raw_prefix: str) -> None:
        from google.cloud import storage as gcs  # lazy

        self._client = gcs.Client()
        self._bucket = self._client.bucket(bucket)
        self.raw_prefix = raw_prefix

    def _blob(self, key: str):
        return self._bucket.blob(f"{self.raw_prefix}/{key}")

    def write_ndjson(self, key: str, records: Iterable[dict[str, Any]]) -> int:
        lines, count = [], 0
        for rec in records:
            lines.append(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
            count += 1
        self._blob(key).upload_from_string(
            "\n".join(lines) + ("\n" if lines else ""),
            content_type="application/x-ndjson",
        )
        log.info("wrote %d records -> gs://%s/%s/%s", count, self._bucket.name, self.raw_prefix, key)
        return count

    def write_json(self, key: str, record: dict[str, Any]) -> None:
        self._blob(key).upload_from_string(
            json.dumps(record, ensure_ascii=False), content_type="application/json"
        )

    def exists(self, key: str) -> bool:
        return self._blob(key).exists()


def get_storage(config: dict[str, Any] | None = None) -> Storage:
    cfg = (config or load_config())["storage"]
    backend = cfg["backend"].lower()
    if backend == "local":
        return LocalStorage(cfg["local_root"], cfg["raw_prefix"])
    if backend == "gcs":
        return GCSStorage(cfg["gcs_bucket"], cfg["raw_prefix"])
    raise ValueError(f"unknown storage backend: {backend!r}")
