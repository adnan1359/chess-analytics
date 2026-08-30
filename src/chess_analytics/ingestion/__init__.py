"""Bronze-layer ingestion: pull raw Chess.com data and land it verbatim.

Principle: the landing zone is **immutable and unmodelled**. We keep the API
payload exactly as returned and only *add* lineage fields (prefixed ``_``) so
downstream Silver transforms can trace every row back to when/where it came
from. No cleaning, casting, or filtering happens here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def stamp_lineage(record: dict[str, Any], source_endpoint: str) -> dict[str, Any]:
    """Return a shallow copy with ingestion lineage fields added."""
    enriched = dict(record)
    enriched["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    enriched["_source_endpoint"] = source_endpoint
    return enriched
