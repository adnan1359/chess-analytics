"""Config loading: YAML on disk, overridable per-key by environment variables.

The env-override convention keeps one config file working across local runs,
Airflow tasks, and Cloud Run containers without editing the file per env:

    CHESS_API__MIN_INTERVAL_SEC=0.5      -> config["api"]["min_interval_sec"]
    CHESS_STORAGE__BACKEND=gcs           -> config["storage"]["backend"]

Nested keys are joined with a double underscore. Values are coerced to match
the type already present in the YAML (int/float/bool/list/str).
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

ENV_PREFIX = "CHESS_"
_NESTED_SEP = "__"

# repo_root/config/config.yaml — this file lives at repo_root/src/chess_analytics/
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"


def _coerce(raw: str, template: Any) -> Any:
    """Coerce an env string to the type of the existing config value."""
    if isinstance(template, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(template, int):
        return int(raw)
    if isinstance(template, float):
        return float(raw)
    if isinstance(template, list):
        return [item.strip() for item in raw.split(",") if item.strip()]
    return raw


def _apply_env_overrides(cfg: dict[str, Any]) -> dict[str, Any]:
    for env_key, raw in os.environ.items():
        if not env_key.startswith(ENV_PREFIX):
            continue
        path = env_key[len(ENV_PREFIX):].lower().split(_NESTED_SEP)
        node: Any = cfg
        for part in path[:-1]:
            if not isinstance(node, dict) or part not in node:
                node = None
                break
            node = node[part]
        leaf = path[-1]
        if isinstance(node, dict) and leaf in node:
            node[leaf] = _coerce(raw, node[leaf])
    return cfg


@lru_cache(maxsize=1)
def load_config(path: str | None = None) -> dict[str, Any]:
    """Load and cache config. Pass a path to override the default location."""
    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH
    with config_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    return _apply_env_overrides(cfg)
