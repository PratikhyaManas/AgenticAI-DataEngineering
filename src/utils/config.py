"""
Shared configuration utilities — used by all ingestion jobs.

Extracts the duplicated load_source_config() function that previously
lived in autoloader_ingestion.py, sql_ingestion.py, and
eventhub_streaming.py.  Centralising here avoids drift and enables
a single unit test for config loading.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


_CONFIGS_ROOT = Path(__file__).parents[2] / "configs" / "sources"


@lru_cache(maxsize=64)
def load_source_config(source_id: str, configs_root: str | None = None) -> dict:
    """
    Load and cache a YAML source config by its ``source_id`` key.

    Scans ``configs/sources/`` for a YAML file whose ``source_id`` field
    matches *source_id*.  Results are cached in-process so repeat calls
    (e.g. Autoloader + metadata logger in the same driver) incur zero I/O.

    Args:
        source_id:    Logical source identifier (e.g. ``"azure_sql_sales"``).
        configs_root: Override directory for unit tests; defaults to the
                      canonical ``configs/sources/`` path relative to repo root.

    Raises:
        ValueError: No YAML config with a matching ``source_id`` is found.
    """
    import yaml  # lazy import — not available in all test environments

    root = Path(configs_root) if configs_root else _CONFIGS_ROOT

    for path in sorted(root.glob("*.yaml")):           # sorted → deterministic
        with path.open() as fh:
            cfg: dict = yaml.safe_load(fh)
        if cfg and cfg.get("source_id") == source_id:  # early return once found
            return cfg

    raise ValueError(
        f"No YAML config found for source_id={source_id!r} in {root}"
    )
