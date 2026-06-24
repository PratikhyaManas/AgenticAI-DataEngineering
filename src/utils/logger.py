"""
src/utils/logger.py
===================
Shared structured logger for all pipeline modules.

Usage:
    from src.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("Job started", extra={"env": "dev", "rows": 1000})

In Databricks notebooks, messages appear in the cluster driver logs.
In local tests, messages go to stdout via StreamHandler.

The logger name is always the calling module's __name__, so log lines
carry the full module path (e.g. src.cleaning.bronze_to_silver).
"""

from __future__ import annotations

import logging
import os
import sys

# ---------------------------------------------------------------------------
# Module-level format — include timestamp, level, and logger name
# ---------------------------------------------------------------------------
_LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Respect LOG_LEVEL env var (default INFO)
_DEFAULT_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Root handler is created once per process
_handler_installed = False


def _install_root_handler() -> None:
    global _handler_installed  # noqa: PLW0603
    if _handler_installed:
        return
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        root.addHandler(handler)
    root.setLevel(_DEFAULT_LEVEL)
    _handler_installed = True


def get_logger(name: str) -> logging.Logger:
    """
    Return a standard Python logger for the given module name.

    Args:
        name: Typically ``__name__`` from the calling module.

    Returns:
        A configured :class:`logging.Logger` instance.
    """
    _install_root_handler()
    return logging.getLogger(name)
