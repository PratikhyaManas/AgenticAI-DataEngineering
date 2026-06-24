"""
src/utils/dbutils_shim.py
=========================
Compatibility shim for Databricks ``dbutils``.

In Databricks runtime, ``dbutils`` is injected automatically into the
notebook / job scope.  Outside that environment (local tests, CI, the
Gold API), the module provides a thin fallback that reads secrets from
environment variables and raises clear errors for widgets/fs/notebook
operations so tests fail fast instead of with an ``NameError``.

Usage (in any ingestion or governance module):
    from src.utils.dbutils_shim import get_dbutils
    dbutils = get_dbutils(spark)          # pass spark only when available
    secret = dbutils.secrets.get(scope="my-scope", key="my-key")

The ``spark`` argument is only required if you want to use the real
Databricks ``dbutils`` via ``DBUtils(spark)``.  Pass ``None`` (or omit)
to get the fallback shim.
"""

from __future__ import annotations

import os
from typing import Any


# ---------------------------------------------------------------------------
# Secrets shim
# ---------------------------------------------------------------------------

class _SecretsShim:
    """
    Reads secrets from environment variables when not on Databricks.
    Variable naming: PII_KEY_{SCOPE}_{KEY} (upper-cased, hyphens → underscores).
    """

    def get(self, scope: str, key: str) -> str:
        env_var = f"{scope.upper().replace('-', '_')}_{key.upper().replace('-', '_')}"
        value = os.environ.get(env_var)
        if value is None:
            raise KeyError(
                f"Secret '{scope}/{key}' not found.  "
                f"Set environment variable '{env_var}' for local development."
            )
        return value

    def list(self, scope: str) -> list[dict]:  # noqa: A003
        raise NotImplementedError("dbutils.secrets.list() is not supported locally.")


# ---------------------------------------------------------------------------
# Notebook / widgets shim (raises with a clear message)
# ---------------------------------------------------------------------------

class _WidgetsShim:
    def get(self, name: str) -> str:
        env_var = f"WIDGET_{name.upper()}"
        value = os.environ.get(env_var)
        if value is None:
            raise EnvironmentError(
                f"Widget '{name}' is not available outside Databricks.  "
                f"Set environment variable '{env_var}' for local development."
            )
        return value

    def text(self, name: str, default_value: str = "", label: str = "") -> None:
        pass   # no-op in local mode


# ---------------------------------------------------------------------------
# fs shim (raises with a clear message for operations not needed locally)
# ---------------------------------------------------------------------------

class _FsShim:
    def ls(self, path: str) -> list:
        raise NotImplementedError(f"dbutils.fs.ls('{path}') is not supported locally.")

    def cp(self, from_: str, to: str, recurse: bool = False) -> None:
        raise NotImplementedError("dbutils.fs.cp() is not supported locally.")

    def rm(self, path: str, recurse: bool = False) -> None:
        raise NotImplementedError("dbutils.fs.rm() is not supported locally.")

    def mkdirs(self, path: str) -> None:
        raise NotImplementedError("dbutils.fs.mkdirs() is not supported locally.")


# ---------------------------------------------------------------------------
# Full shim object
# ---------------------------------------------------------------------------

class _DbutilsShim:
    secrets = _SecretsShim()
    widgets = _WidgetsShim()
    fs      = _FsShim()

    def notebook(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("dbutils.notebook is not supported locally.")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_dbutils(spark: Any = None) -> Any:
    """
    Return the real ``dbutils`` if running on Databricks, or a fallback
    shim that reads secrets from environment variables.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession`.  Only used when
               attempting to import the real DBUtils.  May be ``None``
               in local environments.

    Returns:
        Either the real Databricks ``dbutils`` or :class:`_DbutilsShim`.
    """
    # 1. Already injected into the calling scope (notebooks, jobs)
    import builtins
    if hasattr(builtins, "dbutils"):
        return builtins.dbutils  # type: ignore[attr-defined]

    # 2. Available via DBUtils API (Databricks Connect / interactive cluster)
    if spark is not None:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore[import]
            return DBUtils(spark)
        except (ImportError, ModuleNotFoundError):
            pass

    # 3. Fallback shim for local / CI environments
    return _DbutilsShim()
