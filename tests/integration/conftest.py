"""
Integration Test Configuration — Shared Fixtures
=================================================
Provides a local SparkSession backed by Delta Lake for end-to-end
pipeline tests.  Runs without a Databricks cluster using the
open-source delta-spark library.

Run with:
    pytest tests/integration/ -v

Or against a live cluster via Databricks Connect:
    DATABRICKS_HOST=... DATABRICKS_TOKEN=... pytest tests/integration/ -v
"""

from __future__ import annotations

import os
import pytest
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Session-scoped SparkSession — created once per pytest run
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Return a local SparkSession with Delta Lake extensions enabled.

    If DATABRICKS_HOST and DATABRICKS_TOKEN are set, the session is
    configured for Databricks Connect (remote cluster execution).
    Otherwise a fully local session is used (no cluster required).
    """
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        # Databricks Connect — runs Spark on the remote cluster
        from databricks.connect import DatabricksSession  # type: ignore[import]
        return DatabricksSession.builder.getOrCreate()

    return (
        SparkSession.builder
        .master("local[2]")
        .appName("lakehouse-integration-tests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.preview.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")   # small for local tests
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Function-scoped temp directory for Delta table paths
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_delta_path(tmp_path):
    """Return a fresh temp directory for Delta table writes in each test."""
    return str(tmp_path)


# ---------------------------------------------------------------------------
# Test environment name
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def env() -> str:
    return os.environ.get("TEST_ENV", "dev")
