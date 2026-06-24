"""
OpenLineage Data Lineage Emitter
==================================
Agent: Data Quality & Observability Agent

Emits OpenLineage-compliant events from every PySpark pipeline stage so that
data lineage is captured end-to-end across:
  Ingestion (Bronze) → Cleaning (Silver) → Transformation (Gold) → ML Inference

Events flow to a configurable backend:
  - Marquez (self-hosted, via HTTP transport)
  - Azure Purview (via OpenLineage REST proxy adapter)
  - Console (dev mode — pretty-print to stdout)

OpenLineage spec: https://openlineage.io/spec/

Key concepts used here:
  Job        – a named pipeline stage (e.g. "ingest.autoloader.sales_orders")
  Run        – one execution of a job (UUID, timestamps, run state)
  Dataset    – a named data asset with schema facets (input or output)
  Facets     – structured metadata attached to runs or datasets:
                 schema, dataSource, dataQualityMetrics, sourceCode, stats

Usage — instrument any pipeline function with the @lineage_job decorator:

    from src.lineage.openlineage_emitter import lineage_job, LineageDataset

    @lineage_job(
        job_name="ingest.autoloader.sales_orders",
        inputs=[LineageDataset(namespace="adls", name="landing/sales/orders")],
        outputs=[LineageDataset(namespace="delta", name="bronze.sales_orders")],
    )
    def ingest(...):
        ...

Or use the context manager directly:

    with LineageEmitter(config).run_context(job, inputs, outputs) as run_id:
        do_work()
"""

from __future__ import annotations

import contextlib
import functools
import json
import logging
import os
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class LineageConfig:
    """
    OpenLineage emitter configuration.
    All values can be overridden via environment variables so no secrets
    appear in code.
    """
    transport:   str = "http"       # http | console
    url:         str = ""           # Marquez / Purview proxy base URL
    api_key:     str = ""           # Bearer token for the lineage backend
    namespace:   str = "lakehouse"  # Default OpenLineage namespace
    environment: str = "dev"

    @classmethod
    def from_env(cls) -> "LineageConfig":
        return cls(
            transport=os.environ.get("OPENLINEAGE_TRANSPORT", "console"),
            url=os.environ.get("OPENLINEAGE_URL", ""),
            api_key=os.environ.get("OPENLINEAGE_API_KEY", ""),
            namespace=os.environ.get("OPENLINEAGE_NAMESPACE", "lakehouse"),
            environment=os.environ.get("LAKEHOUSE_ENV", "dev"),
        )


# ---------------------------------------------------------------------------
# Dataset model
# ---------------------------------------------------------------------------

@dataclass
class LineageDataset:
    """Represents one input or output dataset in an OpenLineage event."""
    name:         str
    namespace:    str = ""          # defaults to config.namespace
    schema_fields: list[dict[str, str]] = field(default_factory=list)
    # e.g. [{"name": "order_id", "type": "LONG"}, ...]
    source_uri:   str = ""          # underlying storage URI
    row_count:    int | None = None
    byte_size:    int | None = None

    def to_ol_dataset(self, default_namespace: str) -> dict[str, Any]:
        ns = self.namespace or default_namespace
        ds: dict[str, Any] = {
            "namespace": ns,
            "name":      self.name,
            "facets":    {},
        }

        if self.schema_fields:
            ds["facets"]["schema"] = {
                "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                "fields": self.schema_fields,
            }

        if self.source_uri:
            ds["facets"]["dataSource"] = {
                "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                "name": ns,
                "uri":  self.source_uri,
            }

        if self.row_count is not None or self.byte_size is not None:
            ds["facets"]["stats"] = {
                "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "rowCount": self.row_count,
                "size":     self.byte_size,
            }

        return ds


# ---------------------------------------------------------------------------
# OpenLineage event builder
# ---------------------------------------------------------------------------

def _build_event(
    event_type:  str,           # START | COMPLETE | FAIL | ABORT
    job_name:    str,
    run_id:      str,
    namespace:   str,
    inputs:      list[LineageDataset],
    outputs:     list[LineageDataset],
    run_facets:  dict[str, Any] | None = None,
    error_msg:   str | None = None,
) -> dict[str, Any]:
    """
    Build a spec-compliant OpenLineage RunEvent dict.
    https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent
    """
    now = datetime.now(timezone.utc).isoformat()
    event: dict[str, Any] = {
        "eventType": event_type,
        "eventTime": now,
        "run": {
            "runId": run_id,
            "facets": run_facets or {},
        },
        "job": {
            "namespace": namespace,
            "name":      job_name,
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json",
                    "processingType": "BATCH",
                    "integration":    "SPARK",
                    "jobType":        "JOB",
                },
            },
        },
        "inputs":  [ds.to_ol_dataset(namespace) for ds in inputs],
        "outputs": [ds.to_ol_dataset(namespace) for ds in outputs],
        "producer": "https://github.com/your-org/AgenticAI-DataEngineering",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
    }

    if error_msg:
        event["run"]["facets"]["errorMessage"] = {
            "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
            "message":        error_msg[:2000],
            "programmingLanguage": "Python",
        }

    return event


# ---------------------------------------------------------------------------
# Transport implementations
# ---------------------------------------------------------------------------

class _HttpTransport:
    def __init__(self, url: str, api_key: str, timeout: int = 10):
        self._url     = url.rstrip("/") + "/api/v1/lineage"
        self._headers = {
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {api_key}"} if api_key else {}),
        }
        self._timeout = timeout

    def emit(self, event: dict[str, Any]) -> None:
        try:
            resp = requests.post(
                self._url,
                headers=self._headers,
                data=json.dumps(event, default=str),
                timeout=self._timeout,
            )
            resp.raise_for_status()
        except Exception as exc:
            # Lineage emission must never break the pipeline
            logger.warning("[LINEAGE] Failed to emit event: %s", exc)


class _ConsoleTransport:
    def emit(self, event: dict[str, Any]) -> None:
        print("[LINEAGE]", json.dumps(event, indent=2, default=str))


# ---------------------------------------------------------------------------
# Main emitter class
# ---------------------------------------------------------------------------

class LineageEmitter:
    """
    Thread-safe OpenLineage emitter.  Instantiate once per Spark application
    (typically in the job's main function) and reuse across pipeline stages.
    """

    def __init__(self, config: LineageConfig | None = None):
        self._cfg = config or LineageConfig.from_env()
        if self._cfg.transport == "http" and self._cfg.url:
            self._transport = _HttpTransport(self._cfg.url, self._cfg.api_key)
        else:
            self._transport = _ConsoleTransport()

    # ------------------------------------------------------------------
    # Low-level emit helpers
    # ------------------------------------------------------------------

    def emit_start(
        self,
        job_name: str,
        run_id:   str,
        inputs:   list[LineageDataset],
        outputs:  list[LineageDataset],
        run_facets: dict[str, Any] | None = None,
    ) -> None:
        event = _build_event(
            "START", job_name, run_id, self._cfg.namespace,
            inputs, outputs, run_facets,
        )
        self._transport.emit(event)

    def emit_complete(
        self,
        job_name: str,
        run_id:   str,
        inputs:   list[LineageDataset],
        outputs:  list[LineageDataset],
        run_facets: dict[str, Any] | None = None,
    ) -> None:
        event = _build_event(
            "COMPLETE", job_name, run_id, self._cfg.namespace,
            inputs, outputs, run_facets,
        )
        self._transport.emit(event)

    def emit_fail(
        self,
        job_name:  str,
        run_id:    str,
        inputs:    list[LineageDataset],
        outputs:   list[LineageDataset],
        error_msg: str = "",
    ) -> None:
        event = _build_event(
            "FAIL", job_name, run_id, self._cfg.namespace,
            inputs, outputs, error_msg=error_msg,
        )
        self._transport.emit(event)

    # ------------------------------------------------------------------
    # Context manager for clean START / COMPLETE / FAIL wrapping
    # ------------------------------------------------------------------

    @contextlib.contextmanager
    def run_context(
        self,
        job_name:   str,
        inputs:     list[LineageDataset],
        outputs:    list[LineageDataset],
        run_facets: dict[str, Any] | None = None,
        run_id:     str | None = None,
    ):
        """
        Context manager that emits START on enter and COMPLETE / FAIL on exit.

        Usage:
            with emitter.run_context("ingest.sql.sales", inputs, outputs) as run_id:
                ingest_data(...)
        """
        rid = run_id or str(uuid.uuid4())
        self.emit_start(job_name, rid, inputs, outputs, run_facets)
        try:
            yield rid
            self.emit_complete(job_name, rid, inputs, outputs, run_facets)
        except Exception as exc:
            self.emit_fail(job_name, rid, inputs, outputs, traceback.format_exc())
            raise


# ---------------------------------------------------------------------------
# Decorator — wraps any pipeline function with lineage emission
# ---------------------------------------------------------------------------

def lineage_job(
    job_name:    str,
    inputs:      list[LineageDataset] | None = None,
    outputs:     list[LineageDataset] | None = None,
    config:      LineageConfig | None = None,
):
    """
    Decorator that wraps a pipeline function with OpenLineage START/COMPLETE/FAIL events.

    The wrapped function may optionally accept `_lineage_run_id` as a keyword
    argument to receive the generated run UUID (useful for correlation).

    Example:
        @lineage_job(
            job_name="clean.bronze_to_silver.sales_orders",
            inputs=[LineageDataset(
                name="bronze.sales_orders",
                source_uri="abfss://raw@acct.dfs.core.windows.net/sales/orders",
            )],
            outputs=[LineageDataset(name="silver.sales_orders")],
        )
        def run_cleaning_job(spark, env):
            ...
    """
    _inputs  = inputs  or []
    _outputs = outputs or []

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            emitter = LineageEmitter(config)
            run_id  = str(uuid.uuid4())
            with emitter.run_context(job_name, _inputs, _outputs, run_id=run_id):
                if "lineage_run_id" in fn.__code__.co_varnames:
                    kwargs["lineage_run_id"] = run_id
                return fn(*args, **kwargs)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Pre-built dataset factories for common lakehouse assets
# ---------------------------------------------------------------------------

def adls_dataset(
    zone: str,
    domain: str,
    entity: str,
    storage_account: str,
    schema_fields: list[dict[str, str]] | None = None,
    row_count: int | None = None,
) -> LineageDataset:
    """
    Convenience factory for an ADLS Gen2 raw / landing / quarantine asset.
    """
    container = zone                         # e.g. "raw", "landing", "quarantine"
    path      = f"{domain}/{entity}"
    uri       = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"
    return LineageDataset(
        namespace=f"adls://{storage_account}",
        name=f"{zone}/{domain}/{entity}",
        source_uri=uri,
        schema_fields=schema_fields or [],
        row_count=row_count,
    )


def delta_table_dataset(
    env: str,
    layer: str,
    table: str,
    schema_fields: list[dict[str, str]] | None = None,
    row_count: int | None = None,
) -> LineageDataset:
    """
    Convenience factory for a Unity Catalog Delta table asset.
    """
    fqn = f"{env}_lakehouse.{layer}.{table}"
    return LineageDataset(
        namespace="databricks",
        name=fqn,
        source_uri=f"databricks://unity-catalog/{fqn}",
        schema_fields=schema_fields or [],
        row_count=row_count,
    )


def mlflow_dataset(model_name: str, version: str) -> LineageDataset:
    """Factory for an MLflow model as an OpenLineage dataset."""
    return LineageDataset(
        namespace="mlflow",
        name=f"models:/{model_name}/{version}",
        source_uri=f"mlflow://models/{model_name}/{version}",
    )


# ---------------------------------------------------------------------------
# Run facet builders
# ---------------------------------------------------------------------------

def spark_job_facet(
    spark_version: str = "3.5",
    app_name: str = "lakehouse-pipeline",
) -> dict[str, Any]:
    """Build a Spark-specific run facet for the OpenLineage event."""
    return {
        "spark_version": {
            "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SparkVersionRunFacet.json",
            "sparkVersion": spark_version,
            "openlineageSparkVersion": "1.0.0",
        },
        "processing_engine": {
            "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ProcessingEngineRunFacet.json",
            "version":     spark_version,
            "name":        "Apache Spark",
            "openlineageAdapterVersion": "1.0.0",
        },
    }


def dq_metrics_facet(
    rows_total: int,
    rows_passed: int,
    rows_failed: int,
    checks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a data quality metrics run facet."""
    return {
        "dataQualityMetrics": {
            "_producer": "https://github.com/your-org/AgenticAI-DataEngineering",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsDatasetFacet.json",
            "rowCount":    rows_total,
            "validCount":  rows_passed,
            "invalidCount": rows_failed,
            "columnMetrics": {c["name"]: c for c in (checks or [])},
        }
    }
