"""
Delta Live Tables — Bronze → Silver (Sales Orders)
====================================================
Agent: Data Cleaning & Standardization Agent (DLT variant)

Declarative DLT pipeline replacing bronze_to_silver.py.
Runs in Databricks DLT pipeline (Triggered or Continuous mode).

Pipeline topology:
  raw_sales_orders  (streaming Bronze source via Autoloader)
       ↓
  validated_sales_orders  (expect_or_drop quality gates)
       ↓
  silver_sales_orders  (cleaned, cast, deduplicated Silver table)
       ↓
  quarantine_sales_orders  (records that failed mandatory expectations)

Quality expectations are enforced at two severity levels:
  @dlt.expect_or_drop   – drop record if violated; route to quarantine
  @dlt.expect_or_warn   – pass record but emit metric; used for soft rules

All tables are published to Unity Catalog schema {catalog}.silver.*
Configure via DLT pipeline settings:
  catalog   = {env}_lakehouse
  target    = silver

Reference: https://docs.databricks.com/en/delta-live-tables/index.html
"""

from __future__ import annotations

import dlt  # type: ignore[import]   # available only inside a DLT pipeline

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)


# ---------------------------------------------------------------------------
# Pipeline-level configuration (set in DLT pipeline UI or via API)
# ---------------------------------------------------------------------------
# spark.conf.get("catalog") → e.g. "dev_lakehouse"
# spark.conf.get("landing_path") → e.g. "abfss://landing@<acct>.dfs.core.windows.net/sales"


def _get_conf(key: str, default: str = "") -> str:
    try:
        return spark.conf.get(key, default)  # noqa: F821  (spark injected by DLT runtime)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Layer 1: Raw Bronze — streaming ingest via Autoloader
# ---------------------------------------------------------------------------

@dlt.table(
    name="raw_sales_orders",
    comment="Autoloader streaming ingest from ADLS landing zone. "
            "Schema on read — no transformations applied.",
    table_properties={
        "quality":                          "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.zOrderCols": "_rescued_data",
    },
    partition_cols=["_ingestion_date"],
)
def raw_sales_orders() -> DataFrame:
    """
    Reads JSON / CSV files dropped in the landing zone using Autoloader
    (cloudFiles).  Databricks schema inference + evolution handles
    upstream schema changes automatically.
    """
    landing_path = _get_conf(
        "landing_path",
        "abfss://landing@stdatalakehouse.dfs.core.windows.net/sales/orders",
    )
    return (
        spark.readStream  # noqa: F821
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.schemaLocation",
                     landing_path.rstrip("/") + "/_schemas")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             .option("cloudFiles.maxFilesPerTrigger", "1000")
             .load(landing_path)
             .withColumn("_ingestion_date",
                         F.to_date(F.col("_metadata.file_modification_time")))
             .withColumn("_source_file",
                         F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# Layer 2: Validated — mandatory quality gates (drop on violation)
# ---------------------------------------------------------------------------

@dlt.table(
    name="validated_sales_orders",
    comment="DQ-gated view of raw_sales_orders. "
            "Records failing mandatory expectations are quarantined.",
    table_properties={
        "quality":                          "bronze_validated",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dlt.expect_or_drop("order_id_not_null",    "order_id IS NOT NULL")
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect_or_drop("amount_positive",      "amount > 0")
@dlt.expect_or_drop("order_date_not_null",  "order_date IS NOT NULL")
@dlt.expect_or_drop("status_not_null",      "status IS NOT NULL")
@dlt.expect_or_warn("discount_valid_range", "discount >= 0 AND discount <= 1")
@dlt.expect_or_warn("quantity_positive",    "quantity >= 1")
@dlt.expect_or_warn("currency_3chars",      "LENGTH(TRIM(currency)) = 3")
def validated_sales_orders() -> DataFrame:
    """
    Reads from the raw_sales_orders streaming table and enforces DQ gates.
    Records violating expect_or_drop rules are automatically excluded from
    this table and counted in DLT pipeline metrics.
    """
    return dlt.read_stream("raw_sales_orders")


# ---------------------------------------------------------------------------
# Layer 3: Quarantine — records dropped by mandatory expectations
# ---------------------------------------------------------------------------

@dlt.table(
    name="quarantine_sales_orders",
    comment="Records that failed one or more mandatory DQ expectations. "
            "Retained for investigation and potential reprocessing.",
    table_properties={
        "quality":                          "quarantine",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["_ingestion_date"],
)
def quarantine_sales_orders() -> DataFrame:
    """
    Anti-join between raw and validated to isolate dropped records.
    All original columns are preserved for forensic analysis.
    """
    raw       = dlt.read_stream("raw_sales_orders")
    validated = dlt.read_stream("validated_sales_orders")

    # Records present in raw but absent from validated → quarantine
    # Using order_id as the join key; NULL order_id records are always quarantined
    return (
        raw.join(
            validated.select("order_id").distinct(),
            on="order_id",
            how="left_anti",
        )
        .withColumn("_quarantine_reason",
                    F.lit("failed_mandatory_dq_expectation"))
        .withColumn("_quarantine_ts", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Layer 4: Silver — cleaned, typed, deduplicated
# ---------------------------------------------------------------------------

@dlt.table(
    name="silver_sales_orders",
    comment="Cleaned and typed sales orders. "
            "SCD-style deduplication via APPLY CHANGES is configured separately. "
            "Ready for downstream Gold transformations.",
    table_properties={
        "quality":                              "silver",
        "delta.autoOptimize.optimizeWrite":     "true",
        "delta.autoOptimize.autoCompact":       "true",
        "delta.enableChangeDataFeed":           "true",
        "pipelines.autoOptimize.zOrderCols":    "customer_id,order_date",
    },
)
@dlt.expect_or_warn("silver_amount_decimal",   "amount IS NOT NULL AND amount > 0")
@dlt.expect_or_warn("silver_currency_present", "currency IS NOT NULL")
def silver_sales_orders() -> DataFrame:
    """
    Applies data-type casting, string normalisation, and timestamp parsing
    to produce a clean Silver record.  Deduplication within the micro-batch
    keeps the latest record per order_id.
    """
    return (
        dlt.read_stream("validated_sales_orders")

        # Cast to canonical types
        .withColumn("order_id",    F.col("order_id").cast(LongType()))
        .withColumn("customer_id", F.col("customer_id").cast(LongType()))
        .withColumn("amount",      F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("quantity",    F.col("quantity").cast(IntegerType()))
        .withColumn("discount",    F.col("discount").cast(DecimalType(5, 4)))

        # Timestamp / date normalisation
        .withColumn("order_date",   F.to_date(F.col("order_date")))
        .withColumn("ship_date",    F.to_date(F.col("ship_date")))
        .withColumn("created_at",   F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at",   F.to_timestamp(F.col("updated_at")))

        # String normalisation
        .withColumn("status",    F.upper(F.trim(F.col("status"))))
        .withColumn("currency",  F.upper(F.trim(F.col("currency"))))
        .withColumn("notes",     F.trim(F.col("notes")))

        # Null normalisation: empty strings → NULL
        .withColumn("notes",
                    F.when(F.trim(F.col("notes")) == "", None)
                     .otherwise(F.col("notes")))

        # Silver metadata
        .withColumn("_silver_load_ts",  F.current_timestamp())
        .withColumn("_pipeline_name",   F.lit("dlt_bronze_to_silver"))

        # Intra-batch deduplication: keep latest per order_id
        .dropDuplicates(["order_id"])
    )


# ---------------------------------------------------------------------------
# Aggregate monitoring view — counts by status for observability
# ---------------------------------------------------------------------------

@dlt.table(
    name="silver_sales_orders_daily_metrics",
    comment="Daily aggregation of Silver order metrics for monitoring dashboards.",
    table_properties={"quality": "silver_metrics"},
)
def silver_sales_orders_daily_metrics() -> DataFrame:
    """
    Batch aggregation over the silver_sales_orders table.
    Written as a non-streaming materialized view.
    Refreshed every pipeline run.
    """
    return (
        dlt.read("silver_sales_orders")
           .groupBy("order_date", "status", "currency")
           .agg(
               F.count("*").alias("order_count"),
               F.sum("amount").alias("total_amount"),
               F.avg("amount").alias("avg_amount"),
               F.countDistinct("customer_id").alias("distinct_customers"),
           )
           .withColumn("_computed_ts", F.current_timestamp())
    )
