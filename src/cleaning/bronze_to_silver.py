"""
Bronze → Silver Cleaning Job: Sales Orders
==========================================
Agent: Data Cleaning & Standardization Agent

Reads the Bronze Delta table (raw/sales/orders), applies the cleaning
rules defined in the Data Cleaning spec, and writes clean records to the
Silver Delta table using MERGE (upsert) for idempotency.

Bad records are written to the quarantine Delta table with error reasons.
"""

import argparse
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.cleaning.cleaning_utils import (
    cast_columns,
    trim_string_columns,
    normalize_nulls,
    normalize_to_upper,
    parse_timestamps,
    parse_dates,
    deduplicate,
    flag_null_violations,
    flag_range_violations,
    split_valid_quarantine,
    add_silver_metadata,
)
from src.cleaning.quarantine_handler import write_to_quarantine


# ---------------------------------------------------------------------------
# Cleaning Rules Specification (maps to data_models_quality_security.md)
# ---------------------------------------------------------------------------

CAST_MAP = {
    "order_id":     "long",
    "customer_id":  "long",
    "amount":       "decimal(18,2)",
    "quantity":     "integer",
    "discount":     "decimal(5,4)",
}

TIMESTAMP_COLS = ["created_at", "updated_at", "_ingestion_timestamp"]
DATE_COLS      = ["order_date", "ship_date"]
NOT_NULL_COLS  = ["order_id", "customer_id", "order_date", "status", "amount"]
RANGE_RULES    = [
    {"column": "amount",    "min": 0,    "allow_null": False},
    {"column": "quantity",  "min": 1,    "max": 99999},
    {"column": "discount",  "min": 0,    "max": 1},
]
PRIMARY_KEYS        = ["order_id"]
UPPERCASE_COLS      = ["status", "currency"]
DEDUP_ORDER_COL     = "updated_at"


def run_cleaning_job(spark: SparkSession, env: str) -> None:
    silver_run_id  = str(uuid.uuid4())
    bronze_table   = f"{env}_lakehouse.bronze.sales_orders"
    silver_table   = f"{env}_lakehouse.silver.sales_orders"
    source_system  = "azure_sql_sales"

    print(f"[START] Bronze→Silver cleaning | run_id={silver_run_id}")

    # 1. Read Bronze — only process records not yet promoted to Silver
    #    Uses a high-watermark approach: last _ingestion_timestamp in Silver
    last_silver_ts = spark.sql(f"""
        SELECT COALESCE(MAX(_silver_load_ts), CAST('1900-01-01' AS TIMESTAMP))
        AS last_ts FROM {silver_table}
    """).collect()[0]["last_ts"]

    bronze_df = spark.table(bronze_table).filter(
        F.col("_ingestion_timestamp") > F.lit(last_silver_ts)
    )

    # isEmpty() uses LIMIT 1 — avoids a full count() scan on the Bronze table
    if bronze_df.isEmpty():
        print("[INFO]  Nothing to process. Exiting.")
        return

    # 2. String normalization
    df = trim_string_columns(bronze_df)
    df = normalize_nulls(df)
    df = normalize_to_upper(df, UPPERCASE_COLS)

    # 3. Type casting (safe cast; failures → NULL → caught by null check)
    df = cast_columns(df, CAST_MAP)

    # 4. Date / timestamp parsing
    df = parse_timestamps(df, TIMESTAMP_COLS)
    df = parse_dates(df, DATE_COLS)

    # 5. Validation flags
    df = flag_null_violations(df,  NOT_NULL_COLS)
    df = flag_range_violations(df, RANGE_RULES)

    # 6. Deduplicate within this batch on primary keys
    df = deduplicate(df, PRIMARY_KEYS, order_by_col="updated_at")

    # 7. Split valid / quarantine — persist to avoid recomputing the split logic twice
    from pyspark.storagelevel import StorageLevel
    flagged_df = df.persist(StorageLevel.MEMORY_AND_DISK)
    valid_df, quarantine_df = split_valid_quarantine(flagged_df)

    # Materialize both splits now (single pass over persisted data)
    valid_count      = valid_df.count()
    quarantine_count = quarantine_df.count()
    print(f"[INFO]  Valid: {valid_count}, Quarantined: {quarantine_count}")

    # 8. Add Silver audit columns
    valid_df = add_silver_metadata(valid_df, source_system, silver_run_id)

    # 9. MERGE into Silver (idempotent upsert)
    valid_df.createOrReplaceTempView("_silver_staging")
    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING _silver_staging AS src
          ON target.order_id = src.order_id
        WHEN MATCHED AND src.updated_at > target.updated_at THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    # 10. Write quarantine records
    if quarantine_count > 0:
        write_to_quarantine(
            spark, quarantine_df, env,
            domain="sales", entity="orders",
            silver_run_id=silver_run_id,
        )

    flagged_df.unpersist()
    print(f"[DONE]  Silver table updated: {silver_table}")
    print(f"[DONE]  Quarantine records written: {quarantine_count}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver cleaning job")
    parser.add_argument("--env", required=True, help="dev | test | prod")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    run_cleaning_job(spark, args.env)
