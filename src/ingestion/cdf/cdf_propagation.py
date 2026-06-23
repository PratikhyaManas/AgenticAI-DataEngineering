"""
Delta Change Data Feed (CDF) Propagation — Silver → Gold
=========================================================
Agent: Data Transformation & Modeling Agent

Uses Delta Change Data Feed to propagate ONLY changed Silver rows into Gold
instead of re-reading the entire Silver table on every run.

Benefits over full Silver scan:
  - Reads only the delta of changes since last checkpoint
  - Eliminates shuffle-sort-merge on large Silver tables
  - Naturally handles INSERT / UPDATE_POSTIMAGE / DELETE operation types
  - No watermark column required — uses Delta's internal commit version

Requires Delta Lake with CDF enabled (set in CREATE TABLE or ALTER TABLE):
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

CDF output columns added by Delta:
  _change_type:       insert | update_preimage | update_postimage | delete
  _commit_version:    Delta transaction version (monotonically increasing)
  _commit_timestamp:  When the commit was written

Reference: https://docs.databricks.com/en/delta/delta-change-data-feed.html
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


# ---------------------------------------------------------------------------
# CDF version checkpoint helpers
# ---------------------------------------------------------------------------

def _get_last_processed_version(
    spark: SparkSession,
    env: str,
    source_table: str,
) -> int | None:
    """
    Read the last successfully processed Delta commit version from the
    checkpoint table.  Returns None if no previous run exists (full read).
    """
    checkpoint_table = f"{env}_lakehouse.bronze.cdf_checkpoints"
    _ensure_checkpoint_table(spark, checkpoint_table)

    row = (
        spark.table(checkpoint_table)
             .filter(F.col("source_table") == source_table)
             .agg(F.max("last_commit_version").alias("v"))
             .collect()[0]
    )
    return int(row["v"]) if row["v"] is not None else None


def _save_checkpoint(
    spark: SparkSession,
    env: str,
    source_table: str,
    commit_version: int,
) -> None:
    """Persist the latest successfully processed commit version."""
    checkpoint_table = f"{env}_lakehouse.bronze.cdf_checkpoints"
    spark.sql(f"""
        MERGE INTO {checkpoint_table} AS tgt
        USING (
            SELECT
                '{source_table}'  AS source_table,
                {commit_version}  AS last_commit_version,
                current_timestamp() AS updated_ts
        ) AS src
        ON tgt.source_table = src.source_table
        WHEN MATCHED THEN
            UPDATE SET
                tgt.last_commit_version = src.last_commit_version,
                tgt.updated_ts          = src.updated_ts
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def _ensure_checkpoint_table(spark: SparkSession, checkpoint_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {checkpoint_table} (
            source_table          STRING  NOT NULL,
            last_commit_version   BIGINT  NOT NULL,
            updated_ts            TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)


def _get_latest_version(spark: SparkSession, table_name: str) -> int:
    """Return the current latest Delta commit version for a table."""
    row = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
    return int(row["version"])


# ---------------------------------------------------------------------------
# Read CDF changes from Silver
# ---------------------------------------------------------------------------

def read_silver_changes(
    spark: SparkSession,
    silver_table: str,
    start_version: int | None,
    end_version: int,
) -> DataFrame:
    """
    Read changed rows from a Silver Delta table using table_changes().

    Args:
        start_version: First commit version to read (exclusive). None = read all.
        end_version:   Last commit version to read (inclusive).

    Returns DataFrame with standard CDF columns:
        _change_type, _commit_version, _commit_timestamp, + all Silver columns.

    Filtered to only INSERT and UPDATE_POSTIMAGE — DELETE is propagated as a
    soft-delete flag downstream.
    """
    if start_version is None:
        # First run: read all current data as inserts
        return (
            spark.table(silver_table)
                 .withColumn("_change_type",      F.lit("insert"))
                 .withColumn("_commit_version",   F.lit(0).cast("long"))
                 .withColumn("_commit_timestamp", F.current_timestamp())
        )

    return (
        spark.read
             .format("delta")
             .option("readChangeFeed",          "true")
             .option("startingVersion",         start_version + 1)
             .option("endingVersion",            end_version)
             .table(silver_table)
             .filter(
                 F.col("_change_type").isin(
                     "insert", "update_postimage", "delete"
                 )
             )
    )


# ---------------------------------------------------------------------------
# Apply CDF changes to Gold dim tables
# ---------------------------------------------------------------------------

def apply_cdf_to_dim(
    spark: SparkSession,
    changes_df: DataFrame,
    gold_table: str,
    natural_key: str,
    update_cols: list[str],
    soft_delete_col: str = "_is_current",
) -> int:
    """
    Apply CDF changes to a Gold dimension table.

    - INSERT / UPDATE_POSTIMAGE → MERGE (upsert) tracked columns
    - DELETE  → mark _is_current = False (soft delete)

    Returns the number of rows affected.
    """
    if changes_df.isEmpty():
        print(f"[CDF] No changes to apply to {gold_table}")
        return 0

    # Split into upserts and deletes
    upserts_df = changes_df.filter(
        F.col("_change_type").isin("insert", "update_postimage")
    )
    deletes_df = changes_df.filter(F.col("_change_type") == "delete")

    # --- Upserts ---
    if not upserts_df.isEmpty():
        upserts_df.createOrReplaceTempView("_cdf_upsert_staging")
        update_set = ",\n            ".join(
            [f"tgt.{c} = src.{c}" for c in update_cols]
        )
        spark.sql(f"""
            MERGE INTO {gold_table} AS tgt
            USING _cdf_upsert_staging AS src
              ON tgt.{natural_key} = src.{natural_key}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_set},
                    tgt._gold_load_ts = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    # --- Soft deletes ---
    if not deletes_df.isEmpty():
        deletes_df.select(natural_key).createOrReplaceTempView("_cdf_delete_staging")
        spark.sql(f"""
            MERGE INTO {gold_table} AS tgt
            USING _cdf_delete_staging AS src
              ON tgt.{natural_key} = src.{natural_key}
            WHEN MATCHED THEN
                UPDATE SET tgt.{soft_delete_col} = FALSE
        """)

    affected = changes_df.count()
    print(f"[CDF] Applied {affected} changes to {gold_table}")
    return affected


# ---------------------------------------------------------------------------
# CDF propagation jobs
# ---------------------------------------------------------------------------

def propagate_silver_customers_to_gold(spark: SparkSession, env: str) -> None:
    """Incremental CDF propagation: silver.sales_customers → gold.dim_customer."""
    silver_table = f"{env}_lakehouse.silver.sales_customers"
    gold_table   = f"{env}_lakehouse.gold.dim_customer"

    last_version  = _get_last_processed_version(spark, env, silver_table)
    latest_version = _get_latest_version(spark, silver_table)

    if last_version is not None and last_version >= latest_version:
        print(f"[CDF] No new commits in {silver_table} (version={latest_version}). Skipping.")
        return

    changes = read_silver_changes(spark, silver_table, last_version, latest_version)
    changes = changes.withColumn("_gold_load_ts", F.current_timestamp())

    apply_cdf_to_dim(
        spark, changes, gold_table,
        natural_key="customer_id",
        update_cols=["email", "phone", "country_code", "region",
                     "customer_segment", "is_active", "first_name", "last_name"],
    )
    _save_checkpoint(spark, env, silver_table, latest_version)


def propagate_silver_products_to_gold(spark: SparkSession, env: str) -> None:
    """Incremental CDF propagation: silver.products → gold.dim_product."""
    silver_table = f"{env}_lakehouse.silver.products"
    gold_table   = f"{env}_lakehouse.gold.dim_product"

    last_version   = _get_last_processed_version(spark, env, silver_table)
    latest_version = _get_latest_version(spark, silver_table)

    if last_version is not None and last_version >= latest_version:
        print(f"[CDF] No new commits in {silver_table} (version={latest_version}). Skipping.")
        return

    changes = read_silver_changes(spark, silver_table, last_version, latest_version)
    changes = changes.withColumn("_gold_load_ts", F.current_timestamp())

    apply_cdf_to_dim(
        spark, changes, gold_table,
        natural_key="product_id",
        update_cols=["product_name", "category", "subcategory", "brand",
                     "unit_price", "is_active"],
        soft_delete_col="is_active",
    )
    _save_checkpoint(spark, env, silver_table, latest_version)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta CDF propagation job")
    parser.add_argument("--env",    required=True, help="dev | test | prod")
    parser.add_argument("--entity", required=True, help="customers | products")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    if args.entity == "customers":
        propagate_silver_customers_to_gold(spark, args.env)
    elif args.entity == "products":
        propagate_silver_products_to_gold(spark, args.env)
    else:
        raise ValueError(f"Unknown entity: {args.entity!r}")
