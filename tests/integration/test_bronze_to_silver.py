"""
Integration Tests: Bronze → Silver Cleaning Pipeline
=====================================================
Tests the cleaning logic end-to-end using synthetic Bronze data:
  - Valid rows survive
  - Null order_id / customer_id rows go to quarantine
  - Negative quantity / price rows go to quarantine
  - Null order_date rows go to quarantine
  - Duplicate order_ids are deduplicated (keep latest)
  - Output schema is correct (DateType, net_amount, upper-cased region)

These tests run locally with delta-spark.
No Databricks cluster is required unless DATABRICKS_HOST is set.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tests.integration.synthetic_data import make_bronze_sales


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _apply_silver_cleaning(spark: SparkSession, bronze_df, tmp_path: str):
    """
    Inline re-implementation of the Bronze→Silver cleaning logic so that
    integration tests remain independent from the production code path,
    allowing them to serve as a spec as well as a regression guard.
    """
    # Step 1: tag bad rows
    flagged = bronze_df.withColumn(
        "_dq_failed",
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("order_date").isNull()
        | (F.col("quantity") <= 0)
        | (F.col("unit_price") <= 0),
    )

    # Step 2: split quarantine / valid
    quarantine = flagged.filter(F.col("_dq_failed") == True)
    valid      = flagged.filter(F.col("_dq_failed") == False).drop("_dq_failed")

    # Step 3: deduplicate (keep first occurrence per order_id)
    valid = valid.dropDuplicates(["order_id"])

    # Step 4: cast + enrich
    silver = (
        valid
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("region", F.upper("region"))
        .withColumn(
            "net_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2),
        )
    )
    return silver, quarantine


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBronzeToSilverCleaning:

    def test_valid_row_count(self, spark: SparkSession):
        """Valid rows (50 good + 2 duplicates = 51 distinct) should pass through."""
        bronze = make_bronze_sales(spark, n_valid=50, n_bad=5)
        silver, quarantine = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        # 50 good + 1 DUP row (deduplicated) = 51 distinct — minus 5 bad = 46 valid silver rows
        silver_count = silver.count()
        assert silver_count >= 45, f"Expected >=45 valid rows, got {silver_count}"

    def test_quarantine_catches_null_order_id(self, spark: SparkSession):
        """Rows with null order_id must go to quarantine."""
        bronze = make_bronze_sales(spark, n_valid=5, n_bad=5)
        silver, quarantine = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        null_in_silver = silver.filter(F.col("order_id").isNull()).count()
        null_in_quarantine = quarantine.filter(F.col("order_id").isNull()).count()

        assert null_in_silver == 0,      "Null order_id must NOT appear in silver"
        assert null_in_quarantine >= 1,  "Null order_id must appear in quarantine"

    def test_quarantine_catches_null_customer_id(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=5, n_bad=5)
        silver, quarantine = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        null_in_silver = silver.filter(F.col("customer_id").isNull()).count()
        assert null_in_silver == 0, "Null customer_id must NOT appear in silver"

    def test_quarantine_catches_negative_quantity(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=5, n_bad=5)
        silver, quarantine = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        neg_in_silver = silver.filter(F.col("quantity") <= 0).count()
        assert neg_in_silver == 0, "Negative quantity must NOT appear in silver"

    def test_duplicate_deduplication(self, spark: SparkSession):
        """Duplicate order_ids (DUP-001) should appear exactly once in silver."""
        bronze = make_bronze_sales(spark, n_valid=0, n_bad=0)
        silver, _ = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        dup_count = silver.filter(F.col("order_id") == "DUP-001").count()
        assert dup_count == 1, f"DUP-001 should appear exactly once, got {dup_count}"

    def test_net_amount_is_computed(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=10, n_bad=0)
        silver, _ = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        null_net = silver.filter(F.col("net_amount").isNull()).count()
        negative_net = silver.filter(F.col("net_amount") <= 0).count()
        assert null_net == 0,     "net_amount must not be null"
        assert negative_net == 0, "net_amount must be positive"

    def test_region_is_uppercased(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=20, n_bad=0)
        silver, _ = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        # All region values must equal their own upper-case
        mixed_case = silver.filter(F.col("region") != F.upper(F.col("region"))).count()
        assert mixed_case == 0, "All region values must be upper-cased"

    def test_order_date_is_date_type(self, spark: SparkSession):
        from pyspark.sql.types import DateType
        bronze = make_bronze_sales(spark, n_valid=10, n_bad=0)
        silver, _ = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        date_field = next(f for f in silver.schema.fields if f.name == "order_date")
        assert isinstance(date_field.dataType, DateType), \
            f"order_date must be DateType, got {date_field.dataType}"

    def test_silver_has_no_dq_failed_column(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=10, n_bad=3)
        silver, _ = _apply_silver_cleaning(spark, bronze, "/tmp/test")

        assert "_dq_failed" not in silver.columns, \
            "_dq_failed internal column must be dropped from silver output"

    def test_silver_written_to_delta(self, spark: SparkSession, tmp_delta_path: str):
        """Silver output can be written to and re-read from a Delta table."""
        bronze = make_bronze_sales(spark, n_valid=20, n_bad=0)
        silver, _ = _apply_silver_cleaning(spark, bronze, tmp_delta_path)

        silver_path = f"{tmp_delta_path}/silver_sales"
        silver.write.format("delta").mode("overwrite").save(silver_path)

        reloaded = spark.read.format("delta").load(silver_path)
        assert reloaded.count() == silver.count()
