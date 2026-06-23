"""
Integration Tests: Data Quality Engine
=======================================
Tests the DQ check logic for:
  - Completeness: required columns must not be null
  - Uniqueness: primary key columns must be unique
  - Validity: values must fall within expected ranges / sets
  - Volume: row count must exceed a minimum threshold

Uses synthetic Bronze and Silver data to verify that the DQ engine:
  - Correctly flags failures on bad data
  - Returns PASS on clean data
  - Reports accurate failure counts
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from tests.integration.synthetic_data import make_bronze_sales, make_silver_sales


# ---------------------------------------------------------------------------
# Lightweight inline DQ engine (mirrors dq_checks.py contracts)
# ---------------------------------------------------------------------------

class DQResult:
    def __init__(self, check_name: str, passed: bool, fail_count: int = 0, total: int = 0):
        self.check_name = check_name
        self.passed     = passed
        self.fail_count = fail_count
        self.total      = total

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"DQResult({self.check_name}: {status}, failures={self.fail_count}/{self.total})"


def check_completeness(df: DataFrame, required_cols: list[str]) -> list[DQResult]:
    total = df.count()
    results = []
    for col in required_cols:
        if col not in df.columns:
            results.append(DQResult(f"completeness_{col}", False, total, total))
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        results.append(DQResult(
            f"completeness_{col}",
            passed=(null_count == 0),
            fail_count=null_count,
            total=total,
        ))
    return results


def check_uniqueness(df: DataFrame, pk_cols: list[str]) -> DQResult:
    total    = df.count()
    distinct = df.dropDuplicates(pk_cols).count()
    dups     = total - distinct
    return DQResult(f"uniqueness_{'_'.join(pk_cols)}", passed=(dups == 0), fail_count=dups, total=total)


def check_range(df: DataFrame, col: str, min_val, max_val) -> DQResult:
    total    = df.count()
    out_of_range = df.filter(
        F.col(col).isNull() | (F.col(col) < min_val) | (F.col(col) > max_val)
    ).count()
    return DQResult(f"range_{col}", passed=(out_of_range == 0), fail_count=out_of_range, total=total)


def check_allowed_values(df: DataFrame, col: str, allowed: set) -> DQResult:
    total    = df.count()
    invalid  = df.filter(~F.col(col).isin(list(allowed))).count()
    return DQResult(f"allowed_values_{col}", passed=(invalid == 0), fail_count=invalid, total=total)


def check_min_volume(df: DataFrame, min_rows: int) -> DQResult:
    total = df.count()
    return DQResult("min_volume", passed=(total >= min_rows), fail_count=0, total=total)


# ---------------------------------------------------------------------------
# Tests: Completeness
# ---------------------------------------------------------------------------

class TestCompletenessChecks:

    def test_clean_data_passes_completeness(self, spark: SparkSession):
        clean = make_silver_sales(spark, n=20)
        results = check_completeness(clean, ["order_id", "customer_id", "order_date"])
        for r in results:
            assert r.passed, f"{r.check_name} unexpectedly failed on clean data"

    def test_null_order_id_fails_completeness(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=10, n_bad=5)
        results = check_completeness(bronze, ["order_id"])
        order_id_result = next(r for r in results if "order_id" in r.check_name)
        assert not order_id_result.passed, "Completeness check should FAIL when order_id has nulls"
        assert order_id_result.fail_count >= 1

    def test_null_customer_id_fails_completeness(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=5, n_bad=5)
        results = check_completeness(bronze, ["customer_id"])
        cust_result = next(r for r in results if "customer_id" in r.check_name)
        assert not cust_result.passed
        assert cust_result.fail_count >= 1

    def test_missing_column_fails_completeness(self, spark: SparkSession):
        df = make_bronze_sales(spark, n_valid=5, n_bad=0)
        results = check_completeness(df, ["nonexistent_column"])
        assert not results[0].passed, "Check for missing column must FAIL"


# ---------------------------------------------------------------------------
# Tests: Uniqueness
# ---------------------------------------------------------------------------

class TestUniquenessChecks:

    def test_clean_data_passes_uniqueness(self, spark: SparkSession):
        clean = make_silver_sales(spark, n=20)
        result = check_uniqueness(clean, ["order_id"])
        assert result.passed, "Silver data (after dedup) should pass uniqueness"

    def test_duplicate_order_ids_fail_uniqueness(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=0, n_bad=0)   # includes 2x DUP-001
        result = check_uniqueness(bronze, ["order_id"])
        assert not result.passed, "Duplicate order_ids must FAIL uniqueness check"
        assert result.fail_count >= 1


# ---------------------------------------------------------------------------
# Tests: Range validation
# ---------------------------------------------------------------------------

class TestRangeChecks:

    def test_clean_quantity_passes_range(self, spark: SparkSession):
        clean = make_silver_sales(spark, n=20)
        result = check_range(clean, "quantity", 1, 10000)
        assert result.passed, "Silver quantity values should pass range 1–10000"

    def test_negative_quantity_fails_range(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=5, n_bad=5)
        result = check_range(bronze, "quantity", 1, 10000)
        assert not result.passed, "Negative quantities must fail range check"

    def test_negative_price_fails_range(self, spark: SparkSession):
        bronze = make_bronze_sales(spark, n_valid=5, n_bad=5)
        result = check_range(bronze, "unit_price", 0.01, 1_000_000)
        assert not result.passed, "Negative unit_price must fail range check"


# ---------------------------------------------------------------------------
# Tests: Allowed values
# ---------------------------------------------------------------------------

class TestAllowedValueChecks:

    def test_valid_status_values_pass(self, spark: SparkSession):
        clean = make_silver_sales(spark, n=30)
        result = check_allowed_values(clean, "status",
                                      {"COMPLETED", "PENDING", "SHIPPED", "CANCELLED"})
        assert result.passed, "All synthetic status values should be in the allowed set"


# ---------------------------------------------------------------------------
# Tests: Volume
# ---------------------------------------------------------------------------

class TestVolumeChecks:

    def test_sufficient_volume_passes(self, spark: SparkSession):
        clean = make_silver_sales(spark, n=20)
        result = check_min_volume(clean, min_rows=10)
        assert result.passed

    def test_insufficient_volume_fails(self, spark: SparkSession):
        clean = make_silver_sales(spark, n=5)
        result = check_min_volume(clean, min_rows=100)
        assert not result.passed
        assert result.total == 5
