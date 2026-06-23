"""
Unit Tests — Cleaning Utilities
================================
Agent: CI/CD & DevOps Agent + Data Cleaning Agent

Tests cover all cleaning utility functions in src/cleaning/cleaning_utils.py
using PySpark local mode (no Databricks cluster required in CI).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests-cleaning")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Import utils (adjust path as needed)
# ---------------------------------------------------------------------------
from src.cleaning.cleaning_utils import (
    cast_columns,
    trim_string_columns,
    normalize_nulls,
    parse_dates,
    deduplicate,
    flag_null_violations,
    flag_range_violations,
    split_valid_quarantine,
)


class TestCastColumns:
    def test_cast_string_to_long(self, spark):
        df = spark.createDataFrame([("1",), ("2",), ("abc",)], ["order_id"])
        result = cast_columns(df, {"order_id": "long"})
        rows = result.collect()
        assert rows[0]["order_id"] == 1
        assert rows[1]["order_id"] == 2
        assert rows[2]["order_id"] is None  # failed cast → NULL

    def test_cast_string_to_decimal(self, spark):
        df = spark.createDataFrame([("19.99",), ("0.00",), ("bad",)], ["amount"])
        result = cast_columns(df, {"amount": "decimal(18,2)"})
        rows = result.collect()
        assert float(rows[0]["amount"]) == pytest.approx(19.99)
        assert rows[2]["amount"] is None

    def test_noop_when_no_columns(self, spark):
        df = spark.createDataFrame([("val",)], ["col"])
        result = cast_columns(df, {})
        assert result.count() == df.count()


class TestTrimStringColumns:
    def test_trims_whitespace(self, spark):
        df = spark.createDataFrame([("  hello  ",), ("world",)], ["name"])
        result = trim_string_columns(df, ["name"])
        rows = result.collect()
        assert rows[0]["name"] == "hello"
        assert rows[1]["name"] == "world"

    def test_auto_detect_string_columns(self, spark):
        df = spark.createDataFrame([("  a  ", 1)], ["s_col", "n_col"])
        result = trim_string_columns(df)  # no explicit columns
        rows = result.collect()
        assert rows[0]["s_col"] == "a"
        assert rows[0]["n_col"] == 1


class TestNormalizeNulls:
    def test_replaces_null_strings(self, spark):
        df = spark.createDataFrame([("NULL",), ("N/A",), ("", ), ("actual",)], ["col"])
        result = normalize_nulls(df)
        rows = result.collect()
        assert rows[0]["col"] is None
        assert rows[1]["col"] is None
        assert rows[2]["col"] is None
        assert rows[3]["col"] == "actual"

    def test_case_insensitive(self, spark):
        df = spark.createDataFrame([("null",), ("na",), ("None",)], ["col"])
        result = normalize_nulls(df)
        rows = result.collect()
        for r in rows:
            assert r["col"] is None


class TestParseDates:
    def test_iso_date_format(self, spark):
        df = spark.createDataFrame([("2024-01-15",), ("2023-12-31",)], ["order_date"])
        result = parse_dates(df, ["order_date"])
        rows = result.collect()
        assert str(rows[0]["order_date"]) == "2024-01-15"
        assert str(rows[1]["order_date"]) == "2023-12-31"

    def test_slash_date_format(self, spark):
        df = spark.createDataFrame([("01/15/2024",)], ["order_date"])
        result = parse_dates(df, ["order_date"])
        rows = result.collect()
        assert str(rows[0]["order_date"]) == "2024-01-15"

    def test_invalid_date_produces_null(self, spark):
        df = spark.createDataFrame([("not-a-date",)], ["order_date"])
        result = parse_dates(df, ["order_date"])
        rows = result.collect()
        assert rows[0]["order_date"] is None


class TestDeduplicate:
    def test_keeps_latest_by_timestamp(self, spark):
        from datetime import datetime
        data = [
            (1, "OLD",  datetime(2024, 1, 1)),
            (1, "LATEST", datetime(2024, 6, 1)),
            (2, "ONLY",   datetime(2024, 3, 1)),
        ]
        df = spark.createDataFrame(data, ["order_id", "status", "_ingestion_timestamp"])
        result = deduplicate(df, ["order_id"], order_by_col="_ingestion_timestamp")
        rows = {r["order_id"]: r for r in result.collect()}
        assert rows[1]["status"] == "LATEST"
        assert rows[2]["status"] == "ONLY"
        assert result.count() == 2

    def test_no_duplicates_unchanged(self, spark):
        from datetime import datetime
        data = [(1, datetime(2024, 1, 1)), (2, datetime(2024, 1, 2))]
        df = spark.createDataFrame(data, ["order_id", "_ingestion_timestamp"])
        result = deduplicate(df, ["order_id"])
        assert result.count() == 2


class TestFlagNullViolations:
    def test_flags_null_columns(self, spark):
        df = spark.createDataFrame([(None, "ok"), (1, None)], ["order_id", "status"])
        result = flag_null_violations(df, ["order_id", "status"])
        rows = {i: r for i, r in enumerate(result.collect())}
        assert "NOT_NULL_VIOLATION:order_id" in (rows[0]["_error_reason"] or "")
        assert "NOT_NULL_VIOLATION:status" in (rows[1]["_error_reason"] or "")

    def test_no_flag_when_all_present(self, spark):
        df = spark.createDataFrame([(1, "OK")], ["order_id", "status"])
        result = flag_null_violations(df, ["order_id", "status"])
        rows = result.collect()
        assert rows[0]["_error_reason"] is None


class TestFlagRangeViolations:
    def test_flags_negative_amount(self, spark):
        df = spark.createDataFrame([(-1.0,), (0.0,), (10.0,)], ["amount"])
        rules = [{"column": "amount", "min": 0, "allow_null": True}]
        result = flag_range_violations(df, rules)
        rows = result.collect()
        assert "RANGE_VIOLATION:amount" in (rows[0]["_error_reason"] or "")
        assert rows[1]["_error_reason"] is None
        assert rows[2]["_error_reason"] is None

    def test_flags_above_max(self, spark):
        df = spark.createDataFrame([(0.5,), (1.1,)], ["discount"])
        rules = [{"column": "discount", "min": 0.0, "max": 1.0}]
        result = flag_range_violations(df, rules)
        rows = result.collect()
        assert rows[0]["_error_reason"] is None
        assert "RANGE_VIOLATION:discount" in (rows[1]["_error_reason"] or "")


class TestSplitValidQuarantine:
    def test_splits_correctly(self, spark):
        data = [(1, None), (2, "NOT_NULL_VIOLATION:col")]
        df = spark.createDataFrame(data, ["id", "_error_reason"])
        valid_df, quarantine_df = split_valid_quarantine(df)
        assert valid_df.count() == 1
        assert quarantine_df.count() == 1
        assert "id" in valid_df.columns
        assert "_error_reason" not in valid_df.columns   # dropped from valid
        assert "_error_reason" in quarantine_df.columns

    def test_all_valid(self, spark):
        df = spark.createDataFrame([(1, None), (2, None)], ["id", "_error_reason"])
        valid_df, quarantine_df = split_valid_quarantine(df)
        assert valid_df.count() == 2
        assert quarantine_df.count() == 0

    def test_all_quarantine(self, spark):
        df = spark.createDataFrame([(1, "ERR"), (2, "ERR")], ["id", "_error_reason"])
        valid_df, quarantine_df = split_valid_quarantine(df)
        assert valid_df.count() == 0
        assert quarantine_df.count() == 2
