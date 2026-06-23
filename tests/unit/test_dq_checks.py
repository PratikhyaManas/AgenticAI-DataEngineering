"""
Unit Tests — Data Quality Engine
==================================
Agent: CI/CD & DevOps Agent + DQ Agent
"""

import pytest
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests-dq")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


from src.quality.dq_checks import DQEngine, DQCheckResult


class TestDQEngineNotNull:
    def test_passes_when_no_nulls(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["order_id"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_not_null(df, "order_id", "test_table")
        assert result.status == "PASSED"
        assert result.failure_count == 0

    def test_fails_when_nulls_exceed_threshold(self, spark):
        df = spark.createDataFrame([(None,), (None,), (1,)], ["order_id"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_not_null(df, "order_id", "test_table", threshold=0.0)
        assert result.status == "FAILED"
        assert result.failure_count == 2

    def test_passes_when_nulls_within_threshold(self, spark):
        # 1 null out of 100 = 1% null rate; threshold 0.02 → pass
        data = [(None,)] + [(i,) for i in range(99)]
        df = spark.createDataFrame(data, ["col"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_not_null(df, "col", "test_table", threshold=0.02)
        assert result.status == "PASSED"


class TestDQEngineUniqueness:
    def test_passes_when_all_unique(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["order_id"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_unique(df, ["order_id"], "test_table")
        assert result.status == "PASSED"
        assert result.failure_count == 0

    def test_fails_when_duplicates(self, spark):
        df = spark.createDataFrame([(1,), (1,), (2,)], ["order_id"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_unique(df, ["order_id"], "test_table", threshold=0.0)
        assert result.status == "FAILED"
        assert result.failure_count == 1   # 1 duplicate (3 total - 2 distinct)


class TestDQEngineRange:
    def test_passes_when_in_range(self, spark):
        df = spark.createDataFrame([(10.0,), (0.0,), (999.99,)], ["amount"])
        engine = DQEngine(spark, "dev", "gold", "sales", "fact_sales")
        result = engine.check_range(df, "amount", "test_table", min_val=0)
        assert result.status == "PASSED"

    def test_fails_when_below_min(self, spark):
        df = spark.createDataFrame([(-1.0,), (10.0,)], ["amount"])
        engine = DQEngine(spark, "dev", "gold", "sales", "fact_sales")
        result = engine.check_range(df, "amount", "test_table", min_val=0, threshold=0.0)
        assert result.status == "FAILED"
        assert result.failure_count == 1


class TestDQEngineEnum:
    def test_passes_valid_enum(self, spark):
        df = spark.createDataFrame([("PENDING",), ("CONFIRMED",), ("SHIPPED",)], ["status"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_enum(df, "status", "test_table",
                                   ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"])
        assert result.status == "PASSED"

    def test_fails_invalid_enum(self, spark):
        df = spark.createDataFrame([("PENDING",), ("INVALID_STATUS",)], ["status"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_enum(df, "status", "test_table",
                                   ["PENDING", "CONFIRMED"], threshold=0.0)
        assert result.status == "FAILED"
        assert result.failure_count == 1

    def test_nulls_not_flagged_as_violations(self, spark):
        df = spark.createDataFrame([(None,), ("PENDING",)], ["status"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_enum(df, "status", "test_table", ["PENDING"])
        assert result.failure_count == 0   # NULL is not a violation in enum check


class TestDQEngineFreshness:
    def test_passes_recent_data(self, spark):
        recent_ts = datetime.now(timezone.utc) - timedelta(hours=2)
        df = spark.createDataFrame([(recent_ts,)], ["_silver_load_ts"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_freshness(df, "_silver_load_ts", "test_table", max_age_hours=25)
        assert result.status == "PASSED"

    def test_fails_stale_data(self, spark):
        stale_ts = datetime.now(timezone.utc) - timedelta(hours=30)
        df = spark.createDataFrame([(stale_ts,)], ["_silver_load_ts"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        result = engine.check_freshness(df, "_silver_load_ts", "test_table", max_age_hours=25)
        assert result.status == "FAILED"


class TestDQEngineSummary:
    def test_has_failures_detection(self, spark):
        df = spark.createDataFrame([(None,)], ["col"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        engine.check_not_null(df, "col", "test_table", threshold=0.0)
        assert engine.has_failures() is True

    def test_summary_counts(self, spark):
        df_ok   = spark.createDataFrame([(1,)], ["col"])
        df_fail = spark.createDataFrame([(None,)], ["col"])
        engine = DQEngine(spark, "dev", "silver", "sales", "orders")
        engine.check_not_null(df_ok,   "col", "ok_table",   threshold=0.0)
        engine.check_not_null(df_fail, "col", "fail_table", threshold=0.0)
        summary = engine.summary()
        assert summary["passed"] == 1
        assert summary["failed"] == 1
        assert summary["total"]  == 2
