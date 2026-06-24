"""
scripts/seed_local.py
=====================
Seeds a local developer environment with synthetic Delta Lake tables so that
integration tests and local pipeline runs work without a Databricks cluster.

Run via:
    make seed-local
    # or directly:
    python scripts/seed_local.py [--base-path /tmp/lakehouse_dev] [--clean]

What it creates:
    {base_path}/
        bronze/
            sales_orders/          ← raw sales with intentional quality issues
            customers/             ← raw customer data
            products/              ← raw product data
        silver/
            sales_orders/          ← cleaned + validated sales
            customers/             ← normalised customers
        gold/
            fact_sales/            ← fact table
            dim_customer/          ← customer dimension
            dim_product/           ← product dimension
            gold_customer_clv/     ← CLV feature table (for ML tests)
            sales_predictions/     ← batch inference output placeholder
        mlops/
            drift_metrics/         ← placeholder for drift monitor
            champion_challenger_log/
        governance/
            dq_results/            ← DQ check output
            dq_remediation_log/    ← remediation agent log
            gdpr_erasure_log/      ← GDPR RTBF log

After seeding, set these env vars to point pipelines at the local tables:
    LOCAL_DELTA_BASE=/tmp/lakehouse_dev
    TEST_ENV=dev
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure src/ is importable from any working directory
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType, BooleanType, IntegerType,
)

# Reuse the rich synthetic generators already used by integration tests
from tests.integration.synthetic_data import (
    make_bronze_sales,
    make_bronze_customers,
    make_bronze_products,
    make_silver_sales,
)


# ===========================================================================
# Spark session — local mode with Delta Lake
# ===========================================================================

def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("lakehouse-seed-local")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.preview.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ===========================================================================
# Writers
# ===========================================================================

def _write(df: DataFrame, path: str, *, partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    print(f"  ✓  {path}  ({df.count()} rows)")


# ===========================================================================
# Gold layer derivation helpers (no DLT dependency)
# ===========================================================================

def _make_fact_sales(spark: SparkSession, silver_sales: DataFrame) -> DataFrame:
    return (
        silver_sales
        .withColumn("sales_amount", F.col("net_amount"))
        .withColumn("year_month",   F.date_format("order_date", "yyyy-MM"))
        .select(
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount",
            "net_amount", "sales_amount",
            "order_date", "year_month", "status", "region",
        )
    )


def _make_dim_customer(spark: SparkSession, customers: DataFrame) -> DataFrame:
    return (
        customers
        .withColumn("full_name",
                    F.concat_ws(" ", F.initcap("first_name"), F.initcap("last_name")))
        .withColumn("_effective_start", F.current_timestamp())
        .withColumn("_effective_end",   F.lit(None).cast(TimestampType()))
        .withColumn("_is_current",      F.lit(True))
        .select(
            "customer_id", "full_name", "email", "phone",
            "country_code", "region", "segment",
            "_effective_start", "_effective_end", "_is_current",
        )
    )


def _make_dim_product(spark: SparkSession, products: DataFrame) -> DataFrame:
    return products.select(
        "product_id", "product_name", "category",
        "subcategory", "brand", "unit_price", "is_active",
    )


def _make_clv_features(spark: SparkSession, fact_sales: DataFrame) -> DataFrame:
    return (
        fact_sales
        .groupBy("customer_id")
        .agg(
            F.sum("net_amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("net_amount").alias("avg_order_value"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn("clv_score", F.col("total_revenue") * 0.8 + F.col("order_count") * 5.0)
    )


def _make_sales_predictions(spark: SparkSession, clv: DataFrame) -> DataFrame:
    """Placeholder prediction rows for batch-inference / drift monitor tests."""
    return (
        clv
        .withColumn("predicted_clv",      F.col("clv_score") * 1.05)
        .withColumn("lower_bound",        F.col("clv_score") * 0.90)
        .withColumn("upper_bound",        F.col("clv_score") * 1.20)
        .withColumn("model_version",      F.lit("1.0-seed"))
        .withColumn("inference_ts",       F.current_timestamp())
        .select(
            "customer_id", "predicted_clv", "lower_bound",
            "upper_bound", "model_version", "inference_ts",
        )
    )


# ===========================================================================
# Empty governance / mlops tables
# ===========================================================================

def _empty_dq_results(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("table_name",    StringType(),    nullable=False),
        StructField("check_name",    StringType(),    nullable=False),
        StructField("status",        StringType(),    nullable=False),
        StructField("failed_rows",   LongType(),      nullable=True),
        StructField("run_ts",        TimestampType(), nullable=False),
    ])
    return spark.createDataFrame([], schema)


def _empty_remediation_log(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("run_id",        StringType(),    nullable=False),
        StructField("table_name",    StringType(),    nullable=False),
        StructField("check_name",    StringType(),    nullable=False),
        StructField("strategy",      StringType(),    nullable=False),
        StructField("rows_affected", LongType(),      nullable=True),
        StructField("applied_at",    TimestampType(), nullable=False),
    ])
    return spark.createDataFrame([], schema)


def _empty_gdpr_log(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("request_id",    StringType(),    nullable=False),
        StructField("subject_id",    StringType(),    nullable=False),
        StructField("mode",          StringType(),    nullable=False),
        StructField("tables_updated", LongType(),     nullable=True),
        StructField("executed_at",   TimestampType(), nullable=False),
    ])
    return spark.createDataFrame([], schema)


def _empty_drift_metrics(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("feature",       StringType(),    nullable=False),
        StructField("psi",           DoubleType(),    nullable=True),
        StructField("ks_stat",       DoubleType(),    nullable=True),
        StructField("ks_pvalue",     DoubleType(),    nullable=True),
        StructField("js_divergence", DoubleType(),    nullable=True),
        StructField("drift_flag",    BooleanType(),   nullable=True),
        StructField("measured_at",   TimestampType(), nullable=False),
    ])
    return spark.createDataFrame([], schema)


def _empty_champion_challenger(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("run_date",           StringType(),  nullable=False),
        StructField("champion_rmse",      DoubleType(),  nullable=True),
        StructField("challenger_rmse",    DoubleType(),  nullable=True),
        StructField("champion_promoted",  BooleanType(), nullable=True),
        StructField("logged_at",          TimestampType(), nullable=False),
    ])
    return spark.createDataFrame([], schema)


# ===========================================================================
# Main
# ===========================================================================

def seed(base_path: str) -> None:
    print(f"\nSeeding local Delta tables at: {base_path}\n")
    spark = _build_spark()

    # ------------------------------------------------------------------
    # Bronze
    # ------------------------------------------------------------------
    print("Bronze layer:")
    bronze_sales     = make_bronze_sales(spark, n_valid=200, n_bad=10)
    bronze_customers = make_bronze_customers(spark, n=40)
    bronze_products  = make_bronze_products(spark, n=20)

    _write(bronze_sales,     f"{base_path}/bronze/sales_orders",  partition_by=["region"])
    _write(bronze_customers, f"{base_path}/bronze/customers")
    _write(bronze_products,  f"{base_path}/bronze/products")

    # ------------------------------------------------------------------
    # Silver
    # ------------------------------------------------------------------
    print("\nSilver layer:")
    silver_sales = make_silver_sales(spark, n=200)
    _write(silver_sales, f"{base_path}/silver/sales_orders", partition_by=["region"])

    silver_customers = (
        bronze_customers
        .withColumn("first_name", F.initcap("first_name"))
        .withColumn("last_name",  F.initcap("last_name"))
        .withColumn("email",      F.lower("email"))
    )
    _write(silver_customers, f"{base_path}/silver/customers")

    # ------------------------------------------------------------------
    # Gold
    # ------------------------------------------------------------------
    print("\nGold layer:")
    fact_sales  = _make_fact_sales(spark, silver_sales)
    dim_customer = _make_dim_customer(spark, silver_customers)
    dim_product  = _make_dim_product(spark, bronze_products)
    clv_features = _make_clv_features(spark, fact_sales)
    predictions  = _make_sales_predictions(spark, clv_features)

    _write(fact_sales,    f"{base_path}/gold/fact_sales",         partition_by=["year_month"])
    _write(dim_customer,  f"{base_path}/gold/dim_customer")
    _write(dim_product,   f"{base_path}/gold/dim_product")
    _write(clv_features,  f"{base_path}/gold/gold_customer_clv")
    _write(predictions,   f"{base_path}/gold/sales_predictions")

    # ------------------------------------------------------------------
    # Governance / MLOps (empty — schema stubs for agent/monitor tests)
    # ------------------------------------------------------------------
    print("\nGovernance & MLOps stubs:")
    _write(_empty_dq_results(spark),          f"{base_path}/governance/dq_results")
    _write(_empty_remediation_log(spark),     f"{base_path}/governance/dq_remediation_log")
    _write(_empty_gdpr_log(spark),            f"{base_path}/governance/gdpr_erasure_log")
    _write(_empty_drift_metrics(spark),       f"{base_path}/mlops/drift_metrics")
    _write(_empty_champion_challenger(spark), f"{base_path}/mlops/champion_challenger_log")

    spark.stop()
    print(f"\nDone — run 'make test' to verify everything works.\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed local Delta Lake tables for development"
    )
    parser.add_argument(
        "--base-path",
        default=os.environ.get("LOCAL_DELTA_BASE", "/tmp/lakehouse_dev"),
        help="Root directory for local Delta tables (default: $LOCAL_DELTA_BASE or /tmp/lakehouse_dev)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing tables before seeding (fresh start)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    base = args.base_path

    if args.clean and Path(base).exists():
        print(f"--clean specified: removing {base}")
        shutil.rmtree(base)

    seed(base)
