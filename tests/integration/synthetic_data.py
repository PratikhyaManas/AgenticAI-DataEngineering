"""
Synthetic Data Generators for Integration Tests
================================================
Produces realistic but fictional DataFrames for all pipeline layers:
  Bronze: raw sales orders, customers, products (with intentional quality issues)
  Silver: cleaned versions
  Gold:   star schema tables

All data uses fixed seeds so test assertions are deterministic.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType,
)


# ---------------------------------------------------------------------------
# Bronze: raw sales orders (with deliberate quality issues)
# ---------------------------------------------------------------------------

BRONZE_SALES_SCHEMA = StructType([
    StructField("order_id",       StringType(),    nullable=True),
    StructField("customer_id",    StringType(),    nullable=True),
    StructField("product_id",     StringType(),    nullable=True),
    StructField("quantity",       IntegerType(),   nullable=True),
    StructField("unit_price",     DoubleType(),    nullable=True),
    StructField("discount",       DoubleType(),    nullable=True),
    StructField("order_date",     StringType(),    nullable=True),   # raw string
    StructField("status",         StringType(),    nullable=True),
    StructField("region",         StringType(),    nullable=True),
    StructField("_ingestion_ts",  TimestampType(), nullable=True),
])


def make_bronze_sales(spark: SparkSession, n_valid: int = 50, n_bad: int = 10) -> DataFrame:
    """
    Generate raw Bronze sales rows, including rows with quality issues:
      - null order_id
      - null customer_id
      - negative quantity
      - null order_date
      - duplicate order_ids (2 copies of order "DUP-001")
    """
    rng = random.Random(42)
    now = datetime(2024, 6, 1)
    statuses = ["COMPLETED", "PENDING", "SHIPPED", "CANCELLED"]
    regions  = ["EMEA", "APAC", "AMER", "LATAM"]

    rows = []
    for i in range(n_valid):
        rows.append((
            f"ORD-{i:05d}",
            f"CUST-{rng.randint(1, 20):04d}",
            f"PROD-{rng.randint(1, 10):04d}",
            rng.randint(1, 20),
            round(rng.uniform(10.0, 500.0), 2),
            round(rng.uniform(0.0, 0.3), 2),
            (now - timedelta(days=rng.randint(0, 365))).strftime("%Y-%m-%d"),
            rng.choice(statuses),
            rng.choice(regions),
            now,
        ))

    # Duplicate row
    rows.append(("DUP-001", "CUST-0001", "PROD-0001", 5, 99.99, 0.0, "2024-05-01", "COMPLETED", "EMEA", now))
    rows.append(("DUP-001", "CUST-0001", "PROD-0001", 5, 99.99, 0.0, "2024-05-01", "COMPLETED", "EMEA", now))

    # Bad rows (intentional quality issues)
    bad = [
        (None,      "CUST-0002", "PROD-0002", 2,  50.0, 0.0, "2024-01-15", "COMPLETED", "EMEA", now),   # null order_id
        ("ORD-BAD1", None,       "PROD-0003", 1, 100.0, 0.0, "2024-01-16", "COMPLETED", "AMER", now),   # null customer_id
        ("ORD-BAD2", "CUST-0003", "PROD-0004", -5, 75.0, 0.0, "2024-01-17", "COMPLETED", "APAC", now), # negative qty
        ("ORD-BAD3", "CUST-0004", "PROD-0005", 3,  -10.0, 0.0, "2024-01-18", "COMPLETED", "LATAM", now), # negative price
        ("ORD-BAD4", "CUST-0005", "PROD-0006", 2,  60.0, 0.0, None,        "COMPLETED", "EMEA", now),   # null date
    ]
    rows.extend(bad[:n_bad])

    return spark.createDataFrame(rows, schema=BRONZE_SALES_SCHEMA)


# ---------------------------------------------------------------------------
# Bronze: customers
# ---------------------------------------------------------------------------

BRONZE_CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",  StringType(),  nullable=True),
    StructField("first_name",   StringType(),  nullable=True),
    StructField("last_name",    StringType(),  nullable=True),
    StructField("email",        StringType(),  nullable=True),
    StructField("phone",        StringType(),  nullable=True),
    StructField("country_code", StringType(),  nullable=True),
    StructField("region",       StringType(),  nullable=True),
    StructField("segment",      StringType(),  nullable=True),
    StructField("updated_at",   TimestampType(), nullable=True),
])

_FIRST_NAMES = ["Alice", "Bob", "carol", "DAVE", "eve"]    # mixed case intentional
_LAST_NAMES  = ["Smith", "JONES", "müller", "garcia", "li"]
_SEGMENTS    = ["Enterprise", "SMB", "Consumer"]
_COUNTRIES   = ["DE", "US", "GB", "FR", "JP"]
_REGIONS     = ["EMEA", "AMER", "APAC", "LATAM"]


def make_bronze_customers(spark: SparkSession, n: int = 20) -> DataFrame:
    rng = random.Random(7)
    now = datetime(2024, 6, 1)
    rows = [
        (
            f"CUST-{i:04d}",
            rng.choice(_FIRST_NAMES),
            rng.choice(_LAST_NAMES),
            f"user{i}@example.com",
            f"+49-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
            rng.choice(_COUNTRIES),
            rng.choice(_REGIONS),
            rng.choice(_SEGMENTS),
            now - timedelta(days=rng.randint(0, 90)),
        )
        for i in range(1, n + 1)
    ]
    return spark.createDataFrame(rows, schema=BRONZE_CUSTOMER_SCHEMA)


# ---------------------------------------------------------------------------
# Bronze: products
# ---------------------------------------------------------------------------

BRONZE_PRODUCT_SCHEMA = StructType([
    StructField("product_id",   StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("category",     StringType(), nullable=True),
    StructField("subcategory",  StringType(), nullable=True),
    StructField("brand",        StringType(), nullable=True),
    StructField("unit_price",   DoubleType(), nullable=True),
    StructField("is_active",    BooleanType(), nullable=True),
])

_CATEGORIES = ["Optics", "Microscopy", "Metrology", "Medical", "Consumer"]


def make_bronze_products(spark: SparkSession, n: int = 10) -> DataFrame:
    rng = random.Random(13)
    rows = [
        (
            f"PROD-{i:04d}",
            f"Product {i}",
            rng.choice(_CATEGORIES),
            f"Sub-{rng.randint(1,3)}",
            "Zeiss",
            round(rng.uniform(50.0, 5000.0), 2),
            True,
        )
        for i in range(1, n + 1)
    ]
    return spark.createDataFrame(rows, schema=BRONZE_PRODUCT_SCHEMA)


# ---------------------------------------------------------------------------
# Silver: cleaned sales (what Bronze → Silver job should produce)
# ---------------------------------------------------------------------------

def make_silver_sales(spark: SparkSession, n: int = 50) -> DataFrame:
    """Silver sales — all rows valid, dates as DateType, net_amount computed."""
    bronze = make_bronze_sales(spark, n_valid=n, n_bad=0)
    return (
        bronze
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .filter(F.col("order_date").isNotNull())
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("net_amount",
                    F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")))
        .withColumn("region", F.upper("region"))
        .drop("_ingestion_ts")
    )
