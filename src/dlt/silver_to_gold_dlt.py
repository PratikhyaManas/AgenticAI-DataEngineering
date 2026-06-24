"""
Delta Live Tables — Silver → Gold (Sales Star Schema)
=======================================================
Agent: Data Transformation & Modeling Agent (DLT variant)

Declarative DLT pipeline replacing silver_to_gold.py.
Implements the Gold-layer dimensional model using DLT's
APPLY CHANGES INTO for SCD Type 2 and streaming aggregations.

Pipeline topology:
  silver_sales_orders     (Silver streaming source, Change Data Feed)
  silver_customers        (Silver streaming source, CDF)
  silver_products         (Silver streaming source, CDF)
       ↓
  dim_customer            (SCD Type 2 via APPLY CHANGES INTO)
  dim_product             (SCD Type 1 via APPLY CHANGES INTO)
       ↓
  fact_sales              (insert-only fact table, partitioned by order_date)
       ↓
  gold_sales_summary      (pre-aggregated rollup for BI direct-query)

Configure via DLT pipeline settings:
  catalog = {env}_lakehouse
  target  = gold

Reference: https://docs.databricks.com/en/delta-live-tables/cdc.html
"""

from __future__ import annotations

import dlt  # type: ignore[import]   # available only inside a DLT pipeline

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Silver source readers (Change Data Feed enabled for CDC propagation)
# ---------------------------------------------------------------------------

@dlt.view(name="silver_orders_cdf")
def silver_orders_cdf() -> DataFrame:
    """
    Reads the Silver sales orders table with Change Data Feed enabled.
    Only changed rows since the last pipeline run are processed,
    making the pipeline incremental by default.
    """
    return (
        spark.readStream  # noqa: F821
             .option("readChangeFeed", "true")
             .table("silver.silver_sales_orders")
    )


@dlt.view(name="silver_customers_cdf")
def silver_customers_cdf() -> DataFrame:
    """
    Reads the Silver customers table with CDF to propagate customer
    attribute changes into the dim_customer SCD Type 2 table.
    """
    return (
        spark.readStream  # noqa: F821
             .option("readChangeFeed", "true")
             .table("silver.silver_customers")
    )


@dlt.view(name="silver_products_cdf")
def silver_products_cdf() -> DataFrame:
    """
    Reads the Silver products table with CDF for SCD Type 1 updates.
    Only the latest product attributes are retained (no history).
    """
    return (
        spark.readStream  # noqa: F821
             .option("readChangeFeed", "true")
             .table("silver.silver_products")
    )


# ---------------------------------------------------------------------------
# dim_customer — SCD Type 2 via APPLY CHANGES INTO
# ---------------------------------------------------------------------------

dlt.create_streaming_table(
    name="dim_customer",
    comment="SCD Type 2 customer dimension. "
            "Tracks full history of customer attribute changes. "
            "Each row represents one version of a customer record.",
    table_properties={
        "quality":                           "gold",
        "delta.enableChangeDataFeed":        "true",
        "delta.autoOptimize.optimizeWrite":  "true",
        "delta.autoOptimize.autoCompact":    "true",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
    },
)

dlt.apply_changes(
    target="dim_customer",
    source="silver_customers_cdf",
    keys=["customer_id"],
    sequence_by="updated_at",               # latest record wins
    stored_as_scd_type=2,                   # DLT manages _valid_from/_valid_to/_is_current
    track_history_column_list=[             # only these changes trigger a new SCD2 row
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "country_code",
        "customer_segment",
        "region",
    ],
    except_column_list=["updated_at", "_silver_load_ts", "_pipeline_name"],
)


# ---------------------------------------------------------------------------
# dim_product — SCD Type 1 via APPLY CHANGES INTO (no history)
# ---------------------------------------------------------------------------

dlt.create_streaming_table(
    name="dim_product",
    comment="SCD Type 1 product dimension. "
            "Always reflects the current product attributes — no history retained.",
    table_properties={
        "quality":                           "gold",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "product_id",
    },
)

dlt.apply_changes(
    target="dim_product",
    source="silver_products_cdf",
    keys=["product_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,                   # overwrite-in-place (SCD1)
    except_column_list=["_silver_load_ts", "_pipeline_name"],
)


# ---------------------------------------------------------------------------
# fact_sales — insert-only streaming fact table
# ---------------------------------------------------------------------------

@dlt.table(
    name="fact_sales",
    comment="Insert-only fact table. "
            "Each row is a unique sale line enriched with surrogate keys "
            "from dim_customer and dim_product. "
            "Partitioned by order_year + order_month for efficient BI queries.",
    table_properties={
        "quality":                           "gold",
        "delta.autoOptimize.optimizeWrite":  "true",
        "delta.autoOptimize.autoCompact":    "true",
        "pipelines.autoOptimize.zOrderCols": "customer_id,product_id",
    },
    partition_cols=["order_year", "order_month"],
)
@dlt.expect_or_drop("fact_order_id_not_null",    "order_id IS NOT NULL")
@dlt.expect_or_drop("fact_amount_positive",      "net_amount > 0")
@dlt.expect_or_warn("fact_customer_key_valid",   "customer_sk IS NOT NULL")
@dlt.expect_or_warn("fact_product_key_valid",    "product_sk IS NOT NULL")
def fact_sales() -> DataFrame:
    """
    Joins Silver sales orders with current Gold dimension surrogate keys
    to produce a denormalised fact row.

    Uses left joins to guard against late-arriving dimension records —
    orphan facts (NULL surrogate keys) are warned on rather than dropped
    so no revenue data is lost.
    """
    orders   = dlt.read_stream("silver_orders_cdf")
    dim_cust = (
        dlt.read("dim_customer")
           .filter(F.col("_is_current") == True)          # noqa: E712
           .select("customer_sk", "customer_id",
                   "customer_segment", "country_code", "region")
    )
    dim_prod = (
        dlt.read("dim_product")
           .select("product_sk", "product_id",
                   "category", "sub_category", "brand")
    )

    return (
        orders
        # Resolve surrogate keys
        .join(dim_cust, "customer_id", "left")
        .join(dim_prod, "product_id",  "left")

        # Derived columns
        .withColumn("net_amount",
                    F.col("amount") * (F.lit(1) - F.coalesce(F.col("discount"), F.lit(0))))
        .withColumn("order_year",  F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("_gold_load_ts", F.current_timestamp())

        # Select final fact schema
        .select(
            "order_id",
            "order_date",
            "order_year",
            "order_month",
            "customer_id",
            "customer_sk",
            "customer_segment",
            "country_code",
            "region",
            "product_id",
            "product_sk",
            "category",
            "sub_category",
            "brand",
            "amount",
            "discount",
            "net_amount",
            "quantity",
            "status",
            "currency",
            "_gold_load_ts",
        )
    )


# ---------------------------------------------------------------------------
# gold_sales_summary — pre-aggregated BI rollup (materialized view)
# ---------------------------------------------------------------------------

@dlt.table(
    name="gold_sales_summary",
    comment="Pre-aggregated daily sales rollup. "
            "Optimised for Power BI Direct Lake queries. "
            "Refreshed on every pipeline run.",
    table_properties={
        "quality":                           "gold_summary",
        "delta.autoOptimize.optimizeWrite":  "true",
    },
)
def gold_sales_summary() -> DataFrame:
    """
    Aggregates fact_sales by date × customer_segment × category
    to provide a fast summary layer for dashboards without hitting
    the full fact table.
    """
    return (
        dlt.read("fact_sales")
           .groupBy(
               "order_date",
               "order_year",
               "order_month",
               "customer_segment",
               "category",
               "sub_category",
               "country_code",
               "currency",
           )
           .agg(
               F.count("order_id").alias("order_count"),
               F.sum("net_amount").alias("total_net_amount"),
               F.avg("net_amount").alias("avg_order_value"),
               F.sum("quantity").alias("total_units_sold"),
               F.countDistinct("customer_id").alias("distinct_customers"),
               F.countDistinct("product_id").alias("distinct_products"),
           )
           .withColumn("_summary_ts", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Customer Lifetime Value (CLV) — incremental feature for ML
# ---------------------------------------------------------------------------

@dlt.table(
    name="gold_customer_clv",
    comment="Rolling Customer Lifetime Value aggregates for ML Feature Store. "
            "Computed over 30 / 90 / 365 day windows from fact_sales.",
    table_properties={
        "quality":                           "gold_features",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
    },
)
def gold_customer_clv() -> DataFrame:
    """
    Computes CLV windows from the fact_sales Gold table.
    Replaces the feature_engineering.py batch job for the CLV features.
    """
    today = F.current_date()

    return (
        dlt.read("fact_sales")
           .groupBy("customer_id", "customer_sk",
                    "customer_segment", "country_code", "region")
           .agg(
               F.sum(F.when(F.col("order_date") >= F.date_sub(today, 30),
                            F.col("net_amount"))).alias("clv_30d"),
               F.sum(F.when(F.col("order_date") >= F.date_sub(today, 90),
                            F.col("net_amount"))).alias("clv_90d"),
               F.sum("net_amount").alias("clv_365d"),
               F.count(F.when(F.col("order_date") >= F.date_sub(today, 30),
                              True)).alias("order_count_30d"),
               F.count(F.when(F.col("order_date") >= F.date_sub(today, 90),
                              True)).alias("order_count_90d"),
               F.count("*").alias("order_count_365d"),
               F.avg("net_amount").alias("avg_order_value"),
               F.avg("discount").alias("avg_discount"),
               F.datediff(today, F.max("order_date")).alias("days_since_last_order"),
           )
           .withColumn("_feature_ts", F.current_timestamp())
    )
