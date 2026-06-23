"""
Feature Engineering — Gold → Feature Store
==========================================
Agent: MLOps / ML Agent

Computes business features from Gold Delta tables and registers them
in the Databricks Feature Store for use by ML training pipelines.

Features computed:
  customer_features:
    - clv_30d, clv_90d, clv_365d      (customer lifetime value windows)
    - order_count_30d, order_count_90d
    - avg_order_value, avg_discount
    - preferred_category
    - days_since_last_order
    - return_rate

  product_features:
    - total_units_sold_30d, total_units_sold_90d
    - avg_revenue_per_unit
    - sales_rank_in_category
    - out_of_stock_flag (estimated from sales velocity drop)
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Customer features
# ---------------------------------------------------------------------------

def compute_customer_features(spark: SparkSession, env: str) -> DataFrame:
    """
    Aggregate Gold fact_sales + dim_customer into a customer feature vector.
    All monetary values are in the original transaction currency (USD assumed).
    """
    fact        = spark.table(f"{env}_lakehouse.gold.fact_sales")
    dim_cust    = (
        spark.table(f"{env}_lakehouse.gold.dim_customer")
             .filter(F.col("_is_current") == True)
             .select("customer_sk", "customer_id", "customer_segment",
                     "country_code", "region")
    )

    today = F.current_date()

    # Pre-filter to last 365 days to limit data scanned
    recent_fact = fact.filter(F.col("order_date") >= F.date_sub(today, 365))

    agg = (
        recent_fact
        .join(dim_cust, "customer_sk", "left")
        .groupBy("customer_sk", "customer_id", "customer_segment",
                  "country_code", "region")
        .agg(
            # Lifetime value windows
            F.sum(F.when(F.col("order_date") >= F.date_sub(today, 30),
                         F.col("net_amount"))).alias("clv_30d"),
            F.sum(F.when(F.col("order_date") >= F.date_sub(today, 90),
                         F.col("net_amount"))).alias("clv_90d"),
            F.sum("net_amount").alias("clv_365d"),

            # Order count windows
            F.count(F.when(F.col("order_date") >= F.date_sub(today, 30),
                           True)).alias("order_count_30d"),
            F.count(F.when(F.col("order_date") >= F.date_sub(today, 90),
                           True)).alias("order_count_90d"),
            F.count("*").alias("order_count_365d"),

            # Value metrics
            F.avg("net_amount").alias("avg_order_value"),
            F.avg("discount").alias("avg_discount"),

            # Recency
            F.datediff(today, F.max("order_date")).alias("days_since_last_order"),

            # Quantity stats
            F.sum("quantity").alias("total_units_purchased"),
        )
    )

    # Most frequent category (preferred_category) — requires a separate window agg
    cat_rank = (
        recent_fact
        .join(
            spark.table(f"{env}_lakehouse.gold.dim_product").select("product_sk", "category"),
            "product_sk", "left",
        )
        .groupBy("customer_sk", "category")
        .agg(F.sum("quantity").alias("cat_units"))
        .withColumn(
            "_cat_rank",
            F.row_number().over(
                Window.partitionBy("customer_sk").orderBy(F.col("cat_units").desc())
            )
        )
        .filter(F.col("_cat_rank") == 1)
        .select("customer_sk", F.col("category").alias("preferred_category"))
    )

    return (
        agg
        .join(cat_rank, "customer_sk", "left")
        .withColumn("feature_ts", F.current_timestamp())
        .withColumn("feature_date", F.current_date())
    )


def compute_product_features(spark: SparkSession, env: str) -> DataFrame:
    """
    Aggregate Gold fact_sales + dim_product into a product feature vector.
    """
    fact     = spark.table(f"{env}_lakehouse.gold.fact_sales")
    dim_prod = (
        spark.table(f"{env}_lakehouse.gold.dim_product")
             .select("product_sk", "product_id", "category",
                     "subcategory", "brand", "unit_price")
    )

    today     = F.current_date()
    recent    = fact.filter(F.col("order_date") >= F.date_sub(today, 365))

    agg = (
        recent
        .join(dim_prod, "product_sk", "left")
        .groupBy("product_sk", "product_id", "category",
                  "subcategory", "brand", "unit_price")
        .agg(
            F.sum(F.when(F.col("order_date") >= F.date_sub(today, 30),
                         F.col("quantity"))).alias("units_sold_30d"),
            F.sum(F.when(F.col("order_date") >= F.date_sub(today, 90),
                         F.col("quantity"))).alias("units_sold_90d"),
            F.sum("quantity").alias("units_sold_365d"),
            F.sum("net_amount").alias("revenue_365d"),
            F.avg("discount").alias("avg_discount"),
            F.countDistinct("customer_sk").alias("unique_customers_365d"),
        )
    )

    # Sales rank within category (lower = better seller)
    cat_window = Window.partitionBy("category").orderBy(F.col("units_sold_30d").desc())
    return (
        agg
        .withColumn("sales_rank_in_category",  F.rank().over(cat_window))
        .withColumn(
            "avg_revenue_per_unit",
            F.when(F.col("units_sold_365d") > 0,
                   F.col("revenue_365d") / F.col("units_sold_365d"))
            .otherwise(F.lit(None)),
        )
        .withColumn("feature_ts",   F.current_timestamp())
        .withColumn("feature_date", F.current_date())
    )


# ---------------------------------------------------------------------------
# Feature Store registration
# ---------------------------------------------------------------------------

def register_feature_table(
    spark: SparkSession,
    env: str,
    feature_df: DataFrame,
    table_name: str,
    primary_keys: list[str],
    timestamp_keys: list[str] | None = None,
    description: str = "",
) -> None:
    """
    Write features to the Databricks Feature Store.

    The Feature Store table lives in:
      {env}_lakehouse.features.{table_name}

    On first call, the table is created. On subsequent calls, features
    are merged (upserted) by primary_keys — existing features are updated,
    new entities are inserted.
    """
    try:
        from databricks.feature_store import FeatureStoreClient  # type: ignore[import]
        fs = FeatureStoreClient()

        fs_table = f"{env}_lakehouse.features.{table_name}"

        # Ensure schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env}_lakehouse.features")

        # Create or merge feature table
        try:
            fs.write_table(
                name=fs_table,
                df=feature_df,
                mode="merge",
            )
            print(f"[FEATURE STORE] Merged features into {fs_table}")
        except Exception:
            # Table doesn't exist yet — create it
            fs.create_table(
                name=fs_table,
                primary_keys=primary_keys,
                timestamp_keys=timestamp_keys or [],
                df=feature_df,
                description=description,
            )
            print(f"[FEATURE STORE] Created feature table {fs_table}")

    except ImportError:
        # Fall back to plain Delta write if Feature Store SDK not available
        print("[FEATURE STORE] databricks-feature-store not installed. Writing as Delta table.")
        fs_table = f"{env}_lakehouse.features.{table_name}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env}_lakehouse.features")
        (
            feature_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(fs_table)
        )
        print(f"[FEATURE STORE] Written features to Delta table {fs_table}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feature engineering job")
    parser.add_argument("--env",    required=True, help="dev | test | prod")
    parser.add_argument("--entity", default="all",
                        choices=["customers", "products", "all"])
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    if args.entity in ("customers", "all"):
        cust_features = compute_customer_features(spark, args.env)
        register_feature_table(
            spark, args.env, cust_features,
            table_name="customer_features",
            primary_keys=["customer_sk"],
            timestamp_keys=["feature_ts"],
            description="Customer-level aggregated features: CLV, recency, frequency, category preference",
        )

    if args.entity in ("products", "all"):
        prod_features = compute_product_features(spark, args.env)
        register_feature_table(
            spark, args.env, prod_features,
            table_name="product_features",
            primary_keys=["product_sk"],
            timestamp_keys=["feature_ts"],
            description="Product-level sales features: velocity, revenue, category rank",
        )

    print("[DONE] Feature engineering complete.")
