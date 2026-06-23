"""
Silver → Gold Transformation: Sales Star Schema
===============================================
Agent: Data Transformation & Modeling Agent

Builds Gold-layer dimensional model from Silver Delta tables.
Implements:
  - dim_customer (SCD Type 2)
  - dim_product  (SCD Type 1)
  - fact_sales   (insert-only fact table partitioned by order_date)

All Gold tables are registered in Unity Catalog under:
  {env}_lakehouse.gold.*

Partitioning & Z-Ordering:
  - fact_sales:   PARTITIONED BY (order_year, order_month), ZORDER BY (customer_id, product_id)
  - dim_customer: ZORDER BY (customer_id)
  - dim_product:  ZORDER BY (product_id)
"""

import argparse
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType


# ---------------------------------------------------------------------------
# SCD Type 2 Handler
# ---------------------------------------------------------------------------

def apply_scd2(
    spark: SparkSession,
    source_df,
    target_table: str,
    natural_keys: list,
    tracked_cols: list,
    surrogate_key: str,
    valid_from_col: str = "_valid_from",
    valid_to_col: str = "_valid_to",
    is_current_col: str = "_is_current",
    effective_date_col: str = None,
) -> None:
    """
    Full SCD Type 2 implementation using Delta MERGE.

    For each incoming record:
    - If the natural key doesn't exist → INSERT as current record
    - If the natural key exists and tracked columns changed:
      1. EXPIRE the existing current record (set _valid_to, _is_current=False)
      2. INSERT new version as current record
    - If the natural key exists and no columns changed → do nothing (no-op MERGE)
    """
    now = datetime.now(timezone.utc)

    source_df.createOrReplaceTempView("_scd2_source")

    # Step 1: Expire changed records
    join_cond  = " AND ".join([f"src.{k} = tgt.{k}" for k in natural_keys])
    change_cond = " OR ".join([f"src.{c} <> tgt.{c}" for c in tracked_cols])

    spark.sql(f"""
        MERGE INTO {target_table} AS tgt
        USING _scd2_source AS src
          ON {join_cond} AND tgt.{is_current_col} = TRUE
        WHEN MATCHED AND ({change_cond}) THEN
            UPDATE SET
                tgt.{valid_to_col}    = CAST('{now.isoformat()}' AS TIMESTAMP),
                tgt.{is_current_col}  = FALSE
    """)

    # Step 2: Insert new versions for changed + brand-new records
    rows_to_insert = spark.sql(f"""
        SELECT src.*
        FROM _scd2_source src
        LEFT JOIN {target_table} tgt
          ON {join_cond} AND tgt.{is_current_col} = TRUE
        WHERE tgt.{natural_keys[0]} IS NULL
           OR ({change_cond})
    """)

    # isEmpty() uses LIMIT 1 internally — far cheaper than count() == 0
    if rows_to_insert.isEmpty():
        return

    rows_to_insert = (
        rows_to_insert
        .withColumn(surrogate_key,    F.expr("uuid()"))
        .withColumn(valid_from_col,   F.lit(now).cast(TimestampType()))
        .withColumn(valid_to_col,     F.lit(None).cast(TimestampType()))
        .withColumn(is_current_col,   F.lit(True))
    )

    rows_to_insert.write.format("delta").mode("append").saveAsTable(target_table)


# ---------------------------------------------------------------------------
# dim_customer
# ---------------------------------------------------------------------------

def build_dim_customer(spark: SparkSession, env: str) -> None:
    """
    Builds / incrementally updates dim_customer using SCD Type 2.
    Source: {env}_lakehouse.silver.sales_customers
    Target: {env}_lakehouse.gold.dim_customer
    """
    silver_table = f"{env}_lakehouse.silver.sales_customers"
    gold_table   = f"{env}_lakehouse.gold.dim_customer"

    # Create Gold table if it doesn't exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            customer_sk         STRING        NOT NULL COMMENT 'Surrogate key (UUID)',
            customer_id         LONG          NOT NULL COMMENT 'Natural key from source',
            first_name          STRING,
            last_name           STRING,
            email               STRING        COMMENT 'PII - masked in non-prod',
            phone               STRING        COMMENT 'PII - masked in non-prod',
            country_code        STRING,
            region              STRING,
            customer_segment    STRING,
            is_active           BOOLEAN,
            _valid_from         TIMESTAMP     NOT NULL,
            _valid_to           TIMESTAMP     COMMENT 'NULL means current record',
            _is_current         BOOLEAN       NOT NULL,
            _silver_run_id      STRING,
            _gold_load_ts       TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'comment' = 'SCD Type 2 customer dimension'
        )
    """)
    # Apply Z-Order optimization after MERGE (typically in a separate OPTIMIZE job)

    # Read Silver source
    source_df = (
        spark.table(silver_table)
             .filter(F.col("_is_current") == True)
             .withColumn("_gold_load_ts", F.current_timestamp())
    )

    apply_scd2(
        spark, source_df,
        target_table=gold_table,
        natural_keys=["customer_id"],
        tracked_cols=["email", "phone", "country_code", "region", "customer_segment", "is_active"],
        surrogate_key="customer_sk",
    )

    # OPTIMIZE + ZORDER (async; run in separate maintenance job in prod)
    spark.sql(f"OPTIMIZE {gold_table} ZORDER BY (customer_id)")
    print(f"[DONE] dim_customer built: {gold_table}")


# ---------------------------------------------------------------------------
# dim_product
# ---------------------------------------------------------------------------

def build_dim_product(spark: SparkSession, env: str) -> None:
    """
    Builds / updates dim_product using SCD Type 1 (overwrite current values).
    Source: {env}_lakehouse.silver.products
    Target: {env}_lakehouse.gold.dim_product
    """
    silver_table = f"{env}_lakehouse.silver.products"
    gold_table   = f"{env}_lakehouse.gold.dim_product"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            product_sk      STRING    NOT NULL COMMENT 'Surrogate key (UUID)',
            product_id      LONG      NOT NULL,
            product_name    STRING,
            category        STRING,
            subcategory     STRING,
            brand           STRING,
            unit_price      DECIMAL(18,2),
            is_active       BOOLEAN,
            _gold_load_ts   TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES ('comment' = 'SCD Type 1 product dimension')
    """)

    source_df = (
        spark.table(silver_table)
             .withColumn("product_sk",    F.expr("uuid()"))
             .withColumn("_gold_load_ts", F.current_timestamp())
    )
    source_df.createOrReplaceTempView("_dim_product_staging")

    # SCD Type 1: full overwrite of changed attributes; no history preserved
    spark.sql(f"""
        MERGE INTO {gold_table} AS tgt
        USING _dim_product_staging AS src ON tgt.product_id = src.product_id
        WHEN MATCHED THEN UPDATE SET
            tgt.product_name  = src.product_name,
            tgt.category      = src.category,
            tgt.subcategory   = src.subcategory,
            tgt.brand         = src.brand,
            tgt.unit_price    = src.unit_price,
            tgt.is_active     = src.is_active,
            tgt._gold_load_ts = src._gold_load_ts
        WHEN NOT MATCHED THEN INSERT *
    """)

    spark.sql(f"OPTIMIZE {gold_table} ZORDER BY (product_id, category)")
    print(f"[DONE] dim_product built: {gold_table}")


# ---------------------------------------------------------------------------
# fact_sales
# ---------------------------------------------------------------------------

def build_fact_sales(spark: SparkSession, env: str) -> None:
    """
    Builds / incrementally appends to fact_sales from Silver orders + items.
    Joins to dim_customer and dim_product to resolve surrogate keys.
    Partitioned by (order_year, order_month) for efficient BI queries.
    """
    silver_orders = f"{env}_lakehouse.silver.sales_orders"
    silver_items  = f"{env}_lakehouse.silver.sales_order_items"
    dim_customer  = f"{env}_lakehouse.gold.dim_customer"
    dim_product   = f"{env}_lakehouse.gold.dim_product"
    gold_table    = f"{env}_lakehouse.gold.fact_sales"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            order_item_sk       STRING        NOT NULL COMMENT 'Surrogate key',
            order_id            LONG          NOT NULL,
            order_item_id       LONG          NOT NULL,
            customer_sk         STRING        COMMENT 'FK to dim_customer',
            product_sk          STRING        COMMENT 'FK to dim_product',
            order_date          DATE          NOT NULL,
            order_year          INT           NOT NULL,
            order_month         INT           NOT NULL,
            ship_date           DATE,
            quantity            INT,
            unit_price          DECIMAL(18,2),
            discount            DECIMAL(5,4),
            gross_amount        DECIMAL(18,2) COMMENT 'quantity * unit_price',
            net_amount          DECIMAL(18,2) COMMENT 'gross_amount * (1 - discount)',
            currency            STRING,
            order_status        STRING,
            _gold_load_ts       TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (order_year, order_month)
        TBLPROPERTIES (
            'comment' = 'Sales fact table — insert-only, partitioned by year/month',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

    # Determine last loaded order_date for incremental load
    last_loaded = spark.sql(f"""
        SELECT COALESCE(MAX(order_date), CAST('1900-01-01' AS DATE)) AS max_dt
        FROM {gold_table}
    """).collect()[0]["max_dt"]

    orders = (
        spark.table(silver_orders)
             .filter(F.col("order_date") > F.lit(last_loaded))
             .alias("o")
    )
    items = spark.table(silver_items).alias("i")

    # Resolve customer surrogate key (current record in SCD2 dimension)
    # broadcast() hint: dim tables are small vs fact — avoids shuffle join
    dim_cust = F.broadcast(
        spark.table(dim_customer)
             .filter(F.col("_is_current") == True)
             .select("customer_id", "customer_sk")
             .alias("dc")
    )
    dim_prod = F.broadcast(
        spark.table(dim_product)
             .select("product_id", "product_sk")
             .alias("dp")
    )

    fact_df = (
        orders.join(items,   F.col("o.order_id")    == F.col("i.order_id"), "inner")
              .join(dim_cust, F.col("o.customer_id") == F.col("dc.customer_id"), "left")
              .join(dim_prod, F.col("i.product_id")  == F.col("dp.product_id"), "left")
              .select(
                  F.expr("uuid()").alias("order_item_sk"),
                  F.col("o.order_id"),
                  F.col("i.order_item_id"),
                  F.col("dc.customer_sk"),
                  F.col("dp.product_sk"),
                  F.col("o.order_date"),
                  F.year("o.order_date").alias("order_year"),
                  F.month("o.order_date").alias("order_month"),
                  F.col("o.ship_date"),
                  F.col("i.quantity"),
                  F.col("i.unit_price"),
                  F.col("i.discount").cast("decimal(5,4)"),
                  (F.col("i.quantity") * F.col("i.unit_price")).cast("decimal(18,2)").alias("gross_amount"),
                  (F.col("i.quantity") * F.col("i.unit_price") * (F.lit(1) - F.col("i.discount"))).cast("decimal(18,2)").alias("net_amount"),
                  F.col("o.currency"),
                  F.col("o.status").alias("order_status"),
                  F.current_timestamp().alias("_gold_load_ts"),
              )
    )

    (
        fact_df.write
               .format("delta")
               .mode("append")
               .partitionBy("order_year", "order_month")
               .saveAsTable(gold_table)
    )

    # ZORDER within partitions for BI query performance
    spark.sql(f"OPTIMIZE {gold_table} ZORDER BY (customer_sk, product_sk)")
    print(f"[DONE] fact_sales built: {gold_table}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver → Gold transformation job")
    parser.add_argument("--env", required=True, help="dev | test | prod")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    build_dim_customer(spark, args.env)
    build_dim_product(spark, args.env)
    build_fact_sales(spark, args.env)
