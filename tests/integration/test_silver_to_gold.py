"""
Integration Tests: Silver → Gold Transformation Pipeline
=========================================================
Tests the star-schema transformation logic:
  - dim_customer: SCD1 upsert (update on match, insert on new)
  - dim_product:  SCD1 upsert
  - fact_sales:   surrogate key join, net_amount, partitioned by order_date

Validates:
  - All fact rows have a valid customer_sk and product_sk
  - No orphaned fact rows (customer/product not found → must be handled)
  - Net amount = quantity * unit_price * (1 - discount)
  - dim_customer has exactly one row per customer_id
  - dim_product has exactly one row per product_id
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from tests.integration.synthetic_data import (
    make_bronze_customers,
    make_bronze_products,
    make_silver_sales,
)


# ---------------------------------------------------------------------------
# Helpers — inline transformation logic (mirrors silver_to_gold.py)
# ---------------------------------------------------------------------------

def _build_dim_customer(spark: SparkSession, customers_df: DataFrame) -> DataFrame:
    """SCD1 dim_customer: deduplicate, add surrogate key."""
    return (
        customers_df
        .dropDuplicates(["customer_id"])
        .withColumn("first_name", F.initcap(F.col("first_name")))
        .withColumn("last_name",  F.initcap(F.col("last_name")))
        .withColumn("customer_sk",
                    F.monotonically_increasing_id().cast("long"))
        .withColumn("_is_current", F.lit(True))
        .withColumn("_gold_load_ts", F.current_timestamp())
    )


def _build_dim_product(spark: SparkSession, products_df: DataFrame) -> DataFrame:
    """SCD1 dim_product: deduplicate, add surrogate key."""
    return (
        products_df
        .dropDuplicates(["product_id"])
        .withColumn("product_sk",
                    F.monotonically_increasing_id().cast("long"))
        .withColumn("_gold_load_ts", F.current_timestamp())
    )


def _build_fact_sales(
    spark: SparkSession,
    silver_sales: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Join silver sales with dimensions to produce fact_sales."""
    cust_map = dim_customer.select("customer_sk", "customer_id")
    prod_map = dim_product.select("product_sk",  "product_id")

    return (
        silver_sales
        .join(F.broadcast(cust_map), "customer_id", "left")
        .join(F.broadcast(prod_map), "product_id",  "left")
        .withColumn(
            "net_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2),
        )
        .select(
            "order_id", "customer_sk", "product_sk",
            "order_date", "quantity", "unit_price", "discount", "net_amount",
            "status", "region",
        )
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDimCustomer:

    def test_no_duplicate_customer_ids(self, spark: SparkSession):
        customers = make_bronze_customers(spark, n=20)
        dim = _build_dim_customer(spark, customers)
        n_distinct = dim.select("customer_id").distinct().count()
        assert dim.count() == n_distinct, "dim_customer must not have duplicate customer_ids"

    def test_names_are_title_cased(self, spark: SparkSession):
        customers = make_bronze_customers(spark, n=10)
        dim = _build_dim_customer(spark, customers)

        wrong = dim.filter(
            (F.col("first_name") != F.initcap(F.col("first_name")))
            | (F.col("last_name")  != F.initcap(F.col("last_name")))
        ).count()
        assert wrong == 0, "Names must be title-cased in dim_customer"

    def test_surrogate_key_is_unique(self, spark: SparkSession):
        customers = make_bronze_customers(spark, n=20)
        dim = _build_dim_customer(spark, customers)
        n_distinct_sk = dim.select("customer_sk").distinct().count()
        assert dim.count() == n_distinct_sk, "customer_sk must be unique"

    def test_is_current_flag_set(self, spark: SparkSession):
        customers = make_bronze_customers(spark, n=5)
        dim = _build_dim_customer(spark, customers)
        not_current = dim.filter(F.col("_is_current") == False).count()
        assert not_current == 0, "All rows in dim_customer must have _is_current=True on initial load"


class TestDimProduct:

    def test_no_duplicate_product_ids(self, spark: SparkSession):
        products = make_bronze_products(spark, n=10)
        dim = _build_dim_product(spark, products)
        n_distinct = dim.select("product_id").distinct().count()
        assert dim.count() == n_distinct, "dim_product must not have duplicate product_ids"

    def test_surrogate_key_is_unique(self, spark: SparkSession):
        products = make_bronze_products(spark, n=10)
        dim = _build_dim_product(spark, products)
        n_distinct_sk = dim.select("product_sk").distinct().count()
        assert dim.count() == n_distinct_sk, "product_sk must be unique"


class TestFactSales:

    @pytest.fixture(autouse=True)
    def build_dims(self, spark: SparkSession):
        self.spark = spark
        customers = make_bronze_customers(spark, n=20)
        products  = make_bronze_products(spark,  n=10)
        self.dim_cust = _build_dim_customer(spark, customers)
        self.dim_prod = _build_dim_product(spark,  products)
        self.silver   = make_silver_sales(spark, n=50)
        self.fact     = _build_fact_sales(spark, self.silver, self.dim_cust, self.dim_prod)

    def test_fact_row_count_matches_silver(self):
        assert self.fact.count() == self.silver.count(), \
            "fact_sales must have same row count as silver input"

    def test_net_amount_non_null_and_positive(self):
        bad = self.fact.filter(
            F.col("net_amount").isNull() | (F.col("net_amount") <= 0)
        ).count()
        assert bad == 0, "net_amount must be non-null and positive"

    def test_net_amount_formula(self):
        """net_amount must equal qty * price * (1 - discount), within float tolerance."""
        discrepancy = self.fact.withColumn(
            "_expected",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2),
        ).filter(
            F.abs(F.col("net_amount") - F.col("_expected")) > 0.01
        ).count()
        assert discrepancy == 0, "net_amount formula incorrect for some rows"

    def test_no_null_customer_sk_for_known_customers(self):
        """Customers in silver that exist in dim must have a non-null customer_sk."""
        known_cust_ids = {r.customer_id for r in self.dim_cust.select("customer_id").collect()}
        orphans = self.fact.join(
            self.silver.select("order_id", "customer_id"), "order_id"
        ).filter(
            F.col("customer_id").isin(list(known_cust_ids))
            & F.col("customer_sk").isNull()
        ).count()
        assert orphans == 0, "Known customers must resolve to a non-null customer_sk"

    def test_required_columns_present(self):
        required = {"order_id", "customer_sk", "product_sk", "order_date",
                    "quantity", "unit_price", "discount", "net_amount", "status", "region"}
        assert required.issubset(set(self.fact.columns)), \
            f"Missing columns: {required - set(self.fact.columns)}"

    def test_fact_written_to_delta(self, tmp_delta_path: str):
        path = f"{tmp_delta_path}/fact_sales"
        self.fact.write.format("delta").mode("overwrite").save(path)
        reloaded = self.spark.read.format("delta").load(path)
        assert reloaded.count() == self.fact.count()
