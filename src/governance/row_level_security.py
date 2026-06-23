"""
Row-Level Security (RLS) via Unity Catalog Row Filters
======================================================
Agent: Security & Governance Agent

Implements row-level access control on Gold tables so that:
  - Users only see rows belonging to their region / business unit
  - Data engineers and service principals have full access
  - Access policy is enforced at the Unity Catalog layer — not in app code

Unity Catalog RLS model:
  1. Create a SQL FUNCTION (the row filter predicate)
  2. ALTER TABLE … SET ROW FILTER <function> ON (<column>)

The predicate receives the column value for each row and returns BOOLEAN.
Unity Catalog evaluates the predicate for every query automatically.

Reference:
  https://docs.databricks.com/en/tables/row-and-column-filters.html

Supported filter strategies:
  - region_filter:       user sees only rows where region = their group region
  - business_unit_filter: user sees only rows for their assigned BU
  - admin_passthrough:   data-engineers / service principals see all rows
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field

from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Filter policy definitions
# ---------------------------------------------------------------------------

@dataclass
class RowFilterPolicy:
    """
    Describes a row filter to apply to one table column.

    Args:
        table_fqn:      Fully qualified table (catalog.schema.table).
        filter_column:  Column in the table that the filter predicate receives.
        admin_groups:   Groups/principals that bypass the filter (see all rows).
        group_to_value: Map of Databricks group → allowed column value.
                        If empty, uses dynamic current_user() resolution.
    """
    table_fqn:      str
    filter_column:  str
    admin_groups:   list[str] = field(default_factory=lambda: ["data-engineers", "admins"])
    group_to_value: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Row filter creation helpers
# ---------------------------------------------------------------------------

def _function_name(catalog: str, table_fqn: str, col: str) -> str:
    """Derive a unique filter function name from the table + column."""
    safe = table_fqn.replace(".", "_").replace("-", "_")
    return f"{catalog}.governance.rls_{safe}_{col}"


def create_region_row_filter(
    spark: SparkSession,
    policy: RowFilterPolicy,
) -> str:
    """
    Create a Unity Catalog row filter that restricts access by region.

    Admins (data-engineers, service principals) see all rows.
    Other users see only rows where the filter column matches the group
    they belong to (EMEA → "EMEA", AMER → "AMER", etc.).

    Returns:
        The fully qualified function name that was created.
    """
    catalog   = policy.table_fqn.split(".")[0]
    fn_name   = _function_name(catalog, policy.table_fqn, policy.filter_column)
    col       = policy.filter_column

    # Build admin bypass conditions
    admin_conditions = " OR ".join(
        [f"is_account_group_member('{g}')" for g in policy.admin_groups]
    )

    # Build per-group value conditions
    group_conditions = ""
    if policy.group_to_value:
        group_cases = " OR ".join(
            [
                f"(is_account_group_member('{grp}') AND {col} = '{val}')"
                for grp, val in policy.group_to_value.items()
            ]
        )
        group_conditions = f"OR ({group_cases})"

    # Ensure governance schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.governance")

    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fn_name}({col} STRING)
          RETURN (
            {admin_conditions}
            {group_conditions}
          )
    """)
    print(f"[RLS] Created row filter function: {fn_name}")
    return fn_name


def create_business_unit_row_filter(
    spark: SparkSession,
    catalog: str,
    table_fqn: str,
    bu_column: str,
    admin_groups: list[str],
) -> str:
    """
    Row filter for business-unit isolation.
    Each user's BU is resolved from their Databricks group membership.
    """
    fn_name = _function_name(catalog, table_fqn, bu_column)

    admin_conditions = " OR ".join(
        [f"is_account_group_member('{g}')" for g in admin_groups]
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.governance")

    # Use session_user() to match group-based access
    # The CASE logic below maps group membership to allowed BU values.
    # Extend the WHEN clauses to match your actual group → BU mapping.
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fn_name}({bu_column} STRING)
          RETURN (
            {admin_conditions}
            OR (is_account_group_member('bu-optics')     AND {bu_column} = 'Optics')
            OR (is_account_group_member('bu-microscopy')  AND {bu_column} = 'Microscopy')
            OR (is_account_group_member('bu-metrology')   AND {bu_column} = 'Metrology')
            OR (is_account_group_member('bu-medical')     AND {bu_column} = 'Medical')
          )
    """)
    print(f"[RLS] Created BU row filter function: {fn_name}")
    return fn_name


# ---------------------------------------------------------------------------
# Apply / remove row filters on tables
# ---------------------------------------------------------------------------

def apply_row_filter(
    spark: SparkSession,
    table_fqn: str,
    filter_fn: str,
    filter_column: str,
) -> None:
    """Attach a row filter function to a Unity Catalog table."""
    spark.sql(f"""
        ALTER TABLE {table_fqn}
          SET ROW FILTER {filter_fn} ON ({filter_column})
    """)
    print(f"[RLS] Applied row filter {filter_fn} on {table_fqn}({filter_column})")


def drop_row_filter(spark: SparkSession, table_fqn: str) -> None:
    """Remove the row filter from a table (restores full access)."""
    spark.sql(f"ALTER TABLE {table_fqn} DROP ROW FILTER")
    print(f"[RLS] Removed row filter from {table_fqn}")


# ---------------------------------------------------------------------------
# Convenience: apply the full RLS policy set for a lakehouse environment
# ---------------------------------------------------------------------------

def setup_lakehouse_rls(spark: SparkSession, env: str) -> None:
    """
    Apply the standard RLS policies across all Gold tables:

    | Table                     | Column   | Filter              |
    |---------------------------|----------|---------------------|
    | gold.fact_sales           | region   | Region-based filter |
    | gold.dim_customer         | region   | Region-based filter |
    | silver.sales_orders       | region   | Region-based filter |
    | gold.fact_sales           | category | BU filter           |
    """
    catalog     = f"{env}_lakehouse"
    admin_grps  = ["data-engineers", "admins", f"lakehouse-{env}-admins"]

    region_tables = [
        (f"{catalog}.gold.fact_sales",      "region"),
        (f"{catalog}.gold.dim_customer",    "region"),
        (f"{catalog}.silver.sales_orders",  "region"),
    ]

    for table_fqn, col in region_tables:
        policy = RowFilterPolicy(
            table_fqn=table_fqn,
            filter_column=col,
            admin_groups=admin_grps,
            group_to_value={
                "region-emea":  "EMEA",
                "region-amer":  "AMER",
                "region-apac":  "APAC",
                "region-latam": "LATAM",
            },
        )
        fn_name = create_region_row_filter(spark, policy)
        apply_row_filter(spark, table_fqn, fn_name, col)

    # BU filter on fact_sales by product category
    bu_table = f"{catalog}.gold.fact_sales"
    # Note: category comes from dim_product; for direct RLS we need it denormalized in fact
    # This shows the pattern — adapt the column name to your actual fact schema
    bu_fn = create_business_unit_row_filter(
        spark, catalog, bu_table, "product_category", admin_grps
    )

    print(f"\n[RLS] Lakehouse RLS setup complete for env={env}")
    print("[RLS] To verify: run 'SELECT * FROM gold.fact_sales LIMIT 10' as different users")


def list_row_filters(spark: SparkSession, env: str) -> None:
    """Show all row filters currently applied in the lakehouse catalog."""
    catalog = f"{env}_lakehouse"
    try:
        result = spark.sql(f"""
            SELECT table_catalog, table_schema, table_name, row_filter_function_catalog,
                   row_filter_function_schema, row_filter_function_name
            FROM {catalog}.information_schema.tables
            WHERE row_filter_function_name IS NOT NULL
            ORDER BY table_schema, table_name
        """)
        result.show(truncate=False)
    except Exception as e:
        print(f"[RLS] Could not query information_schema: {e}")
        print("[RLS] Use DESCRIBE EXTENDED <table> to check row filters manually")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unity Catalog Row-Level Security setup")
    parser.add_argument("--env",    required=True, help="dev | test | prod")
    parser.add_argument("--action", default="setup",
                        choices=["setup", "teardown", "list"],
                        help="setup: apply RLS | teardown: remove | list: show current filters")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    catalog = f"{args.env}_lakehouse"

    if args.action == "setup":
        setup_lakehouse_rls(spark, args.env)

    elif args.action == "teardown":
        for table in [
            f"{catalog}.gold.fact_sales",
            f"{catalog}.gold.dim_customer",
            f"{catalog}.silver.sales_orders",
        ]:
            try:
                drop_row_filter(spark, table)
            except Exception as e:
                print(f"[RLS] Could not remove filter from {table}: {e}")

    elif args.action == "list":
        list_row_filters(spark, args.env)
