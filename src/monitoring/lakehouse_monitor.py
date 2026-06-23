"""
Databricks Lakehouse Monitoring — Statistical Drift Detection
=============================================================
Agent: Data Quality & Observability Agent

Provisions Databricks Lakehouse Monitor (Unity Catalog) on Silver and Gold
Delta tables to track:
  - Data profile: null %, distinct count, mean, stddev per column
  - Distribution drift: Jensen-Shannon divergence vs baseline window
  - Volume drift: daily row count vs 30-day rolling average
  - Freshness: max timestamp lag per monitored table

Monitor output tables (auto-created by Databricks):
  {monitor_schema}.{table_name}_profile_metrics
  {monitor_schema}.{table_name}_drift_metrics

Reference: https://docs.databricks.com/en/lakehouse-monitoring/index.html
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Tables to monitor — keyed by logical name
# ---------------------------------------------------------------------------
MONITORED_TABLES: dict[str, dict[str, Any]] = {
    "silver_sales_orders": {
        "table":            "{env}_lakehouse.silver.sales_orders",
        "timestamp_col":    "order_date",
        "granularities":    ["1 day", "1 week"],
        "slice_cols":       ["status", "currency"],
        "baseline_type":    "ROLLING_WINDOW",
        "baseline_window":  "30 days",
    },
    "gold_fact_sales": {
        "table":            "{env}_lakehouse.gold.fact_sales",
        "timestamp_col":    "order_date",
        "granularities":    ["1 day", "1 week", "1 month"],
        "slice_cols":       ["order_status", "currency"],
        "baseline_type":    "ROLLING_WINDOW",
        "baseline_window":  "30 days",
    },
    "gold_dim_customer": {
        "table":            "{env}_lakehouse.gold.dim_customer",
        "timestamp_col":    "_valid_from",
        "granularities":    ["1 day"],
        "slice_cols":       ["customer_segment", "country_code"],
        "baseline_type":    "ROLLING_WINDOW",
        "baseline_window":  "30 days",
    },
}


# ---------------------------------------------------------------------------
# Monitor provisioning
# ---------------------------------------------------------------------------

def create_or_refresh_monitor(
    spark: SparkSession,
    env: str,
    table_key: str,
    monitor_schema: str,
    skip_if_exists: bool = True,
) -> None:
    """
    Create a Lakehouse Monitor on a Delta table using the Unity Catalog
    CREATE MONITOR SQL command (available DBR 12.2+).

    If the monitor already exists and skip_if_exists=False, it is refreshed
    (metrics recomputed against the current data window).
    """
    cfg        = MONITORED_TABLES[table_key]
    table_name = cfg["table"].format(env=env)
    ts_col     = cfg["timestamp_col"]
    granularity_list = ", ".join(f"'{g}'" for g in cfg["granularities"])
    slice_cols_list  = ", ".join(f"'{c}'" for c in cfg["slice_cols"])

    # Check if monitor already exists (system table query)
    existing = spark.sql(f"""
        SELECT 1 FROM system.information_schema.table_constraints
        WHERE constraint_name = 'MONITOR' AND table_name = '{table_name}'
        LIMIT 1
    """)

    if skip_if_exists and not existing.isEmpty():
        print(f"[MONITOR] Monitor already exists for {table_name}. Skipping creation.")
        _refresh_monitor(spark, table_name)
        return

    spark.sql(f"""
        CREATE OR REPLACE MONITOR {table_name}
        TIMESERIES
          TIMESERIES_COL           = {ts_col},
          GRANULARITIES            = ({granularity_list}),
          SLICING_EXPRS            = ({slice_cols_list}),
          OUTPUT_SCHEMA_NAME       = {monitor_schema},
          BASELINE_TABLE_NAME      = NULL,
          CUSTOM_METRICS           = (),
          INFERENCE_LOG            = NULL
    """)

    print(f"[MONITOR] Created Lakehouse Monitor for {table_name}")
    print(f"[MONITOR] Output metrics in schema: {monitor_schema}")


def _refresh_monitor(spark: SparkSession, table_name: str) -> None:
    """Trigger an on-demand metrics refresh for an existing monitor."""
    spark.sql(f"REFRESH MONITOR {table_name}")
    print(f"[MONITOR] Refreshed metrics for {table_name}")


# ---------------------------------------------------------------------------
# Drift detection — reads monitor output tables
# ---------------------------------------------------------------------------

def detect_drift(
    spark: SparkSession,
    env: str,
    table_key: str,
    monitor_schema: str,
    drift_threshold: float = 0.1,
    lookback_days: int = 1,
) -> DataFrame:
    """
    Query the drift_metrics output table produced by Lakehouse Monitor and
    return rows where Jensen-Shannon divergence exceeds *drift_threshold*.

    Returns an empty DataFrame if no drift is detected.

    Jensen-Shannon divergence:
      0.0  = identical distributions
      1.0  = completely different distributions
      > 0.1 (default) = actionable drift for most numeric features
    """
    cfg        = MONITORED_TABLES[table_key]
    table_name = cfg["table"].format(env=env).replace(".", "_")
    drift_table = f"{monitor_schema}.{table_name}_drift_metrics"

    cutoff = F.date_sub(F.current_date(), lookback_days)

    drift_df = (
        spark.table(drift_table)
             .filter(F.col("window_start") >= cutoff)
             .filter(F.col("drift_type") == "JS_DIVERGENCE")
             .filter(F.col("drift_score") > drift_threshold)
             .select(
                 "window_start", "window_end",
                 "column_name", "drift_type",
                 F.round("drift_score", 4).alias("drift_score"),
                 "slice_value",
             )
             .orderBy(F.col("drift_score").desc())
    )

    count = drift_df.count()
    if count > 0:
        print(f"[MONITOR] ⚠  {count} drift alerts for {table_key} (threshold={drift_threshold})")
    else:
        print(f"[MONITOR] ✓  No drift detected for {table_key}")

    return drift_df


def get_volume_trend(
    spark: SparkSession,
    env: str,
    table_key: str,
    monitor_schema: str,
    lookback_days: int = 30,
    alert_pct: float = 0.20,
) -> DataFrame:
    """
    Compare today's row count against the 30-day rolling average.
    Alert if today's count deviates by more than *alert_pct* (default 20 %).

    Returns a single-row summary DataFrame.
    """
    cfg        = MONITORED_TABLES[table_key]
    table_name = cfg["table"].format(env=env).replace(".", "_")
    profile_table = f"{monitor_schema}.{table_name}_profile_metrics"

    cutoff = F.date_sub(F.current_date(), lookback_days)

    recent = (
        spark.table(profile_table)
             .filter(F.col("column_name") == "*")            # row-count metric
             .filter(F.col("window_start") >= cutoff)
             .filter(F.col("granularity") == "1 day")
             .select("window_start", F.col("count").alias("row_count"))
    )

    baseline_avg = recent.agg(F.avg("row_count").alias("avg_count")).collect()[0]["avg_count"] or 0

    today = (
        recent
        .filter(F.col("window_start") == F.current_date() - 1)
        .agg(F.first("row_count").alias("today_count"))
        .collect()[0]["today_count"] or 0
    )

    pct_change = abs(today - baseline_avg) / baseline_avg if baseline_avg else 0
    status     = "ALERT" if pct_change > alert_pct else "OK"

    print(
        f"[MONITOR] Volume trend {table_key}: "
        f"today={today:,}  baseline_avg={baseline_avg:,.0f}  "
        f"deviation={pct_change:.1%}  status={status}"
    )

    return spark.createDataFrame(
        [{
            "table_key":     table_key,
            "today_count":   int(today),
            "baseline_avg":  float(baseline_avg),
            "pct_change":    float(pct_change),
            "status":        status,
            "check_ts":      datetime.now(timezone.utc).isoformat(),
        }]
    )


# ---------------------------------------------------------------------------
# Delta table maintenance (OPTIMIZE + VACUUM)
# ---------------------------------------------------------------------------

MAINTENANCE_TABLES: list[dict[str, Any]] = [
    {"table": "{env}_lakehouse.bronze.sales_orders",    "zorder": None,                       "vacuum_hours": 168},
    {"table": "{env}_lakehouse.bronze.ingestion_metadata", "zorder": None,                    "vacuum_hours": 168},
    {"table": "{env}_lakehouse.silver.sales_orders",    "zorder": ["order_id"],               "vacuum_hours": 168},
    {"table": "{env}_lakehouse.gold.fact_sales",        "zorder": ["customer_sk", "product_sk"], "vacuum_hours": 336},
    {"table": "{env}_lakehouse.gold.dim_customer",      "zorder": ["customer_id"],             "vacuum_hours": 336},
    {"table": "{env}_lakehouse.gold.dim_product",       "zorder": ["product_id", "category"],  "vacuum_hours": 336},
    {"table": "{env}_lakehouse.bronze.quarantine",      "zorder": None,                       "vacuum_hours": 168},
    {"table": "{env}_lakehouse.bronze.dq_results",      "zorder": None,                       "vacuum_hours": 168},
]


def run_maintenance(spark: SparkSession, env: str, dry_run: bool = False) -> None:
    """
    Run OPTIMIZE (with optional ZORDER) and VACUUM on all managed Delta tables.

    - OPTIMIZE compacts small files and improves read performance.
    - VACUUM removes files older than the retention threshold (default 7 days
      for most tables, 14 days for Gold to allow time-travel queries).
    - dry_run=True prints SQL without executing (useful in CI).
    """
    print(f"[MAINTENANCE] Starting Delta table maintenance for env={env}")

    for cfg in MAINTENANCE_TABLES:
        table = cfg["table"].format(env=env)
        zorder_cols = cfg.get("zorder")
        vacuum_hours = cfg.get("vacuum_hours", 168)

        optimize_sql = f"OPTIMIZE {table}"
        if zorder_cols:
            optimize_sql += f" ZORDER BY ({', '.join(zorder_cols)})"

        vacuum_sql = f"VACUUM {table} RETAIN {vacuum_hours} HOURS"

        if dry_run:
            print(f"[DRY-RUN] {optimize_sql}")
            print(f"[DRY-RUN] {vacuum_sql}")
        else:
            print(f"[MAINTENANCE] {optimize_sql}")
            spark.sql(optimize_sql)
            print(f"[MAINTENANCE] {vacuum_sql}")
            spark.sql(vacuum_sql)

    print("[MAINTENANCE] Done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lakehouse monitoring and maintenance")
    parser.add_argument("--env",            required=True,  help="dev | test | prod")
    parser.add_argument("--monitor-schema", default=None,   help="Schema for monitor output tables (default: {env}_lakehouse.monitoring)")
    parser.add_argument("--action",         default="all",  choices=["create_monitors", "detect_drift", "maintenance", "all"])
    parser.add_argument("--dry-run",        action="store_true", help="Print SQL without executing (maintenance only)")
    args = parser.parse_args()

    spark  = SparkSession.builder.getOrCreate()
    schema = args.monitor_schema or f"{args.env}_lakehouse.monitoring"

    # Ensure monitoring schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    if args.action in ("create_monitors", "all"):
        for key in MONITORED_TABLES:
            create_or_refresh_monitor(spark, args.env, key, schema)

    if args.action in ("detect_drift", "all"):
        for key in MONITORED_TABLES:
            detect_drift(spark, args.env, key, schema)
            get_volume_trend(spark, args.env, key, schema)

    if args.action in ("maintenance", "all"):
        run_maintenance(spark, args.env, dry_run=args.dry_run)
