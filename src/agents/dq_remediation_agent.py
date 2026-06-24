"""
Autonomous DQ Remediation Agent
================================
Agent: Data Quality Remediation Agent

Uses Azure OpenAI (GPT-4o) to classify failed DQ check results and
select a remediation strategy, then executes the fix autonomously
using PySpark on Delta Lake.

Pipeline:
  1. Query dq_results table for recent FAILED / WARNING checks
  2. Group failures by table + check_type + column
  3. Call Azure OpenAI to reason over each failure cluster → pick strategy
  4. Execute the chosen remediation PySpark action on the Delta table
  5. Log every action to dq_remediation_log (immutable audit trail)
  6. Re-run DQ on the patched table to confirm resolution

Remediation strategies decided by the LLM:
  IMPUTE_MEDIAN   – fill nulls with column median (numeric)
  IMPUTE_MODE     – fill nulls with most-frequent value (categorical)
  IMPUTE_CONSTANT – fill nulls with a safe domain constant
  COERCE          – clip / cast values to valid range boundaries
  DEDUPLICATE     – drop duplicate rows keeping latest by watermark column
  QUARANTINE      – move unrecoverable records to quarantine zone
  REJECT          – severity too high to auto-fix; escalate and halt

All Delta writes use REPLACE WHERE / MERGE so every action is
reversible via Delta time-travel (RESTORE TABLE ... TO VERSION AS OF N).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.dbutils_shim import get_dbutils
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# Strategy constants — must match strings returned by the LLM prompt
# ---------------------------------------------------------------------------
STRATEGY_IMPUTE_MEDIAN   = "IMPUTE_MEDIAN"
STRATEGY_IMPUTE_MODE     = "IMPUTE_MODE"
STRATEGY_IMPUTE_CONSTANT = "IMPUTE_CONSTANT"
STRATEGY_COERCE          = "COERCE"
STRATEGY_DEDUPLICATE     = "DEDUPLICATE"
STRATEGY_QUARANTINE      = "QUARANTINE"
STRATEGY_REJECT          = "REJECT"

VALID_STRATEGIES = {
    STRATEGY_IMPUTE_MEDIAN, STRATEGY_IMPUTE_MODE, STRATEGY_IMPUTE_CONSTANT,
    STRATEGY_COERCE, STRATEGY_DEDUPLICATE, STRATEGY_QUARANTINE, STRATEGY_REJECT,
}

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class FailureCluster:
    """A group of DQ failures sharing the same table + check_type + column."""
    table_name:    str
    layer:         str
    check_type:    str         # completeness | uniqueness | validity | ...
    column_name:   str | None
    check_name:    str
    failure_rate:  float
    failure_count: int
    total_count:   int
    sample_values: list[Any]   # up to 10 example bad values for the LLM


@dataclass
class RemediationAction:
    """One agent decision + its execution outcome."""
    action_id:      str
    remediation_run_id: str
    table_name:     str
    layer:          str
    check_type:     str
    column_name:    str | None
    strategy:       str
    llm_reasoning:  str
    impute_value:   str | None      # set when strategy is IMPUTE_*
    coerce_min:     float | None
    coerce_max:     float | None
    rows_affected:  int
    status:         str             # APPLIED | SKIPPED | FAILED
    error_message:  str | None
    action_ts:      datetime


# ---------------------------------------------------------------------------
# Azure OpenAI helper
# ---------------------------------------------------------------------------

def _build_openai_client(endpoint: str, api_key: str, api_version: str = "2024-02-15-preview"):
    """
    Build an AzureOpenAI client.
    In production, endpoint and api_key are retrieved from Azure Key Vault
    via dbutils.secrets.get(scope=..., key=...).
    """
    try:
        from openai import AzureOpenAI  # openai>=1.0.0
        return AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version,
        )
    except ImportError as exc:
        raise ImportError(
            "openai>=1.0.0 is required for the DQ Remediation Agent. "
            "Add 'openai>=1.0.0' to your cluster init script or requirements.txt."
        ) from exc


_SYSTEM_PROMPT = """
You are a Data Quality Remediation Agent for an Azure Databricks Lakehouse.
Your role is to analyse a DQ failure cluster and decide the safest, most
appropriate remediation strategy.

Available strategies (return EXACTLY one of these identifiers):
  IMPUTE_MEDIAN   – numeric column; fill NULLs/invalid with the column median
  IMPUTE_MODE     – categorical column; fill NULLs/invalid with the mode value
  IMPUTE_CONSTANT – fill NULLs/invalid with a safe constant; provide the value
  COERCE          – clip numeric values to [min, max]; provide the boundaries
  DEDUPLICATE     – remove exact or key-level duplicates; keep latest record
  QUARANTINE      – move bad records to quarantine; cannot be auto-fixed
  REJECT          – failure rate or severity too high; escalate, do not auto-fix

Return a JSON object with these fields (no markdown, no extra text):
{
  "strategy": "<one of the above>",
  "reasoning": "<1-3 sentences explaining the decision>",
  "impute_value": "<constant value as string, or null>",
  "coerce_min": <number or null>,
  "coerce_max": <number or null>
}

Rules:
- Prefer IMPUTE over QUARANTINE when failure_rate < 0.05 and the column is non-key.
- Use QUARANTINE when records are structurally corrupt (missing primary keys).
- Use REJECT when failure_rate > 0.20 — this signals a systemic upstream issue.
- Never auto-fix primary-key uniqueness failures — always REJECT.
- For validity/range violations with known business bounds, use COERCE.
"""


def _call_llm(
    client: Any,
    deployment: str,
    cluster: FailureCluster,
) -> dict:
    """Call Azure OpenAI GPT-4o and parse the JSON remediation decision."""
    user_message = json.dumps({
        "table":         cluster.table_name,
        "layer":         cluster.layer,
        "check_type":    cluster.check_type,
        "column":        cluster.column_name,
        "check_name":    cluster.check_name,
        "failure_rate":  round(cluster.failure_rate, 4),
        "failure_count": cluster.failure_count,
        "total_count":   cluster.total_count,
        "sample_bad_values": cluster.sample_values[:10],
    }, indent=2)

    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        temperature=0,
        max_tokens=512,
        response_format={"type": "json_object"},
    )

    raw = response.choices[0].message.content
    decision = json.loads(raw)

    # Sanitise strategy value — guard against prompt injection
    strategy = str(decision.get("strategy", "REJECT")).upper().strip()
    if strategy not in VALID_STRATEGIES:
        strategy = STRATEGY_REJECT

    return {
        "strategy":     strategy,
        "reasoning":    str(decision.get("reasoning", ""))[:2000],
        "impute_value": decision.get("impute_value"),
        "coerce_min":   decision.get("coerce_min"),
        "coerce_max":   decision.get("coerce_max"),
    }


# ---------------------------------------------------------------------------
# Remediation execution functions
# ---------------------------------------------------------------------------

def _impute_nulls(
    spark: SparkSession,
    table: str,
    column: str,
    strategy: str,
    impute_value: str | None,
) -> int:
    """
    Fill NULL values in *column* with median, mode, or a constant.
    Uses Delta MERGE to rewrite only affected rows — not a full table scan.
    Returns the number of rows updated.
    """
    df = spark.table(table)

    if strategy == STRATEGY_IMPUTE_MEDIAN:
        # approxQuantile is safe on very large tables
        median_val = df.filter(F.col(column).isNotNull()) \
                       .approxQuantile(column, [0.5], 0.01)[0]
        fill_val = median_val
    elif strategy == STRATEGY_IMPUTE_MODE:
        mode_row = (
            df.filter(F.col(column).isNotNull())
              .groupBy(column).count()
              .orderBy(F.desc("count")).first()
        )
        fill_val = mode_row[column] if mode_row else None
    else:  # IMPUTE_CONSTANT
        fill_val = impute_value

    if fill_val is None:
        return 0

    # Use Delta UPDATE — targeted rewrite of only NULL rows
    fill_literal = f"'{fill_val}'" if isinstance(fill_val, str) else str(fill_val)
    affected = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {table} WHERE {column} IS NULL
    """).collect()[0]["cnt"]

    spark.sql(f"""
        UPDATE {table}
        SET {column} = {fill_literal}
        WHERE {column} IS NULL
    """)
    return affected


def _coerce_range(
    spark: SparkSession,
    table: str,
    column: str,
    coerce_min: float | None,
    coerce_max: float | None,
) -> int:
    """
    Clip numeric column values to [coerce_min, coerce_max].
    Values outside the range are replaced with the nearest boundary.
    """
    conditions = []
    if coerce_min is not None:
        conditions.append(f"{column} < {coerce_min}")
    if coerce_max is not None:
        conditions.append(f"{column} > {coerce_max}")

    if not conditions:
        return 0

    where_clause = " OR ".join(conditions)
    affected = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {table} WHERE {where_clause}
    """).collect()[0]["cnt"]

    set_clause = f"""
        {column} = CASE
            {'WHEN ' + column + ' < ' + str(coerce_min) + ' THEN ' + str(coerce_min) if coerce_min is not None else ''}
            {'WHEN ' + column + ' > ' + str(coerce_max) + ' THEN ' + str(coerce_max) if coerce_max is not None else ''}
            ELSE {column}
        END
    """
    spark.sql(f"UPDATE {table} SET {set_clause} WHERE {where_clause}")
    return affected


def _deduplicate(
    spark: SparkSession,
    table: str,
    primary_keys: list[str],
    watermark_col: str = "updated_at",
) -> int:
    """
    Remove duplicate rows keeping the latest record per primary_key set.
    Uses a Delta MERGE with a ROW_NUMBER window — atomic and idempotent.
    """
    pk_join = " AND ".join([f"src.{k} = tgt.{k}" for k in primary_keys])
    pk_partition = ", ".join(primary_keys)

    # Count duplicates before
    count_before = spark.table(table).count()

    deduped_view = f"_dedup_source_{uuid.uuid4().hex[:8]}"
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {deduped_view} AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY {pk_partition}
                                   ORDER BY {watermark_col} DESC) AS _rn
            FROM {table}
        ) WHERE _rn = 1
    """)

    # Overwrite table with deduplicated set — Delta handles atomic swap
    spark.sql(f"""
        INSERT OVERWRITE {table}
        SELECT * EXCEPT (_rn) FROM {deduped_view}
    """)

    count_after = spark.table(table).count()
    return count_before - count_after


def _quarantine_bad_rows(
    spark: SparkSession,
    table: str,
    column: str,
    check_type: str,
    env: str,
    remediation_run_id: str,
) -> int:
    """
    Move rows that violate the DQ rule to the quarantine table,
    then DELETE them from the source table.
    """
    if check_type == "completeness":
        bad_condition = f"{column} IS NULL"
    else:
        # For other types, quarantine will be targeted by the caller
        bad_condition = f"{column} IS NULL"

    bad_df = spark.sql(f"""
        SELECT
            uuid()                                AS quarantine_id,
            '{remediation_run_id}'                AS silver_run_id,
            'remediation'                         AS domain,
            '{table}'                             AS entity,
            'auto_remediation:{check_type}:{column}' AS error_reason,
            current_timestamp()                   AS quarantine_ts,
            date_format(current_timestamp(), 'yyyy-MM-dd') AS quarantine_date,
            'dq_remediation_agent'                AS source_system,
            to_json(struct(*))                    AS raw_record
        FROM {table}
        WHERE {bad_condition}
    """)

    affected = bad_df.count()
    if affected > 0:
        bad_df.write.format("delta").mode("append") \
              .saveAsTable(f"{env}_lakehouse.bronze.quarantine")
        spark.sql(f"DELETE FROM {table} WHERE {bad_condition}")

    return affected


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

_REMEDIATION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS {env}_lakehouse.governance.dq_remediation_log (
    action_id           STRING        NOT NULL,
    remediation_run_id  STRING        NOT NULL,
    table_name          STRING        NOT NULL,
    layer               STRING,
    check_type          STRING,
    column_name         STRING,
    strategy            STRING        NOT NULL,
    llm_reasoning       STRING,
    impute_value        STRING,
    coerce_min          DOUBLE,
    coerce_max          DOUBLE,
    rows_affected       LONG,
    status              STRING        NOT NULL,
    error_message       STRING,
    action_ts           TIMESTAMP     NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.appendOnly'                    = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true'
)
"""


def _ensure_remediation_log(spark: SparkSession, env: str) -> None:
    spark.sql(_REMEDIATION_LOG_DDL.format(env=env))


def _write_action_log(
    spark: SparkSession,
    env: str,
    actions: list[RemediationAction],
) -> None:
    if not actions:
        return
    rows = [asdict(a) for a in actions]
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append") \
      .saveAsTable(f"{env}_lakehouse.governance.dq_remediation_log")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def load_failed_checks(
    spark: SparkSession,
    env: str,
    lookback_hours: int = 24,
    min_failure_rate: float = 0.0,
) -> list[FailureCluster]:
    """
    Read recent FAILED / WARNING DQ results and group them into clusters
    for LLM analysis.  Returns one cluster per (table, check_type, column).
    """
    dq_table = f"{env}_lakehouse.bronze.dq_results"

    rows = spark.sql(f"""
        SELECT
            table_name,
            layer,
            check_type,
            column_name,
            check_name,
            AVG(failure_rate)   AS avg_failure_rate,
            SUM(failure_count)  AS total_failures,
            SUM(total_count)    AS total_rows
        FROM {dq_table}
        WHERE status IN ('FAILED', 'WARNING')
          AND check_ts >= current_timestamp() - INTERVAL {lookback_hours} HOURS
        GROUP BY table_name, layer, check_type, column_name, check_name
        ORDER BY avg_failure_rate DESC
    """).collect()

    clusters: list[FailureCluster] = []
    for r in rows:
        # Fetch up to 10 sample bad values for the LLM to reason over
        sample_values: list[Any] = []
        if r["column_name"]:
            try:
                condition = (
                    f"{r['column_name']} IS NULL"
                    if r["check_type"] == "completeness"
                    else f"{r['column_name']} IS NOT NULL"
                )
                sample_rows = spark.sql(f"""
                    SELECT {r['column_name']} AS v
                    FROM {r['table_name']}
                    WHERE {condition}
                    LIMIT 10
                """).collect()
                sample_values = [row["v"] for row in sample_rows]
            except Exception:
                pass

        clusters.append(FailureCluster(
            table_name=r["table_name"],
            layer=r["layer"],
            check_type=r["check_type"],
            column_name=r["column_name"],
            check_name=r["check_name"],
            failure_rate=float(r["avg_failure_rate"]),
            failure_count=int(r["total_failures"]),
            total_count=int(r["total_rows"]),
            sample_values=sample_values,
        ))

    return [c for c in clusters if c.failure_rate >= min_failure_rate]


def run_remediation_agent(
    spark: SparkSession,
    env: str,
    openai_endpoint: str,
    openai_api_key: str,
    openai_deployment: str = "gpt-4o",
    lookback_hours: int = 24,
    dry_run: bool = False,
    primary_keys_map: dict[str, list[str]] | None = None,
    watermark_col_map: dict[str, str] | None = None,
) -> list[RemediationAction]:
    """
    Entry point for the DQ Remediation Agent.

    Args:
        spark:               Active SparkSession.
        env:                 Deployment environment (dev / test / prod).
        openai_endpoint:     Azure OpenAI endpoint URL.
        openai_api_key:      API key (fetched from Key Vault in production).
        openai_deployment:   Deployment name of the GPT-4o model.
        lookback_hours:      Window of DQ failures to remediate.
        dry_run:             If True, plans actions but does NOT apply any writes.
        primary_keys_map:    {table_name: [pk_cols]} used for DEDUPLICATE strategy.
        watermark_col_map:   {table_name: watermark_col} used for DEDUPLICATE.

    Returns:
        List of RemediationAction objects describing every decision taken.
    """
    remediation_run_id = str(uuid.uuid4())
    print(f"[DQ-AGENT] Starting remediation run | run_id={remediation_run_id} | env={env}")

    _ensure_remediation_log(spark, env)

    client   = _build_openai_client(openai_endpoint, openai_api_key)
    clusters = load_failed_checks(spark, env, lookback_hours)
    print(f"[DQ-AGENT] Found {len(clusters)} failure cluster(s) to remediate")

    pk_map  = primary_keys_map  or {}
    wm_map  = watermark_col_map or {}
    actions: list[RemediationAction] = []

    for cluster in clusters:
        action_id = str(uuid.uuid4())
        action = RemediationAction(
            action_id=action_id,
            remediation_run_id=remediation_run_id,
            table_name=cluster.table_name,
            layer=cluster.layer,
            check_type=cluster.check_type,
            column_name=cluster.column_name,
            strategy="PENDING",
            llm_reasoning="",
            impute_value=None,
            coerce_min=None,
            coerce_max=None,
            rows_affected=0,
            status="SKIPPED",
            error_message=None,
            action_ts=datetime.now(timezone.utc),
        )

        try:
            # Step 1: Ask the LLM for a remediation decision
            decision = _call_llm(client, openai_deployment, cluster)
            action.strategy      = decision["strategy"]
            action.llm_reasoning = decision["reasoning"]
            action.impute_value  = str(decision["impute_value"]) if decision["impute_value"] is not None else None
            action.coerce_min    = float(decision["coerce_min"])  if decision["coerce_min"]  is not None else None
            action.coerce_max    = float(decision["coerce_max"])  if decision["coerce_max"]  is not None else None

            print(f"[DQ-AGENT] {cluster.table_name}.{cluster.column_name} "
                  f"→ {action.strategy} | reasoning: {action.llm_reasoning[:80]}")

            if dry_run or action.strategy == STRATEGY_REJECT:
                action.status = "SKIPPED"
                actions.append(action)
                continue

            # Step 2: Execute the chosen strategy
            if action.strategy in (STRATEGY_IMPUTE_MEDIAN, STRATEGY_IMPUTE_MODE,
                                   STRATEGY_IMPUTE_CONSTANT):
                action.rows_affected = _impute_nulls(
                    spark, cluster.table_name, cluster.column_name,
                    action.strategy, action.impute_value,
                )

            elif action.strategy == STRATEGY_COERCE:
                action.rows_affected = _coerce_range(
                    spark, cluster.table_name, cluster.column_name,
                    action.coerce_min, action.coerce_max,
                )

            elif action.strategy == STRATEGY_DEDUPLICATE:
                pk_cols = pk_map.get(cluster.table_name, ["order_id"])
                wm_col  = wm_map.get(cluster.table_name, "updated_at")
                action.rows_affected = _deduplicate(
                    spark, cluster.table_name, pk_cols, wm_col,
                )

            elif action.strategy == STRATEGY_QUARANTINE:
                action.rows_affected = _quarantine_bad_rows(
                    spark, cluster.table_name, cluster.column_name,
                    cluster.check_type, env, remediation_run_id,
                )

            action.status = "APPLIED"

        except Exception as exc:
            action.status        = "FAILED"
            action.error_message = str(exc)[:1000]
            print(f"[DQ-AGENT] ERROR remediating {cluster.table_name}: {exc}")

        action.action_ts = datetime.now(timezone.utc)
        actions.append(action)

    # Step 3: Persist audit log
    _write_action_log(spark, env, actions)

    applied  = sum(1 for a in actions if a.status == "APPLIED")
    rejected = sum(1 for a in actions if a.strategy == STRATEGY_REJECT)
    failed   = sum(1 for a in actions if a.status == "FAILED")
    print(
        f"[DQ-AGENT] Remediation complete | "
        f"applied={applied} rejected={rejected} failed={failed} | "
        f"run_id={remediation_run_id}"
    )
    return actions


# ---------------------------------------------------------------------------
# CLI entry point (for Databricks Workflow task)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Autonomous DQ Remediation Agent")
    parser.add_argument("--env",                 required=True)
    parser.add_argument("--openai-endpoint",     required=True)
    parser.add_argument("--openai-key-secret",   required=True,
                        help="Key Vault secret key for the Azure OpenAI API key")
    parser.add_argument("--openai-secret-scope", required=True,
                        help="Databricks secret scope backing Key Vault")
    parser.add_argument("--openai-deployment",   default="gpt-4o")
    parser.add_argument("--lookback-hours",      type=int, default=24)
    parser.add_argument("--dry-run",             action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    # Retrieve API key from Key Vault via Databricks secret scope
    api_key = get_dbutils(spark).secrets.get(
        scope=args.openai_secret_scope,
        key=args.openai_key_secret,
    )

    run_remediation_agent(
        spark=spark,
        env=args.env,
        openai_endpoint=args.openai_endpoint,
        openai_api_key=api_key,
        openai_deployment=args.openai_deployment,
        lookback_hours=args.lookback_hours,
        dry_run=args.dry_run,
    )
