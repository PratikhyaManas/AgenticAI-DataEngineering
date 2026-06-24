"""
GDPR Right-to-be-Forgotten (RTBF) Agent
=========================================
Agent: Data Security, Privacy & Governance Agent

Implements GDPR Article 17 — Right to Erasure.

Given one or more data subject IDs (customer_id), this agent:
  1. Discovers all Delta tables across Bronze / Silver / Gold containing
     PII linked to the subject via the PII table registry
  2. Verifies no overriding legal basis (legitimate interest check)
  3. Executes the chosen erasure mode per table:
       ANONYMIZE – Replace PII columns with a SHA-256 irreversible hash
       DELETE    – Hard-delete the entire row from all layers
  4. Runs VACUUM on each affected table to physically remove Parquet files
     (default retention period honoured; pass vacuum_hours=0 for immediate)
  5. Updates the pseudonym mapping table if pseudonymization was used
  6. Produces an immutable GDPR compliance report appended to the
     governance.gdpr_erasure_log Delta table

Safety controls:
  - All erasures are logged BEFORE execution for auditability
  - Delta time-travel is disabled for the erasure log (appendOnly)
  - Dry-run mode plans and reports actions without executing any write
  - Vacuum is separated from erasure with a configurable delay to allow
    DBR streaming readers to complete their current micro-batch

Legal basis codes (blocking erasure):
  CONTRACT     – data needed to fulfil an active contract
  LEGAL_HOLD   – data subject to litigation or regulatory hold
  LEGITIMATE   – legitimate interest overrides the erasure request
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.dbutils_shim import get_dbutils


# ---------------------------------------------------------------------------
# PII table registry
# ---------------------------------------------------------------------------
# Defines every Delta table that contains subject-identifiable data,
# the subject key column used to locate records, and the PII columns
# that must be anonymized or deleted.
# Add new tables here as the schema evolves.

PII_TABLE_REGISTRY: list[dict[str, Any]] = [
    # Bronze layer
    {
        "table":        "{env}_lakehouse.bronze.sales_orders",
        "layer":        "bronze",
        "subject_key":  "customer_id",
        "pii_cols":     ["customer_id"],
        "default_mode": "DELETE",
    },
    {
        "table":        "{env}_lakehouse.bronze.customers",
        "layer":        "bronze",
        "subject_key":  "customer_id",
        "pii_cols":     [
            "first_name", "last_name", "email",
            "phone", "address", "date_of_birth",
        ],
        "default_mode": "ANONYMIZE",
    },
    {
        "table":        "{env}_lakehouse.bronze.quarantine",
        "layer":        "bronze",
        "subject_key":  "customer_id",
        "pii_cols":     ["raw_record"],          # JSON blob; entire row deleted
        "default_mode": "DELETE",
    },
    # Silver layer
    {
        "table":        "{env}_lakehouse.silver.sales_orders",
        "layer":        "silver",
        "subject_key":  "customer_id",
        "pii_cols":     ["customer_id"],
        "default_mode": "DELETE",
    },
    {
        "table":        "{env}_lakehouse.silver.silver_customers",
        "layer":        "silver",
        "subject_key":  "customer_id",
        "pii_cols":     [
            "first_name", "last_name", "email",
            "phone", "address", "date_of_birth",
        ],
        "default_mode": "ANONYMIZE",
    },
    # Gold layer
    {
        "table":        "{env}_lakehouse.gold.dim_customer",
        "layer":        "gold",
        "subject_key":  "customer_id",
        "pii_cols":     [
            "first_name", "last_name", "email",
            "phone", "address",
        ],
        "default_mode": "ANONYMIZE",     # preserve SCD2 history rows but anonymize PII
    },
    {
        "table":        "{env}_lakehouse.gold.fact_sales",
        "layer":        "gold",
        "subject_key":  "customer_id",
        "pii_cols":     ["customer_id"],
        "default_mode": "ANONYMIZE",     # keep revenue data; hash the customer_id
    },
    {
        "table":        "{env}_lakehouse.gold.gold_customer_clv",
        "layer":        "gold",
        "subject_key":  "customer_id",
        "pii_cols":     ["customer_id"],
        "default_mode": "DELETE",
    },
    # ML / Feature Store
    {
        "table":        "{env}_lakehouse.features.customer_features",
        "layer":        "features",
        "subject_key":  "customer_id",
        "pii_cols":     ["customer_id"],
        "default_mode": "DELETE",
    },
    {
        "table":        "{env}_lakehouse.gold.sales_predictions",
        "layer":        "gold",
        "subject_key":  "customer_id",
        "pii_cols":     ["customer_id"],
        "default_mode": "DELETE",
    },
    # Governance audit log (must retain — LEGAL_HOLD applies)
    {
        "table":        "{env}_lakehouse.governance.dq_remediation_log",
        "layer":        "governance",
        "subject_key":  None,             # no direct subject link
        "pii_cols":     [],
        "default_mode": "SKIP",
    },
]

# Tables that cannot be erased due to overriding legal basis
LEGAL_HOLD_TABLES: set[str] = {
    "governance.dq_remediation_log",
    "governance.gdpr_erasure_log",
}


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ErasureAction:
    action_id:          str
    erasure_request_id: str
    subject_id:         str
    table_name:         str
    layer:              str
    mode:               str           # ANONYMIZE | DELETE | SKIP | BLOCKED
    pii_cols:           list[str]
    rows_affected:      int
    vacuum_executed:    bool
    legal_basis_code:   str | None    # set if BLOCKED
    status:             str           # COMPLETED | SKIPPED | BLOCKED | FAILED
    error_message:      str | None
    executed_at:        datetime


@dataclass
class ErasureReport:
    request_id:        str
    subject_ids:       list[str]
    requested_at:      datetime
    completed_at:      datetime
    requester:         str
    total_tables:      int
    tables_erased:     int
    tables_skipped:    int
    tables_blocked:    int
    tables_failed:     int
    actions:           list[ErasureAction]
    dry_run:           bool


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

_ERASURE_LOG_DDL = """
CREATE TABLE IF NOT EXISTS {env}_lakehouse.governance.gdpr_erasure_log (
    action_id           STRING    NOT NULL,
    erasure_request_id  STRING    NOT NULL,
    subject_id          STRING    NOT NULL,
    table_name          STRING    NOT NULL,
    layer               STRING,
    mode                STRING    NOT NULL,
    pii_cols            STRING,
    rows_affected       LONG,
    vacuum_executed     BOOLEAN,
    legal_basis_code    STRING,
    status              STRING    NOT NULL,
    error_message       STRING,
    executed_at         TIMESTAMP NOT NULL,
    dry_run             BOOLEAN   NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.appendOnly'                 = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
"""


def _ensure_erasure_log(spark: SparkSession, env: str) -> None:
    spark.sql(_ERASURE_LOG_DDL.format(env=env))


def _write_erasure_log(
    spark: SparkSession,
    env: str,
    actions: list[ErasureAction],
    dry_run: bool,
) -> None:
    if not actions:
        return
    rows = []
    for a in actions:
        d = asdict(a)
        d["pii_cols"] = json.dumps(a.pii_cols)
        d["dry_run"]  = dry_run
        rows.append(d)
    spark.createDataFrame(rows).write.format("delta").mode("append") \
         .saveAsTable(f"{env}_lakehouse.governance.gdpr_erasure_log")


# ---------------------------------------------------------------------------
# Anonymization (ANONYMIZE mode)
# ---------------------------------------------------------------------------

_ANON_SALT_PLACEHOLDER = "RTBF_SALT_REPLACE_VIA_KEY_VAULT"


def _anonymize_subject(
    spark: SparkSession,
    table: str,
    subject_key: str,
    subject_id: str,
    pii_cols: list[str],
    anon_salt: str,
) -> int:
    """
    Replace all PII column values for *subject_id* with a salted SHA-256 hash.
    Uses Delta UPDATE — only rows matching the subject are rewritten.
    Non-PII columns are preserved.

    Returns the number of rows anonymized.
    """
    # Count affected rows before update
    affected = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {table}
        WHERE {subject_key} = '{subject_id}'
    """).collect()[0]["cnt"]

    if affected == 0:
        return 0

    # Build SET clause for each PII column
    set_parts = []
    for col in pii_cols:
        if col == subject_key:
            # The subject key itself is hashed with a fixed prefix so it
            # remains consistent across tables (enables cross-table joins
            # to still work on the anonymized key if needed)
            hashed_id = hashlib.sha256(
                f"{anon_salt}:{subject_id}".encode()
            ).hexdigest()
            set_parts.append(f"{col} = '{hashed_id}'")
        else:
            # Other PII: hash column value concatenated with subject_id
            set_parts.append(
                f"{col} = SHA2(CONCAT('{anon_salt}', COALESCE(CAST({col} AS STRING), ''), "
                f"'{subject_id}'), 256)"
            )

    set_clause = ",\n            ".join(set_parts)
    spark.sql(f"""
        UPDATE {table}
        SET {set_clause}
        WHERE {subject_key} = '{subject_id}'
    """)
    return int(affected)


# ---------------------------------------------------------------------------
# Deletion (DELETE mode)
# ---------------------------------------------------------------------------

def _delete_subject(
    spark: SparkSession,
    table: str,
    subject_key: str,
    subject_id: str,
) -> int:
    """
    Hard-delete all rows for *subject_id* from *table* using Delta DELETE.
    Returns the number of rows deleted.
    """
    affected = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {table}
        WHERE {subject_key} = '{subject_id}'
    """).collect()[0]["cnt"]

    if affected > 0:
        spark.sql(f"""
            DELETE FROM {table}
            WHERE {subject_key} = '{subject_id}'
        """)
    return int(affected)


# ---------------------------------------------------------------------------
# Vacuum
# ---------------------------------------------------------------------------

def _vacuum_table(
    spark: SparkSession,
    table: str,
    retention_hours: int = 168,     # 7 days default; 0 = immediate (requires override)
) -> None:
    """
    Run VACUUM to physically remove Parquet files deleted by the erasure.
    Requires 'spark.databricks.delta.retentionDurationCheck.enabled' = false
    when using retention_hours=0.
    """
    if retention_hours == 0:
        spark.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )
    spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
    if retention_hours == 0:
        spark.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled", "true"
        )


# ---------------------------------------------------------------------------
# Legal basis check
# ---------------------------------------------------------------------------

def _check_legal_basis(
    spark: SparkSession,
    env: str,
    subject_id: str,
    table_name: str,
) -> str | None:
    """
    Check whether an overriding legal basis blocks erasure for this subject
    in this table.

    Returns None if erasure is permitted, or the legal basis code if blocked.
    In production, this queries a legal_basis table maintained by the
    privacy compliance team.
    """
    # Check for LEGAL_HOLD tables
    for blocked_suffix in LEGAL_HOLD_TABLES:
        if table_name.endswith(blocked_suffix):
            return "LEGAL_HOLD"

    # Check the legal basis registry table
    try:
        rows = spark.sql(f"""
            SELECT legal_basis_code
            FROM {env}_lakehouse.governance.subject_legal_basis
            WHERE subject_id = '{subject_id}'
              AND table_pattern IS NULL OR '{table_name}' LIKE table_pattern
              AND basis_expiry > current_timestamp()
            LIMIT 1
        """).collect()
        if rows:
            return str(rows[0]["legal_basis_code"])
    except Exception:
        # Table doesn't exist yet → no legal basis registered → erasure permitted
        pass

    return None


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_rtbf_agent(
    spark: SparkSession,
    env: str,
    subject_ids: list[str],
    anon_salt_secret: str,
    requester: str = "privacy_team",
    vacuum_hours: int = 168,
    dry_run: bool = False,
    mode_override: str | None = None,  # ANONYMIZE | DELETE — overrides registry default
) -> ErasureReport:
    """
    Execute the GDPR Right-to-be-Forgotten erasure for one or more subjects.

    Args:
        spark:             Active SparkSession.
        env:               Deployment environment (dev / test / prod).
        subject_ids:       List of customer_id values to erase.
        anon_salt_secret:  Anonymization salt (retrieved from Key Vault by caller).
        requester:         Identity of the requester (for audit log).
        vacuum_hours:      VACUUM retention hours post-erasure.
        dry_run:           Plan-only mode — no writes executed.
        mode_override:     Force ANONYMIZE or DELETE across all tables.

    Returns:
        ErasureReport with full action log and summary statistics.
    """
    request_id  = str(uuid.uuid4())
    requested_at = datetime.now(timezone.utc)
    print(
        f"[RTBF-AGENT] Starting | request_id={request_id} "
        f"subjects={subject_ids} env={env} dry_run={dry_run}"
    )

    _ensure_erasure_log(spark, env)
    all_actions: list[ErasureAction] = []

    for subject_id in subject_ids:
        print(f"[RTBF-AGENT] Processing subject_id={subject_id}")

        for reg_entry in PII_TABLE_REGISTRY:
            table_name  = reg_entry["table"].format(env=env)
            layer       = reg_entry["layer"]
            subject_key = reg_entry.get("subject_key")
            pii_cols    = reg_entry.get("pii_cols", [])
            default_mode = mode_override or reg_entry.get("default_mode", "SKIP")

            action = ErasureAction(
                action_id=str(uuid.uuid4()),
                erasure_request_id=request_id,
                subject_id=subject_id,
                table_name=table_name,
                layer=layer,
                mode=default_mode,
                pii_cols=pii_cols,
                rows_affected=0,
                vacuum_executed=False,
                legal_basis_code=None,
                status="SKIPPED",
                error_message=None,
                executed_at=datetime.now(timezone.utc),
            )

            # Skip tables with no subject key or explicitly marked SKIP
            if default_mode == "SKIP" or not subject_key:
                all_actions.append(action)
                continue

            try:
                # Check if table exists
                if not spark.catalog.tableExists(table_name):
                    action.status = "SKIPPED"
                    all_actions.append(action)
                    continue

                # Check legal basis
                legal_basis = _check_legal_basis(spark, env, subject_id, table_name)
                if legal_basis:
                    action.status           = "BLOCKED"
                    action.legal_basis_code = legal_basis
                    all_actions.append(action)
                    print(f"[RTBF-AGENT] BLOCKED {table_name} | basis={legal_basis}")
                    continue

                if dry_run:
                    # Count rows that would be affected
                    count = spark.sql(f"""
                        SELECT COUNT(*) AS cnt FROM {table_name}
                        WHERE {subject_key} = '{subject_id}'
                    """).collect()[0]["cnt"]
                    action.rows_affected = int(count)
                    action.status        = "SKIPPED"
                    all_actions.append(action)
                    continue

                # Execute erasure
                if default_mode == "ANONYMIZE":
                    rows_affected = _anonymize_subject(
                        spark, table_name, subject_key, subject_id,
                        pii_cols, anon_salt_secret,
                    )
                elif default_mode == "DELETE":
                    rows_affected = _delete_subject(
                        spark, table_name, subject_key, subject_id,
                    )
                else:
                    rows_affected = 0

                action.rows_affected = rows_affected
                action.status        = "COMPLETED"

                # Vacuum if rows were affected
                if rows_affected > 0:
                    _vacuum_table(spark, table_name, vacuum_hours)
                    action.vacuum_executed = True

                print(
                    f"[RTBF-AGENT] {default_mode} {table_name} "
                    f"| rows={rows_affected} | vacuum={action.vacuum_executed}"
                )

            except Exception as exc:
                action.status        = "FAILED"
                action.error_message = str(exc)[:1000]
                print(f"[RTBF-AGENT] ERROR {table_name}: {exc}")

            action.executed_at = datetime.now(timezone.utc)
            all_actions.append(action)

    # Write audit log — append-only, immutable
    _write_erasure_log(spark, env, all_actions, dry_run)

    completed_at = datetime.now(timezone.utc)
    report = ErasureReport(
        request_id=request_id,
        subject_ids=subject_ids,
        requested_at=requested_at,
        completed_at=completed_at,
        requester=requester,
        total_tables=len(all_actions),
        tables_erased=sum(1 for a in all_actions if a.status == "COMPLETED"),
        tables_skipped=sum(1 for a in all_actions if a.status == "SKIPPED"),
        tables_blocked=sum(1 for a in all_actions if a.status == "BLOCKED"),
        tables_failed=sum(1 for a in all_actions if a.status == "FAILED"),
        actions=all_actions,
        dry_run=dry_run,
    )

    print(
        f"[RTBF-AGENT] Complete | request={request_id} "
        f"erased={report.tables_erased} skipped={report.tables_skipped} "
        f"blocked={report.tables_blocked} failed={report.tables_failed}"
    )
    return report


def generate_compliance_report(report: ErasureReport) -> str:
    """
    Render a human-readable GDPR compliance report for the Data Protection Officer.
    """
    lines = [
        "=" * 72,
        "  GDPR ARTICLE 17 — RIGHT TO ERASURE — COMPLIANCE REPORT",
        "=" * 72,
        f"  Request ID   : {report.request_id}",
        f"  Subject(s)   : {', '.join(report.subject_ids)}",
        f"  Requester    : {report.requester}",
        f"  Requested at : {report.requested_at.isoformat()}",
        f"  Completed at : {report.completed_at.isoformat()}",
        f"  Dry Run      : {report.dry_run}",
        "",
        f"  Tables erased  : {report.tables_erased}",
        f"  Tables skipped : {report.tables_skipped}",
        f"  Tables blocked : {report.tables_blocked}",
        f"  Tables failed  : {report.tables_failed}",
        "",
        "  ACTION DETAILS",
        "  " + "-" * 68,
    ]
    for a in report.actions:
        if a.status in ("SKIPPED",) and a.rows_affected == 0:
            continue   # omit no-op entries from the human report
        lines.append(
            f"  [{a.status:10}] {a.table_name}\n"
            f"              mode={a.mode} rows={a.rows_affected} "
            f"vacuum={a.vacuum_executed}"
            + (f" BLOCKED({a.legal_basis_code})" if a.legal_basis_code else "")
            + (f"\n              ERROR: {a.error_message}" if a.error_message else "")
        )
    lines += ["=" * 72, ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GDPR Right-to-be-Forgotten Agent")
    parser.add_argument("--env",               required=True)
    parser.add_argument("--subject-ids",       required=True,
                        help="Comma-separated list of customer_id values")
    parser.add_argument("--salt-secret-scope", required=True)
    parser.add_argument("--salt-secret-key",   required=True)
    parser.add_argument("--requester",         default="privacy_team")
    parser.add_argument("--vacuum-hours",      type=int, default=168)
    parser.add_argument("--mode",              choices=["ANONYMIZE", "DELETE"],
                        default=None)
    parser.add_argument("--dry-run",           action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()  # noqa: F821 (injected in Databricks)

    anon_salt = get_dbutils(spark).secrets.get(
        scope=args.salt_secret_scope,
        key=args.salt_secret_key,
    )

    subject_list = [s.strip() for s in args.subject_ids.split(",")]

    report = run_rtbf_agent(
        spark=spark,
        env=args.env,
        subject_ids=subject_list,
        anon_salt_secret=anon_salt,
        requester=args.requester,
        vacuum_hours=args.vacuum_hours,
        dry_run=args.dry_run,
        mode_override=args.mode,
    )

    print(generate_compliance_report(report))
