"""
Delta Sharing — Governed Data Sharing via Unity Catalog
=======================================================
Agent: Security & Governance Agent

Provisions Delta Shares and Recipients so Gold-layer tables can be
accessed by external consumers (Power BI Direct Lake, partner teams,
Azure ML) without copying data.

All sharing is governed through Unity Catalog:
  - SHARE  : logical grouping of tables to expose
  - RECIPIENT : external consumer identity (with token-based auth)
  - GRANT  : permission to SELECT from a share

Reference: https://docs.databricks.com/en/delta-sharing/index.html
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Share definitions — declared as configuration, not hardcoded SQL
# ---------------------------------------------------------------------------

@dataclass
class TableShare:
    """Represents one table entry in a Delta Share."""
    catalog:   str
    schema:    str
    table:     str
    alias:     str | None = None       # name exposed to recipient (defaults to table name)
    partitions: list[str] = field(default_factory=list)  # partition filter strings

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def shared_as(self) -> str:
        return self.alias or self.table


@dataclass
class ShareDefinition:
    """Top-level share with its tables and allowed recipients."""
    share_name:  str
    description: str
    tables:      list[TableShare]
    recipients:  list[str]             # recipient names (created separately)


def _build_share_definitions(env: str) -> list[ShareDefinition]:
    """
    Returns the canonical list of shares for a given environment.
    Only prod shares are intended for external consumers; dev/test
    shares exist for integration testing.
    """
    catalog = f"{env}_lakehouse"
    return [
        ShareDefinition(
            share_name=f"lakehouse_{env}_bi_share",
            description="Gold layer tables for Power BI Direct Lake and BI consumers",
            tables=[
                TableShare(catalog, "gold", "fact_sales"),
                TableShare(catalog, "gold", "dim_customer",
                           alias="customer_dimension"),
                TableShare(catalog, "gold", "dim_product",
                           alias="product_dimension"),
            ],
            recipients=[f"recipient_powerbi_{env}"],
        ),
        ShareDefinition(
            share_name=f"lakehouse_{env}_ml_share",
            description="Feature tables for Azure ML training pipelines",
            tables=[
                TableShare(catalog, "gold", "fact_sales"),
                TableShare(catalog, "gold", "dim_customer"),
                TableShare(catalog, "features", "customer_features",
                           alias="customer_features"),
                TableShare(catalog, "features", "product_features",
                           alias="product_features"),
            ],
            recipients=[f"recipient_azureml_{env}"],
        ),
        ShareDefinition(
            share_name=f"lakehouse_{env}_partner_share",
            description="Curated Gold data for external partner consumption",
            tables=[
                # Partners see only aggregate fact — no customer PII
                TableShare(catalog, "gold", "fact_sales"),
                TableShare(catalog, "gold", "dim_product"),
            ],
            recipients=[f"recipient_partner_{env}"],
        ),
    ]


# ---------------------------------------------------------------------------
# Share provisioning
# ---------------------------------------------------------------------------

def create_share(spark: SparkSession, share: ShareDefinition) -> None:
    """Create a Delta Share and add all configured tables to it."""
    spark.sql(f"""
        CREATE SHARE IF NOT EXISTS {share.share_name}
        COMMENT '{share.description}'
    """)
    print(f"[SHARING] Created share: {share.share_name}")

    for tbl in share.tables:
        # ADD TABLE is idempotent when PARTITION clause matches
        partition_clause = ""
        if tbl.partitions:
            partition_clause = "PARTITION (" + ", ".join(tbl.partitions) + ")"

        spark.sql(f"""
            ALTER SHARE {share.share_name}
            ADD TABLE {tbl.full_name}
            AS {tbl.shared_as}
            {partition_clause}
        """)
        print(f"[SHARING]   + {tbl.full_name} → shared as '{tbl.shared_as}'")


def create_recipient(
    spark: SparkSession,
    recipient_name: str,
    comment: str = "",
    data_recipient_global_metastore_id: str | None = None,
) -> str:
    """
    Create a Delta Sharing recipient.

    If *data_recipient_global_metastore_id* is provided (for Unity Catalog
    to Unity Catalog sharing), the recipient is authenticated via Databricks
    managed identity — no token download required.

    Otherwise, a download URL is returned for token-based (open protocol)
    sharing with non-Databricks consumers (Power BI, Azure ML SDK, etc.).
    """
    if data_recipient_global_metastore_id:
        spark.sql(f"""
            CREATE RECIPIENT IF NOT EXISTS {recipient_name}
            USING ID '{data_recipient_global_metastore_id}'
            COMMENT '{comment}'
        """)
        activation_url = "N/A (Unity Catalog to Unity Catalog sharing)"
    else:
        result = spark.sql(f"""
            CREATE RECIPIENT IF NOT EXISTS {recipient_name}
            COMMENT '{comment}'
        """)
        # The activation URL is returned in the result row
        rows = result.collect()
        activation_url = rows[0]["activation_link"] if rows else "already exists"

    print(f"[SHARING] Recipient created: {recipient_name}")
    print(f"[SHARING] Activation URL: {activation_url}")
    return activation_url


def grant_share_to_recipient(
    spark: SparkSession,
    share_name: str,
    recipient_name: str,
) -> None:
    """Grant SELECT privilege on a share to a recipient."""
    spark.sql(f"""
        GRANT SELECT ON SHARE {share_name} TO RECIPIENT {recipient_name}
    """)
    print(f"[SHARING] Granted SELECT on {share_name} → {recipient_name}")


def revoke_share_from_recipient(
    spark: SparkSession,
    share_name: str,
    recipient_name: str,
) -> None:
    """Revoke SELECT privilege from a recipient (e.g. on contract expiry)."""
    spark.sql(f"""
        REVOKE SELECT ON SHARE {share_name} FROM RECIPIENT {recipient_name}
    """)
    print(f"[SHARING] Revoked SELECT on {share_name} from {recipient_name}")


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def list_shares(spark: SparkSession) -> DataFrame:
    """Return all shares visible to the current user."""
    return spark.sql("SHOW SHARES")


def list_share_tables(spark: SparkSession, share_name: str) -> DataFrame:
    """Return tables included in a share."""
    return spark.sql(f"SHOW ALL IN SHARE {share_name}")


def get_share_access_history(spark: SparkSession, share_name: str) -> DataFrame:
    """
    Query the Unity Catalog system audit table for share access events.
    Useful for compliance reporting and detecting anomalous access patterns.
    """
    return spark.sql(f"""
        SELECT
            event_time,
            user_identity.email          AS accessed_by,
            request_params.share_name    AS share_name,
            request_params.table_name    AS table_name,
            response.status_code         AS http_status,
            source_ip_address
        FROM system.access.audit
        WHERE action_name IN ('deltaSharingQueryTable', 'deltaSharingGetTableVersion')
          AND request_params.share_name = '{share_name}'
        ORDER BY event_time DESC
        LIMIT 1000
    """)


# ---------------------------------------------------------------------------
# Full setup
# ---------------------------------------------------------------------------

def setup_delta_sharing(spark: SparkSession, env: str) -> None:
    """
    Idempotent one-shot setup: create all shares, tables, recipients,
    and grant privileges for the given environment.
    """
    print(f"[SHARING] Setting up Delta Sharing for env={env}")
    shares = _build_share_definitions(env)

    for share_def in shares:
        create_share(spark, share_def)
        for recipient_name in share_def.recipients:
            create_recipient(spark, recipient_name)
            grant_share_to_recipient(spark, share_def.share_name, recipient_name)

    print(f"[SHARING] Delta Sharing setup complete. {len(shares)} shares configured.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta Sharing setup")
    parser.add_argument("--env",    required=True, help="dev | test | prod")
    parser.add_argument("--action", default="setup",
                        choices=["setup", "list", "audit"])
    parser.add_argument("--share",  default=None,  help="Share name (for list/audit)")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    if args.action == "setup":
        setup_delta_sharing(spark, args.env)
    elif args.action == "list":
        list_shares(spark).show(truncate=False)
    elif args.action == "audit":
        if not args.share:
            raise ValueError("--share required for audit action")
        get_share_access_history(spark, args.share).show(50, truncate=False)
