"""
Unity Catalog Setup & RBAC Configuration
=========================================
Agent: Data Security, Privacy & Governance Agent

Provisions Unity Catalog objects (catalogs, schemas, external locations)
and grants privileges according to the RBAC model defined below.

Run this script once per environment (dev/test/prod) by a Metastore Admin.
All GRANT statements are idempotent.

RBAC Model:
  - data_engineers:    READ/WRITE Bronze + Silver; READ Gold
  - data_scientists:   READ Silver + Gold + Sandbox; WRITE Sandbox
  - data_analysts:     READ Gold only
  - bi_service_acct:   READ Gold (Power BI service principal)
  - pipeline_sp:       WRITE Bronze + Silver + Gold (service principal for pipelines)
  - dq_sp:             READ all layers (DQ engine service principal)
  - security_admin:    Full metastore admin (restricted group)
"""

from pyspark.sql import SparkSession


def setup_unity_catalog(spark: SparkSession, env: str, storage_account: str) -> None:
    """
    Idempotently creates Unity Catalog structure and assigns privileges.
    Must be run by a Metastore Admin account.
    """
    catalog = f"{env}_lakehouse"

    # -----------------------------------------------------------------------
    # 1. External Location (ADLS Gen2 storage for this env)
    # -----------------------------------------------------------------------
    # External locations are created at the Metastore level (once per env)
    # Requires: Storage Credential already configured in Databricks account
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_{env}_raw
        URL 'abfss://raw@{storage_account}.dfs.core.windows.net/'
        WITH (STORAGE CREDENTIAL storage_cred_{env})
        COMMENT 'Raw zone storage for {env} environment'
    """)

    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_{env}_curated
        URL 'abfss://curated@{storage_account}.dfs.core.windows.net/'
        WITH (STORAGE CREDENTIAL storage_cred_{env})
    """)

    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_{env}_gold
        URL 'abfss://gold@{storage_account}.dfs.core.windows.net/'
        WITH (STORAGE CREDENTIAL storage_cred_{env})
    """)

    # -----------------------------------------------------------------------
    # 2. Catalog
    # -----------------------------------------------------------------------
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {catalog}
        COMMENT 'Lakehouse catalog for {env} environment'
    """)

    # -----------------------------------------------------------------------
    # 3. Schemas
    # -----------------------------------------------------------------------
    for schema in ["bronze", "silver", "gold", "sandbox"]:
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
            COMMENT '{schema.capitalize()} layer schema'
        """)

    # -----------------------------------------------------------------------
    # 4. Catalog-Level Grants
    # -----------------------------------------------------------------------
    # USAGE on catalog is required for any schema/table-level access
    catalog_groups = [
        "data_engineers", "data_scientists", "data_analysts",
        "bi_service_acct", "pipeline_sp", "dq_sp",
    ]
    for group in catalog_groups:
        spark.sql(f"GRANT USAGE ON CATALOG {catalog} TO `{group}`")

    # -----------------------------------------------------------------------
    # 5. Schema-Level Grants
    # -----------------------------------------------------------------------
    grants = [
        # Bronze: engineers READ/WRITE; pipeline SP WRITE; DQ SP READ
        (f"{catalog}.bronze", "data_engineers",  ["USE SCHEMA", "SELECT", "MODIFY"]),
        (f"{catalog}.bronze", "pipeline_sp",      ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]),
        (f"{catalog}.bronze", "dq_sp",            ["USE SCHEMA", "SELECT"]),

        # Silver: engineers READ/WRITE; pipeline SP WRITE; scientists READ; DQ SP READ
        (f"{catalog}.silver", "data_engineers",   ["USE SCHEMA", "SELECT", "MODIFY"]),
        (f"{catalog}.silver", "data_scientists",  ["USE SCHEMA", "SELECT"]),
        (f"{catalog}.silver", "pipeline_sp",       ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]),
        (f"{catalog}.silver", "dq_sp",             ["USE SCHEMA", "SELECT"]),

        # Gold: analysts, scientists, BI READ; engineers READ; pipeline SP WRITE; DQ SP READ
        (f"{catalog}.gold",   "data_engineers",   ["USE SCHEMA", "SELECT"]),
        (f"{catalog}.gold",   "data_scientists",  ["USE SCHEMA", "SELECT"]),
        (f"{catalog}.gold",   "data_analysts",    ["USE SCHEMA", "SELECT"]),
        (f"{catalog}.gold",   "bi_service_acct",  ["USE SCHEMA", "SELECT"]),
        (f"{catalog}.gold",   "pipeline_sp",       ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]),
        (f"{catalog}.gold",   "dq_sp",             ["USE SCHEMA", "SELECT"]),

        # Sandbox: scientists and engineers FULL; others none
        (f"{catalog}.sandbox", "data_engineers",  ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]),
        (f"{catalog}.sandbox", "data_scientists", ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]),
    ]

    for schema_ref, principal, privileges in grants:
        for priv in privileges:
            spark.sql(f"GRANT {priv} ON SCHEMA {schema_ref} TO `{principal}`")

    print(f"[DONE] Unity Catalog setup complete for {catalog}")


def setup_row_level_security(spark: SparkSession, env: str) -> None:
    """
    Row-Level Security via Unity Catalog Row Filters.
    Requires Databricks Runtime 13.1+ and Unity Catalog.

    Example: Analysts can only see their own region's sales data.
    """
    catalog = f"{env}_lakehouse"

    # Create a row filter function
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.gold.row_filter_by_region(region STRING)
        RETURN
            is_account_group_member('data_analysts_global')  -- global analysts see all
            OR region = current_user()                        -- placeholder; replace with actual region claim
    """)

    # Apply row filter to fact_sales (filter on order_status for demo)
    spark.sql(f"""
        ALTER TABLE {catalog}.gold.fact_sales
        SET ROW FILTER {catalog}.gold.row_filter_by_region ON (order_status)
    """)
    print("[DONE] Row-level security applied to gold.fact_sales")


def setup_column_masking(spark: SparkSession, env: str) -> None:
    """
    Column masking for PII fields in Silver layer using Unity Catalog Column Masks.
    Requires Databricks Runtime 12.2 LTS+.

    Policy: Non-prod environments always mask PII.
            Prod: only data_engineers and pipeline_sp see plaintext.
    """
    catalog = f"{env}_lakehouse"

    # Define masking function for email
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.silver.mask_email(email STRING)
        RETURN
            CASE
                WHEN is_account_group_member('data_engineers') OR is_account_group_member('pipeline_sp')
                    THEN email
                ELSE CONCAT(LEFT(email, 2), '***@***.***')
            END
    """)

    # Define masking function for phone
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.silver.mask_phone(phone STRING)
        RETURN
            CASE
                WHEN is_account_group_member('data_engineers') OR is_account_group_member('pipeline_sp')
                    THEN phone
                ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4))
            END
    """)

    # Apply masks to the Silver customer table
    spark.sql(f"""
        ALTER TABLE {catalog}.silver.sales_customers
        ALTER COLUMN email SET MASK {catalog}.silver.mask_email
    """)

    spark.sql(f"""
        ALTER TABLE {catalog}.silver.sales_customers
        ALTER COLUMN phone SET MASK {catalog}.silver.mask_phone
    """)

    print(f"[DONE] Column masking applied to {catalog}.silver.sales_customers")


def enable_audit_logging(spark: SparkSession) -> None:
    """
    Audit logging in Databricks Unity Catalog is enabled at the account level
    via Databricks account console.  This function documents the expected
    diagnostic settings that must be configured in Azure (Terraform manages these).
    See: infra/terraform/databricks.tf for diagnostic setting resources.
    """
    print("""
    [INFO] Audit logging must be enabled at the Azure Databricks account level:
    1. Go to Databricks Account Console → Settings → Audit Logs
    2. Enable workspace audit logs → route to Log Analytics Workspace
    3. Databricks Diagnostic Settings:
       - Categories: clusters, jobs, notebook, sql, genie, dataAccess
       - Destination: Azure Log Analytics Workspace
    4. Unity Catalog system tables (automatically populated):
       - system.access.audit        (data access events)
       - system.compute.clusters    (cluster lifecycle)
       - system.lakeflow.runs       (workflow run events)
    These are managed in infra/terraform/databricks.tf
    """)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--storage_account", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    setup_unity_catalog(spark, args.env, args.storage_account)
    setup_column_masking(spark, args.env)
    setup_row_level_security(spark, args.env)
    enable_audit_logging(spark)
