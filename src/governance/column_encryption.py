"""
Column-Level Encryption for PII Fields
=======================================
Agent: Security & Governance Agent

Encrypts sensitive PII columns (email, phone, SSN, etc.) at write time
using AES-256 via the Fernet symmetric encryption scheme.

Design:
  - Encryption key is stored in Databricks Secret Scope (never in code)
  - Encrypted values are base64-encoded strings stored as StringType columns
  - Original column is replaced in-place; column name is unchanged
  - A companion decrypt function restores plaintext for authorised callers

Key rotation:
  - New key version is written alongside the old key in the secret scope
  - Re-encrypt job reads with old key, writes with new key, then drops old key

Usage:
    from src.governance.column_encryption import encrypt_pii_columns, decrypt_pii_columns

    # At Bronze write time:
    encrypted_df = encrypt_pii_columns(
        spark, df,
        pii_columns=["email", "phone"],
        secret_scope="pii-encryption",
        secret_key="aes-key-v1",
    )
    encrypted_df.write.format("delta").saveAsTable(...)

    # For authorised downstream queries:
    plain_df = decrypt_pii_columns(
        spark, encrypted_df,
        pii_columns=["email", "phone"],
        secret_scope="pii-encryption",
        secret_key="aes-key-v1",
    )
"""

from __future__ import annotations

import argparse
import base64

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# Key retrieval from Databricks Secrets
# ---------------------------------------------------------------------------

def _get_encryption_key(spark: SparkSession, secret_scope: str, secret_key: str) -> bytes:
    """
    Fetch the AES key from a Databricks Secret Scope.
    The key must be a URL-safe base64-encoded 32-byte value (Fernet format).
    Generate a new key with: from cryptography.fernet import Fernet; Fernet.generate_key()
    """
    try:
        # Databricks runtime path
        raw = spark.sparkContext._jvm \
            .com.databricks.dbutils_v1.DBUtilsHolder.dbutils() \
            .secrets().get(secret_scope, secret_key)
        return raw.encode()
    except Exception:
        import os
        # Local dev fallback — read from environment variable (never use in prod)
        env_var = f"PII_KEY_{secret_key.upper().replace('-', '_')}"
        raw = os.environ.get(env_var)
        if raw:
            return raw.encode()
        raise RuntimeError(
            f"Could not retrieve encryption key from secret scope '{secret_scope}' "
            f"key '{secret_key}'. Set env var {env_var} for local testing."
        )


# ---------------------------------------------------------------------------
# Encrypt / Decrypt via Spark UDF
# ---------------------------------------------------------------------------

def _make_encrypt_udf(key_bytes: bytes):
    """Return a Spark UDF that encrypts a string value with the given Fernet key."""
    from cryptography.fernet import Fernet

    key = key_bytes  # captured in closure

    def _encrypt(value: str | None) -> str | None:
        if value is None:
            return None
        fernet = Fernet(key)
        return fernet.encrypt(value.encode()).decode()

    return F.udf(_encrypt, StringType())


def _make_decrypt_udf(key_bytes: bytes):
    """Return a Spark UDF that decrypts a Fernet-encrypted base64 string."""
    from cryptography.fernet import Fernet

    key = key_bytes

    def _decrypt(value: str | None) -> str | None:
        if value is None:
            return None
        fernet = Fernet(key)
        return fernet.decrypt(value.encode()).decode()

    return F.udf(_decrypt, StringType())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def encrypt_pii_columns(
    spark: SparkSession,
    df: DataFrame,
    pii_columns: list[str],
    secret_scope: str,
    secret_key: str,
) -> DataFrame:
    """
    Encrypt specified PII columns in-place using Fernet AES-256.

    Args:
        pii_columns:  Column names containing PII to encrypt (must be StringType).
        secret_scope: Databricks secret scope containing the encryption key.
        secret_key:   Key name within the scope.

    Returns:
        DataFrame with PII columns replaced by their encrypted base64 values.
        An additional column `_pii_key_version` records which key version was used.
    """
    key_bytes   = _get_encryption_key(spark, secret_scope, secret_key)
    encrypt_udf = _make_encrypt_udf(key_bytes)

    result = df
    for col in pii_columns:
        if col not in df.columns:
            print(f"[ENCRYPT] Warning: column '{col}' not found in DataFrame — skipping")
            continue
        result = result.withColumn(col, encrypt_udf(F.col(col)))
        print(f"[ENCRYPT] Encrypted column: {col}")

    result = result.withColumn("_pii_key_version", F.lit(secret_key))
    return result


def decrypt_pii_columns(
    spark: SparkSession,
    df: DataFrame,
    pii_columns: list[str],
    secret_scope: str,
    secret_key: str,
) -> DataFrame:
    """
    Decrypt PII columns that were encrypted with encrypt_pii_columns().

    This function should only be called by authorised pipelines / users.
    In production, restrict access via Unity Catalog column masks instead
    of calling this function directly in ad-hoc queries.

    Args:
        pii_columns:  Column names to decrypt.
        secret_scope: Databricks secret scope containing the decryption key.
        secret_key:   Key name within the scope.

    Returns:
        DataFrame with PII columns restored to plaintext.
    """
    key_bytes   = _get_encryption_key(spark, secret_scope, secret_key)
    decrypt_udf = _make_decrypt_udf(key_bytes)

    result = df
    for col in pii_columns:
        if col not in df.columns:
            print(f"[DECRYPT] Warning: column '{col}' not found in DataFrame — skipping")
            continue
        result = result.withColumn(col, decrypt_udf(F.col(col)))
        print(f"[DECRYPT] Decrypted column: {col}")

    return result


# ---------------------------------------------------------------------------
# Unity Catalog column mask (declarative alternative)
# ---------------------------------------------------------------------------

def apply_column_masks(
    spark: SparkSession,
    env: str,
    table_fqn: str,
    pii_columns: list[str],
    allowed_group: str = "data-engineers",
) -> None:
    """
    Apply Unity Catalog column masks so that PII columns appear as NULL
    for users not in the allowed_group.  Members of the group see plaintext.

    Unity Catalog column masks are applied at query time and require no
    physical encryption — they are complementary to Fernet encryption.

    Args:
        table_fqn:     Fully qualified table name (catalog.schema.table).
        pii_columns:   Columns to mask.
        allowed_group: Databricks workspace group that can see plaintext.
    """
    catalog = table_fqn.split(".")[0]

    for col in pii_columns:
        # Create mask function in the same catalog
        mask_fn = f"{catalog}.governance.mask_{col.lower()}"
        spark.sql(f"""
            CREATE OR REPLACE FUNCTION {mask_fn}(value STRING)
              RETURN CASE
                WHEN is_account_group_member('{allowed_group}') THEN value
                ELSE '***MASKED***'
              END
        """)

        spark.sql(f"""
            ALTER TABLE {table_fqn}
              ALTER COLUMN {col}
              SET MASK {mask_fn}
        """)
        print(f"[MASK] Applied column mask on {table_fqn}.{col} — visible to group: {allowed_group}")


# ---------------------------------------------------------------------------
# Key generation helper (run once, store output in Databricks secrets)
# ---------------------------------------------------------------------------

def generate_fernet_key() -> str:
    """
    Generate a new Fernet key.
    Store the output in your Databricks Secret Scope:
      databricks secrets put --scope pii-encryption --key aes-key-v1
    """
    from cryptography.fernet import Fernet
    return Fernet.generate_key().decode()


# ---------------------------------------------------------------------------
# Entry point — setup column masks on existing tables
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply PII column masks via Unity Catalog")
    parser.add_argument("--env",           required=True, help="dev | test | prod")
    parser.add_argument("--action",        default="mask",
                        choices=["mask", "generate-key"],
                        help="mask: apply UC column masks; generate-key: print a new Fernet key")
    parser.add_argument("--allowed-group", default="data-engineers",
                        help="Databricks group allowed to see plaintext PII")
    args = parser.parse_args()

    if args.action == "generate-key":
        print(f"New Fernet key: {generate_fernet_key()}")
        print("Store this in: databricks secrets put --scope pii-encryption --key aes-key-v1")
    else:
        spark = SparkSession.builder.getOrCreate()

        pii_tables = {
            f"{args.env}_lakehouse.silver.sales_customers": ["email", "phone"],
            f"{args.env}_lakehouse.gold.dim_customer":      ["email", "phone"],
        }

        for table, cols in pii_tables.items():
            apply_column_masks(spark, args.env, table, cols, args.allowed_group)

        print("[DONE] Column masks applied.")
