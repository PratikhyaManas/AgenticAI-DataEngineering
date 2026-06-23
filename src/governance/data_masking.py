"""
Data Masking & Tokenization Utilities — Security Agent
======================================================
Reusable PySpark functions for PII anonymization in Delta Lake.
These are used in both cleaning jobs and DQ output sanitization.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib


# ---------------------------------------------------------------------------
# 1. Deterministic Pseudonymization (SHA-256 HMAC)
# ---------------------------------------------------------------------------

def pseudonymize_column(df: DataFrame, column: str, salt_col_name: str = None, salt_value: str = None) -> DataFrame:
    """
    Replace PII column with a deterministic SHA-256 hash.
    Consistent across runs (same input → same hash); not reversible without salt.

    In production, salt_value is fetched from Azure Key Vault.
    Use a consistent salt per data domain (stored in Key Vault).

    IMPORTANT: Do NOT use this for search/join use cases without a secure
    pseudonym mapping table; use tokenization instead.
    """
    if salt_value:
        salted = F.concat(F.lit(salt_value), F.col(column).cast(StringType()))
    else:
        salted = F.col(column).cast(StringType())

    hashed = F.sha2(salted, 256)
    return df.withColumn(column, hashed)


# ---------------------------------------------------------------------------
# 2. Email Redaction
# ---------------------------------------------------------------------------

def redact_email(df: DataFrame, column: str) -> DataFrame:
    """
    Replaces email with format: xx***@***.<domain_tld>
    Preserves domain TLD for analytics purposes while removing identifying info.
    Example: john.doe@example.com → jo***@***.com
    """
    redacted = F.regexp_replace(
        F.col(column),
        r"^(.{2})(.+)(@)(.+\.)(.+)$",
        r"$1***$3***.$5"
    )
    return df.withColumn(column, F.when(F.col(column).isNotNull(), redacted).otherwise(F.lit(None)))


# ---------------------------------------------------------------------------
# 3. Phone Masking
# ---------------------------------------------------------------------------

def mask_phone(df: DataFrame, column: str, keep_last_digits: int = 4) -> DataFrame:
    """
    Masks phone number, keeping only the last N digits.
    Example: +49-89-123456789 → ***-***-6789
    """
    digits_only = F.regexp_replace(F.col(column), r"[^0-9]", "")
    masked = F.concat(
        F.lit("***-***-"),
        F.substring(digits_only, -keep_last_digits, keep_last_digits)
    )
    return df.withColumn(column, F.when(F.col(column).isNotNull(), masked).otherwise(F.lit(None)))


# ---------------------------------------------------------------------------
# 4. Partial Masking (Name, Address)
# ---------------------------------------------------------------------------

def mask_name(df: DataFrame, column: str) -> DataFrame:
    """
    Partially masks names: 'John Doe' → 'J*** D**'
    Retains first character of each word.
    """
    mask_expr = F.regexp_replace(F.col(column), r"(\w)\w+", r"$1***")
    return df.withColumn(column, F.when(F.col(column).isNotNull(), mask_expr).otherwise(F.lit(None)))


# ---------------------------------------------------------------------------
# 5. Generalization (Age → Age Band)
# ---------------------------------------------------------------------------

def generalize_age(df: DataFrame, age_column: str, output_column: str = None) -> DataFrame:
    """
    Replace precise age with age band for k-anonymity.
    18 → '18-24', 27 → '25-34', etc.
    """
    out_col = output_column or f"{age_column}_band"
    return df.withColumn(
        out_col,
        F.when(F.col(age_column) < 18,  F.lit("<18"))
         .when(F.col(age_column) < 25,  F.lit("18-24"))
         .when(F.col(age_column) < 35,  F.lit("25-34"))
         .when(F.col(age_column) < 45,  F.lit("35-44"))
         .when(F.col(age_column) < 55,  F.lit("45-54"))
         .when(F.col(age_column) < 65,  F.lit("55-64"))
         .otherwise(F.lit("65+"))
    )


# ---------------------------------------------------------------------------
# 6. Data Classification Tagging (for Unity Catalog tags)
# ---------------------------------------------------------------------------

# Data Classification Scheme (aligned with GDPR / SOC2)
DATA_CLASSIFICATION = {
    # Column name → (classification, description)
    "email":            ("PII",       "Direct identifier – GDPR Art. 4"),
    "phone":            ("PII",       "Direct identifier – GDPR Art. 4"),
    "first_name":       ("PII",       "Direct identifier"),
    "last_name":        ("PII",       "Direct identifier"),
    "full_name":        ("PII",       "Direct identifier"),
    "date_of_birth":    ("PII",       "Special category data – GDPR Art. 9"),
    "national_id":      ("PII",       "Government identifier"),
    "ip_address":       ("PII",       "Online identifier – GDPR Recital 30"),
    "credit_card":      ("PCI",       "Payment card data"),
    "iban":             ("PCI",       "Bank account identifier"),
    "order_id":         ("INTERNAL",  "Business identifier – not PII"),
    "customer_id":      ("INTERNAL",  "Pseudonymous business key"),
    "amount":           ("SENSITIVE", "Financial data – SOC2 in-scope"),
    "product_name":     ("PUBLIC",    "Public catalog data"),
    "category":         ("PUBLIC",    "Public catalog data"),
}


def apply_unity_catalog_tags(spark, catalog: str, schema: str, table: str) -> None:
    """
    Applies Unity Catalog column tags based on DATA_CLASSIFICATION mapping.
    Requires Databricks Runtime 13.1+ with Unity Catalog.
    """
    for column, (classification, description) in DATA_CLASSIFICATION.items():
        try:
            spark.sql(f"""
                ALTER TABLE {catalog}.{schema}.{table}
                ALTER COLUMN {column}
                SET TAGS ('classification' = '{classification}', 'description' = '{description}')
            """)
        except Exception:
            pass  # Column may not exist in every table; skip silently
    print(f"[DONE] Column tags applied to {catalog}.{schema}.{table}")
