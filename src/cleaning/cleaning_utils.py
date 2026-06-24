"""
Shared PySpark Cleaning Utilities — Bronze → Silver
====================================================
Agent: Data Cleaning & Standardization Agent

Reusable, deterministic transformation functions applied in all Silver jobs.
All functions are pure (no side effects) and tested independently.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, TimestampType, DateType, IntegerType, LongType, StringType,
)
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# 1. Type Casting
# ---------------------------------------------------------------------------

def cast_columns(df: DataFrame, cast_map: Dict[str, str]) -> DataFrame:
    """
    Cast columns to target types using a single select() pass.
    cast_map: { column_name: spark_type_string }
    Example: {"amount": "decimal(18,2)", "order_date": "date"}

    Records that fail casting produce NULL (Spark default for safe cast).
    Downstream null checks will route these to quarantine.
    """
    return df.select(
        *[
            F.col(f.name).cast(cast_map[f.name]).alias(f.name)
            if f.name in cast_map else F.col(f.name)
            for f in df.schema.fields
        ]
    )


# ---------------------------------------------------------------------------
# 2. String Standardization
# ---------------------------------------------------------------------------

def trim_string_columns(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
    """Trim leading/trailing whitespace from string columns in one select() pass."""
    trim_set = set(
        columns if columns is not None
        else [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    )
    return df.select(
        *[
            F.trim(F.col(f.name)).alias(f.name) if f.name in trim_set else F.col(f.name)
            for f in df.schema.fields
        ]
    )


def normalize_to_upper(df: DataFrame, columns: List[str]) -> DataFrame:
    """Uppercase categorical string columns in one select() pass."""
    upper_set = set(columns)
    return df.select(
        *[
            F.upper(F.col(f.name)).alias(f.name) if f.name in upper_set else F.col(f.name)
            for f in df.schema.fields
        ]
    )


def normalize_nulls(df: DataFrame, null_strings: Optional[List[str]] = None) -> DataFrame:
    """
    Replace common null-equivalent string values with actual NULL in one select() pass.
    Handles: 'NULL', 'N/A', 'NA', 'none', '', 'undefined', 'unknown'
    """
    if null_strings is None:
        null_strings = ["NULL", "N/A", "NA", "none", "", "undefined", "unknown", "UNKNOWN", "#N/A"]
    upper_nulls = {s.upper() for s in null_strings}
    string_cols = {f.name for f in df.schema.fields if isinstance(f.dataType, StringType)}
    return df.select(
        *[
            F.when(
                F.upper(F.trim(F.col(f.name))).isin(upper_nulls),
                F.lit(None).cast("string"),
            ).otherwise(F.col(f.name)).alias(f.name)
            if f.name in string_cols else F.col(f.name)
            for f in df.schema.fields
        ]
    )


# ---------------------------------------------------------------------------
# 3. Timestamp / Date Normalization
# ---------------------------------------------------------------------------

STANDARD_TZ = "UTC"

def parse_timestamps(
    df: DataFrame,
    timestamp_cols: List[str],
    input_formats: Optional[List[str]] = None,
    target_tz: str = STANDARD_TZ,
) -> DataFrame:
    """
    Parse string timestamp columns into TimestampType and normalize to UTC
    in a single select() pass — all columns transformed in one Spark stage.
    Tries multiple input formats in sequence; produces NULL if none match.
    """
    if input_formats is None:
        input_formats = [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssXXX",
            "MM/dd/yyyy HH:mm:ss",
            "dd-MM-yyyy HH:mm:ss",
        ]

    ts_set = set(timestamp_cols)

    def _parse_expr(name):
        parsed = F.lit(None).cast(TimestampType())
        for fmt in input_formats:
            parsed = F.coalesce(parsed, F.to_timestamp(F.col(name), fmt))
        return F.to_utc_timestamp(parsed, target_tz).alias(name)

    return df.select(
        *[
            _parse_expr(f.name) if f.name in ts_set else F.col(f.name)
            for f in df.schema.fields
        ]
    )


def parse_dates(
    df: DataFrame,
    date_cols: List[str],
    input_formats: Optional[List[str]] = None,
) -> DataFrame:
    """Parse string date columns into DateType in a single select() pass."""
    if input_formats is None:
        input_formats = [
            "yyyy-MM-dd",
            "MM/dd/yyyy",
            "dd-MM-yyyy",
            "yyyyMMdd",
        ]

    date_set = set(date_cols)

    def _date_expr(name):
        parsed = F.lit(None).cast(DateType())
        for fmt in input_formats:
            parsed = F.coalesce(parsed, F.to_date(F.col(name), fmt))
        return parsed.alias(name)

    return df.select(
        *[
            _date_expr(f.name) if f.name in date_set else F.col(f.name)
            for f in df.schema.fields
        ]
    )


# ---------------------------------------------------------------------------
# 4. Deduplication
# ---------------------------------------------------------------------------

def deduplicate(
    df: DataFrame,
    primary_keys: List[str],
    order_by_col: str = "_ingestion_timestamp",
    descending: bool = True,
) -> DataFrame:
    """
    Deduplicate DataFrame by primary keys, keeping the latest record
    per key (by order_by_col).  Deterministic: same input → same output.
    """
    order_col = F.col(order_by_col).desc() if descending else F.col(order_by_col).asc()
    window = Window.partitionBy(*primary_keys).orderBy(order_col)
    return (
        df.withColumn("_row_number", F.row_number().over(window))
          .filter(F.col("_row_number") == 1)
          .drop("_row_number")
    )


# ---------------------------------------------------------------------------
# 5. Null / Completeness Checks
# ---------------------------------------------------------------------------

def flag_null_violations(
    df: DataFrame,
    not_null_cols: List[str],
    error_col: str = "_error_reason",
) -> DataFrame:
    """
    Adds an _error_reason column listing all NOT NULL violations.
    Records with no violations get NULL in _error_reason.
    """
    null_checks = [
        F.when(F.col(c).isNull(), F.lit(f"NOT_NULL_VIOLATION:{c}"))
        for c in not_null_cols
    ]
    errors_array = F.array(*null_checks)
    non_null_errors = F.array_compact(errors_array)   # requires Spark 3.4 / DBR 12+

    df = df.withColumn(
        error_col,
        F.when(F.size(non_null_errors) > 0,
               F.concat_ws("|", non_null_errors))
         .otherwise(F.lit(None))
    )
    return df


# ---------------------------------------------------------------------------
# 6. Range Validation
# ---------------------------------------------------------------------------

def flag_range_violations(
    df: DataFrame,
    range_rules: List[Dict],
    error_col: str = "_error_reason",
) -> DataFrame:
    """
    range_rules: list of dicts with keys:
        column (str), min (optional), max (optional), allow_null (bool)

    Appends violation messages to existing _error_reason column.

    Example:
        range_rules = [
            {"column": "amount",   "min": 0,    "allow_null": False},
            {"column": "quantity", "min": 1,    "max": 9999},
        ]
    """
    existing_errors = F.col(error_col) if error_col in df.columns else F.lit(None)

    violation_exprs = []
    for rule in range_rules:
        col_name   = rule["column"]
        min_val    = rule.get("min")
        max_val    = rule.get("max")
        allow_null = rule.get("allow_null", True)

        conditions = []
        if not allow_null:
            conditions.append(F.col(col_name).isNull())
        if min_val is not None:
            conditions.append(F.col(col_name) < F.lit(min_val))
        if max_val is not None:
            conditions.append(F.col(col_name) > F.lit(max_val))

        if conditions:
            combined = conditions[0]
            for c in conditions[1:]:
                combined = combined | c
            violation_exprs.append(
                F.when(combined, F.lit(f"RANGE_VIOLATION:{col_name}"))
            )

    if not violation_exprs:
        return df

    new_errors = F.array_compact(F.array(*violation_exprs))
    df = df.withColumn(
        error_col,
        F.when(
            F.size(new_errors) > 0,
            F.concat_ws(
                "|",
                F.coalesce(existing_errors, F.lit("")),
                F.concat_ws("|", new_errors)
            )
        ).otherwise(existing_errors)
    )
    return df


# ---------------------------------------------------------------------------
# 7. Reference Data Validation (lookup against Delta table)
# ---------------------------------------------------------------------------

def flag_invalid_codes(
    df: DataFrame,
    spark,
    ref_table: str,
    ref_column: str,
    source_column: str,
    error_col: str = "_error_reason",
) -> DataFrame:
    """
    Validates source_column values against a reference Delta table.
    Rows with codes not in the reference are flagged.

    ref_table: fully qualified catalog.schema.table
    ref_column: column in reference table containing valid codes
    """
    valid_codes = (
        spark.table(ref_table)
             .select(F.col(ref_column).alias("_valid_code"))
             .distinct()
    )
    joined = df.join(valid_codes, df[source_column] == F.col("_valid_code"), how="left")
    existing_errors = F.col(error_col) if error_col in df.columns else F.lit(None)

    result = (
        joined
        .withColumn(
            error_col,
            F.when(
                (F.col(source_column).isNotNull()) & (F.col("_valid_code").isNull()),
                F.concat_ws("|",
                    F.coalesce(existing_errors, F.lit("")),
                    F.lit(f"INVALID_CODE:{source_column}")
                )
            ).otherwise(existing_errors)
        )
        .drop("_valid_code")
    )
    return result


# ---------------------------------------------------------------------------
# 8. Quarantine Splitter
# ---------------------------------------------------------------------------

def split_valid_quarantine(
    df: DataFrame,
    error_col: str = "_error_reason",
) -> Tuple[DataFrame, DataFrame]:
    """
    Splits a DataFrame into:
      - valid_df:      rows where _error_reason IS NULL
      - quarantine_df: rows where _error_reason IS NOT NULL

    Returns (valid_df, quarantine_df).
    """
    valid_df      = df.filter(F.col(error_col).isNull()).drop(error_col)
    quarantine_df = df.filter(F.col(error_col).isNotNull())
    return valid_df, quarantine_df


# ---------------------------------------------------------------------------
# 9. Silver Metadata Columns
# ---------------------------------------------------------------------------

def add_silver_metadata(
    df: DataFrame,
    source_system: str,
    silver_run_id: str,
) -> DataFrame:
    """Enrich Silver records with standard audit columns."""
    return (
        df
        .withColumn("_silver_run_id",       F.lit(silver_run_id))
        .withColumn("_silver_load_ts",       F.current_timestamp())
        .withColumn("_source_system",        F.lit(source_system))
        .withColumn("_valid_from",           F.current_timestamp())
        .withColumn("_valid_to",             F.lit(None).cast(TimestampType()))
        .withColumn("_is_current",           F.lit(True))
    )
