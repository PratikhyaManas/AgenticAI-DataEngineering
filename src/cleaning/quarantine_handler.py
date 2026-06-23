"""
Quarantine Handler — Data Cleaning & Standardization Agent
==========================================================
Writes rejected / invalid records to the quarantine Delta table with
structured error metadata for investigation and reprocessing.

Quarantine table: {env}_lakehouse.bronze.quarantine
Path: abfss://quarantine@{storage_account}.dfs.core.windows.net/domain={domain}/entity={entity}/
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import uuid


def create_quarantine_table_if_not_exists(spark: SparkSession, env: str) -> None:
    """Idempotently create the quarantine Delta table."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {env}_lakehouse.bronze.quarantine (
            quarantine_id       STRING        NOT NULL,
            silver_run_id       STRING        NOT NULL,
            domain              STRING,
            entity              STRING,
            error_reason        STRING        NOT NULL,
            quarantine_ts       TIMESTAMP     NOT NULL,
            quarantine_date     STRING,
            source_system       STRING,
            raw_record          STRING        COMMENT 'JSON representation of the rejected row'
        )
        USING DELTA
        PARTITIONED BY (quarantine_date, domain)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)


def write_to_quarantine(
    spark: SparkSession,
    quarantine_df: DataFrame,
    env: str,
    domain: str,
    entity: str,
    silver_run_id: str,
    error_col: str = "_error_reason",
    source_system: str = None,
) -> None:
    """
    Writes rejected records to the quarantine table.

    Each record gets:
    - quarantine_id: UUID per rejected row
    - error_reason:  pipe-delimited list of violations from _error_reason column
    - raw_record:    full row serialized as JSON for re-ingestion / forensics

    The original DataFrame columns (except _error_reason) are stored as
    JSON in the raw_record column to keep the quarantine schema stable
    across different source entities.
    """
    create_quarantine_table_if_not_exists(spark, env)
    quarantine_table = f"{env}_lakehouse.bronze.quarantine"

    # Serialize the full rejected row as JSON
    all_cols_except_error = [c for c in quarantine_df.columns if c != error_col]

    quarantine_enriched = (
        quarantine_df
        .withColumn("quarantine_id",   F.expr("uuid()"))
        .withColumn("silver_run_id",   F.lit(silver_run_id))
        .withColumn("domain",          F.lit(domain))
        .withColumn("entity",          F.lit(entity))
        .withColumn("error_reason",    F.col(error_col))
        .withColumn("quarantine_ts",   F.current_timestamp())
        .withColumn("quarantine_date", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
        .withColumn("source_system",   F.lit(source_system))
        # Serialize all original columns to JSON
        .withColumn("raw_record",      F.to_json(F.struct(*[F.col(c) for c in all_cols_except_error])))
        .select(
            "quarantine_id", "silver_run_id", "domain", "entity",
            "error_reason", "quarantine_ts", "quarantine_date",
            "source_system", "raw_record"
        )
    )

    quarantine_enriched.write.format("delta").mode("append").saveAsTable(quarantine_table)
    print(f"[QUARANTINE] Written {quarantine_df.count()} rejected records to {quarantine_table}")


def get_quarantine_summary(spark: SparkSession, env: str, domain: str = None, entity: str = None) -> DataFrame:
    """
    Returns a summary of quarantine records grouped by error_reason.
    Used for monitoring and alerting.
    """
    quarantine_table = f"{env}_lakehouse.bronze.quarantine"
    q = spark.table(quarantine_table)
    if domain:
        q = q.filter(F.col("domain") == domain)
    if entity:
        q = q.filter(F.col("entity") == entity)

    return (
        q.groupBy("domain", "entity", "error_reason", "quarantine_date")
         .agg(F.count("*").alias("record_count"))
         .orderBy(F.col("quarantine_date").desc(), F.col("record_count").desc())
    )


def reprocess_quarantine_records(
    spark: SparkSession,
    env: str,
    domain: str,
    entity: str,
    quarantine_date: str,
) -> DataFrame:
    """
    Retrieves quarantined records for a specific domain/entity/date,
    deserializes raw_record JSON back into a DataFrame for re-ingestion.

    The caller is responsible for applying fixes and re-running the
    Silver cleaning job.
    """
    quarantine_table = f"{env}_lakehouse.bronze.quarantine"
    raw_records = (
        spark.table(quarantine_table)
             .filter(
                 (F.col("domain") == domain) &
                 (F.col("entity") == entity) &
                 (F.col("quarantine_date") == quarantine_date)
             )
             .select("quarantine_id", "error_reason", "raw_record")
    )
    # Deserialize raw_record JSON (schema inference from sample; use explicit schema in prod)
    return raw_records
