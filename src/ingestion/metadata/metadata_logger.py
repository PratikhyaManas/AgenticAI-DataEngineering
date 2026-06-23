"""
Metadata Logger — Data Ingestion Agent
======================================
Writes structured ingestion run metadata to a Delta table for auditability.
Every ingestion job calls this utility at the start and end of execution.

Target table: {env}_lakehouse.bronze.ingestion_metadata
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, IntegerType
)
from datetime import datetime, timezone
import uuid


METADATA_SCHEMA = StructType([
    StructField("run_id",              StringType(),    nullable=False),
    StructField("source_id",           StringType(),    nullable=False),
    StructField("source_type",         StringType(),    nullable=True),
    StructField("domain",              StringType(),    nullable=True),
    StructField("entity",              StringType(),    nullable=True),
    StructField("pipeline_name",       StringType(),    nullable=True),
    StructField("job_id",              StringType(),    nullable=True),
    StructField("run_start_ts",        TimestampType(), nullable=False),
    StructField("run_end_ts",          TimestampType(), nullable=True),
    StructField("status",              StringType(),    nullable=False),   # RUNNING | SUCCESS | FAILED
    StructField("rows_read",           LongType(),      nullable=True),
    StructField("rows_written",        LongType(),      nullable=True),
    StructField("rows_rejected",       LongType(),      nullable=True),
    StructField("target_path",         StringType(),    nullable=True),
    StructField("watermark_value",     StringType(),    nullable=True),
    StructField("error_message",       StringType(),    nullable=True),
    StructField("environment",         StringType(),    nullable=True),
    StructField("ingestion_date",      StringType(),    nullable=True),    # partition column yyyy-MM-dd
])


def _get_catalog_table(spark: SparkSession, env: str) -> str:
    return f"{env}_lakehouse.bronze.ingestion_metadata"


def create_metadata_table_if_not_exists(spark: SparkSession, env: str) -> None:
    """
    Idempotently creates the metadata Delta table.
    Safe to call on every pipeline run.
    """
    catalog_table = _get_catalog_table(spark, env)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_table} (
            run_id              STRING       NOT NULL,
            source_id           STRING       NOT NULL,
            source_type         STRING,
            domain              STRING,
            entity              STRING,
            pipeline_name       STRING,
            job_id              STRING,
            run_start_ts        TIMESTAMP    NOT NULL,
            run_end_ts          TIMESTAMP,
            status              STRING       NOT NULL,
            rows_read           BIGINT,
            rows_written        BIGINT,
            rows_rejected       BIGINT,
            target_path         STRING,
            watermark_value     STRING,
            error_message       STRING,
            environment         STRING,
            ingestion_date      STRING
        )
        USING DELTA
        PARTITIONED BY (ingestion_date)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'false',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)


def start_run(
    spark: SparkSession,
    env: str,
    source_id: str,
    source_type: str = None,
    domain: str = None,
    entity: str = None,
    pipeline_name: str = None,
    job_id: str = None,
    target_path: str = None,
    watermark_value: str = None,
) -> str:
    """
    Inserts a RUNNING record and returns the run_id (UUID).
    Call at the start of every ingestion job.
    """
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    row = [(
        run_id, source_id, source_type, domain, entity,
        pipeline_name, job_id, now, None, "RUNNING",
        None, None, None, target_path, watermark_value,
        None, env, today
    )]
    df = spark.createDataFrame(row, schema=METADATA_SCHEMA)
    catalog_table = _get_catalog_table(spark, env)
    df.write.format("delta").mode("append").saveAsTable(catalog_table)
    return run_id


def end_run(
    spark: SparkSession,
    env: str,
    run_id: str,
    status: str,           # SUCCESS | FAILED
    rows_read: int = None,
    rows_written: int = None,
    rows_rejected: int = None,
    watermark_value: str = None,
    error_message: str = None,
) -> None:
    """
    Updates the metadata record with final status and row counts.
    Uses Delta MERGE to update the existing RUNNING record.
    """
    catalog_table = _get_catalog_table(spark, env)
    now = datetime.now(timezone.utc)

    update_row = [(
        run_id, None, None, None, None,
        None, None, None, now, status,
        rows_read, rows_written, rows_rejected, None,
        watermark_value, error_message, env,
        datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )]
    update_df = spark.createDataFrame(update_row, schema=METADATA_SCHEMA)
    update_df.createOrReplaceTempView("_meta_update")

    spark.sql(f"""
        MERGE INTO {catalog_table} AS target
        USING _meta_update AS src
          ON target.run_id = src.run_id
        WHEN MATCHED THEN UPDATE SET
            target.run_end_ts      = src.run_end_ts,
            target.status          = src.status,
            target.rows_read       = src.rows_read,
            target.rows_written    = src.rows_written,
            target.rows_rejected   = src.rows_rejected,
            target.watermark_value = COALESCE(src.watermark_value, target.watermark_value),
            target.error_message   = src.error_message
    """)


def get_last_watermark(
    spark: SparkSession,
    env: str,
    source_id: str,
    entity: str,
    initial_value: str = "1900-01-01 00:00:00",
) -> str:
    """
    Returns the highest successful watermark for the given source + entity.
    Used by incremental ingestion jobs to determine the starting extraction point.
    """
    catalog_table = _get_catalog_table(spark, env)
    result = spark.sql(f"""
        SELECT COALESCE(MAX(watermark_value), '{initial_value}') AS last_watermark
        FROM {catalog_table}
        WHERE source_id = '{source_id}'
          AND entity    = '{entity}'
          AND status    = 'SUCCESS'
          AND watermark_value IS NOT NULL
    """).collect()
    return result[0]["last_watermark"] if result else initial_value
