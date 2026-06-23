"""
Batch Ingestion: Azure SQL Database → Bronze Delta (via JDBC)
=============================================================
Agent: Data Ingestion Agent

Pattern: Incremental watermark-based extraction from Azure SQL DB using
Databricks JDBC reader with parallel partitioning, writing to Bronze Delta.

Secrets are fetched from Azure Key Vault-backed Databricks secret scope.
No credentials are stored in code or config files.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from src.ingestion.metadata.metadata_logger import (
    create_metadata_table_if_not_exists,
    start_run,
    end_run,
    get_last_watermark,
)
from src.utils.config import load_source_config


def build_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def ingest_sql_table(
    spark: SparkSession,
    config: dict,
    table_cfg: dict,
    env: str,
) -> None:
    """
    Incrementally extracts one table from Azure SQL DB and appends to Bronze Delta.
    Uses watermark tracking and parallel JDBC partitioning for performance.
    """
    conn_cfg  = config["connection"]
    tgt_cfg   = config["target"]
    source_id = config["source_id"]
    domain    = config.get("domain", "unknown")
    entity    = table_cfg["target_entity"]

    secret_scope = conn_cfg["secret_scope"]
    jdbc_url     = dbutils.secrets.get(scope=secret_scope, key=conn_cfg["secret_key_jdbc_url"])  # noqa: F821

    ingest_cfg       = config["ingestion"]
    watermark_col    = ingest_cfg.get("watermark_column", "updated_at")
    initial_wm       = ingest_cfg.get("watermark_initial", "1900-01-01 00:00:00")
    strategy         = ingest_cfg.get("strategy", "incremental")
    source_table     = table_cfg["source_table"]
    partition_column = table_cfg.get("partition_column")
    num_partitions   = table_cfg.get("num_partitions", 8)
    lower_bound      = str(table_cfg.get("lower_bound", "1"))
    upper_bound      = str(table_cfg.get("upper_bound", "999999999"))
    fetch_size       = table_cfg.get("fetch_size", 10000)

    storage_account = spark.conf.get("spark.lakehouse.storage_account")
    target_path = tgt_cfg["adls_landing_path"].replace("{storage_account}", storage_account)
    catalog_table = (
        f"{env}_lakehouse.{tgt_cfg['bronze_schema']}"
        f".{domain}_{entity}"
    )

    create_metadata_table_if_not_exists(spark, env)

    last_watermark = (
        initial_wm if strategy == "full_refresh"
        else get_last_watermark(spark, env, source_id, entity, initial_wm)
    )

    run_id = start_run(
        spark, env, source_id,
        source_type=config.get("source_type"),
        domain=domain, entity=entity,
        pipeline_name="sql_batch_ingestion",
        target_path=target_path,
        watermark_value=last_watermark,
    )

    rows_read = rows_written = 0
    error_msg = new_watermark = None
    status = "FAILED"

    try:
        # Build SQL query with incremental filter
        if strategy == "incremental":
            sql_query = (
                f"(SELECT * FROM {source_table} "
                f"WHERE {watermark_col} > CAST('{last_watermark}' AS DATETIME2)) AS incremental_data"
            )
        else:
            sql_query = f"(SELECT * FROM {source_table}) AS full_data"

        jdbc_options = {
            "url":             jdbc_url,
            "dbtable":         sql_query,
            "fetchsize":       str(fetch_size),
            "encrypt":         "true",
            "trustServerCertificate": "false",
        }

        # Add parallel partitioning if a numeric/date partition column is given
        if partition_column:
            jdbc_options.update({
                "partitionColumn": partition_column,
                "lowerBound":      lower_bound,
                "upperBound":      upper_bound,
                "numPartitions":   str(num_partitions),
            })

        raw_df = spark.read.format("jdbc").options(**jdbc_options).load()

        # Persist once — reused for count, max-watermark and the write below
        raw_df = raw_df.persist(StorageLevel.MEMORY_AND_DISK)

        # Compute row count and new watermark in a single aggregation action
        if strategy == "incremental":
            agg_row = raw_df.agg(
                F.count("*").alias("rows"),
                F.max(F.col(watermark_col).cast("string")).alias("max_wm"),
            ).collect()[0]
            rows_read     = int(agg_row["rows"])
            new_watermark = agg_row["max_wm"]
        else:
            rows_read     = raw_df.count()
            new_watermark = None

        if rows_read == 0:
            print(f"[INFO] No new rows for {entity} since {last_watermark}. Skipping write.")
            status = "SUCCESS"
            new_watermark = last_watermark
        else:
            enriched_df = (
                raw_df
                .withColumn("_ingestion_run_id",    F.lit(run_id))
                .withColumn("_ingestion_timestamp", F.current_timestamp())
                .withColumn("_source_system",        F.lit(source_id))
                .withColumn("ingestion_date",        F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
            )

            (
                enriched_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("ingestion_date")
                .saveAsTable(catalog_table)
            )

            rows_written = rows_read
            status = "SUCCESS"
            print(f"[INFO] Written {rows_written} rows to {catalog_table}. New watermark: {new_watermark}")

        raw_df.unpersist()

    except Exception as exc:
        error_msg = str(exc)
        raise

    finally:
        end_run(
            spark, env, run_id, status,
            rows_read=rows_read,
            rows_written=rows_written,
            watermark_value=new_watermark,
            error_message=error_msg,
        )


def ingest_all_tables(spark: SparkSession, config: dict, env: str) -> None:
    """Iterates through all tables in the source config."""
    for table_cfg in config["ingestion"].get("tables", []):
        print(f"[START] Ingesting table: {table_cfg['source_table']} → {table_cfg['target_entity']}")
        ingest_sql_table(spark, config, table_cfg, env)
        print(f"[DONE]  Ingested table: {table_cfg['target_entity']}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Azure SQL batch ingestion job")
    parser.add_argument("--env",       required=True, help="Environment: dev | test | prod")
    parser.add_argument("--source_id", required=True, help="Source ID from YAML config")
    args = parser.parse_args()

    spark = build_spark_session()
    config = load_source_config(args.source_id)
    ingest_all_tables(spark, config, args.env)
