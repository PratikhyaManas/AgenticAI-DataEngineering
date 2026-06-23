"""
Batch Ingestion: Databricks Autoloader (File → Bronze Delta)
============================================================
Agent: Data Ingestion Agent

Pattern: ADLS Gen2 landing zone → Databricks Autoloader → Bronze Delta table
Supports: CSV, JSON, Parquet, Avro files with schema evolution.

Usage (from Databricks Workflow task or notebook):
    python autoloader_ingestion.py --env dev --source_id file_finance_invoices
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, BooleanType, IntegerType

from src.ingestion.metadata.metadata_logger import (
    create_metadata_table_if_not_exists,
    start_run,
    end_run,
)
from src.utils.config import load_source_config

_TYPE_MAP = {
    "string":    StringType(),
    "long":      LongType(),
    "double":    DoubleType(),
    "timestamp": TimestampType(),
    "boolean":   BooleanType(),
    "integer":   IntegerType(),
}


def build_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def ingest_files_autoloader(
    spark: SparkSession,
    config: dict,
    env: str,
) -> None:
    """
    Core Autoloader ingestion function.
    Reads from ADLS Gen2 landing path using cloudFiles source and writes to
    Bronze Delta table in append mode with schema evolution.
    """
    al_cfg   = config["autoloader"]
    tgt_cfg  = config["target"]
    meta_cfg = config.get("metadata", {})
    source_id = config["source_id"]
    domain    = config.get("domain", "unknown")
    entity    = tgt_cfg.get("bronze_table", "unknown")

    # Resolve environment placeholders in paths
    storage_account = spark.conf.get("spark.lakehouse.storage_account")
    source_path  = al_cfg["source_path"].replace("{storage_account}", storage_account)
    schema_loc   = al_cfg["schema_location"].replace("{storage_account}", storage_account)
    ckpt_loc     = al_cfg["checkpoint_location"].replace("{storage_account}", storage_account)
    target_path  = tgt_cfg["adls_path"].replace("{storage_account}", storage_account)
    catalog_table = f"{env}_lakehouse.{tgt_cfg['bronze_schema']}.{tgt_cfg['bronze_table']}"

    create_metadata_table_if_not_exists(spark, env)
    run_id = start_run(
        spark, env, source_id,
        source_type=config.get("source_type"),
        domain=domain, entity=entity,
        pipeline_name="autoloader_batch_ingestion",
        target_path=target_path,
    )

    rows_written = 0
    rows_rejected = 0
    error_msg = None

    try:
        reader_options = {
            "cloudFiles.format":          al_cfg["format"],
            "cloudFiles.schemaLocation":  schema_loc,
            "cloudFiles.schemaEvolutionMode": al_cfg.get("schema_evolution_mode", "addNewColumns"),
            "rescuedDataColumn":          al_cfg.get("rescue_data_column", "_rescued_data"),
        }

        # Inject format-specific options (e.g., CSV header, delimiter)
        format_options = al_cfg.get(f"{al_cfg['format']}_options", {})
        reader_options.update(format_options)

        # --- Build explicit schema if provided ---
        schema_fields = al_cfg.get("schema", {}).get("fields", [])
        if schema_fields:
            schema = StructType([
                StructField(f["name"], _TYPE_MAP.get(f["type"], StringType()), f.get("nullable", True))
                for f in schema_fields
            ])
            reader_options["cloudFiles.schemaHints"] = ",".join(
                f"{f['name']} {f['type']}" for f in schema_fields
            )

        raw_df = (
            spark.readStream
                 .format("cloudFiles")
                 .options(**reader_options)
                 .load(source_path)
        )

        # Enrich with standard metadata columns
        enriched_df = (
            raw_df
            .withColumn("_ingestion_run_id",    F.lit(run_id))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_path",         F.input_file_name())
            .withColumn("ingestion_date",        F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
        )

        partition_cols = tgt_cfg.get("partition_by", ["ingestion_date"])

        # Write as streaming append to Delta table
        query = (
            enriched_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", ckpt_loc)
            .option("mergeSchema", "true")
            .partitionBy(*partition_cols)
            .trigger(availableNow=True)          # process all available files then stop
            .toTable(catalog_table)
        )

        query.awaitTermination()

        # Collect row count from Delta history for metadata
        history = spark.sql(f"DESCRIBE HISTORY {catalog_table} LIMIT 1").collect()
        rows_written = (history[0].operationMetrics or {}).get("numOutputRows", 0)
        status = "SUCCESS"

    except Exception as exc:
        error_msg = str(exc)
        status    = "FAILED"
        raise

    finally:
        end_run(
            spark, env, run_id, status,
            rows_written=int(rows_written) if rows_written else None,
            rows_rejected=int(rows_rejected) if rows_rejected else None,
            error_message=error_msg,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Autoloader file ingestion job")
    parser.add_argument("--env",             required=True, help="Environment: dev | test | prod")
    parser.add_argument("--source_id",       required=True, help="Source ID from YAML config")
    parser.add_argument("--storage-account", required=True,
                        dest="storage_account",
                        help="ADLS Gen2 storage account name")
    args = parser.parse_args()

    spark = build_spark_session()
    spark.conf.set("spark.lakehouse.storage_account", args.storage_account)
    config = load_source_config(args.source_id)
    ingest_files_autoloader(spark, config, args.env)
