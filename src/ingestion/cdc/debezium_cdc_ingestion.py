"""
CDC Ingestion: Debezium → Azure Event Hubs → Bronze Delta
==========================================================
Agent: Data Ingestion Agent

Pattern: Debezium captures row-level changes (INSERT / UPDATE / DELETE)
from Azure SQL Server and publishes Debezium envelope JSON to Azure Event Hubs.
This Spark Structured Streaming job consumes those events and applies them
to Bronze Delta tables using MERGE (upsert) and DELETE semantics.

Debezium envelope format (simplified):
  {
    "before": {<row_state_before_change>},   # null for INSERT
    "after":  {<row_state_after_change>},    # null for DELETE
    "op":     "c" | "u" | "d" | "r",        # create | update | delete | read (snapshot)
    "ts_ms":  1712345678901,                 # event timestamp (epoch ms)
    "source": { "table": "orders", ... }
  }

This approach eliminates watermark-based full-table scans in sql_ingestion.py,
replacing them with row-level CDC events that capture every change exactly once.

Reference:
  https://debezium.io/documentation/reference/connectors/sqlserver.html
  https://github.com/Azure/azure-event-hubs-spark
"""

from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, BooleanType,
    MapType,
)
from pyspark.storagelevel import StorageLevel

from src.utils.dbutils_shim import get_dbutils

from src.ingestion.metadata.metadata_logger import (
    create_metadata_table_if_not_exists,
    start_run,
    end_run,
)
from src.utils.config import load_source_config


# ---------------------------------------------------------------------------
# Debezium envelope schema
# ---------------------------------------------------------------------------
DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("before",  MapType(StringType(), StringType()), nullable=True),
    StructField("after",   MapType(StringType(), StringType()), nullable=True),
    StructField("op",      StringType(),  nullable=False),   # c | u | d | r
    StructField("ts_ms",   LongType(),    nullable=False),
    StructField("source",  StructType([
        StructField("table",   StringType(), nullable=True),
        StructField("db",      StringType(), nullable=True),
        StructField("version", StringType(), nullable=True),
        StructField("schema",  StringType(), nullable=True),
    ]), nullable=True),
])

# Operation codes from Debezium
_OP_INSERT   = "c"   # create / INSERT
_OP_UPDATE   = "u"   # UPDATE
_OP_DELETE   = "d"   # DELETE
_OP_SNAPSHOT = "r"   # initial snapshot read (treated as INSERT)


# ---------------------------------------------------------------------------
# Event Hubs configuration
# ---------------------------------------------------------------------------

def _build_eventhub_conf(config: dict, spark: SparkSession) -> dict:
    conn_cfg     = config["connection"]
    stream_cfg   = config.get("streaming", {})
    secret_scope = conn_cfg["secret_scope"]
    conn_str     = get_dbutils(spark).secrets.get(
        scope=secret_scope,
        key=conn_cfg["connection_string_secret_key"],
    )
    eh_conf = {
        "eventhubs.connectionString": spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str),  # noqa: SLF001
        "eventhubs.consumerGroup":    conn_cfg.get("consumer_group", "$Default"),
        "eventhubs.startingPosition": json.dumps({
            "offset": "@latest", "seqNo": -1,
            "enqueuedTime": None, "isInclusive": True,
        }),
        "eventhubs.maxEventsPerTrigger": str(stream_cfg.get("max_events_per_trigger", 10000)),
    }
    return eh_conf


# ---------------------------------------------------------------------------
# Debezium envelope parsing
# ---------------------------------------------------------------------------

def _parse_debezium_events(raw_df: DataFrame) -> DataFrame:
    """
    Deserialize Event Hubs body bytes → Debezium envelope → flat row.

    Returns a DataFrame with columns:
      _op, _table, _event_ts, _cdc_ts, _before (map), _after (map)
    The caller applies the 'after' map to the target schema.
    """
    return (
        raw_df
        .withColumn("body_str", F.col("body").cast("string"))
        .withColumn("envelope", F.from_json(F.col("body_str"), DEBEZIUM_ENVELOPE_SCHEMA))
        .filter(F.col("envelope").isNotNull())
        .select(
            F.col("envelope.op").alias("_op"),
            F.col("envelope.source.table").alias("_table"),
            (F.col("envelope.ts_ms") / 1000).cast(TimestampType()).alias("_event_ts"),
            F.current_timestamp().alias("_cdc_ingest_ts"),
            F.col("envelope.before").alias("_before"),
            F.col("envelope.after").alias("_after"),
        )
        # Filter out unsupported ops (schema-change events from Debezium)
        .filter(F.col("_op").isin([_OP_INSERT, _OP_UPDATE, _OP_DELETE, _OP_SNAPSHOT]))
    )


# ---------------------------------------------------------------------------
# Apply CDC events to Bronze Delta table
# ---------------------------------------------------------------------------

def _apply_cdc_to_bronze(
    spark: SparkSession,
    cdc_df: DataFrame,
    target_table: str,
    primary_key: str,
    env: str,
    source_id: str,
    run_id: str,
) -> None:
    """
    Apply a micro-batch of CDC events to the Bronze Delta table using MERGE.

    Logic per op:
      INSERT (c, r) → INSERT new row if not exists; update if exists (idempotent)
      UPDATE  (u)   → UPDATE matched row with 'after' state
      DELETE  (d)   → Soft-delete: set _cdc_deleted=true, _cdc_deleted_ts=now
                      (hard deletes violate immutability / audit requirements)
    """
    # Resolve primary key from the 'after' map (or 'before' for deletes)
    cdc_with_pk = cdc_df.withColumn(
        "_pk",
        F.coalesce(
            F.col("_after")[primary_key],
            F.col("_before")[primary_key],
        )
    ).filter(F.col("_pk").isNotNull())

    # Convert the 'after' map to individual columns for non-delete events
    # We store the full JSON payload in Bronze for schema flexibility
    upsert_df = (
        cdc_with_pk
        .filter(F.col("_op").isin([_OP_INSERT, _OP_UPDATE, _OP_SNAPSHOT]))
        .withColumn("_cdc_payload",     F.to_json(F.col("_after")))
        .withColumn("_cdc_op",          F.col("_op"))
        .withColumn("_cdc_event_ts",    F.col("_event_ts"))
        .withColumn("_cdc_ingest_ts",   F.col("_cdc_ingest_ts"))
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_source_system",   F.lit(source_id))
        .withColumn("_cdc_deleted",     F.lit(False))
        .withColumn("_cdc_deleted_ts",  F.lit(None).cast(TimestampType()))
        .withColumn("ingestion_date",   F.date_format(F.col("_cdc_ingest_ts"), "yyyy-MM-dd"))
        .select(
            F.col("_pk").alias(primary_key),
            "_cdc_payload", "_cdc_op", "_cdc_event_ts", "_cdc_ingest_ts",
            "_ingestion_run_id", "_source_system", "_cdc_deleted",
            "_cdc_deleted_ts", "ingestion_date",
        )
    )

    delete_df = (
        cdc_with_pk
        .filter(F.col("_op") == _OP_DELETE)
        .withColumn("_cdc_payload",     F.to_json(F.col("_before")))
        .withColumn("_cdc_op",          F.lit("d"))
        .withColumn("_cdc_event_ts",    F.col("_event_ts"))
        .withColumn("_cdc_ingest_ts",   F.col("_cdc_ingest_ts"))
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_source_system",   F.lit(source_id))
        .withColumn("_cdc_deleted",     F.lit(True))
        .withColumn("_cdc_deleted_ts",  F.col("_cdc_ingest_ts"))
        .withColumn("ingestion_date",   F.date_format(F.col("_cdc_ingest_ts"), "yyyy-MM-dd"))
        .select(
            F.col("_pk").alias(primary_key),
            "_cdc_payload", "_cdc_op", "_cdc_event_ts", "_cdc_ingest_ts",
            "_ingestion_run_id", "_source_system", "_cdc_deleted",
            "_cdc_deleted_ts", "ingestion_date",
        )
    )

    combined = upsert_df.union(delete_df)

    # Ensure Bronze table exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            {primary_key}           STRING  NOT NULL,
            _cdc_payload            STRING  COMMENT 'Full row JSON from Debezium after/before image',
            _cdc_op                 STRING  COMMENT 'c=insert u=update d=delete r=snapshot',
            _cdc_event_ts           TIMESTAMP NOT NULL COMMENT 'Event time from Debezium',
            _cdc_ingest_ts          TIMESTAMP NOT NULL COMMENT 'When this row landed in Bronze',
            _ingestion_run_id       STRING,
            _source_system          STRING,
            _cdc_deleted            BOOLEAN DEFAULT false,
            _cdc_deleted_ts         TIMESTAMP,
            ingestion_date          STRING
        )
        USING DELTA
        PARTITIONED BY (ingestion_date)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

    combined.createOrReplaceTempView("_cdc_staging")

    # MERGE: latest event wins (order by _cdc_event_ts)
    spark.sql(f"""
        MERGE INTO {target_table} AS tgt
        USING (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {primary_key}
                    ORDER BY _cdc_event_ts DESC
                ) AS _rn
                FROM _cdc_staging
            ) WHERE _rn = 1
        ) AS src
        ON tgt.{primary_key} = src.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET
                tgt._cdc_payload     = src._cdc_payload,
                tgt._cdc_op          = src._cdc_op,
                tgt._cdc_event_ts    = src._cdc_event_ts,
                tgt._cdc_ingest_ts   = src._cdc_ingest_ts,
                tgt._cdc_deleted     = src._cdc_deleted,
                tgt._cdc_deleted_ts  = src._cdc_deleted_ts
        WHEN NOT MATCHED THEN
            INSERT *
    """)


# ---------------------------------------------------------------------------
# Streaming CDC ingestion job
# ---------------------------------------------------------------------------

def ingest_cdc_stream(spark: SparkSession, config: dict, env: str) -> None:
    """
    Start a Structured Streaming job that:
      1. Reads Debezium CDC events from Azure Event Hubs
      2. Parses the Debezium envelope
      3. Applies MERGE to the Bronze Delta table for each micro-batch

    The job runs continuously (trigger=processingTime) and applies micro-batches
    to maintain Bronze as a current image of the source table with full CDC history.
    """
    source_id = config["source_id"]
    conn_cfg  = config["connection"]
    tgt_cfg   = config["target"]
    stream_cfg = config.get("streaming", {})

    storage_account = spark.conf.get("spark.lakehouse.storage_account")
    target_table    = (
        f"{env}_lakehouse.{tgt_cfg['bronze_schema']}"
        f".{config.get('domain', 'unknown')}_{tgt_cfg.get('bronze_table', 'cdc_events')}"
    )
    ckpt_location   = (
        tgt_cfg.get("checkpoint_location", "abfss://raw@{sa}.dfs.core.windows.net/checkpoints/cdc/{source_id}")
        .replace("{storage_account}", storage_account)
        .replace("{source_id}", source_id)
    )
    primary_key     = config.get("primary_key", "id")
    trigger_secs    = stream_cfg.get("trigger_interval_seconds", 30)

    create_metadata_table_if_not_exists(spark, env)
    run_id = start_run(
        spark, env, source_id,
        source_type="cdc_event_hubs",
        domain=config.get("domain", "unknown"),
        entity=tgt_cfg.get("bronze_table", "cdc_events"),
        pipeline_name="debezium_cdc_ingestion",
        target_path=target_table,
    )

    eh_conf = _build_eventhub_conf(config, spark)

    raw_stream = (
        spark.readStream
             .format("eventhubs")
             .options(**eh_conf)
             .load()
    )

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        cdc_df = _parse_debezium_events(batch_df)
        _apply_cdc_to_bronze(
            spark, cdc_df, target_table,
            primary_key, env, source_id, run_id,
        )
        print(f"[CDC] Processed micro-batch {batch_id} → {target_table}")

    query = (
        raw_stream.writeStream
        .foreachBatch(_process_batch)
        .option("checkpointLocation", ckpt_location)
        .trigger(processingTime=f"{trigger_secs} seconds")
        .start()
    )

    query.awaitTermination()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Debezium CDC ingestion via Event Hubs")
    parser.add_argument("--env",       required=True, help="dev | test | prod")
    parser.add_argument("--source_id", required=True, help="Source ID from YAML config")
    args = parser.parse_args()

    spark  = SparkSession.builder.getOrCreate()
    config = load_source_config(args.source_id)
    ingest_cdc_stream(spark, config, args.env)
