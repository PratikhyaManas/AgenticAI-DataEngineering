"""
Streaming Ingestion: Azure Event Hubs → Bronze Delta
====================================================
Agent: Data Ingestion Agent

Pattern: Databricks Structured Streaming reads from Azure Event Hubs using
the spark-eventhubs connector, deserializes JSON payloads, and writes to
a Bronze Delta table continuously.

Connection string is retrieved from Azure Key Vault; no plaintext secrets
appear in code.
"""

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

from src.ingestion.metadata.metadata_logger import (
    create_metadata_table_if_not_exists,
    start_run,
)


# ---------------------------------------------------------------------------
# Payload schema (matches eventhub_source.yaml schema definition)
# ---------------------------------------------------------------------------
SALES_EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),    nullable=False),
    StructField("order_id",         LongType(),      nullable=False),
    StructField("event_type",       StringType(),    nullable=False),
    StructField("customer_id",      LongType(),      nullable=True),
    StructField("amount",           DoubleType(),    nullable=True),
    StructField("currency",         StringType(),    nullable=True),
    StructField("event_timestamp",  TimestampType(), nullable=False),
    StructField("partition_key",    StringType(),    nullable=True),
])


def build_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def load_source_config(source_id: str) -> dict:
    import yaml, os
    config_dir = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "configs", "sources"
    )
    for fname in os.listdir(config_dir):
        if fname.endswith(".yaml"):
            with open(os.path.join(config_dir, fname)) as f:
                cfg = yaml.safe_load(f)
            if cfg.get("source_id") == source_id:
                return cfg
    raise ValueError(f"No config found for source_id={source_id}")


def build_eventhub_conf(config: dict, spark: SparkSession) -> dict:
    """
    Builds the Event Hubs configuration dict expected by the Databricks
    spark-eventhubs connector.  The connection string is fetched from
    Azure Key Vault at runtime.
    """
    conn_cfg   = config["connection"]
    stream_cfg = config["streaming"]
    secret_scope = conn_cfg["secret_scope"]
    conn_str     = dbutils.secrets.get(  # noqa: F821
        scope=secret_scope,
        key=conn_cfg["connection_string_secret_key"]
    )

    # Build EventHubs position (start from latest by default)
    starting_offset = stream_cfg.get("starting_offset", "latest")
    if starting_offset == "latest":
        starting_event_position = json.dumps({"offset": "@latest", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
    else:
        starting_event_position = json.dumps({"offset": starting_offset, "seqNo": -1, "enqueuedTime": None, "isInclusive": True})

    eh_conf = {
        "eventhubs.connectionString": spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str),  # noqa: SLF001
        "eventhubs.consumerGroup":    conn_cfg.get("consumer_group", "$Default"),
        "eventhubs.startingPosition": starting_event_position,
        "eventhubs.maxEventsPerTrigger": str(stream_cfg.get("max_events_per_trigger", 50000)),
    }
    return eh_conf


def ingest_eventhub_stream(
    spark: SparkSession,
    config: dict,
    env: str,
) -> None:
    """
    Starts a Structured Streaming job that reads from Event Hubs and
    continuously appends deserialized records to a Bronze Delta table.

    Watermarking is applied to handle late-arriving events.
    The stream uses a trigger interval from config (micro-batch mode).
    """
    stream_cfg = config["streaming"]
    tgt_cfg    = config["target"]
    wm_cfg     = config.get("watermark", {})
    source_id  = config["source_id"]
    domain     = config.get("domain", "unknown")
    entity     = tgt_cfg.get("bronze_table", "stream")

    storage_account = spark.conf.get("spark.lakehouse.storage_account")
    ckpt_location = stream_cfg["checkpoint_location"].replace("{storage_account}", storage_account)
    catalog_table = f"{env}_lakehouse.{tgt_cfg['bronze_schema']}.{tgt_cfg['bronze_table']}"

    create_metadata_table_if_not_exists(spark, env)
    start_run(
        spark, env, source_id,
        source_type=config.get("source_type"),
        domain=domain, entity=entity,
        pipeline_name="eventhub_streaming_ingestion",
        target_path=catalog_table,
    )

    eh_conf = build_eventhub_conf(config, spark)

    # Read raw bytes from Event Hubs
    raw_stream = (
        spark.readStream
             .format("eventhubs")
             .options(**eh_conf)
             .load()
    )

    # Event Hubs wraps payload in 'body' column (binary)
    # Deserialize JSON body and extract EventHub system properties
    parsed_stream = (
        raw_stream
        .withColumn("body_str",           F.col("body").cast("string"))
        .withColumn("payload",            F.from_json(F.col("body_str"), SALES_EVENT_SCHEMA))
        .withColumn("eh_enqueued_time",   F.col("enqueuedTime"))
        .withColumn("eh_partition_id",    F.col("partitionId").cast("string"))
        .withColumn("eh_sequence_number", F.col("sequenceNumber"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system",       F.lit(source_id))
        .withColumn("ingestion_date",        F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
        # Flatten payload fields
        .select(
            F.col("payload.event_id"),
            F.col("payload.order_id"),
            F.col("payload.event_type"),
            F.col("payload.customer_id"),
            F.col("payload.amount"),
            F.col("payload.currency"),
            F.col("payload.event_timestamp"),
            F.col("payload.partition_key"),
            F.col("body_str").alias("_raw_payload"),       # retain raw for debugging
            F.col("eh_enqueued_time"),
            F.col("eh_partition_id"),
            F.col("eh_sequence_number"),
            F.col("_ingestion_timestamp"),
            F.col("_source_system"),
            F.col("ingestion_date"),
        )
    )

    # Apply watermark for late data tolerance
    event_time_col     = wm_cfg.get("event_time_column", "event_timestamp")
    late_arrival_delay = wm_cfg.get("late_arrival_threshold", "10 minutes")
    watermarked_stream = parsed_stream.withWatermark(event_time_col, late_arrival_delay)

    trigger_interval = stream_cfg.get("trigger_interval", "30 seconds")

    write_query = (
        watermarked_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", ckpt_location)
        .option("mergeSchema", "true")
        .partitionBy("ingestion_date")
        .trigger(processingTime=trigger_interval)
        .toTable(catalog_table)
    )

    print(f"[INFO] Streaming query started: {write_query.id} → {catalog_table}")
    write_query.awaitTermination()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Event Hubs streaming ingestion job")
    parser.add_argument("--env",       required=True, help="Environment: dev | test | prod")
    parser.add_argument("--source_id", required=True, help="Source ID from YAML config")
    args = parser.parse_args()

    spark = build_spark_session()
    config = load_source_config(args.source_id)
    ingest_eventhub_stream(spark, config, args.env)
