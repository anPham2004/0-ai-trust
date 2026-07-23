"""Bronze ingestion: native landing files to Unity Catalog external Delta tables.

The pipeline uses the legacy DLT-compatible declarative interface because it mirrors
the target platform. All business data is written to S3-backed external table
sinks; Databricks supplies compute, checkpoints, lineage, and UC governance.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Catalog and landing path configuration for Unity Catalog and file ingestion
CATALOG = spark.conf.get("zero_ai_trust.catalog", "0-ai-trust")
LANDING = spark.conf.get(
    "zero_ai_trust.landing_path",
    "/Volumes/0-ai-trust/bronze/landing/raw",
)

# Schema definition for transport records ingested from landing files
TRANSPORT_SCHEMA = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
        StructField("timestamp", LongType()),
        StructField("timestamp_type", LongType()),
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("captured_at", StringType()),
    ]
)

# Helper to generate fully qualified external table names in Unity Catalog
def external_table(name: str) -> str:
    return f"`{CATALOG}`.bronze.{name}"

# Reads streaming text files from the landing path and parses them into transport schema
def text_stream(source: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .load(f"{LANDING}/{source}")
        .select(
            F.from_json("value", TRANSPORT_SCHEMA).alias("transport"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        )
        .select("transport.*", "_source_file", "_source_file_modified_at")
    )

# Sink definition for CDC changes, writing to an external Delta table
dlt.create_sink(
    "cdc_changes_sink",
    "delta",
    {"tableName": external_table("cdc_changes")},
)

# Append flow for ingesting CDC changes from text files into the CDC changes sink
@dlt.append_flow(name="ingest_cdc_changes", target="cdc_changes_sink")
def ingest_cdc_changes():
    source_schema = F.get_json_object("value", "$.source.schema")
    source_table = F.get_json_object("value", "$.source.table")
    return (
        text_stream("cdc")
        .select(
            "topic",
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("key").alias("record_key"),
            F.concat_ws(".", source_schema, source_table).alias("source_dataset"),
            F.get_json_object("value", "$.op").alias("operation"),
            F.get_json_object("value", "$.source.snapshot").alias("snapshot_state"),
            F.get_json_object("value", "$.source.lsn").cast("long").alias("source_lsn"),
            F.get_json_object("value", "$.ts_ms").cast("long").alias("source_timestamp_ms"),
            F.col("value").alias("raw_payload"),
            F.to_timestamp("captured_at").alias("captured_at"),
            "_source_file",
            "_source_file_modified_at",
            F.current_timestamp().alias("_ingested_at"),
        )
    )

# Sink definition for Kafka events, writing to an external Delta table
dlt.create_sink(
    "kafka_events_sink",
    "delta",
    {"tableName": external_table("kafka_events")},
)

# Append flow for ingesting Kafka events from text files into the Kafka events sink
@dlt.append_flow(name="ingest_kafka_events", target="kafka_events_sink")
def ingest_kafka_events():
    return (
        text_stream("kafka")
        .select(
            "topic",
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("key").alias("event_key"),
            F.get_json_object("value", "$.event_id").alias("event_id"),
            F.get_json_object("value", "$.event_type").alias("event_type"),
            F.get_json_object("value", "$.source_dataset").alias("source_dataset"),
            F.get_json_object("value", "$.occurred_at").alias("occurred_at"),
            F.col("value").alias("raw_payload"),
            F.to_timestamp("captured_at").alias("captured_at"),
            "_source_file",
            "_source_file_modified_at",
            F.current_timestamp().alias("_ingested_at"),
        )
    )

# Sink definition for file arrivals, writing to an external Delta table
dlt.create_sink(
    "file_arrivals_sink",
    "delta",
    {"tableName": external_table("file_arrivals")},
)

# Append flow for ingesting binary files and their metadata into the file arrivals sink
@dlt.append_flow(name="ingest_file_arrivals", target="file_arrivals_sink")
def ingest_file_arrivals():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .load(f"{LANDING}/file")
        .select(
            F.regexp_extract("path", r"/file/([^/]+)/", 1).alias("source_dataset"),
            F.col("path").alias("source_file"),
            F.col("content").alias("raw_content"),
            F.col("length").alias("source_file_size"),
            F.col("modificationTime").alias("source_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )