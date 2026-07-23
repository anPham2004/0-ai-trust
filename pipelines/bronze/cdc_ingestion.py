"""Append source-native database CDC envelopes to the external Bronze ledger."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.landing_stream_reader import external_bronze_table, read_transport_stream


dp.create_sink(
    "cdc_changes_sink",
    "delta",
    {"tableName": external_bronze_table("cdc_changes")},
)


@dp.append_flow(name="ingest_cdc_changes", target="cdc_changes_sink")
def ingest_cdc_changes():
    source_schema = F.get_json_object("value", "$.source.schema")
    source_table = F.get_json_object("value", "$.source.table")
    return read_transport_stream("cdc").select(
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
