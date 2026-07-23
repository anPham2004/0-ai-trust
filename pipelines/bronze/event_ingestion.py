"""Append source-native business-event envelopes to the external Bronze ledger."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.landing_stream_reader import external_bronze_table, read_transport_stream


dp.create_sink(
    "kafka_events_sink",
    "delta",
    {"tableName": external_bronze_table("kafka_events")},
)


@dp.append_flow(name="ingest_kafka_events", target="kafka_events_sink")
def ingest_kafka_events():
    return read_transport_stream("event").select(
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
