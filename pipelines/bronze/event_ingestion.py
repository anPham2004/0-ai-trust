"""Load source-native business-event envelopes into a Bronze streaming table."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.landing_stream_reader import bronze_table_path, read_transport_stream


@dp.table(
    name="kafka_events",
    path=bronze_table_path("kafka_events"),
    comment="Immutable Kafka business-event history with source payload",
    spark_conf={"pipelines.trigger.interval": "1 minute"},
    table_properties={
        "quality": "bronze",
        "data_classification": "Highly Confidential",
        "delta.appendOnly": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
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
