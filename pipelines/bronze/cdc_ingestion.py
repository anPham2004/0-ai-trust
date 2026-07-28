"""Load source-native database CDC envelopes into a Bronze streaming table."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.landing_stream_reader import read_transport_stream


@dp.table(
    name="cdc_changes",
    comment="Append-only source-native CDC history with Debezium transport metadata",
    spark_conf={"pipelines.trigger.interval": "1 minute"}, # continuous/micro-batch stream, checking for new data every minute 
    table_properties={
        "quality": "bronze", #tags it as raw/bronze medallion layer
        "data_classification": "Highly Confidential", #governance tag (enforced by Unity Catalog policies elsewhere)
        "delta.appendOnly": "true", # the table is insert-only, no updates/deletes allowed on it
        "delta.enableChangeDataFeed": "true", #turns on Delta CDF so downstream consumers (e.g., Silver layer) can read incremental changes off this table itself.
    },
)
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
