"""Declare one source-aligned Bronze streaming table per business-event dataset."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.landing_stream_reader import read_json_dataset_stream
from framework.source_dataset_registry import EVENT_DATASETS


def register_event_table(dataset: str) -> None:
    @dp.table(
        name=f"event_{dataset}",
        comment=f"Append-only logical raw business-event history for {dataset}",
        spark_conf={"pipelines.trigger.interval": "1 minute"},
        table_properties={
            "quality": "bronze",
            "source_type": "event",
            "data_classification": "Highly Confidential",
            "delta.appendOnly": "true",
            "delta.enableChangeDataFeed": "true",
        },
    )
    def source_aligned_event_table():
        source = read_json_dataset_stream("event", dataset)
        return source.select(
            "record.*",
            F.lit(dataset).alias("_source_dataset"),
            F.col("load_type").alias("_load_type"),
            F.col("payload.event_id").alias("_event_id"),
            F.col("payload.event_type").alias("_event_type"),
            F.col("payload.event_version").alias("_event_version"),
            F.col("payload.occurred_at").cast("timestamp").alias("_occurred_at"),
            F.col("payload.producer").alias("_producer"),
            F.col("payload.correlation_id").alias("_correlation_id"),
            F.col("topic").alias("_topic"),
            F.col("partition").cast("long").alias("_kafka_partition"),
            F.col("offset").cast("long").alias("_kafka_offset"),
            F.col("key").alias("_event_key"),
            F.col("batch_id").alias("_batch_id"),
            F.to_timestamp("captured_at").alias("_captured_at"),
            F.col("_rescued_data"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )


for dataset_name in EVENT_DATASETS:
    register_event_table(dataset_name)
