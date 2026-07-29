"""Declare one source-aligned Bronze streaming table per database dataset."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.landing_stream_reader import read_json_dataset_stream
from framework.source_dataset_registry import CDC_DATASETS


def register_cdc_table(dataset: str, source_directory: str) -> None:
    @dp.table(
        name=f"cdc_{dataset}",
        comment=f"Append-only logical raw CDC history for {dataset}",
        spark_conf={"pipelines.trigger.interval": "1 minute"},
        table_properties={
            "quality": "bronze",
            "source_type": "database",
            "data_classification": "Highly Confidential",
            "delta.appendOnly": "true",
            "delta.enableChangeDataFeed": "true",
        },
    )
    def source_aligned_cdc_table():
        source = read_json_dataset_stream("cdc", source_directory)
        return source.select(
            "record.*",
            F.lit(dataset).alias("_source_dataset"),
            F.col("load_type").alias("_load_type"),
            F.when(F.col("payload.op") == "r", "SNAPSHOT")
            .when(F.col("payload.op") == "c", "INSERT")
            .when(F.col("payload.op") == "u", "UPDATE")
            .when(F.col("payload.op") == "d", "DELETE")
            .otherwise("UNKNOWN")
            .alias("_operation"),
            F.col("payload.source.snapshot").cast("string").alias("_snapshot_state"),
            F.col("payload.source.lsn").cast("long").alias("_source_lsn"),
            F.expr("timestamp_millis(CAST(payload.ts_ms AS BIGINT))").alias("_commit_ts"),
            F.col("topic").alias("_topic"),
            F.col("partition").cast("long").alias("_kafka_partition"),
            F.col("offset").cast("long").alias("_kafka_offset"),
            F.col("key").alias("_record_key"),
            F.col("batch_id").alias("_batch_id"),
            F.to_timestamp("captured_at").alias("_captured_at"),
            F.col("_rescued_data"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )


for dataset_name, landing_directory in CDC_DATASETS.items():
    register_cdc_table(dataset_name, landing_directory)
