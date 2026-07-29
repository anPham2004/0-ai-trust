"""Declare one row-level Bronze streaming table per source-file dataset."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.landing_stream_reader import read_csv_dataset_stream
from framework.source_dataset_registry import FILE_DATASETS


def register_file_table(dataset: str) -> None:
    @dp.table(
        name=f"file_{dataset}",
        comment=f"Append-only logical raw CSV records for {dataset}",
        spark_conf={"pipelines.trigger.interval": "1 minute"},
        table_properties={
            "quality": "bronze",
            "source_type": "file",
            "data_classification": "Highly Confidential",
            "delta.appendOnly": "true",
            "delta.enableChangeDataFeed": "true",
        },
    )
    def source_aligned_file_table():
        source = read_csv_dataset_stream(dataset)
        source_file = F.col("_metadata.file_path")
        return source.select(
            "*",
            F.lit(dataset).alias("_source_dataset"),
            F.when(
                source_file.rlike(r"-[0-9]{8}T[0-9]{6}Z\.csv$"), "INCREMENTAL"
            ).otherwise("INITIAL").alias("_load_type"),
            F.regexp_extract(source_file, r"([^/]+)\.csv$", 1).alias("_batch_id"),
            source_file.alias("_source_file"),
            F.col("_metadata.file_size").alias("_source_file_size"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )


for dataset_name in FILE_DATASETS:
    register_file_table(dataset_name)
