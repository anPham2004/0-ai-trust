"""Materialize ingestion manifests and all nonconforming Bronze records."""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, functions as F

from framework.landing_stream_reader import read_invalid_transport_stream, read_manifest_stream
from framework.source_dataset_registry import bronze_table_names


@dp.table(
    name="control_ingestion_manifests",
    comment="Queryable reconciliation, freshness, checksum, LSN, and offset evidence",
    spark_conf={"pipelines.trigger.interval": "1 minute"},
    table_properties={
        "quality": "bronze",
        "source_type": "control",
        "data_classification": "Internal",
        "delta.appendOnly": "true",
    },
)
def ingestion_manifests():
    source = read_manifest_stream()
    return source.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


def rescued_records() -> list[DataFrame]:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active Spark session is required")
    frames = []
    for table_name in bronze_table_names():
        source_type, dataset = table_name.split("_", 1)
        frames.append(
            spark.readStream.table(table_name)
            .where(F.col("_rescued_data").isNotNull())
            .select(
                F.lit(source_type.upper()).alias("source_type"),
                F.lit(dataset).alias("source_dataset"),
                F.lit("SCHEMA_RESCUED_DATA").alias("failure_reason"),
                F.col("_rescued_data").alias("rescued_data"),
                F.col("_source_file").alias("source_file"),
                F.col("_batch_id").alias("batch_id"),
                F.current_timestamp().alias("quarantined_at"),
            )
        )
    return frames


def invalid_transport_records(source_type: str) -> DataFrame:
    source = read_invalid_transport_stream(source_type)
    return source.select(
        F.lit(source_type.upper()).alias("source_type"),
        F.lit(None).cast("string").alias("source_dataset"),
        F.col("parse_error").alias("failure_reason"),
        F.coalesce(F.col("raw_value"), F.col("_rescued_data")).alias("rescued_data"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("batch_id").alias("batch_id"),
        F.current_timestamp().alias("quarantined_at"),
    )


@dp.table(
    name="ingestion_quarantine",
    comment="Append-only rejected transport and schema-rescued ingestion records",
    spark_conf={"pipelines.trigger.interval": "1 minute"},
    table_properties={
        "quality": "bronze",
        "source_type": "control",
        "data_classification": "Highly Confidential",
        "delta.appendOnly": "true",
    },
)
def ingestion_quarantine():
    frames = rescued_records() + [
        invalid_transport_records("cdc"),
        invalid_transport_records("event"),
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)
