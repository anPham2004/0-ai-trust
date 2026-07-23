"""Append byte-for-byte source file arrivals to the external Bronze ledger."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

from framework.landing_stream_reader import external_bronze_table


dp.create_sink(
    "file_arrivals_sink",
    "delta",
    {"tableName": external_bronze_table("file_arrivals")},
)


@dp.append_flow(name="ingest_file_arrivals", target="file_arrivals_sink")
def ingest_file_arrivals():
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active Spark session is required")
    landing = spark.conf.get(
        "zero_ai_trust.landing_path",
        "/Volumes/0-ai-trust/bronze/landing",
    )
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .load(f"{landing}/file")
        .select(
            F.regexp_extract("path", r"/file/([^/]+)/", 1).alias("source_dataset"),
            F.col("path").alias("source_file"),
            F.col("content").alias("raw_content"),
            F.col("length").alias("source_file_size"),
            F.col("modificationTime").alias("source_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
