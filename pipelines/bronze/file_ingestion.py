"""Load byte-for-byte source file arrivals into a Bronze streaming table."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

from framework.landing_stream_reader import bronze_table_path


@dp.table(
    name="file_arrivals",
    path=bronze_table_path("file_arrivals"),
    comment="Immutable byte-for-byte source file arrivals; parsing is deferred to Silver",
    spark_conf={"pipelines.trigger.interval": "1 minute"},
    table_properties={
        "quality": "bronze",
        "data_classification": "Highly Confidential",
        "delta.appendOnly": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
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
