"""Source-aligned Auto Loader readers shared by Bronze definitions."""

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType


INVALID_TRANSPORT_SCHEMA = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
        StructField("timestamp", LongType()),
        StructField("timestamp_type", LongType()),
        StructField("key", StringType()),
        StructField("parse_error", StringType()),
        StructField("raw_value", StringType()),
        StructField("captured_at", StringType()),
        StructField("batch_id", StringType()),
        StructField("_rescued_data", StringType()),
    ]
)


def _spark() -> SparkSession:
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("An active Spark session is required")
    return session


def _landing() -> str:
    return _spark().conf.get(
        "zero_ai_trust.landing_path",
        "/Volumes/0-ai-trust/bronze/landing",
    )


def read_json_dataset_stream(source_type: str, source_directory: str):
    return (
        _spark().readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{_landing()}/{source_type}/{source_directory}")
    )


def read_csv_dataset_stream(source_directory: str):
    return (
        _spark().readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .load(f"{_landing()}/file/{source_directory}")
    )


def read_manifest_stream():
    return (
        _spark().readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("multiLine", "true")
        .load(f"{_landing()}/manifests")
    )


def read_invalid_transport_stream(source_type: str):
    return (
        _spark().readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(INVALID_TRANSPORT_SCHEMA)
        .load(f"{_landing()}/quarantine/{source_type}")
    )
