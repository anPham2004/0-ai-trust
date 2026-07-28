"""Data cleansing and schema enforcement shared by Silver modelling files.

Built on Databricks' native rescued-data-column mechanism — the same feature
that powers Auto Loader schema evolution — instead of hand-rolled
field-by-field checks. Passed as an option to `from_json`, it makes the Spark
JSON parser itself responsible for catching drift:
  - a JSON key not declared in the reference schema  -> rescued, not silently dropped
  - a value that fails to cast to its declared type   -> rescued, field is NULL
"Rescued" means captured verbatim as a JSON object in `_rescued_data` instead
of being discarded, so a reviewer can see exactly what did not fit.
"""

import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RESCUED_DATA_COLUMN = "_rescued_data"


def to_snake_case(name):
    """camelCase -> snake_case, leaving already-snake names alone.

    Handles acronym runs correctly: isACNCRegistered -> is_acnc_registered.
    """
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()


def with_rescued_data(record_schema):
    """Add Databricks' rescued-data column to a reference schema.

    This must be the schema handed DIRECTLY to `from_json` — the option does
    not activate for a schema nested inside an already-parsed struct, which is
    why callers re-parse the record's raw JSON text rather than reusing a
    struct column parsed earlier.
    """
    return StructType(list(record_schema.fields) + [StructField(RESCUED_DATA_COLUMN, StringType())])


def enforce_record_schema(dataframe, record_json_column, record_schema):
    """Parse one record's JSON text against its reference schema, enforcing it natively.

    `record_json_column` must hold the JSON text of the record block itself —
    e.g. Debezium's chosen before/after image, or a Kafka event's `payload` —
    not the whole envelope.

    Adds four columns to `dataframe`:
      record             struct parsed against `record_schema` (business
                         fields are read off this column by the caller)
      _rescued_data       JSON text of whatever did not fit the schema, or
                         NULL if the record matched exactly
      _schema_valid       boolean, True when `_rescued_data` is NULL
      _schema_violations  alias of `_rescued_data`, named for what callers use
                         it for: the reason a row was quarantined

    What this catches, via the Databricks Runtime JSON parser itself:
      - column name / data type enforcement: any value that cannot be cast to
        its declared type is rescued and the typed column is left NULL.
      - new columns appearing: any JSON key not declared in `record_schema` is
        rescued instead of silently discarded by from_json.

    What it does NOT catch — a column disappearing from the source entirely.
    There is nothing to rescue when a key is simply absent; the field is just
    NULL, indistinguishable from a legitimately optional field being NULL on
    one row. That is a fact about a whole batch (every row missing the same
    field), not one row, so per-row rescue cannot see it — use
    `find_missing_columns` below instead.
    """
    parsed = F.from_json(
        record_json_column,
        with_rescued_data(record_schema),
        {"rescuedDataColumn": RESCUED_DATA_COLUMN},
    )
    return (
        dataframe
        .withColumn("record", parsed)
        .withColumn("_rescued_data", F.col("record").getField(RESCUED_DATA_COLUMN))
        .withColumn("_schema_valid", F.col("_rescued_data").isNull())
        .withColumn("_schema_violations", F.col("_rescued_data"))
    )


def split_by_schema_validity(dataframe):
    """Split an enforced DataFrame into (conforming_rows, quarantined_rows).

    Call `enforce_record_schema` first. Conforming rows continue on to normal
    Silver modelling; quarantined rows keep `_schema_violations` — the rescued
    JSON — and should be written, as-is, to a `<table>_quarantine` table for
    manual review. This is the same reject-and-inspect pattern used elsewhere
    in Silver for hard DQ failures: nothing is ever dropped outright, only
    routed to whichever table matches its outcome.
    """
    conforming = dataframe.filter(F.col("_schema_valid")).drop("_schema_valid", "_schema_violations")
    quarantined = dataframe.filter(~F.col("_schema_valid"))
    return conforming, quarantined


def validate_required_columns(dataframe, required_columns):
    """Tag rows with the same _schema_valid / _schema_violations pair, for post-join checks.

    Meant for validating a DataFrame after a join (e.g. `required_columns` is
    the set of join keys / dimension fields that must resolve): a LEFT JOIN
    that found no match leaves those columns NULL, which is the streaming-safe,
    per-row signal that the join failed for that row.

    Produces the exact column pair `enforce_record_schema` does, so
    `split_by_schema_validity` works unchanged on the result — one quarantine
    mechanism shared by schema enforcement and post-join validation alike.
    """
    violations = F.array_compact(F.array(*[
        F.when(F.col(column).isNull(), F.lit(f"{column}: required value missing after join"))
        for column in required_columns
    ]))
    return (
        dataframe
        .withColumn("_schema_violations", violations)
        .withColumn("_schema_valid", F.size("_schema_violations") == 0)
    )


def find_missing_columns(dataframe, record_schema):
    """Batch diagnostic: which reference-schema fields are NULL across every row?

    Not a streaming transform — run this ad hoc (a notebook, or a scheduled
    job) against a batch read of a parsed table, e.g.:

        find_missing_columns(spark.read.table("silver.customers"), CDC_TABLE_SCHEMAS["customers"])

    A field that is legitimately optional will still show a few non-NULL rows
    in a real batch. A field stuck at zero non-NULL rows is the signal that
    the source stopped sending it — the "missing column" case that per-row
    rescue cannot see (nothing arrives to rescue when a key is simply absent).
    """
    business_columns = [to_snake_case(field.name) for field in record_schema.fields]
    non_null_counts = dataframe.select(
        [F.count(F.col(column)).alias(column) for column in business_columns]
    ).first()
    return [column for column in business_columns if non_null_counts[column] == 0]
