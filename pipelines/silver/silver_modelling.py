"""Bronze -> Silver modelling, organised as one pipeline shape per table:

    extract_bronze_layer -> pre_transform -> transform -> post_transform -> load_silver_table

All schema/cleansing/validation logic is delegated to
framework.silver_validator so this file stays about SHAPE (extract, join,
select, register), not about re-implementing DQ mechanics.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from framework.source_schemas import (
    CDC_TABLE_SCHEMAS,
    EVENT_TABLE_SCHEMAS,
    debezium_envelope_schema,
    kafka_event_envelope_schema,
)
from framework.silver_validator import (
    enforce_record_schema,
    split_by_schema_validity,
    to_snake_case,
    validate_required_columns,
)

BRONZE_CDC_TABLE = "`0-ai-trust`.bronze.cdc_changes"
BRONZE_EVENT_TABLE = "`0-ai-trust`.bronze.kafka_events"
BRONZE_FILE_TABLE = "`0-ai-trust`.bronze.file_arrivals"

DEFAULT_SILVER_SPARK_CONF = {"pipelines.trigger.interval": "15 minutes"}
DEFAULT_SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "data_classification": "Highly Confidential",
    "delta.enableChangeDataFeed": "true",
}


def _record_columns(record_schema, source_column):
    """One renamed column per business field of a parsed record struct.

    getField() is used rather than a dotted path because some source columns are
    SQL reserved words (`from`, `type`, `timestamp`) that a dotted path would
    fail to parse.
    """
    return [
        F.col(source_column).getField(field.name).alias(to_snake_case(field.name))
        for field in record_schema.fields
    ]


# ---------------------------------------------------------------------------
# extract_bronze_layer — Bronze -> schema-enforced flat DataFrame, by table name
# ---------------------------------------------------------------------------


def _record_json_column(payload_column):
    """Pick the RAW JSON text of the CDC row image that describes the record, by operation.

        delete (d)                     -> `$.before`, because `$.after` is NULL
        insert / update / read (c/u/r) -> `$.after`, the new state of the row

    Without this, deletes would come through with every business column NULL —
    including the key — so they could never be matched to the row they delete.

    Kept as JSON text (via get_json_object), not a parsed struct: schema
    enforcement re-parses this text with `rescuedDataColumn` enabled, and that
    option only activates for the schema handed directly to `from_json` — it
    would not fire on a schema buried inside an already-parsed struct.
    """
    return F.when(
        F.get_json_object(payload_column, "$.op") == "d",
        F.get_json_object(payload_column, "$.before"),
    ).otherwise(F.get_json_object(payload_column, "$.after"))


def _extract_cdc_table(table):
    """Extract one Debezium CDC table from bronze.cdc_changes, schema-enforced.

    Business fields are taken from `before` for deletes and from `after` for
    inserts/updates/snapshots, so every row carries a populated key regardless
    of operation. Deletes rely on the source table's PostgreSQL REPLICA
    IDENTITY publishing a before-image; if `before` is NULL on deletes, the
    connector never sent that data and no pipeline code can recover it.
    """
    record_schema = CDC_TABLE_SCHEMAS[table]

    envelope = (
        spark.readStream.table(BRONZE_CDC_TABLE)
        .filter(F.col("source_dataset").endswith(f".{table}"))
        .select("source_dataset", F.col("raw_payload").alias("payload"))
        .withColumn("envelope", F.from_json("payload", debezium_envelope_schema(record_schema)))
        .withColumn("record_json", _record_json_column("payload"))
    )

    enforced = enforce_record_schema(envelope, "record_json", record_schema)

    return enforced.select(
        *_record_columns(record_schema, "record"),
        "_schema_valid",
        "_schema_violations",

        F.col("envelope.op").alias("_cdc_operation"),
        # When the event was EMITTED to the stream. This is the only ordering
        # key kept, so note it is millisecond-granular: two changes to the same
        # row inside one millisecond cannot be ordered by it.
        F.col("envelope.ts_ms").alias("_cdc_emitted_at_ms"),

        # lineage: bare table name from the envelope, `<schema>.<table>` from Bronze
        F.col("envelope.source.table").alias("_source_table"),
        F.col("source_dataset").alias("_source_dataset"),
    )


def _extract_event_table(table):
    """Extract one Kafka business-event table from bronze.kafka_events, schema-enforced.

    Events are immutable historical facts, so there is no before/after image and
    no operation type — the record arrives once, under `payload`. That makes
    this simpler than the CDC path: no state selection is needed.
    """
    record_schema = EVENT_TABLE_SCHEMAS[table]

    envelope = (
        spark.readStream.table(BRONZE_EVENT_TABLE)
        .filter(F.col("source_dataset") == table)
        .select(F.col("raw_payload").alias("payload"))
        .withColumn("envelope", F.from_json("payload", kafka_event_envelope_schema(record_schema)))
        .withColumn("record_json", F.get_json_object("payload", "$.payload"))
    )

    enforced = enforce_record_schema(envelope, "record_json", record_schema)

    return enforced.select(
        *_record_columns(record_schema, "record"),
        "_schema_valid",
        "_schema_violations",

        # event envelope metadata
        F.col("envelope.event_id").alias("_event_id"),
        F.col("envelope.event_type").alias("_event_type"),
        F.col("envelope.event_version").alias("_event_version"),
        F.col("envelope.occurred_at").alias("_event_occurred_at"),
        F.col("envelope.producer").alias("_event_producer"),
        # ties related events together (application id, or global_id where the
        # producer had one) — the join handle for event correlation.
        F.col("envelope.correlation_id").alias("_event_correlation_id"),
        F.col("envelope.source_dataset").alias("_event_source_dataset"),
    )


def extract_bronze_layer(table_name):
    """Extract and schema-enforce one Silver source table from Bronze, by name.

    Dispatches on which Bronze stream `table_name` belongs to:
      - a DATABASE/Debezium table (framework.source_schemas.CDC_TABLE_SCHEMAS)
        -> bronze.cdc_changes
      - an EVENT/Kafka table (framework.source_schemas.EVENT_TABLE_SCHEMAS)
        -> bronze.kafka_events

    Both paths call framework.silver_validator.enforce_record_schema, so every
    row leaving this function already carries `_schema_valid` /
    `_schema_violations` for `pre_transform` to act on.
    """
    if table_name in CDC_TABLE_SCHEMAS:
        return _extract_cdc_table(table_name)
    if table_name in EVENT_TABLE_SCHEMAS:
        return _extract_event_table(table_name)
    raise ValueError(f"Unknown Silver source table: {table_name}")


# ---------------------------------------------------------------------------
# pre_transform — cleanse a freshly extracted DataFrame
# ---------------------------------------------------------------------------


def pre_transform(dataframe):
    """Cleanse a freshly extracted DataFrame before it is joined or reshaped.

    Two things happen, both delegated to framework.silver_validator so this
    stays the one place cleansing happens, rather than being reinvented per
    table:
      1. `split_by_schema_validity` separates rows that already failed schema
         enforcement in `extract_bronze_layer` — those never reach `transform`.
      2. Every remaining STRING column has surrounding whitespace trimmed and
         blank values folded to NULL, so "no value" always means one thing
         instead of two ("" vs NULL) to every rule and every join downstream.

    Returns (clean, quarantined). `quarantined` should be written, unmodified,
    to a `<table>_quarantine` table by the caller's `load_silver_table` call.
    """
    clean, quarantined = split_by_schema_validity(dataframe)

    for field in clean.schema.fields:
        if isinstance(field.dataType, StringType):
            clean = clean.withColumn(field.name, F.nullif(F.trim(F.col(field.name)), F.lit("")))

    return clean, quarantined


# ---------------------------------------------------------------------------
# transform — join tables together and select the fields Silver needs
# ---------------------------------------------------------------------------


def transform(dataframe, joins=None, select_columns=None):
    """Join `dataframe` with any number of other tables and select the needed fields.

    `joins`: optional list of (other_dataframe, on, how) tuples, applied in
    order with a plain DataFrame join — Lakeflow has no special join API of
    its own; joining a stream against a batch-read dimension is the standard
    "enrich with a dimension" pattern. `how` defaults to "left" so a dimension
    that has no match doesn't drop the fact row outright — `post_transform`
    is what checks for that afterwards.

    `select_columns`: optional explicit column list for the final shape; when
    omitted, every column produced by the join(s) is kept.
    """
    joined = dataframe
    for other, on, *rest in joins or []:
        how = rest[0] if rest else "left"
        joined = joined.join(other, on=on, how=how)

    return joined.select(*select_columns) if select_columns else joined


# ---------------------------------------------------------------------------
# post_transform — validate the joined result
# ---------------------------------------------------------------------------


def post_transform(dataframe, required_columns):
    """Validate a joined DataFrame, flagging rows a join left incomplete.

    Delegates to framework.silver_validator.validate_required_columns:
    `required_columns` is typically the join keys / dimension fields `transform`
    just resolved. A LEFT JOIN with no match leaves those NULL, which is the
    per-row signal that the join failed for that row.

    Produces the same `_schema_valid` / `_schema_violations` pair
    `extract_bronze_layer` does, so the caller can run
    `split_by_schema_validity` again to separate rows the join resolved
    cleanly from ones it didn't, before calling `load_silver_table`.
    """
    return validate_required_columns(dataframe, required_columns)


# ---------------------------------------------------------------------------
# load_silver_table — register the result as a Silver table
# ---------------------------------------------------------------------------


def load_silver_table(table_name, build_dataframe, comment=None, table_properties=None, spark_conf=None, cluster_by=None):
    """Register `table_name` as a Silver table produced by `build_dataframe`.

    Lakeflow Declarative Pipelines never take an explicit `.write(...)` call —
    a table is defined by decorating a zero-argument function with `@dp.table`
    and returning the DataFrame to materialise. This wraps that registration so
    every Silver table declares the same default `spark_conf` /
    `table_properties` instead of repeating the decorator boilerplate per
    table, and so a table can be registered dynamically (e.g. from a loop over
    several table names) rather than only via a literal `@dp.table` at import
    time.

    `build_dataframe` must be a zero-argument callable returning the final
    DataFrame — typically a small closure chaining `extract_bronze_layer`,
    `pre_transform`, `transform`, and `post_transform` for one table.
    """
    dp.table(
        name=table_name,
        comment=comment,
        spark_conf=spark_conf or DEFAULT_SILVER_SPARK_CONF,
        table_properties={**DEFAULT_SILVER_TABLE_PROPERTIES, **(table_properties or {})},
        cluster_by=cluster_by,
    )(build_dataframe)
