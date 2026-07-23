-- Run once with a principal that owns the storage credential/external location.
CREATE CATALOG IF NOT EXISTS `0-ai-trust`
MANAGED LOCATION 's3://g3-assignment/g3/0-ai-trust/__managed'
COMMENT 'Zero Trust AI data product; all business data is stored in customer-owned S3';

CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.bronze
COMMENT 'Incremental native ingestion and source history';
CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.silver
COMMENT 'Reserved modelling template: validated and quarantined data';
CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.gold
COMMENT 'Reserved modelling template: masked AI-ready data products';

-- Databricks creates this convenience schema with every new catalog. It is not
-- part of the data product and is removed to keep the project namespace strict.
DROP SCHEMA IF EXISTS `0-ai-trust`.default;

CREATE EXTERNAL VOLUME IF NOT EXISTS `0-ai-trust`.bronze.landing
LOCATION 's3://g3-assignment/g3/0-ai-trust/bronze/landing'
COMMENT 'Immutable source-native landing zone';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.bronze.cdc_changes (
  topic STRING,
  kafka_partition BIGINT,
  kafka_offset BIGINT,
  record_key STRING,
  source_dataset STRING,
  operation STRING,
  snapshot_state STRING,
  source_lsn BIGINT,
  source_timestamp_ms BIGINT,
  raw_payload STRING,
  captured_at TIMESTAMP,
  _source_file STRING,
  _source_file_modified_at TIMESTAMP,
  _ingested_at TIMESTAMP
)
USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/bronze/tables/cdc_changes'
TBLPROPERTIES (
  'quality' = 'bronze',
  'data_classification' = 'Highly Confidential',
  'delta.appendOnly' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Append-only HVR-compatible CDC history; raw Debezium envelopes are retained';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.bronze.kafka_events (
  topic STRING,
  kafka_partition BIGINT,
  kafka_offset BIGINT,
  event_key STRING,
  event_id STRING,
  event_type STRING,
  source_dataset STRING,
  occurred_at STRING,
  raw_payload STRING,
  captured_at TIMESTAMP,
  _source_file STRING,
  _source_file_modified_at TIMESTAMP,
  _ingested_at TIMESTAMP
)
USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/bronze/tables/kafka_events'
TBLPROPERTIES (
  'quality' = 'bronze',
  'data_classification' = 'Highly Confidential',
  'delta.appendOnly' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Immutable Kafka event history with transport envelope and source payload';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.bronze.file_arrivals (
  source_dataset STRING,
  source_file STRING,
  raw_content BINARY,
  source_file_size BIGINT,
  source_file_modified_at TIMESTAMP,
  _ingested_at TIMESTAMP
)
USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/bronze/tables/file_arrivals'
TBLPROPERTIES (
  'quality' = 'bronze',
  'data_classification' = 'Highly Confidential',
  'delta.appendOnly' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Immutable byte-for-byte file arrivals; parsing is deferred until Silver';

CREATE OR REPLACE VIEW `0-ai-trust`.bronze.cdc_scd2 AS
WITH transport_deduplicated AS (
  SELECT * EXCEPT (_transport_rank)
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY topic, kafka_partition, kafka_offset
      ORDER BY _ingested_at
    ) AS _transport_rank
    FROM `0-ai-trust`.bronze.cdc_changes
  )
  WHERE _transport_rank = 1 AND operation IS NOT NULL
), history AS (
  SELECT *,
    TIMESTAMP_MILLIS(source_timestamp_ms) AS valid_from,
    LEAD(TIMESTAMP_MILLIS(source_timestamp_ms)) OVER (
      PARTITION BY source_dataset, record_key
      ORDER BY source_lsn, kafka_partition, kafka_offset
    ) AS valid_to
  FROM transport_deduplicated
)
SELECT *,
  valid_to IS NULL AND operation <> 'd' AS is_current,
  operation = 'd' AS is_deleted
FROM history;

CREATE OR REPLACE VIEW `0-ai-trust`.bronze.kafka_events_deduplicated AS
SELECT * EXCEPT (_transport_rank)
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY topic, kafka_partition, kafka_offset
    ORDER BY _ingested_at
  ) AS _transport_rank
  FROM `0-ai-trust`.bronze.kafka_events
)
WHERE _transport_rank = 1;
