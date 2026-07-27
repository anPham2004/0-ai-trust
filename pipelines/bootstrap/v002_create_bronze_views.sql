-- Run after the Bronze streaming tables have completed their first update.
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
