-- Interface required by the assignment; run only after the Silver workstream is deployed.
WITH required_columns(column_name) AS (
  SELECT * FROM VALUES
    ('source_dataset'),
    ('source_record_id'),
    ('rule_id'),
    ('failure_reason'),
    ('disposition'),
    ('pipeline_run_id'),
    ('processed_at')
), missing AS (
  SELECT required.column_name
  FROM required_columns required
  LEFT ANTI JOIN `0-ai-trust`.information_schema.columns actual
    ON actual.table_schema = 'silver'
   AND actual.table_name = 'record_quarantine'
   AND actual.column_name = required.column_name
)
SELECT assert_true(
  COUNT(*) = 0,
  'Silver quarantine must identify the source record, rule, reason, disposition, and run'
)
FROM missing;

SELECT assert_true(COUNT(*) = 0, 'Quarantine contains an unsupported disposition')
FROM `0-ai-trust`.silver.record_quarantine
WHERE disposition NOT IN ('QUARANTINED', 'DROPPED', 'REVIEW_REQUIRED');

SELECT assert_true(COUNT(*) = 0, 'Quarantine row lacks auditable failure context')
FROM `0-ai-trust`.silver.record_quarantine
WHERE source_dataset IS NULL
   OR source_record_id IS NULL
   OR rule_id IS NULL
   OR failure_reason IS NULL
   OR pipeline_run_id IS NULL
   OR processed_at IS NULL;
