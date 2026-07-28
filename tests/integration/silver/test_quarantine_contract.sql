-- Run after Silver deployment. Every entity quarantine keeps typed source
-- columns and adds the same auditable failure evidence.
WITH quarantine_tables AS (
  SELECT table_name
  FROM `0-ai-trust`.information_schema.tables
  WHERE table_schema = 'silver'
    AND table_name LIKE '%\_quarantine' ESCAPE '\\'
), required_columns(column_name) AS (
  SELECT * FROM VALUES
    ('rule_ids'), ('failure_reason'), ('pipeline_run_id'),
    ('processed_at'), ('dq_status'), ('source_table')
), missing AS (
  SELECT tables.table_name, required.column_name
  FROM quarantine_tables tables
  CROSS JOIN required_columns required
  LEFT ANTI JOIN `0-ai-trust`.information_schema.columns actual
    ON actual.table_schema = 'silver'
   AND actual.table_name = tables.table_name
   AND actual.column_name = required.column_name
)
SELECT assert_true(COUNT(*) = 0, 'A typed quarantine table is missing audit evidence')
FROM missing;

SELECT assert_true(COUNT(*) = 19, 'Expected one typed quarantine table per Silver entity')
FROM `0-ai-trust`.information_schema.tables
WHERE table_schema = 'silver'
  AND table_name LIKE '%\_quarantine' ESCAPE '\\';
