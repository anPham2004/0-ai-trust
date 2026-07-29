-- Run after Silver deployment. Mutable entities must expose native SCD2
-- validity, while canonical Silver contains no row-level DQ/quarantine model.

WITH mutable_entities(table_name) AS (
  SELECT * FROM VALUES
    ('ip_individual'), ('ip_kyc'), ('ip_organisation'),
    ('ip_party_relationship'), ('ip_org_relationship'),
    ('arr_banking_arrangement'), ('arr_loan'),
    ('arr_mortgage'), ('arr_credit_card'),
    ('app_application'), ('app_missing_document'), ('evt_service_case')
), required_columns(column_name) AS (
  SELECT * FROM VALUES ('__START_AT'), ('__END_AT')
), missing AS (
  SELECT entities.table_name, required.column_name
  FROM mutable_entities entities
  CROSS JOIN required_columns required
  LEFT ANTI JOIN `0-ai-trust`.information_schema.columns actual
    ON actual.table_schema = 'silver'
   AND actual.table_name = entities.table_name
   AND UPPER(actual.column_name) = required.column_name
)
SELECT assert_true(COUNT(*) = 0, 'A mutable Silver entity is missing native SCD2 validity')
FROM missing;

SELECT assert_true(COUNT(*) = 0, 'Canonical Silver must not expose DQ status columns')
FROM `0-ai-trust`.information_schema.columns
WHERE table_schema = 'silver'
  AND LOWER(column_name) = 'dq_status';

SELECT assert_true(COUNT(*) = 0, 'Canonical Silver must not publish quarantine tables')
FROM `0-ai-trust`.information_schema.tables
WHERE table_schema = 'silver'
  AND table_name LIKE '%\_quarantine' ESCAPE '\\';

-- A business key can have many historical versions but only one open version.
SELECT assert_true(COUNT(*) = 0, 'Application has multiple open SCD2 versions')
FROM (
  SELECT application_id
  FROM `0-ai-trust`.silver.app_application
  WHERE __END_AT IS NULL
  GROUP BY application_id
  HAVING COUNT(*) > 1
);
