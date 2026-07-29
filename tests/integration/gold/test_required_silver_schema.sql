-- Run after Silver is deployed and before the first Gold refresh.
-- Additive schema evolution is accepted; removal/renaming of required inputs fails.
WITH required_columns(table_name, column_name) AS (
  SELECT * FROM VALUES
    ('ip_individual', 'global_id'),
    ('ip_individual', 'pipeline_run_id'),
    ('ip_individual', 'processed_at'),
    ('ip_individual', 'masking_status'),
    ('ip_individual', '__START_AT'),
    ('ip_individual', '__END_AT'),
    ('ip_organisation', 'organisation_id'),
    ('ip_organisation', 'global_id'),
    ('ip_organisation', 'legal_name'),
    ('ip_organisation', 'pipeline_run_id'),
    ('ip_organisation', 'processed_at'),
    ('ip_organisation', 'masking_status'),
    ('ip_organisation', '__START_AT'),
    ('ip_organisation', '__END_AT'),
    ('ip_party_relationship', 'organisation_id'),
    ('ip_party_relationship', 'global_id'),
    ('ip_party_relationship', 'start_date'),
    ('ip_party_relationship', 'end_date'),
    ('ip_org_relationship', 'source_org_id'),
    ('ip_org_relationship', 'target_org_id'),
    ('ip_kyc', 'kyc_id'),
    ('ip_kyc', 'global_id'),
    ('arr_banking_arrangement', 'account_id'),
    ('arr_loan', 'loan_id'),
    ('arr_loan', 'start_date'),
    ('arr_mortgage', 'mortgage_id'),
    ('arr_mortgage', 'start_date'),
    ('arr_credit_card', 'credit_card_id'),
    ('app_application', 'application_id'),
    ('app_application', 'global_id'),
    ('app_application', 'pipeline_run_id'),
    ('app_application', 'processed_at'),
    ('app_application', 'masking_status'),
    ('app_application', '__START_AT'),
    ('app_application', '__END_AT'),
    ('app_missing_document', 'document_id'),
    ('app_missing_document', 'application_id'),
    ('app_stage_history', 'history_id'),
    ('app_stage_history', 'application_id'),
    ('app_status_change', 'change_id'),
    ('app_lifecycle_event', 'event_id'),
    ('evt_service_case', 'case_id'),
    ('evt_service_case', 'global_id'),
    ('evt_support_interaction', 'interaction_id'),
    ('evt_case_event', 'event_id')
), missing AS (
  SELECT required.table_name, required.column_name
  FROM required_columns required
  LEFT ANTI JOIN `0-ai-trust`.information_schema.columns actual
    ON actual.table_schema = 'silver'
   AND actual.table_name = required.table_name
   AND actual.column_name = required.column_name
)
SELECT assert_true(
  COUNT(*) = 0,
  'Breaking Silver schema change: a Gold-required table or column is missing'
)
FROM missing;

-- Critical timestamps must remain timestamp-compatible.
WITH timestamp_columns(table_name, column_name) AS (
  SELECT * FROM VALUES
    ('ip_individual', 'processed_at'),
    ('ip_organisation', 'processed_at'),
    ('app_application', 'processed_at'),
    ('app_stage_history', 'entered_at'),
    ('app_status_change', 'changed_at'),
    ('evt_service_case', 'created_at')
), incompatible AS (
  SELECT expected.table_name, expected.column_name
  FROM timestamp_columns expected
  JOIN `0-ai-trust`.information_schema.columns actual
    ON actual.table_schema = 'silver'
   AND actual.table_name = expected.table_name
   AND actual.column_name = expected.column_name
  WHERE actual.data_type NOT IN ('TIMESTAMP', 'TIMESTAMP_NTZ')
)
SELECT assert_true(
  COUNT(*) = 0,
  'Breaking Silver schema change: a required timestamp changed type'
)
FROM incompatible;
