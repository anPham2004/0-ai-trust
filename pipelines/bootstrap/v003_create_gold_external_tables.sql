-- Run only after the approved Silver model is available. Gold remains in customer-owned S3.
-- AI-ready objects are logical views created by pipelines/gold-sql/ai-ready.

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.dim_party (
  party_key STRING,
  global_id STRING,
  customer_id STRING,
  customer_type STRING,
  name_token STRING,
  preferred_contact_channel STRING,
  preferred_language STRING,
  marketing_opt_in BOOLEAN,
  satisfaction_score STRING,
  address_state STRING,
  address_postcode STRING,
  customer_since TIMESTAMP,
  last_updated_at TIMESTAMP,
  organisation_id STRING,
  business_name STRING,
  legal_name STRING,
  organisation_type STRING,
  industry_code STRING,
  registered_country STRING,
  establishment_date DATE,
  org_last_updated_at TIMESTAMP,
  is_acnc_registered BOOLEAN,
  abn_masked STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/dim_party'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Conformed individual and organisation party dimension; one row per party_key';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.dim_kyc (
  kyc_key STRING,
  global_id STRING,
  organisation_id STRING,
  record_type STRING,
  verification_status STRING,
  verification_date DATE,
  verification_method STRING,
  last_review_date DATE,
  next_review_date DATE,
  document_types STRING,
  abn_verified BOOLEAN,
  asic_check_status STRING,
  beneficial_ownership_verified BOOLEAN,
  risk_rating STRING,
  pep_status BOOLEAN,
  sanctions_check STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/dim_kyc'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Highly Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'KYC and KYB status dimension; highly confidential attributes remain banker-restricted';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.dim_organisation_party (
  org_party_key STRING,
  organisation_id STRING,
  global_id STRING,
  name_token STRING,
  party_role STRING,
  authority_level STRING,
  is_active BOOLEAN,
  start_date DATE,
  end_date DATE,
  preferred_contact_channel STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/dim_organisation_party'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'People authorised for an organisation; one row per source relationship';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.dim_organisation_relationship (
  org_rel_key STRING,
  source_org_id STRING,
  target_org_id STRING,
  relationship_type STRING,
  is_active BOOLEAN,
  start_date DATE,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/dim_organisation_relationship'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Internal', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Organisation-to-organisation relationship dimension';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.dim_arrangement (
  arrangement_key STRING,
  global_id STRING,
  organisation_id STRING,
  arrangement_type STRING,
  account_type STRING,
  loan_type STRING,
  card_type STRING,
  interest_type STRING,
  status STRING,
  is_active BOOLEAN,
  is_business_arrangement BOOLEAN,
  is_approaching_maturity BOOLEAN,
  open_date DATE,
  start_date DATE,
  maturity_date DATE,
  repayment_frequency STRING,
  reward_program STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/dim_arrangement'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Unified banking, loan, mortgage, and credit-card arrangement dimension';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.dim_application (
  application_key STRING,
  application_id STRING,
  global_id STRING,
  organisation_id STRING,
  loan_goal STRING,
  application_type STRING,
  final_outcome STRING,
  is_business_application BOOLEAN,
  submitted_at TIMESTAMP,
  last_updated_at TIMESTAMP,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/dim_application'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Loan application dimension; one row per application';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.dim_document (
  document_key STRING,
  document_id STRING,
  application_id STRING,
  global_id STRING,
  document_type STRING,
  status STRING,
  is_invalid_or_expired BOOLEAN,
  rejection_reason STRING,
  requested_at TIMESTAMP,
  received_at TIMESTAMP,
  expiry_date DATE,
  reminders_sent INT,
  last_reminder_at TIMESTAMP,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/dim_document'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Application document checklist dimension; one row per document';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.fact_application_stage (
  stage_fact_key STRING,
  application_key STRING,
  application_id STRING,
  global_id STRING,
  event_source STRING,
  event_timestamp TIMESTAMP,
  stage_name STRING,
  entered_at TIMESTAMP,
  exited_at TIMESTAMP,
  duration_days INT,
  is_current_stage BOOLEAN,
  sla_deadline TIMESTAMP,
  is_sla_breached BOOLEAN,
  pending_action_party STRING,
  assigned_team STRING,
  old_status STRING,
  new_status STRING,
  change_reason STRING,
  lifecycle_transition STRING,
  event_origin STRING,
  action STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/fact_application_stage'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Application stage, status-change, and lifecycle event timeline';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.fact_service_interaction (
  service_fact_key STRING,
  global_id STRING,
  organisation_id STRING,
  linked_application_id STRING,
  record_type STRING,
  case_id STRING,
  interaction_id STRING,
  case_type STRING,
  subject STRING,
  case_status STRING,
  priority STRING,
  assigned_team STRING,
  sla_deadline TIMESTAMP,
  is_sla_breached BOOLEAN,
  case_created_at TIMESTAMP,
  case_resolved_at TIMESTAMP,
  resolution_summary STRING,
  latest_event_type STRING,
  latest_event_timestamp TIMESTAMP,
  latest_event_description STRING,
  channel STRING,
  interaction_timestamp TIMESTAMP,
  topic STRING,
  resolution STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/fact_service_interaction'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Unified service cases, support interactions, and case-event history';

CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold.fact_arrangement_snapshot (
  snapshot_key STRING,
  arrangement_key STRING,
  global_id STRING,
  organisation_id STRING,
  arrangement_type STRING,
  status STRING,
  is_active BOOLEAN,
  is_approaching_maturity BOOLEAN,
  snapshot_date DATE,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  dq_status STRING
) USING DELTA
LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/fact_arrangement_snapshot'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential', 'delta.enableChangeDataFeed' = 'true')
COMMENT 'Daily idempotent snapshot of current arrangements';
