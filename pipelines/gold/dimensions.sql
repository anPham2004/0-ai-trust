SET pipelines.trigger.interval=15 minutes;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.dim_customer
COMMENT 'Current masked customer profile; one row per customer_id'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  global_id,
  customer_id,
  customer_type,
  name_token,
  CASE
    WHEN age IS NULL THEN 'UNKNOWN'
    WHEN age < 18 THEN 'UNDER_18'
    WHEN age < 25 THEN '18_24'
    WHEN age < 35 THEN '25_34'
    WHEN age < 45 THEN '35_44'
    WHEN age < 55 THEN '45_54'
    WHEN age < 65 THEN '55_64'
    ELSE '65_PLUS'
  END AS age_band,
  state,
  occupation_code,
  email_masked,
  phone_masked,
  preferred_contact_channel,
  preferred_language,
  marketing_opt_in,
  satisfaction_score,
  customer_since,
  last_updated_at AS profile_last_updated_at,
  'PASSED' AS dq_status,
  masking_status,
  pipeline_run_id,
  processed_at
FROM `0-ai-trust`.silver.ip_individual
WHERE `__END_AT` IS NULL
  AND masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.dim_organisation
COMMENT 'Current masked organisation profile; one row per organisation_id'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  global_id,
  organisation_id,
  business_name,
  legal_name,
  short_name,
  organisation_type,
  industry_code,
  industry_code_version,
  abn_masked,
  acn_masked,
  establishment_date,
  last_updated_at AS profile_last_updated_at,
  'PASSED' AS dq_status,
  masking_status,
  pipeline_run_id,
  processed_at
FROM `0-ai-trust`.silver.ip_organisation
WHERE `__END_AT` IS NULL
  AND masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.dim_date
COMMENT 'Role-playing calendar dimension bounded by the minimum and maximum business dates observed in Silver'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Internal')
AS
WITH business_dates AS (
  SELECT explode(array(to_date(customer_since), to_date(last_updated_at))) AS business_date
  FROM `0-ai-trust`.silver.ip_individual
  UNION ALL
  SELECT explode(array(establishment_date, to_date(last_updated_at)))
  FROM `0-ai-trust`.silver.ip_organisation
  UNION ALL
  SELECT explode(array(verification_date, last_review_date, next_review_date))
  FROM `0-ai-trust`.silver.ip_kyc
  UNION ALL
  SELECT explode(array(start_date, end_date))
  FROM `0-ai-trust`.silver.ip_party_relationship
  UNION ALL
  SELECT start_date
  FROM `0-ai-trust`.silver.ip_org_relationship
  UNION ALL
  SELECT open_date
  FROM `0-ai-trust`.silver.arr_banking_arrangement
  UNION ALL
  SELECT explode(array(start_date, maturity_date))
  FROM `0-ai-trust`.silver.arr_loan
  UNION ALL
  SELECT start_date
  FROM `0-ai-trust`.silver.arr_mortgage
  UNION ALL
  SELECT issued_date
  FROM `0-ai-trust`.silver.arr_credit_card
  UNION ALL
  SELECT explode(array(to_date(submitted_at), to_date(last_updated_at)))
  FROM `0-ai-trust`.silver.app_application
  UNION ALL
  SELECT explode(array(to_date(entered_at), to_date(exited_at), to_date(sla_deadline)))
  FROM `0-ai-trust`.silver.app_stage_history
  UNION ALL
  SELECT to_date(changed_at)
  FROM `0-ai-trust`.silver.app_status_change
  UNION ALL
  SELECT to_date(event_timestamp)
  FROM `0-ai-trust`.silver.app_lifecycle_event
  UNION ALL
  SELECT explode(array(to_date(requested_at), to_date(received_at), expiry_date, to_date(last_reminder_at)))
  FROM `0-ai-trust`.silver.app_missing_document
  UNION ALL
  SELECT issued_at
  FROM `0-ai-trust`.silver.app_accepted_loan
  UNION ALL
  SELECT application_date
  FROM `0-ai-trust`.silver.app_rejected_application
  UNION ALL
  SELECT explode(array(to_date(sla_deadline), to_date(created_at), to_date(resolved_at)))
  FROM `0-ai-trust`.silver.evt_service_case
  UNION ALL
  SELECT to_date(interaction_timestamp)
  FROM `0-ai-trust`.silver.evt_support_interaction
  UNION ALL
  SELECT to_date(event_timestamp)
  FROM `0-ai-trust`.silver.evt_case_event
), date_bounds AS (
  SELECT MIN(business_date) AS min_date, MAX(business_date) AS max_date
  FROM business_dates
  WHERE business_date IS NOT NULL
)
SELECT
  CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
  full_date,
  date_format(full_date, 'EEEE') AS day_of_week,
  weekofyear(full_date) AS week_of_year,
  month(full_date) AS month,
  quarter(full_date) AS quarter,
  year(full_date) AS year,
  dayofweek(full_date) NOT IN (1, 7) AS is_business_day
FROM (
  SELECT explode(sequence(min_date, max_date, INTERVAL 1 DAY)) AS full_date
  FROM date_bounds
);

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.dim_stage_action_policy
COMMENT 'Observed stage/action combinations; approved wording remains unavailable until supplied by the policy owner'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Internal')
AS
SELECT DISTINCT
  stage,
  COALESCE(pending_action_party, 'UNASSIGNED') AS pending_action_party,
  'POLICY_NOT_CONFIGURED' AS next_action_code,
  CAST(NULL AS STRING) AS banker_action_text,
  CAST(NULL AS STRING) AS customer_safe_action_text,
  CAST(NULL AS INT) AS default_sla_days,
  UPPER(stage) IN ('APPROVED', 'DENIED', 'CANCELLED', 'WITHDRAWN') AS is_terminal_stage,
  DATE '1970-01-01' AS effective_from,
  CAST(NULL AS DATE) AS effective_to,
  'UNASSIGNED' AS policy_owner
FROM `0-ai-trust`.silver.app_stage_history
WHERE stage IS NOT NULL;
