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
COMMENT 'Role-playing calendar dimension; weekends are non-business days and holidays require an approved calendar'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Internal')
AS
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
  SELECT explode(sequence(DATE '1900-01-01', DATE '2100-12-31', INTERVAL 1 DAY)) AS full_date
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
