SET pipelines.trigger.interval=15 minutes;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_subject_context_snapshot (
  snapshot_id STRING NOT NULL,
  entity_id STRING,
  entity_type STRING,
  customer_id STRING,
  organisation_id STRING,
  snapshot_timestamp TIMESTAMP,
  preferred_contact_channel STRING,
  preferred_language STRING,
  marketing_opt_in BOOLEAN,
  satisfaction_score_current STRING,
  last_support_contact_at TIMESTAMP,
  last_support_channel STRING,
  recent_support_interaction_count_30d BIGINT,
  top_case_type_90d STRING,
  open_case_count BIGINT,
  sla_breached_case_count BIGINT,
  open_application_count BIGINT,
  latest_application_id STRING,
  latest_application_status STRING,
  latest_application_stage STRING,
  latest_pending_action_party STRING,
  latest_assigned_team STRING,
  missing_document_count BIGINT,
  rejected_document_count BIGINT,
  expired_document_count BIGINT,
  total_reminders_sent BIGINT,
  active_arrangement_count BIGINT,
  active_account_count BIGINT,
  active_loan_count BIGINT,
  active_mortgage_count BIGINT,
  active_credit_card_count BIGINT,
  verification_status STRING,
  verification_review_overdue_flag BOOLEAN,
  active_authorised_representative_count BIGINT,
  active_director_count BIGINT,
  warning_codes ARRAY<STRING>,
  source_max_processed_at TIMESTAMP,
  CONSTRAINT pk_fact_subject_context_snapshot PRIMARY KEY (snapshot_id),
  CONSTRAINT fk_fact_subject_context_customer FOREIGN KEY (customer_id)
    REFERENCES `0-ai-trust`.gold.dim_customer (customer_id),
  CONSTRAINT fk_fact_subject_context_organisation FOREIGN KEY (organisation_id)
    REFERENCES `0-ai-trust`.gold.dim_organisation (organisation_id)
)
COMMENT 'Deterministic AI-ready customer and organisation context as of the latest contributing Gold record'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
WITH customer_support AS (
  SELECT
    customer_id,
    max(CASE WHEN is_customer_contact THEN activity_timestamp END) AS last_support_contact_at,
    max_by(CASE WHEN is_customer_contact THEN channel END, activity_timestamp) AS last_support_channel,
    count_if(activity_type = 'SUPPORT_INTERACTION'
      AND activity_timestamp >= current_timestamp() - INTERVAL 30 DAYS) AS recent_support_interaction_count_30d,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_service_activity
  GROUP BY customer_id
), customer_case_type_counts AS (
  SELECT customer_id, case_type, count(*) AS case_count
  FROM `0-ai-trust`.gold.fact_service_case_current
  WHERE created_at >= current_timestamp() - INTERVAL 90 DAYS
  GROUP BY customer_id, case_type
), customer_case_types AS (
  SELECT customer_id, max_by(case_type, case_count) AS top_case_type_90d
  FROM customer_case_type_counts
  GROUP BY customer_id
), customer_cases AS (
  SELECT
    customer_id,
    count_if(is_open) AS open_case_count,
    count_if(is_sla_breached) AS sla_breached_case_count,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_service_case_current
  GROUP BY customer_id
), customer_applications AS (
  SELECT
    customer_id,
    count_if(UPPER(COALESCE(final_outcome, latest_status, 'PENDING'))
      NOT IN ('ACCEPTED', 'APPROVED', 'DENIED', 'REJECTED', 'CANCELLED', 'WITHDRAWN')) AS open_application_count,
    max_by(application_id, COALESCE(last_updated_at, submitted_at)) AS latest_application_id,
    max_by(latest_status, COALESCE(last_updated_at, submitted_at)) AS latest_application_status,
    max_by(current_stage, COALESCE(last_updated_at, submitted_at)) AS latest_application_stage,
    max_by(pending_action_party, COALESCE(last_updated_at, submitted_at)) AS latest_pending_action_party,
    max_by(assigned_team, COALESCE(last_updated_at, submitted_at)) AS latest_assigned_team,
    sum(missing_document_count) AS missing_document_count,
    sum(rejected_document_count) AS rejected_document_count,
    sum(expired_document_count) AS expired_document_count,
    sum(total_reminders_sent) AS total_reminders_sent,
    max(last_refreshed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_application_current
  GROUP BY customer_id
), customer_arrangements AS (
  SELECT
    customer_id,
    count_if(is_active) AS active_arrangement_count,
    count_if(is_active AND arrangement_type = 'BANK_ACCOUNT') AS active_account_count,
    count_if(is_active AND arrangement_type = 'LOAN') AS active_loan_count,
    count_if(is_active AND arrangement_type = 'MORTGAGE') AS active_mortgage_count,
    count_if(is_active AND arrangement_type = 'CREDIT_CARD') AS active_credit_card_count,
    count_if(arrangement_type = 'MORTGAGE') > 0 AS has_inferred_mortgage_status,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_arrangement_current
  GROUP BY customer_id
), customer_verification AS (
  SELECT
    customer_id,
    max_by(verification_status, processed_at) AS verification_status,
    max_by(review_overdue_flag, processed_at) AS review_overdue_flag,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_verification_current
  WHERE organisation_id IS NULL
  GROUP BY customer_id
), customer_roles AS (
  SELECT
    customer_id,
    count_if(is_active AND party_role = 'AUTHORISED_REPRESENTATIVE') AS active_authorised_representative_count,
    count_if(is_active AND party_role = 'DIRECTOR') AS active_director_count,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.bridge_organisation_party_role
  GROUP BY customer_id
), customer_context AS (
  SELECT
    c.customer_id AS entity_id,
    'CUSTOMER' AS entity_type,
    c.customer_id,
    CAST(NULL AS STRING) AS organisation_id,
    c.preferred_contact_channel,
    c.preferred_language,
    c.marketing_opt_in,
    c.satisfaction_score AS satisfaction_score_current,
    s.last_support_contact_at,
    s.last_support_channel,
    COALESCE(s.recent_support_interaction_count_30d, 0) AS recent_support_interaction_count_30d,
    ct.top_case_type_90d,
    COALESCE(sc.open_case_count, 0) AS open_case_count,
    COALESCE(sc.sla_breached_case_count, 0) AS sla_breached_case_count,
    COALESCE(a.open_application_count, 0) AS open_application_count,
    a.latest_application_id,
    a.latest_application_status,
    a.latest_application_stage,
    a.latest_pending_action_party,
    a.latest_assigned_team,
    COALESCE(a.missing_document_count, 0) AS missing_document_count,
    COALESCE(a.rejected_document_count, 0) AS rejected_document_count,
    COALESCE(a.expired_document_count, 0) AS expired_document_count,
    COALESCE(a.total_reminders_sent, 0) AS total_reminders_sent,
    COALESCE(ar.active_arrangement_count, 0) AS active_arrangement_count,
    COALESCE(ar.active_account_count, 0) AS active_account_count,
    COALESCE(ar.active_loan_count, 0) AS active_loan_count,
    COALESCE(ar.active_mortgage_count, 0) AS active_mortgage_count,
    COALESCE(ar.active_credit_card_count, 0) AS active_credit_card_count,
    v.verification_status,
    v.review_overdue_flag AS verification_review_overdue_flag,
    COALESCE(r.active_authorised_representative_count, 0) AS active_authorised_representative_count,
    COALESCE(r.active_director_count, 0) AS active_director_count,
    filter(array(
      CASE WHEN v.verification_status IS NULL THEN 'VERIFICATION_CONTEXT_UNAVAILABLE' END,
      CASE WHEN ar.has_inferred_mortgage_status THEN 'MORTGAGE_STATUS_INFERRED' END,
      CASE WHEN a.latest_application_id IS NOT NULL THEN 'ACTION_POLICY_NOT_CONFIGURED' END,
      CASE WHEN greatest(c.processed_at, s.processed_at, sc.processed_at, a.processed_at, ar.processed_at, v.processed_at, r.processed_at)
        < current_timestamp() - INTERVAL 24 HOURS THEN 'STALE_CONTEXT' END
    ), x -> x IS NOT NULL) AS warning_codes,
    greatest(c.processed_at, s.processed_at, sc.processed_at, a.processed_at, ar.processed_at, v.processed_at, r.processed_at)
      AS source_max_processed_at
  FROM `0-ai-trust`.gold.dim_customer c
  LEFT JOIN customer_support s ON s.customer_id = c.customer_id
  LEFT JOIN customer_case_types ct ON ct.customer_id = c.customer_id
  LEFT JOIN customer_cases sc ON sc.customer_id = c.customer_id
  LEFT JOIN customer_applications a ON a.customer_id = c.customer_id
  LEFT JOIN customer_arrangements ar ON ar.customer_id = c.customer_id
  LEFT JOIN customer_verification v ON v.customer_id = c.customer_id
  LEFT JOIN customer_roles r ON r.customer_id = c.customer_id
), organisation_case_type_counts AS (
  SELECT organisation_id, case_type, count(*) AS case_count
  FROM `0-ai-trust`.gold.fact_service_case_current
  WHERE organisation_id IS NOT NULL
    AND created_at >= current_timestamp() - INTERVAL 90 DAYS
  GROUP BY organisation_id, case_type
), organisation_case_types AS (
  SELECT organisation_id, max_by(case_type, case_count) AS top_case_type_90d
  FROM organisation_case_type_counts
  GROUP BY organisation_id
), organisation_cases AS (
  SELECT
    organisation_id,
    count_if(is_open) AS open_case_count,
    count_if(is_sla_breached) AS sla_breached_case_count,
    max(created_at) AS last_support_contact_at,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_service_case_current
  WHERE organisation_id IS NOT NULL
  GROUP BY organisation_id
), organisation_application_links AS (
  SELECT DISTINCT application_id, organisation_id
  FROM `0-ai-trust`.gold.bridge_application_party_authority
), organisation_applications AS (
  SELECT
    l.organisation_id,
    count_if(UPPER(COALESCE(a.final_outcome, a.latest_status, 'PENDING'))
      NOT IN ('ACCEPTED', 'APPROVED', 'DENIED', 'REJECTED', 'CANCELLED', 'WITHDRAWN')) AS open_application_count,
    max_by(a.application_id, COALESCE(a.last_updated_at, a.submitted_at)) AS latest_application_id,
    max_by(a.latest_status, COALESCE(a.last_updated_at, a.submitted_at)) AS latest_application_status,
    max_by(a.current_stage, COALESCE(a.last_updated_at, a.submitted_at)) AS latest_application_stage,
    max_by(a.pending_action_party, COALESCE(a.last_updated_at, a.submitted_at)) AS latest_pending_action_party,
    max_by(a.assigned_team, COALESCE(a.last_updated_at, a.submitted_at)) AS latest_assigned_team,
    sum(a.missing_document_count) AS missing_document_count,
    sum(a.rejected_document_count) AS rejected_document_count,
    sum(a.expired_document_count) AS expired_document_count,
    sum(a.total_reminders_sent) AS total_reminders_sent,
    max(a.last_refreshed_at) AS processed_at
  FROM organisation_application_links l
  JOIN `0-ai-trust`.gold.fact_application_current a ON a.application_id = l.application_id
  GROUP BY l.organisation_id
), organisation_arrangements AS (
  SELECT
    organisation_id,
    count_if(is_active) AS active_arrangement_count,
    count_if(is_active AND arrangement_type = 'BANK_ACCOUNT') AS active_account_count,
    count_if(is_active AND arrangement_type = 'LOAN') AS active_loan_count,
    count_if(is_active AND arrangement_type = 'MORTGAGE') AS active_mortgage_count,
    count_if(is_active AND arrangement_type = 'CREDIT_CARD') AS active_credit_card_count,
    count_if(arrangement_type = 'MORTGAGE') > 0 AS has_inferred_mortgage_status,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_arrangement_current
  WHERE organisation_id IS NOT NULL
  GROUP BY organisation_id
), organisation_verification AS (
  SELECT
    organisation_id,
    max_by(verification_status, processed_at) AS verification_status,
    max_by(review_overdue_flag, processed_at) AS review_overdue_flag,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.fact_verification_current
  WHERE organisation_id IS NOT NULL
  GROUP BY organisation_id
), organisation_roles AS (
  SELECT
    organisation_id,
    count_if(is_active AND party_role = 'AUTHORISED_REPRESENTATIVE') AS active_authorised_representative_count,
    count_if(is_active AND party_role = 'DIRECTOR') AS active_director_count,
    max(processed_at) AS processed_at
  FROM `0-ai-trust`.gold.bridge_organisation_party_role
  GROUP BY organisation_id
), organisation_context AS (
  SELECT
    o.organisation_id AS entity_id,
    'ORGANISATION' AS entity_type,
    CAST(NULL AS STRING) AS customer_id,
    o.organisation_id,
    CAST(NULL AS STRING) AS preferred_contact_channel,
    CAST(NULL AS STRING) AS preferred_language,
    CAST(NULL AS BOOLEAN) AS marketing_opt_in,
    CAST(NULL AS STRING) AS satisfaction_score_current,
    sc.last_support_contact_at,
    CAST(NULL AS STRING) AS last_support_channel,
    CAST(0 AS BIGINT) AS recent_support_interaction_count_30d,
    ct.top_case_type_90d,
    COALESCE(sc.open_case_count, 0) AS open_case_count,
    COALESCE(sc.sla_breached_case_count, 0) AS sla_breached_case_count,
    COALESCE(a.open_application_count, 0) AS open_application_count,
    a.latest_application_id,
    a.latest_application_status,
    a.latest_application_stage,
    a.latest_pending_action_party,
    a.latest_assigned_team,
    COALESCE(a.missing_document_count, 0) AS missing_document_count,
    COALESCE(a.rejected_document_count, 0) AS rejected_document_count,
    COALESCE(a.expired_document_count, 0) AS expired_document_count,
    COALESCE(a.total_reminders_sent, 0) AS total_reminders_sent,
    COALESCE(ar.active_arrangement_count, 0) AS active_arrangement_count,
    COALESCE(ar.active_account_count, 0) AS active_account_count,
    COALESCE(ar.active_loan_count, 0) AS active_loan_count,
    COALESCE(ar.active_mortgage_count, 0) AS active_mortgage_count,
    COALESCE(ar.active_credit_card_count, 0) AS active_credit_card_count,
    v.verification_status,
    v.review_overdue_flag AS verification_review_overdue_flag,
    COALESCE(r.active_authorised_representative_count, 0) AS active_authorised_representative_count,
    COALESCE(r.active_director_count, 0) AS active_director_count,
    filter(array(
      'ORGANISATION_CONTACT_PREFERENCE_UNAVAILABLE',
      CASE WHEN a.latest_application_id IS NOT NULL THEN 'APPLICATION_AUTHORITY_DERIVED' END,
      CASE WHEN a.latest_application_id IS NOT NULL THEN 'ACTION_POLICY_NOT_CONFIGURED' END,
      CASE WHEN v.verification_status IS NULL THEN 'VERIFICATION_CONTEXT_UNAVAILABLE' END,
      CASE WHEN ar.has_inferred_mortgage_status THEN 'MORTGAGE_STATUS_INFERRED' END,
      CASE WHEN greatest(o.processed_at, sc.processed_at, a.processed_at, ar.processed_at, v.processed_at, r.processed_at)
        < current_timestamp() - INTERVAL 24 HOURS THEN 'STALE_CONTEXT' END
    ), x -> x IS NOT NULL) AS warning_codes,
    greatest(o.processed_at, sc.processed_at, a.processed_at, ar.processed_at, v.processed_at, r.processed_at)
      AS source_max_processed_at
  FROM `0-ai-trust`.gold.dim_organisation o
  LEFT JOIN organisation_case_types ct ON ct.organisation_id = o.organisation_id
  LEFT JOIN organisation_cases sc ON sc.organisation_id = o.organisation_id
  LEFT JOIN organisation_applications a ON a.organisation_id = o.organisation_id
  LEFT JOIN organisation_arrangements ar ON ar.organisation_id = o.organisation_id
  LEFT JOIN organisation_verification v ON v.organisation_id = o.organisation_id
  LEFT JOIN organisation_roles r ON r.organisation_id = o.organisation_id
), all_context AS (
  SELECT * FROM customer_context
  UNION ALL
  SELECT * FROM organisation_context
)
SELECT
  sha2(concat_ws('|', entity_type, entity_id, CAST(source_max_processed_at AS STRING)), 256) AS snapshot_id,
  entity_id,
  entity_type,
  customer_id,
  organisation_id,
  source_max_processed_at AS snapshot_timestamp,
  preferred_contact_channel,
  preferred_language,
  marketing_opt_in,
  satisfaction_score_current,
  last_support_contact_at,
  last_support_channel,
  recent_support_interaction_count_30d,
  top_case_type_90d,
  open_case_count,
  sla_breached_case_count,
  open_application_count,
  latest_application_id,
  latest_application_status,
  latest_application_stage,
  latest_pending_action_party,
  latest_assigned_team,
  missing_document_count,
  rejected_document_count,
  expired_document_count,
  total_reminders_sent,
  active_arrangement_count,
  active_account_count,
  active_loan_count,
  active_mortgage_count,
  active_credit_card_count,
  verification_status,
  verification_review_overdue_flag,
  active_authorised_representative_count,
  active_director_count,
  warning_codes,
  source_max_processed_at
FROM all_context;
