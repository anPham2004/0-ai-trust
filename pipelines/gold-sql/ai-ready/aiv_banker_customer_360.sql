CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_banker_customer_360 AS
WITH arrangement AS (
  SELECT global_id,
    COUNT_IF(is_active) AS active_arrangement_count,
    CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(arrangement_type))) AS arrangement_types,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.fact_arrangement_snapshot
  WHERE organisation_id IS NULL
  GROUP BY global_id
), application AS (
  SELECT a.global_id,
    MAX_BY(a.application_id, a.submitted_at) AS latest_application_id,
    MAX_BY(a.final_outcome, a.submitted_at) AS latest_application_outcome,
    MAX(a.dq_status) AS dq_status
  FROM `0-ai-trust`.gold.dim_application AS a
  WHERE a.organisation_id IS NULL
  GROUP BY a.global_id
), current_stage AS (
  SELECT global_id,
    MAX_BY(application_id, event_timestamp) AS application_id,
    MAX_BY(stage_name, event_timestamp) AS current_stage,
    MAX_BY(entered_at, event_timestamp) AS current_stage_entered_at,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.fact_application_stage
  WHERE is_current_stage
  GROUP BY global_id
), service AS (
  SELECT global_id,
    COUNT_IF(record_type = 'SERVICE_CASE' AND UPPER(case_status) IN ('OPEN', 'IN_PROGRESS')) AS open_service_case_count,
    MAX_BY(channel, interaction_timestamp) AS last_support_channel,
    MAX(interaction_timestamp) AS last_support_timestamp,
    MAX_BY(topic, interaction_timestamp) AS last_support_topic,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.fact_service_interaction
  GROUP BY global_id
), document AS (
  SELECT global_id,
    COUNT_IF(UPPER(status) = 'PENDING') AS pending_document_count,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.dim_document
  GROUP BY global_id
), kyc AS (
  SELECT * EXCEPT (_rank)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY global_id ORDER BY next_review_date DESC, processed_at DESC) AS _rank
    FROM `0-ai-trust`.gold.dim_kyc
    WHERE organisation_id IS NULL
  ) WHERE _rank = 1
)
SELECT
  p.global_id,
  p.customer_id,
  p.customer_type,
  p.name_token,
  p.preferred_contact_channel,
  p.preferred_language,
  p.satisfaction_score,
  p.marketing_opt_in,
  k.verification_status AS kyc_status,
  COALESCE(ar.active_arrangement_count, 0) AS active_arrangement_count,
  ar.arrangement_types,
  COALESCE(ap.latest_application_id, cs.application_id) AS latest_application_id,
  ap.latest_application_outcome,
  cs.current_stage,
  cs.current_stage_entered_at,
  COALESCE(s.open_service_case_count, 0) AS open_service_case_count,
  s.last_support_channel,
  s.last_support_timestamp,
  s.last_support_topic,
  COALESCE(d.pending_document_count, 0) AS pending_document_count,
  p.organisation_id,
  p.business_name AS organisation_name,
  'BANKER_ONLY' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  p.pipeline_run_id,
  CASE WHEN p.dq_status = 'WARNING'
         OR k.dq_status = 'WARNING'
         OR ar.dq_status = 'WARNING'
         OR ap.dq_status = 'WARNING'
         OR cs.dq_status = 'WARNING'
         OR s.dq_status = 'WARNING'
         OR d.dq_status = 'WARNING'
    THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
  CASE
    WHEN k.kyc_key IS NULL THEN 'KYC context is unavailable'
    WHEN k.next_review_date < current_date() THEN 'KYC review is overdue'
    WHEN p.dq_status = 'WARNING'
      OR k.dq_status = 'WARNING'
      OR ar.dq_status = 'WARNING'
      OR ap.dq_status = 'WARNING'
      OR cs.dq_status = 'WARNING'
      OR s.dq_status = 'WARNING'
      OR d.dq_status = 'WARNING'
      THEN 'One or more contributing records contain non-critical quality warnings'
    ELSE CAST(NULL AS STRING)
  END AS known_limitations
FROM `0-ai-trust`.gold.dim_party AS p
LEFT JOIN kyc AS k ON k.global_id = p.global_id
LEFT JOIN arrangement AS ar ON ar.global_id = p.global_id
LEFT JOIN application AS ap ON ap.global_id = p.global_id
LEFT JOIN current_stage AS cs ON cs.global_id = p.global_id
LEFT JOIN service AS s ON s.global_id = p.global_id
LEFT JOIN document AS d ON d.global_id = p.global_id
WHERE p.organisation_id IS NULL;
