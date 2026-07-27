CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_banker_org_360 AS
WITH arrangement AS (
  SELECT organisation_id,
    COUNT_IF(is_active) AS active_arrangement_count,
    CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(arrangement_type))) AS arrangement_types,
    COUNT_IF(is_active AND arrangement_type = 'LOAN') > 0 AS has_active_loan,
    COUNT_IF(is_active AND arrangement_type = 'CREDIT_CARD') > 0 AS has_active_credit_card,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.fact_arrangement_snapshot
  WHERE organisation_id IS NOT NULL
  GROUP BY organisation_id
), application AS (
  SELECT organisation_id,
    COUNT_IF(UPPER(COALESCE(final_outcome, 'PENDING')) NOT IN ('ACCEPTED', 'REJECTED', 'CANCELLED', 'WITHDRAWN')) AS open_application_count,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.dim_application
  WHERE organisation_id IS NOT NULL
  GROUP BY organisation_id
), service AS (
  SELECT organisation_id,
    COUNT_IF(record_type = 'SERVICE_CASE' AND UPPER(case_status) IN ('OPEN', 'IN_PROGRESS')) AS open_service_case_count,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.fact_service_interaction
  WHERE organisation_id IS NOT NULL
  GROUP BY organisation_id
), relationship AS (
  SELECT source_org_id AS organisation_id,
    COUNT_IF(is_active) AS related_org_count,
    CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(relationship_type))) AS related_org_types
  FROM `0-ai-trust`.gold.dim_organisation_relationship
  GROUP BY source_org_id
), kyb AS (
  SELECT * EXCEPT (_rank)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY organisation_id ORDER BY next_review_date DESC, processed_at DESC) AS _rank
    FROM `0-ai-trust`.gold.dim_kyc
    WHERE organisation_id IS NOT NULL
  ) WHERE _rank = 1
)
SELECT
  p.organisation_id,
  p.global_id,
  p.business_name,
  p.legal_name,
  p.organisation_type,
  p.industry_code,
  p.registered_country,
  p.is_acnc_registered,
  p.establishment_date,
  p.customer_since,
  p.org_last_updated_at,
  p.abn_masked,
  k.verification_status AS kyb_status,
  k.abn_verified,
  k.asic_check_status,
  k.beneficial_ownership_verified,
  COALESCE(ar.active_arrangement_count, 0) AS active_arrangement_count,
  ar.arrangement_types,
  COALESCE(ar.has_active_loan, false) AS has_active_loan,
  COALESCE(ar.has_active_credit_card, false) AS has_active_credit_card,
  COALESCE(ap.open_application_count, 0) AS open_application_count,
  COALESCE(s.open_service_case_count, 0) AS open_service_case_count,
  COALESCE(r.related_org_count, 0) AS related_org_count,
  r.related_org_types,
  p.preferred_contact_channel,
  'BANKER_ONLY' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  p.pipeline_run_id,
  CASE WHEN p.dq_status = 'WARNING'
         OR k.dq_status = 'WARNING'
         OR ar.dq_status = 'WARNING'
         OR ap.dq_status = 'WARNING'
         OR s.dq_status = 'WARNING'
    THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
  CASE
    WHEN k.kyc_key IS NULL THEN 'KYB context is unavailable'
    WHEN k.next_review_date < current_date() THEN 'KYB review is overdue'
    WHEN p.dq_status = 'WARNING'
      OR k.dq_status = 'WARNING'
      OR ar.dq_status = 'WARNING'
      OR ap.dq_status = 'WARNING'
      OR s.dq_status = 'WARNING'
      THEN 'One or more contributing records contain non-critical quality warnings'
    ELSE CAST(NULL AS STRING)
  END AS known_limitations
FROM `0-ai-trust`.gold.dim_party AS p
LEFT JOIN kyb AS k ON k.organisation_id = p.organisation_id
LEFT JOIN arrangement AS ar ON ar.organisation_id = p.organisation_id
LEFT JOIN application AS ap ON ap.organisation_id = p.organisation_id
LEFT JOIN service AS s ON s.organisation_id = p.organisation_id
LEFT JOIN relationship AS r ON r.organisation_id = p.organisation_id
WHERE p.organisation_id IS NOT NULL;
