SET pipelines.trigger.interval=15 minutes;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.bridge_organisation_party_role
COMMENT 'Current person-to-organisation role and authority relationships'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  r.global_id,
  r.relationship_id,
  r.organisation_id,
  c.customer_id,
  r.party_role,
  r.authority_level,
  r.is_active,
  r.start_date,
  r.end_date,
  'PASSED' AS dq_status,
  r.pipeline_run_id,
  r.processed_at
FROM `0-ai-trust`.silver.ip_party_relationship r
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = r.global_id
WHERE r.`__END_AT` IS NULL
  AND r.masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.bridge_organisation_relationship
COMMENT 'Current organisation-to-organisation relationships'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Internal')
AS
SELECT
  relationship_id,
  source_org_id AS source_organisation_id,
  target_org_id AS target_organisation_id,
  relationship_type,
  is_active,
  start_date,
  'PASSED' AS dq_status,
  pipeline_run_id,
  processed_at
FROM `0-ai-trust`.silver.ip_org_relationship
WHERE `__END_AT` IS NULL
  AND masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.bridge_application_party_authority
COMMENT 'Application authority derived from organisation-level roles; not proof of an application-specific mandate'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  a.application_id,
  a.organisation_id,
  r.customer_id,
  r.party_role,
  max_by(r.authority_level, r.start_date) AS authority_level,
  bool_or(r.is_active) AS is_active,
  'ORGANISATION_PARTY_ROLE_LOOKUP' AS derivation_method,
  'APPLICATION_SPECIFIC_MANDATE_UNAVAILABLE' AS limitation_code
FROM `0-ai-trust`.silver.app_application a
JOIN `0-ai-trust`.gold.bridge_organisation_party_role r
  ON r.organisation_id = a.organisation_id
WHERE a.`__END_AT` IS NULL
  AND a.organisation_id IS NOT NULL
  AND a.masking_status IN ('MASKED', 'CLEAN')
GROUP BY a.application_id, a.organisation_id, r.customer_id, r.party_role;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_verification_current
COMMENT 'Current KYC or KYB verification state; restricted compliance fields require separate grants'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Highly Confidential')
AS
SELECT
  k.kyc_id,
  c.customer_id,
  k.organisation_id,
  k.record_type,
  k.verification_status,
  k.verification_method,
  k.verification_date,
  k.last_review_date,
  k.next_review_date,
  k.next_review_date < current_date() AS review_overdue_flag,
  k.document_types,
  k.abn_verified,
  k.asic_check_status,
  k.beneficial_ownership_verified,
  k.risk_rating,
  k.pep_status,
  k.sanctions_check,
  'PASSED' AS dq_status,
  k.masking_status,
  k.processed_at
FROM `0-ai-trust`.silver.ip_kyc k
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = k.global_id
WHERE k.`__END_AT` IS NULL
  AND k.masking_status IN ('MASKED', 'CLEAN');
