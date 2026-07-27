CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_banker_kyb_status AS
WITH document AS (
  SELECT global_id,
    COUNT_IF(is_invalid_or_expired AND UPPER(document_type) RLIKE 'ABN|ASIC|BUSINESS|IDENTITY') > 0 AS has_expired_documents,
    CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(CASE
      WHEN is_invalid_or_expired AND UPPER(document_type) RLIKE 'ABN|ASIC|BUSINESS|IDENTITY' THEN document_type
    END))) AS expired_document_types
  FROM `0-ai-trust`.gold.dim_document
  GROUP BY global_id
)
SELECT
  k.kyc_key,
  k.global_id,
  k.organisation_id,
  k.record_type,
  k.verification_status,
  k.verification_date,
  k.verification_method,
  k.abn_verified,
  k.asic_check_status,
  k.beneficial_ownership_verified,
  k.document_types,
  k.last_review_date,
  k.next_review_date,
  COALESCE(d.has_expired_documents, false) AS has_expired_documents,
  d.expired_document_types,
  'BANKER_ONLY' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  k.pipeline_run_id,
  k.dq_status
FROM `0-ai-trust`.gold.dim_kyc AS k
LEFT JOIN document AS d ON d.global_id = k.global_id
WHERE k.organisation_id IS NOT NULL;
