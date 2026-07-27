-- Run after Silver is deployed. These assertions define the hand-off expected by Gold.

-- Invalid terminal values must be quarantined rather than curated.
SELECT assert_true(COUNT(*) = 0, 'Invalid application status reached curated Silver')
FROM `0-ai-trust`.silver.app_application
WHERE final_outcome IS NOT NULL
  AND UPPER(final_outcome) NOT IN ('ACCEPTED', 'CANCELLED', 'DENIED', 'REJECTED', 'WITHDRAWN');

SELECT assert_true(COUNT(*) = 0, 'Invalid service-case status reached curated Silver')
FROM `0-ai-trust`.silver.evt_service_case
WHERE LOWER(status) NOT IN ('open', 'in_progress', 'resolved', 'closed');

-- History may arrive out of order, but its business sequence must remain valid.
SELECT assert_true(COUNT(*) = 0, 'Application has multiple current stages')
FROM (
  SELECT application_id
  FROM `0-ai-trust`.silver.app_application_stage_history
  WHERE is_current_stage
  GROUP BY application_id
  HAVING COUNT(*) > 1
);

SELECT assert_true(COUNT(*) = 0, 'Invalid application status transition survived Silver')
FROM `0-ai-trust`.silver.app_status_change_history
WHERE CONCAT(UPPER(old_status), '->', UPPER(new_status)) NOT IN (
  'ASSESSMENT->CANCELLED',
  'ASSESSMENT->CONDITIONALLY_APPROVED',
  'ASSESSMENT->CREDIT_CHECK',
  'ASSESSMENT->WITHDRAWN',
  'CONDITIONALLY_APPROVED->DOCUMENT_VERIFICATION',
  'CREDIT_CHECK->APPROVED',
  'CREDIT_CHECK->DENIED',
  'DOCUMENT_VERIFICATION->ASSESSMENT',
  'SUBMITTED->DOCUMENT_VERIFICATION'
);

-- Silver schema must expose only masked/tokenized identity attributes.
SELECT assert_true(COUNT(*) = 0, 'Raw PII column exists in curated Silver')
FROM `0-ai-trust`.information_schema.columns
WHERE table_schema = 'silver'
  AND table_name IN ('ip_individual', 'ip_organisation')
  AND LOWER(column_name) IN (
    'first_name', 'last_name', 'email', 'phone_number', 'tfn', 'abn', 'acn'
  );

-- The provided invalid-data fixture must exercise several independent failure modes.
SELECT assert_true(
  COUNT(DISTINCT rule_id) >= 4,
  'Invalid-data fixture did not exercise enough quarantine rules'
)
FROM `0-ai-trust`.silver.record_quarantine;

-- Warning records remain usable but must be explicitly labelled.
SELECT assert_true(
  COUNT_IF(dq_status = 'WARNING') > 0,
  'Freshness or non-critical warning fixture was not retained'
)
FROM `0-ai-trust`.silver.app_application;
