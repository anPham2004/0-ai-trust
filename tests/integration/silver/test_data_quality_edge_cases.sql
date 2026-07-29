-- Run after Silver is deployed. These assertions define the hand-off expected by Gold.

-- Invalid terminal values are dropped from canonical Silver and counted by
-- native pipeline expectation metrics.
SELECT assert_true(COUNT(*) = 0, 'Invalid application status reached canonical Silver')
FROM `0-ai-trust`.silver.app_application
WHERE final_outcome IS NOT NULL
  AND UPPER(final_outcome) NOT IN (
    'ACCEPTED', 'APPROVED', 'CANCELLED', 'DENIED', 'PENDING', 'REJECTED', 'WITHDRAWN'
  );

SELECT assert_true(COUNT(*) = 0, 'Invalid service-case status reached canonical Silver')
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

-- At-least-once event delivery and repeated file drops must remain idempotent.
SELECT assert_true(COUNT(*) = 0, 'Duplicate immutable records reached Silver')
FROM (
  SELECT history_id AS record_id FROM `0-ai-trust`.silver.app_application_stage_history GROUP BY history_id HAVING COUNT(*) > 1
  UNION ALL
  SELECT change_id FROM `0-ai-trust`.silver.app_status_change_history GROUP BY change_id HAVING COUNT(*) > 1
  UNION ALL
  SELECT event_id FROM `0-ai-trust`.silver.app_application_event GROUP BY event_id HAVING COUNT(*) > 1
  UNION ALL
  SELECT loan_id FROM `0-ai-trust`.silver.app_accepted_loan GROUP BY loan_id HAVING COUNT(*) > 1
  UNION ALL
  SELECT loan_id FROM `0-ai-trust`.silver.app_rejected_application GROUP BY loan_id HAVING COUNT(*) > 1
  UNION ALL
  SELECT interaction_id FROM `0-ai-trust`.silver.evt_support_interaction GROUP BY interaction_id HAVING COUNT(*) > 1
  UNION ALL
  SELECT event_id FROM `0-ai-trust`.silver.evt_service_case_event GROUP BY event_id HAVING COUNT(*) > 1
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
SELECT assert_true(COUNT(*) = 0, 'Raw PII column exists in canonical Silver')
FROM `0-ai-trust`.information_schema.columns
WHERE table_schema = 'silver'
  AND table_name IN ('ip_individual', 'ip_organisation')
  AND LOWER(column_name) IN (
    'first_name', 'last_name', 'email', 'phone_number', 'tfn', 'abn', 'acn'
  );
