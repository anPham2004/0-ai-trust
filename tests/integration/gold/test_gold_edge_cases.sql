-- Business timestamp ordering remains coherent after late or out-of-order arrivals.
SELECT assert_true(COUNT(*) = 0, 'Document receipt precedes request')
FROM `0-ai-trust`.gold.fact_application_document
WHERE received_at < requested_at;

SELECT assert_true(COUNT(*) = 0, 'Arrangement maturity precedes start')
FROM `0-ai-trust`.gold.fact_arrangement_current
WHERE maturity_date < start_date;

-- Every eligible Silver lifecycle record survives the canonical timeline union.
SELECT assert_true(
  (SELECT COUNT(*) FROM `0-ai-trust`.gold.fact_application_timeline_event) =
  (SELECT
     (SELECT COUNT(*) FROM `0-ai-trust`.silver.app_stage_history WHERE masking_status IN ('MASKED', 'CLEAN')) +
     (SELECT COUNT(*) FROM `0-ai-trust`.silver.app_status_change WHERE masking_status IN ('MASKED', 'CLEAN')) +
     (SELECT COUNT(*) FROM `0-ai-trust`.silver.app_lifecycle_event WHERE masking_status IN ('MASKED', 'CLEAN'))),
  'Late or out-of-order application history was lost'
);

-- Current context has exactly one row per subject and explicit limitations.
SELECT assert_true(COUNT(*) = 0, 'Duplicate subject context')
FROM (
  SELECT entity_type, entity_id
  FROM `0-ai-trust`.gold.fact_subject_context_snapshot
  GROUP BY entity_type, entity_id
  HAVING COUNT(*) > 1
);

SELECT assert_true(COUNT(*) = 0, 'Organisation context lacks its known contact-preference limitation')
FROM `0-ai-trust`.gold.fact_subject_context_snapshot
WHERE entity_type = 'ORGANISATION'
  AND NOT array_contains(warning_codes, 'ORGANISATION_CONTACT_PREFERENCE_UNAVAILABLE');

SELECT assert_true(COUNT(*) = 0, 'Unapproved action policy text was invented')
FROM `0-ai-trust`.gold.dim_stage_action_policy
WHERE next_action_code <> 'POLICY_NOT_CONFIGURED'
   OR banker_action_text IS NOT NULL
   OR customer_safe_action_text IS NOT NULL;
