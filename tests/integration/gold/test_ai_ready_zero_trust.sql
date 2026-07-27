-- Curated outputs must carry traceability and trust metadata.
SELECT assert_true(COUNT(*) = 0, 'AI-ready customer context lacks trust metadata')
FROM `0-ai-trust`.gold.aiv_banker_customer_360
WHERE dq_status IS NULL
   OR pipeline_run_id IS NULL
   OR last_refreshed_at IS NULL
   OR context_version IS NULL
   OR usage_restriction <> 'BANKER_ONLY';

SELECT assert_true(COUNT(*) = 0, 'Customer-safe context lacks trust metadata')
FROM `0-ai-trust`.gold.aiv_customer_self_profile
WHERE dq_status IS NULL
   OR pipeline_run_id IS NULL
   OR last_refreshed_at IS NULL
   OR context_version IS NULL
   OR usage_restriction <> 'CUSTOMER_SAFE';

-- Customer-safe and shared views must not define raw PII/financial-risk columns.
SELECT assert_true(COUNT(*) = 0, 'Forbidden raw field exists in a customer-safe view')
FROM `0-ai-trust`.information_schema.columns
WHERE table_schema = 'gold'
  AND table_name IN (
    'aiv_customer_self_profile',
    'aiv_shared_application_status',
    'aiv_shared_application_timeline',
    'aiv_shared_document_checklist',
    'aiv_shared_service_summary'
  )
  AND LOWER(column_name) IN (
    'email', 'phone_number', 'tfn', 'card_number', 'credit_score',
    'risk_rating', 'pep_status', 'sanctions_status', 'current_balance'
  );

-- Restricted fields dynamically disappear outside the approved banker group.
SELECT assert_true(
  is_account_group_member('banker-assist-users') OR COUNT_IF(linked_application_id IS NOT NULL) = 0,
  'Banker-only application linkage leaked to an unapproved principal'
)
FROM `0-ai-trust`.gold.aiv_shared_service_summary;
