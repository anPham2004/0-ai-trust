-- Every AI-ready context row identifies exactly one subject and includes trust metadata.
SELECT assert_true(COUNT(*) = 0, 'AI-ready context lacks identity or trust metadata')
FROM `0-ai-trust`.gold.fact_subject_context_snapshot
WHERE snapshot_id IS NULL
   OR entity_id IS NULL
   OR entity_type NOT IN ('CUSTOMER', 'ORGANISATION')
   OR snapshot_timestamp IS NULL
   OR warning_codes IS NULL
   OR source_max_processed_at IS NULL
   OR (customer_id IS NULL) = (organisation_id IS NULL);

-- Raw PII and restricted compliance attributes cannot leak into the denormalised AI context.
SELECT assert_true(COUNT(*) = 0, 'Forbidden field exists in AI-ready context')
FROM `0-ai-trust`.information_schema.columns
WHERE table_schema = 'gold'
  AND table_name = 'fact_subject_context_snapshot'
  AND LOWER(column_name) IN (
    'email', 'phone_number', 'full_name', 'tfn', 'card_number',
    'risk_rating', 'pep_status', 'sanctions_check', 'raw_payload'
  );

-- Restricted compliance fields exist only in the governed verification fact.
SELECT assert_true(COUNT(*) = 0, 'Restricted compliance field leaked outside verification fact')
FROM `0-ai-trust`.information_schema.columns
WHERE table_schema = 'gold'
  AND LOWER(column_name) IN ('risk_rating', 'pep_status', 'sanctions_check')
  -- SDP exposes internal MV backing tables in information_schema; they are
  -- implementation storage for the governed public verification fact.
  AND table_name NOT RLIKE '^__materialization_'
  AND table_name <> 'fact_verification_current';
