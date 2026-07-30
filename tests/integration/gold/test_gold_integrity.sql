-- Natural keys remain unique after every declarative refresh.
SELECT assert_true(COUNT(*) = 0, 'Duplicate Gold natural keys')
FROM (
  SELECT customer_id FROM `0-ai-trust`.gold.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
  UNION ALL SELECT organisation_id FROM `0-ai-trust`.gold.dim_organisation GROUP BY organisation_id HAVING COUNT(*) > 1
  UNION ALL SELECT CAST(date_key AS STRING) FROM `0-ai-trust`.gold.dim_date GROUP BY date_key HAVING COUNT(*) > 1
  UNION ALL SELECT CONCAT(stage, ':', pending_action_party, ':', effective_from) FROM `0-ai-trust`.gold.dim_stage_action_policy GROUP BY stage, pending_action_party, effective_from HAVING COUNT(*) > 1
  UNION ALL SELECT service_activity_id FROM `0-ai-trust`.gold.fact_service_activity GROUP BY service_activity_id HAVING COUNT(*) > 1
  UNION ALL SELECT case_id FROM `0-ai-trust`.gold.fact_service_case_current GROUP BY case_id HAVING COUNT(*) > 1
  UNION ALL SELECT CONCAT(arrangement_type, ':', arrangement_id) FROM `0-ai-trust`.gold.fact_arrangement_current GROUP BY arrangement_type, arrangement_id HAVING COUNT(*) > 1
  UNION ALL SELECT relationship_id FROM `0-ai-trust`.gold.bridge_organisation_party_role GROUP BY relationship_id HAVING COUNT(*) > 1
  UNION ALL SELECT relationship_id FROM `0-ai-trust`.gold.bridge_organisation_relationship GROUP BY relationship_id HAVING COUNT(*) > 1
  UNION ALL SELECT CONCAT(application_id, ':', organisation_id, ':', customer_id, ':', party_role) FROM `0-ai-trust`.gold.bridge_application_party_authority GROUP BY application_id, organisation_id, customer_id, party_role HAVING COUNT(*) > 1
  UNION ALL SELECT kyc_id FROM `0-ai-trust`.gold.fact_verification_current GROUP BY kyc_id HAVING COUNT(*) > 1
  UNION ALL SELECT application_id FROM `0-ai-trust`.gold.fact_application_current GROUP BY application_id HAVING COUNT(*) > 1
  UNION ALL SELECT document_id FROM `0-ai-trust`.gold.fact_application_document GROUP BY document_id HAVING COUNT(*) > 1
  UNION ALL SELECT timeline_event_id FROM `0-ai-trust`.gold.fact_application_timeline_event GROUP BY timeline_event_id HAVING COUNT(*) > 1
  UNION ALL SELECT snapshot_id FROM `0-ai-trust`.gold.fact_subject_context_snapshot GROUP BY snapshot_id HAVING COUNT(*) > 1
) duplicates;

-- Informational Unity Catalog primary keys are not enforced, so nullability is
-- verified explicitly before the optimizer or semantic consumers rely on them.
SELECT assert_true(COUNT(*) = 0, 'Null Gold primary key component')
FROM (
  SELECT customer_id AS key_value FROM `0-ai-trust`.gold.dim_customer WHERE customer_id IS NULL
  UNION ALL SELECT organisation_id FROM `0-ai-trust`.gold.dim_organisation WHERE organisation_id IS NULL
  UNION ALL SELECT CAST(date_key AS STRING) FROM `0-ai-trust`.gold.dim_date WHERE date_key IS NULL
  UNION ALL SELECT stage FROM `0-ai-trust`.gold.dim_stage_action_policy WHERE stage IS NULL OR pending_action_party IS NULL OR effective_from IS NULL
  UNION ALL SELECT application_id FROM `0-ai-trust`.gold.fact_application_current WHERE application_id IS NULL
  UNION ALL SELECT document_id FROM `0-ai-trust`.gold.fact_application_document WHERE document_id IS NULL
  UNION ALL SELECT timeline_event_id FROM `0-ai-trust`.gold.fact_application_timeline_event WHERE timeline_event_id IS NULL
  UNION ALL SELECT arrangement_id FROM `0-ai-trust`.gold.fact_arrangement_current WHERE arrangement_id IS NULL OR arrangement_type IS NULL
  UNION ALL SELECT relationship_id FROM `0-ai-trust`.gold.bridge_organisation_party_role WHERE relationship_id IS NULL
  UNION ALL SELECT relationship_id FROM `0-ai-trust`.gold.bridge_organisation_relationship WHERE relationship_id IS NULL
  UNION ALL SELECT application_id FROM `0-ai-trust`.gold.bridge_application_party_authority WHERE application_id IS NULL OR organisation_id IS NULL OR customer_id IS NULL OR party_role IS NULL
  UNION ALL SELECT kyc_id FROM `0-ai-trust`.gold.fact_verification_current WHERE kyc_id IS NULL
  UNION ALL SELECT case_id FROM `0-ai-trust`.gold.fact_service_case_current WHERE case_id IS NULL
  UNION ALL SELECT service_activity_id FROM `0-ai-trust`.gold.fact_service_activity WHERE service_activity_id IS NULL
  UNION ALL SELECT snapshot_id FROM `0-ai-trust`.gold.fact_subject_context_snapshot WHERE snapshot_id IS NULL
) null_keys;

-- Canonical Gold cannot contain records rejected before Silver publication.
SELECT assert_true(COUNT(*) = 0, 'Hard-failed records leaked into Gold')
FROM (
  SELECT dq_status FROM `0-ai-trust`.gold.dim_customer
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.dim_organisation
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_service_activity
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_service_case_current
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_arrangement_current
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.bridge_organisation_party_role
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.bridge_organisation_relationship
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_verification_current
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_application_current
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_application_document
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_application_timeline_event
) records
WHERE dq_status = 'FAILED';

-- Required AI-facing joins have no orphan parents.
SELECT assert_true(COUNT(*) = 0, 'Orphan application customer')
FROM `0-ai-trust`.gold.fact_application_current a
LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer c ON c.customer_id = a.customer_id
WHERE a.customer_id IS NOT NULL;

SELECT assert_true(COUNT(*) = 0, 'Orphan application document')
FROM `0-ai-trust`.gold.fact_application_document d
LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current a ON a.application_id = d.application_id;

SELECT assert_true(COUNT(*) = 0, 'Orphan application timeline event')
FROM `0-ai-trust`.gold.fact_application_timeline_event e
LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current a ON a.application_id = e.application_id;

SELECT assert_true(COUNT(*) = 0, 'Orphan organisation-party bridge')
FROM `0-ai-trust`.gold.bridge_organisation_party_role r
LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation o ON o.organisation_id = r.organisation_id;

-- Reconciliation allows the declared 15-minute Gold refresh lag, while still
-- detecting any eligible Silver record that remains missing after 30 minutes.
SELECT assert_true(COUNT(*) = 0, 'Application reconciliation failed')
FROM `0-ai-trust`.silver.app_application s
LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current g
  ON g.application_id = s.application_id
WHERE s.`__END_AT` IS NULL
  AND s.masking_status IN ('MASKED', 'CLEAN')
  AND s.processed_at < current_timestamp() - INTERVAL 30 MINUTES;

SELECT assert_true(COUNT(*) = 0, 'Document reconciliation failed')
FROM `0-ai-trust`.silver.app_missing_document s
LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_document g
  ON g.document_id = s.document_id
WHERE s.`__END_AT` IS NULL
  AND s.masking_status IN ('MASKED', 'CLEAN')
  AND s.processed_at < current_timestamp() - INTERVAL 30 MINUTES;
