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

-- All 24 physical/optional FK relationships declared by the Gold model have
-- no orphan child records. Nullable optional keys are excluded when absent.
SELECT assert_true(COUNT(*) = 0, 'Orphan Gold foreign key')
FROM (
  SELECT 'application_customer' relation_name, a.application_id record_id FROM `0-ai-trust`.gold.fact_application_current a LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = a.customer_id WHERE a.customer_id IS NOT NULL
  UNION ALL SELECT 'document_application', d.document_id FROM `0-ai-trust`.gold.fact_application_document d LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current p ON p.application_id = d.application_id WHERE d.application_id IS NOT NULL
  UNION ALL SELECT 'document_customer', d.document_id FROM `0-ai-trust`.gold.fact_application_document d LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = d.customer_id WHERE d.customer_id IS NOT NULL
  UNION ALL SELECT 'timeline_application', e.timeline_event_id FROM `0-ai-trust`.gold.fact_application_timeline_event e LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current p ON p.application_id = e.application_id WHERE e.application_id IS NOT NULL
  UNION ALL SELECT 'timeline_customer', e.timeline_event_id FROM `0-ai-trust`.gold.fact_application_timeline_event e LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = e.customer_id WHERE e.customer_id IS NOT NULL
  UNION ALL SELECT 'arrangement_customer', a.arrangement_id FROM `0-ai-trust`.gold.fact_arrangement_current a LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = a.customer_id WHERE a.customer_id IS NOT NULL
  UNION ALL SELECT 'arrangement_organisation', a.arrangement_id FROM `0-ai-trust`.gold.fact_arrangement_current a LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = a.organisation_id WHERE a.organisation_id IS NOT NULL
  UNION ALL SELECT 'party_role_organisation', r.relationship_id FROM `0-ai-trust`.gold.bridge_organisation_party_role r LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = r.organisation_id WHERE r.organisation_id IS NOT NULL
  UNION ALL SELECT 'party_role_customer', r.relationship_id FROM `0-ai-trust`.gold.bridge_organisation_party_role r LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = r.customer_id WHERE r.customer_id IS NOT NULL
  UNION ALL SELECT 'organisation_relationship_source', r.relationship_id FROM `0-ai-trust`.gold.bridge_organisation_relationship r LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = r.source_organisation_id WHERE r.source_organisation_id IS NOT NULL
  UNION ALL SELECT 'organisation_relationship_target', r.relationship_id FROM `0-ai-trust`.gold.bridge_organisation_relationship r LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = r.target_organisation_id WHERE r.target_organisation_id IS NOT NULL
  UNION ALL SELECT 'authority_application', b.application_id FROM `0-ai-trust`.gold.bridge_application_party_authority b LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current p ON p.application_id = b.application_id WHERE b.application_id IS NOT NULL
  UNION ALL SELECT 'authority_organisation', b.application_id FROM `0-ai-trust`.gold.bridge_application_party_authority b LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = b.organisation_id WHERE b.organisation_id IS NOT NULL
  UNION ALL SELECT 'authority_customer', b.application_id FROM `0-ai-trust`.gold.bridge_application_party_authority b LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = b.customer_id WHERE b.customer_id IS NOT NULL
  UNION ALL SELECT 'verification_customer', v.kyc_id FROM `0-ai-trust`.gold.fact_verification_current v LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = v.customer_id WHERE v.customer_id IS NOT NULL
  UNION ALL SELECT 'verification_organisation', v.kyc_id FROM `0-ai-trust`.gold.fact_verification_current v LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = v.organisation_id WHERE v.organisation_id IS NOT NULL
  UNION ALL SELECT 'service_case_customer', c.case_id FROM `0-ai-trust`.gold.fact_service_case_current c LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = c.customer_id WHERE c.customer_id IS NOT NULL
  UNION ALL SELECT 'service_case_organisation', c.case_id FROM `0-ai-trust`.gold.fact_service_case_current c LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = c.organisation_id WHERE c.organisation_id IS NOT NULL
  UNION ALL SELECT 'service_case_application', c.case_id FROM `0-ai-trust`.gold.fact_service_case_current c LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current p ON p.application_id = c.application_id WHERE c.application_id IS NOT NULL
  UNION ALL SELECT 'service_activity_customer', a.service_activity_id FROM `0-ai-trust`.gold.fact_service_activity a LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = a.customer_id WHERE a.customer_id IS NOT NULL
  UNION ALL SELECT 'service_activity_case', a.service_activity_id FROM `0-ai-trust`.gold.fact_service_activity a LEFT ANTI JOIN `0-ai-trust`.gold.fact_service_case_current p ON p.case_id = a.case_id WHERE a.case_id IS NOT NULL
  UNION ALL SELECT 'service_activity_application', a.service_activity_id FROM `0-ai-trust`.gold.fact_service_activity a LEFT ANTI JOIN `0-ai-trust`.gold.fact_application_current p ON p.application_id = a.application_id WHERE a.application_id IS NOT NULL
  UNION ALL SELECT 'context_customer', s.snapshot_id FROM `0-ai-trust`.gold.fact_subject_context_snapshot s LEFT ANTI JOIN `0-ai-trust`.gold.dim_customer p ON p.customer_id = s.customer_id WHERE s.customer_id IS NOT NULL
  UNION ALL SELECT 'context_organisation', s.snapshot_id FROM `0-ai-trust`.gold.fact_subject_context_snapshot s LEFT ANTI JOIN `0-ai-trust`.gold.dim_organisation p ON p.organisation_id = s.organisation_id WHERE s.organisation_id IS NOT NULL
) orphans;

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
