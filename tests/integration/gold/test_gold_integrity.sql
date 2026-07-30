-- Natural keys remain unique after every declarative refresh.
SELECT assert_true(COUNT(*) = 0, 'Duplicate Gold natural keys')
FROM (
  SELECT customer_id FROM `0-ai-trust`.gold.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
  UNION ALL SELECT organisation_id FROM `0-ai-trust`.gold.dim_organisation GROUP BY organisation_id HAVING COUNT(*) > 1
  UNION ALL SELECT CAST(date_key AS STRING) FROM `0-ai-trust`.gold.dim_date GROUP BY date_key HAVING COUNT(*) > 1
  UNION ALL SELECT service_activity_id FROM `0-ai-trust`.gold.fact_service_activity GROUP BY service_activity_id HAVING COUNT(*) > 1
  UNION ALL SELECT case_id FROM `0-ai-trust`.gold.fact_service_case_current GROUP BY case_id HAVING COUNT(*) > 1
  UNION ALL SELECT CONCAT(arrangement_type, ':', arrangement_id) FROM `0-ai-trust`.gold.fact_arrangement_current GROUP BY arrangement_type, arrangement_id HAVING COUNT(*) > 1
  UNION ALL SELECT relationship_id FROM `0-ai-trust`.gold.bridge_organisation_party_role GROUP BY relationship_id HAVING COUNT(*) > 1
  UNION ALL SELECT relationship_id FROM `0-ai-trust`.gold.bridge_organisation_relationship GROUP BY relationship_id HAVING COUNT(*) > 1
  UNION ALL SELECT kyc_id FROM `0-ai-trust`.gold.fact_verification_current GROUP BY kyc_id HAVING COUNT(*) > 1
  UNION ALL SELECT application_id FROM `0-ai-trust`.gold.fact_application_current GROUP BY application_id HAVING COUNT(*) > 1
  UNION ALL SELECT document_id FROM `0-ai-trust`.gold.fact_application_document GROUP BY document_id HAVING COUNT(*) > 1
  UNION ALL SELECT timeline_event_id FROM `0-ai-trust`.gold.fact_application_timeline_event GROUP BY timeline_event_id HAVING COUNT(*) > 1
  UNION ALL SELECT snapshot_id FROM `0-ai-trust`.gold.fact_subject_context_snapshot GROUP BY snapshot_id HAVING COUNT(*) > 1
) duplicates;

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

-- Reconciliation catches loss or duplication of current Silver records.
SELECT assert_true(
  (SELECT COUNT(*) FROM `0-ai-trust`.gold.fact_application_current) =
  (SELECT COUNT(*) FROM `0-ai-trust`.silver.app_application
   WHERE `__END_AT` IS NULL AND masking_status IN ('MASKED', 'CLEAN')),
  'Application reconciliation failed'
);

SELECT assert_true(
  (SELECT COUNT(*) FROM `0-ai-trust`.gold.fact_application_document) =
  (SELECT COUNT(*) FROM `0-ai-trust`.silver.app_missing_document
   WHERE `__END_AT` IS NULL AND masking_status IN ('MASKED', 'CLEAN')),
  'Document reconciliation failed'
);
