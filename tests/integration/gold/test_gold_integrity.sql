-- Natural/business keys remain unique after an idempotent rerun.
SELECT assert_true(COUNT(*) = 0, 'Duplicate Gold dimension or fact keys')
FROM (
  SELECT party_key FROM `0-ai-trust`.gold.dim_party GROUP BY party_key HAVING COUNT(*) > 1
  UNION ALL
  SELECT kyc_key FROM `0-ai-trust`.gold.dim_kyc GROUP BY kyc_key HAVING COUNT(*) > 1
  UNION ALL
  SELECT arrangement_key FROM `0-ai-trust`.gold.dim_arrangement GROUP BY arrangement_key HAVING COUNT(*) > 1
  UNION ALL
  SELECT application_key FROM `0-ai-trust`.gold.dim_application GROUP BY application_key HAVING COUNT(*) > 1
  UNION ALL
  SELECT document_key FROM `0-ai-trust`.gold.dim_document GROUP BY document_key HAVING COUNT(*) > 1
  UNION ALL
  SELECT stage_fact_key FROM `0-ai-trust`.gold.fact_application_stage GROUP BY stage_fact_key HAVING COUNT(*) > 1
  UNION ALL
  SELECT service_fact_key FROM `0-ai-trust`.gold.fact_service_interaction GROUP BY service_fact_key HAVING COUNT(*) > 1
  UNION ALL
  SELECT snapshot_key FROM `0-ai-trust`.gold.fact_arrangement_snapshot GROUP BY snapshot_key HAVING COUNT(*) > 1
) duplicates;

-- Gold must not contain hard-failed Silver records.
SELECT assert_true(COUNT(*) = 0, 'Hard-failed records leaked into Gold')
FROM (
  SELECT dq_status FROM `0-ai-trust`.gold.dim_party
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.dim_kyc
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.dim_arrangement
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.dim_application
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.dim_document
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_application_stage
  UNION ALL SELECT dq_status FROM `0-ai-trust`.gold.fact_service_interaction
) records
WHERE dq_status = 'FAILED';

-- Referential integrity for AI-facing joins.
SELECT assert_true(COUNT(*) = 0, 'Orphan application party keys')
FROM `0-ai-trust`.gold.dim_application application
LEFT ANTI JOIN `0-ai-trust`.gold.dim_party party
  ON party.party_key = application.party_key
WHERE application.party_key IS NOT NULL;

SELECT assert_true(COUNT(*) = 0, 'Orphan document application keys')
FROM `0-ai-trust`.gold.dim_document document
LEFT ANTI JOIN `0-ai-trust`.gold.dim_application application
  ON application.application_key = document.application_key;

SELECT assert_true(COUNT(*) = 0, 'Orphan stage application keys')
FROM `0-ai-trust`.gold.fact_application_stage stage
LEFT ANTI JOIN `0-ai-trust`.gold.dim_application application
  ON application.application_key = stage.application_key;

-- Reconciliation catches dropped, duplicated, or filtered eligible applications.
SELECT assert_true(
  (SELECT COUNT(*) FROM `0-ai-trust`.gold.dim_application) =
  (SELECT COUNT(DISTINCT application_id)
   FROM `0-ai-trust`.silver.app_application
   WHERE dq_status IN ('PASSED', 'WARNING')
     AND masking_status IN ('MASKED', 'CLEAN')),
  'Application reconciliation failed'
);

SELECT assert_true(
  (SELECT COUNT(*) FROM `0-ai-trust`.gold.dim_document) =
  (SELECT COUNT(DISTINCT document_id)
   FROM `0-ai-trust`.silver.app_missing_document
   WHERE dq_status IN ('PASSED', 'WARNING')
     AND masking_status IN ('MASKED', 'CLEAN')),
  'Document reconciliation failed'
);
