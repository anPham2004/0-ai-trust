-- Out-of-order arrivals remain valid history; business timestamps must still be coherent.
SELECT assert_true(COUNT(*) = 0, 'Stage exit precedes stage entry')
FROM `0-ai-trust`.gold.fact_application_stage
WHERE exited_at < entered_at;

SELECT assert_true(COUNT(*) = 0, 'Document receipt precedes request')
FROM `0-ai-trust`.gold.dim_document
WHERE received_at < requested_at;

SELECT assert_true(COUNT(*) = 0, 'Arrangement maturity precedes start')
FROM `0-ai-trust`.gold.dim_arrangement
WHERE maturity_date < loan_start_date;

-- A late event must not disappear merely because its event time is old.
SELECT assert_true(
  (SELECT COUNT(*) FROM `0-ai-trust`.gold.fact_application_stage) =
  (SELECT COUNT(*) FROM (
     SELECT CONCAT('STAGE:', history_id) AS event_key
     FROM `0-ai-trust`.silver.app_application_stage_history
     WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')
     UNION
     SELECT CONCAT('STATUS:', change_id)
     FROM `0-ai-trust`.silver.app_status_change_history
     WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')
     UNION
     SELECT CONCAT('APP_EVENT:', event_id)
     FROM `0-ai-trust`.silver.app_application_event
     WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')
   )),
  'Late or out-of-order application history was lost'
);

-- One daily snapshot per arrangement makes reruns safe.
SELECT assert_true(COUNT(*) = 0, 'Duplicate arrangement daily snapshots')
FROM (
  SELECT arrangement_key, snapshot_date
  FROM `0-ai-trust`.gold.fact_arrangement_snapshot
  GROUP BY arrangement_key, snapshot_date
  HAVING COUNT(*) > 1
);

-- Warning-quality context must tell the AI that the answer has limitations.
SELECT assert_true(COUNT(*) = 0, 'Warning context is missing known limitations')
FROM `0-ai-trust`.gold.aiv_banker_customer_360
WHERE dq_status = 'WARNING'
  AND (known_limitations IS NULL OR TRIM(known_limitations) = '');
