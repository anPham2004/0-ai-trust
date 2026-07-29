CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_banker_org_arrangement_summary AS
SELECT
  a.arrangement_key,
  s.organisation_id,
  s.global_id,
  a.arrangement_type,
  a.account_type,
  a.loan_type,
  a.card_type,
  a.interest_type,
  a.status,
  a.is_active,
  a.is_approaching_maturity,
  a.repayment_frequency,
  a.loan_start_date,
  a.maturity_date,
  s.snapshot_date,
  'BANKER_ONLY' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  s.pipeline_run_id,
  CASE WHEN a.dq_status = 'WARNING' OR s.dq_status = 'WARNING' THEN 'WARNING' ELSE 'PASSED' END AS dq_status
FROM `0-ai-trust`.gold.fact_arrangement_snapshot AS s
JOIN `0-ai-trust`.gold.dim_arrangement AS a ON a.arrangement_key = s.arrangement_key
WHERE s.organisation_id IS NOT NULL;
