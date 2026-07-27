CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_banker_org_party_summary AS
SELECT
  r.org_party_key,
  r.organisation_id,
  r.global_id,
  r.name_token,
  r.party_role,
  r.authority_level,
  r.is_active,
  r.start_date,
  r.end_date,
  r.preferred_contact_channel,
  p.customer_type,
  'BANKER_ONLY' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  r.pipeline_run_id,
  CASE WHEN r.dq_status = 'WARNING' OR p.dq_status = 'WARNING' THEN 'WARNING' ELSE 'PASSED' END AS dq_status
FROM `0-ai-trust`.gold.dim_organisation_party AS r
JOIN `0-ai-trust`.gold.dim_party AS p
  ON p.party_key = r.global_id AND p.organisation_id IS NULL;
