CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_customer_self_profile AS
SELECT
  global_id,
  preferred_contact_channel,
  preferred_language,
  marketing_opt_in,
  'CUSTOMER_SAFE' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  pipeline_run_id,
  dq_status
FROM `0-ai-trust`.gold.dim_party
WHERE organisation_id IS NULL;
