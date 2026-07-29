MERGE INTO `0-ai-trust`.gold.dim_organisation_relationship AS target
USING (
  SELECT
    relationship_id AS org_rel_key,
    source_org_id,
    target_org_id,
    relationship_type,
    is_active,
    start_date,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    'PASSED' AS dq_status
  FROM `0-ai-trust`.silver.ip_org_relationship
  WHERE `__END_AT` IS NULL
    AND masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.org_rel_key = source.org_rel_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
