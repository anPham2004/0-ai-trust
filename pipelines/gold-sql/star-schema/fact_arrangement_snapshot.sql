MERGE INTO `0-ai-trust`.gold.fact_arrangement_snapshot AS target
USING (
  SELECT
    SHA2(CONCAT_WS('|', arrangement_key, CAST(current_date() AS STRING)), 256) AS snapshot_key,
    arrangement_key,
    global_id,
    organisation_id,
    arrangement_type,
    status,
    is_active,
    is_approaching_maturity,
    current_date() AS snapshot_date,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    dq_status
  FROM `0-ai-trust`.gold.dim_arrangement
) AS source
ON target.snapshot_key = source.snapshot_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
