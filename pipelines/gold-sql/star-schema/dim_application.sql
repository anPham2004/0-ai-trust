MERGE INTO `0-ai-trust`.gold.dim_application AS target
USING (
  SELECT
    application_id AS application_key,
    application_id,
    global_id,
    organisation_id,
    loan_goal,
    application_type,
    final_outcome,
    is_business_application,
    CAST(submitted_at AS TIMESTAMP) AS submitted_at,
    CAST(last_updated_at AS TIMESTAMP) AS last_updated_at,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    dq_status
  FROM `0-ai-trust`.silver.app_application
  WHERE dq_status IN ('PASSED', 'WARNING')
    AND masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.application_key = source.application_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
