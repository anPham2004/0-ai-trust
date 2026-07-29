MERGE INTO `0-ai-trust`.gold.dim_kyc AS target
USING (
  SELECT
    kyc_id AS kyc_key,
    global_id,
    organisation_id,
    record_type,
    verification_status,
    verification_date,
    verification_method,
    last_review_date,
    next_review_date,
    document_types,
    abn_verified,
    asic_check_status,
    beneficial_ownership_verified,
    risk_rating,
    pep_status,
    sanctions_check,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    'PASSED' AS dq_status
  FROM `0-ai-trust`.silver.ip_kyc
  WHERE `__END_AT` IS NULL
    AND masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.kyc_key = source.kyc_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
