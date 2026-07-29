MERGE INTO `0-ai-trust`.gold.dim_arrangement AS target
USING (
  SELECT
    CONCAT('BANKING:', account_id) AS arrangement_key,
    global_id,
    organisation_id,
    'BANKING' AS arrangement_type,
    account_type,
    CAST(NULL AS STRING) AS loan_type,
    CAST(NULL AS STRING) AS card_type,
    CAST(NULL AS STRING) AS interest_type,
    CASE WHEN is_active THEN 'ACTIVE' ELSE 'CLOSED' END AS status,
    is_active,
    is_business_arrangement,
    false AS is_approaching_maturity,
    open_date,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS maturity_date,
    CAST(NULL AS STRING) AS repayment_frequency,
    CAST(NULL AS STRING) AS reward_program,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    dq_status
  FROM `0-ai-trust`.silver.arr_banking_arrangement
  WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('LOAN:', loan_id), global_id, organisation_id, 'LOAN',
    CAST(NULL AS STRING), loan_type, CAST(NULL AS STRING), interest_type,
    status, UPPER(status) = 'ACTIVE', is_business_arrangement,
    maturity_date BETWEEN current_date() AND date_add(current_date(), 90),
    CAST(NULL AS DATE), loan_start_date, maturity_date, repayment_frequency,
    CAST(NULL AS STRING), pipeline_run_id, current_timestamp(), dq_status
  FROM `0-ai-trust`.silver.arr_loan_arrangement
  WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('MORTGAGE:', mortgage_id), global_id, organisation_id, 'MORTGAGE',
    CAST(NULL AS STRING), 'MORTGAGE', CAST(NULL AS STRING), interest_type,
    'ACTIVE', true, is_business_arrangement, false,
    CAST(NULL AS DATE), mortgage_start_date, CAST(NULL AS DATE), repayment_frequency,
    CAST(NULL AS STRING), pipeline_run_id, current_timestamp(), dq_status
  FROM `0-ai-trust`.silver.arr_mortgage_arrangement
  WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('CREDIT_CARD:', credit_card_id), global_id, organisation_id, 'CREDIT_CARD',
    CAST(NULL AS STRING), CAST(NULL AS STRING), card_type, CAST(NULL AS STRING),
    status, UPPER(status) = 'ACTIVE', is_business_arrangement, false,
    issued_date, CAST(NULL AS DATE), CAST(NULL AS DATE), CAST(NULL AS STRING), reward_program,
    pipeline_run_id, current_timestamp(), dq_status
  FROM `0-ai-trust`.silver.arr_credit_card_arrangement
  WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.arrangement_key = source.arrangement_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
