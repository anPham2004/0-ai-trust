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
    'PASSED' AS dq_status
  FROM `0-ai-trust`.silver.arr_banking_arrangement
  WHERE `__END_AT` IS NULL AND masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('LOAN:', l.loan_id), l.global_id, b.organisation_id, 'LOAN',
    CAST(NULL AS STRING), l.loan_type, CAST(NULL AS STRING), l.interest_type,
    l.status, UPPER(l.status) = 'ACTIVE', coalesce(b.is_business_arrangement, false),
    l.maturity_date BETWEEN current_date() AND date_add(current_date(), 90),
    CAST(NULL AS DATE), l.start_date, l.maturity_date, l.repayment_frequency,
    CAST(NULL AS STRING), l.pipeline_run_id, current_timestamp(), 'PASSED'
  FROM `0-ai-trust`.silver.arr_loan l
  LEFT JOIN `0-ai-trust`.silver.arr_banking_arrangement b
    ON b.account_id = l.account_id AND b.`__END_AT` IS NULL
  WHERE l.`__END_AT` IS NULL AND l.masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('MORTGAGE:', m.mortgage_id), m.global_id, b.organisation_id, 'MORTGAGE',
    CAST(NULL AS STRING), 'MORTGAGE', CAST(NULL AS STRING), m.interest_type,
    'ACTIVE', true, coalesce(b.is_business_arrangement, false), false,
    CAST(NULL AS DATE), m.start_date, CAST(NULL AS DATE), m.repayment_frequency,
    CAST(NULL AS STRING), m.pipeline_run_id, current_timestamp(), 'PASSED'
  FROM `0-ai-trust`.silver.arr_mortgage m
  LEFT JOIN `0-ai-trust`.silver.arr_banking_arrangement b
    ON b.account_id = m.account_id AND b.`__END_AT` IS NULL
  WHERE m.`__END_AT` IS NULL AND m.masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('CREDIT_CARD:', credit_card_id), global_id, organisation_id, 'CREDIT_CARD',
    CAST(NULL AS STRING), CAST(NULL AS STRING), card_type, CAST(NULL AS STRING),
    status, UPPER(status) = 'ACTIVE', is_business_arrangement, false,
    issued_date, CAST(NULL AS DATE), CAST(NULL AS DATE), CAST(NULL AS STRING), reward_program,
    pipeline_run_id, current_timestamp(), 'PASSED'
  FROM `0-ai-trust`.silver.arr_credit_card
  WHERE `__END_AT` IS NULL AND masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.arrangement_key = source.arrangement_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
