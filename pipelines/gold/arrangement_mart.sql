SET pipelines.trigger.interval=15 minutes;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_arrangement_current
COMMENT 'Current canonical banking-account, loan, mortgage, and credit-card portfolio'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  a.global_id,
  a.account_id AS arrangement_id,
  'BANK_ACCOUNT' AS arrangement_type,
  CAST(NULL AS STRING) AS parent_account_id,
  c.customer_id,
  a.organisation_id,
  a.is_business_arrangement,
  a.account_type AS arrangement_subtype,
  CASE WHEN a.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS arrangement_status,
  a.is_active,
  a.open_date AS start_date,
  CAST(NULL AS DATE) AS maturity_date,
  CAST(NULL AS INT) AS days_to_maturity,
  CAST(NULL AS STRING) AS interest_type,
  CAST(NULL AS STRING) AS repayment_frequency,
  CAST(NULL AS INT) AS term_months,
  a.current_balance_masked AS balance_band,
  a.available_balance_masked AS available_balance_band,
  CAST(NULL AS STRING) AS original_amount_band,
  CAST(NULL AS STRING) AS loan_amount_band,
  CAST(NULL AS STRING) AS credit_limit_band,
  a.currency,
  CAST(NULL AS STRING) AS masked_identifier,
  'PASSED' AS dq_status,
  a.masking_status,
  a.processed_at
FROM `0-ai-trust`.silver.arr_banking_arrangement a
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = a.global_id
WHERE a.`__END_AT` IS NULL AND a.masking_status IN ('MASKED', 'CLEAN')

UNION ALL

SELECT
  l.global_id,
  l.loan_id,
  'LOAN',
  l.account_id,
  c.customer_id,
  a.organisation_id,
  a.organisation_id IS NOT NULL,
  l.loan_type,
  l.status,
  UPPER(l.status) = 'ACTIVE',
  l.start_date,
  l.maturity_date,
  datediff(l.maturity_date, current_date()),
  l.interest_type,
  l.repayment_frequency,
  l.term_months,
  l.current_balance_masked,
  CAST(NULL AS STRING),
  l.original_amount_masked,
  CAST(NULL AS STRING),
  CAST(NULL AS STRING),
  COALESCE(a.currency, 'AUD'),
  CAST(NULL AS STRING),
  'PASSED',
  l.masking_status,
  l.processed_at
FROM `0-ai-trust`.silver.arr_loan l
LEFT JOIN `0-ai-trust`.silver.arr_banking_arrangement a
  ON a.account_id = l.account_id AND a.`__END_AT` IS NULL
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = l.global_id
WHERE l.`__END_AT` IS NULL AND l.masking_status IN ('MASKED', 'CLEAN')

UNION ALL

SELECT
  m.global_id,
  m.mortgage_id,
  'MORTGAGE',
  m.account_id,
  c.customer_id,
  a.organisation_id,
  a.organisation_id IS NOT NULL,
  'MORTGAGE',
  CASE
    WHEN m.start_date <= current_date()
      AND current_date() < add_months(m.start_date, m.loan_term * 12) THEN 'ACTIVE_INFERRED'
    ELSE 'INACTIVE_INFERRED'
  END,
  m.start_date <= current_date()
    AND current_date() < add_months(m.start_date, m.loan_term * 12),
  m.start_date,
  add_months(m.start_date, m.loan_term * 12),
  datediff(add_months(m.start_date, m.loan_term * 12), current_date()),
  m.interest_type,
  m.repayment_frequency,
  m.loan_term * 12,
  CAST(NULL AS STRING),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING),
  m.loan_amount_masked,
  CAST(NULL AS STRING),
  COALESCE(a.currency, 'AUD'),
  CAST(NULL AS STRING),
  'PASSED',
  m.masking_status,
  m.processed_at
FROM `0-ai-trust`.silver.arr_mortgage m
LEFT JOIN `0-ai-trust`.silver.arr_banking_arrangement a
  ON a.account_id = m.account_id AND a.`__END_AT` IS NULL
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = m.global_id
WHERE m.`__END_AT` IS NULL AND m.masking_status IN ('MASKED', 'CLEAN')

UNION ALL

SELECT
  cc.global_id,
  cc.credit_card_id,
  'CREDIT_CARD',
  CAST(NULL AS STRING),
  c.customer_id,
  cc.organisation_id,
  cc.is_business_arrangement,
  cc.card_type,
  cc.status,
  UPPER(cc.status) = 'ACTIVE',
  cc.issued_date,
  CAST(NULL AS DATE),
  CAST(NULL AS INT),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING),
  CAST(NULL AS INT),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING),
  cc.credit_limit_masked,
  'AUD',
  cc.card_number_masked,
  'PASSED',
  cc.masking_status,
  cc.processed_at
FROM `0-ai-trust`.silver.arr_credit_card cc
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = cc.global_id
WHERE cc.`__END_AT` IS NULL AND cc.masking_status IN ('MASKED', 'CLEAN');
