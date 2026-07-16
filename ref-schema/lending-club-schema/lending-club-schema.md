# Lending Club -- Schema Reference

## Purpose

Schema catalog for the **Lending Club** peer-to-peer lending platform dataset (2007-2018Q4). Two entities:

- **AcceptedLoan** -- funded loans with full borrower profile, credit history, and repayment performance
- **RejectedApplication** -- applications that did not meet credit policy

**Generated:** 2026-07-16

---

## Entity Summary

| Entity | Fields | Rows | Source |
|--------|--------|------|--------|
| AcceptedLoan | 151 | ~2.3M | all_lending_club_accepted_2007_to_2018Q4.csv |
| RejectedApplication | 9 | ~27.6M | all_lending_club_rejected_2007_to_2018Q4.csv |

---

## AcceptedLoan Field Reference

### Loan Identification (5 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique loan identifier |
| `member_id` | string | No | Member identifier |
| `url` | string (uri) | No | Loan detail page URL |
| `policy_code` | integer | No | Policy code, always 1.0 |
| `disbursement_method` | enum | No | Method of loan disbursement |

### Loan Terms (8 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `loan_amnt` | decimal | Yes | Requested loan amount in dollars |
| `funded_amnt` | decimal | No | Total amount committed to loan |
| `funded_amnt_inv` | decimal | No | Total amount committed by investors |
| `term` | enum | Yes | Loan term in months |
| `int_rate` | decimal | Yes | Interest rate percentage |
| `installment` | decimal | No | Monthly payment amount in dollars |
| `grade` | enum | Yes | LC assigned loan grade |
| `sub_grade` | enum | Yes | LC assigned loan sub-grade |

### Borrower Profile (8 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `emp_title` | string | No | Borrower job title |
| `emp_length` | enum | No | Employment length in years |
| `home_ownership` | enum | No | Home ownership status |
| `annual_inc` | decimal | No | Borrower annual income in dollars |
| `verification_status` | enum | No | Income verification status |
| `application_type` | enum | No | Individual or joint application |
| `dti` | decimal | No | Debt-to-income ratio |
| `purpose` | enum | No | Stated purpose of the loan |

### Loan Description (3 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `desc` | string | No | Borrower-provided loan description |
| `title` | string | No | Loan listing title/category |
| `pymnt_plan` | enum | No | Whether payment plan is in place |

### Borrower Location (2 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `zip_code` | string | No | First 3 digits of zip + xx |
| `addr_state` | string | No | 2-letter US state code |

### Credit History (18 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `earliest_cr_line` | string (Mon-YYYY) | No | Date of earliest credit line |
| `fico_range_low` | decimal | No | Lower FICO bound at origination |
| `fico_range_high` | decimal | No | Upper FICO bound at origination |
| `delinq_2yrs` | integer | No | Delinquencies in past 2 years |
| `inq_last_6mths` | integer | No | Credit inquiries in last 6 months |
| `mths_since_last_delinq` | integer | No | Months since last delinquency |
| `mths_since_last_record` | integer | No | Months since last public record |
| `open_acc` | integer | No | Number of open credit lines |
| `pub_rec` | integer | No | Number of derogatory public records |
| `total_acc` | integer | No | Total number of credit lines |
| `collections_12_mths_ex_med` | integer | No | Collections in 12 months excl. medical |
| `mths_since_last_major_derog` | integer | No | Months since last 90+ day delinquency |
| `acc_now_delinq` | integer | No | Accounts currently delinquent |
| `tot_coll_amt` | decimal | No | Total collection amounts ever |
| `pub_rec_bankruptcies` | integer | No | Public record bankruptcies |
| `tax_liens` | integer | No | Number of tax liens |
| `chargeoff_within_12_mths` | integer | No | Charge-offs in last 12 months |
| `delinq_amnt` | decimal | No | Past-due amount currently owed |

### Revolving Credit (20 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `revol_bal` | decimal | No | Total revolving balance in dollars |
| `revol_util` | decimal | No | Revolving line utilization rate % |
| `total_rev_hi_lim` | decimal | No | Total revolving high credit/limit |
| `bc_open_to_buy` | decimal | No | Open-to-buy on revolving bankcards |
| `bc_util` | decimal | No | Bankcard utilization ratio % |
| `max_bal_bc` | decimal | No | Max current balance on bankcards |
| `num_actv_bc_tl` | integer | No | Number of active bankcard accounts |
| `num_bc_sats` | integer | No | Satisfactory bankcard accounts |
| `num_bc_tl` | integer | No | Total bankcard accounts |
| `num_actv_rev_tl` | integer | No | Number of active revolving trades |
| `num_op_rev_tl` | integer | No | Number of open revolving accounts |
| `num_rev_accts` | integer | No | Total revolving accounts |
| `num_rev_tl_bal_gt_0` | integer | No | Revolving trades with balance > 0 |
| `open_rv_12m` | integer | No | Revolving trades opened in last 12m |
| `open_rv_24m` | integer | No | Revolving trades opened in last 24m |
| `percent_bc_gt_75` | decimal | No | % of bankcards with util > 75% |
| `pct_tl_nvr_dlq` | decimal | No | % of trades never delinquent |
| `mths_since_recent_bc` | integer | No | Months since most recent bankcard |
| `mths_since_recent_bc_dlq` | integer | No | Months since recent bankcard delinq |
| `mths_since_recent_revol_delinq` | integer | No | Months since recent revolving delinq |

### Installment Credit (10 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `total_bal_il` | decimal | No | Total current installment balance |
| `il_util` | decimal | No | Installment utilization ratio |
| `open_act_il` | integer | No | Active installment accounts |
| `open_il_12m` | integer | No | Installment accts opened in last 12m |
| `open_il_24m` | integer | No | Installment accts opened in last 24m |
| `mths_since_rcnt_il` | integer | No | Months since recent installment acct |
| `num_il_tl` | integer | No | Total installment accounts |
| `total_il_high_credit_limit` | decimal | No | Total installment high credit limit |
| `mo_sin_old_il_acct` | integer | No | Months since oldest installment acct |
| `total_bal_ex_mort` | decimal | No | Total balance excluding mortgage |

### Aggregate Credit Metrics (16 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tot_cur_bal` | decimal | No | Total current balance all accounts |
| `tot_hi_cred_lim` | decimal | No | Total high credit limit |
| `total_bc_limit` | decimal | No | Total bankcard high credit limit |
| `all_util` | decimal | No | Balance-to-limit ratio on all trades |
| `avg_cur_bal` | decimal | No | Average current balance of accounts |
| `acc_open_past_24mths` | integer | No | Accounts opened in past 24 months |
| `num_sats` | integer | No | Number of satisfactory accounts |
| `num_tl_120dpd_2m` | integer | No | Accounts 120+ dpd in last 2 months |
| `num_tl_30dpd` | integer | No | Accounts currently 30+ dpd |
| `num_tl_90g_dpd_24m` | integer | No | Accounts 90+ dpd in last 24 months |
| `num_tl_op_past_12m` | integer | No | Accounts opened in past 12 months |
| `inq_fi` | integer | No | Number of personal finance inquiries |
| `total_cu_tl` | integer | No | Total credit union tradelines |
| `inq_last_12m` | integer | No | Credit inquiries in last 12 months |
| `open_acc_6m` | integer | No | Accounts opened in last 6 months |
| `mort_acc` | integer | No | Number of mortgage accounts |

### Additional Aggregate (5 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mo_sin_old_rev_tl_op` | integer | No | Months since oldest revolving acct |
| `mo_sin_rcnt_rev_tl_op` | integer | No | Months since most recent revolving acct |
| `mo_sin_rcnt_tl` | integer | No | Months since most recent account |
| `mths_since_recent_inq` | integer | No | Months since most recent inquiry |
| `num_accts_ever_120_pd` | integer | No | Accounts ever 120+ days past due |

### Loan Performance (18 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `loan_status` | enum | Yes | Current status of the loan |
| `issue_d` | string (Mon-YYYY) | Yes | Month-year loan was issued |
| `initial_list_status` | enum | No | Listing status: whole or fractional |
| `out_prncp` | decimal | No | Remaining outstanding principal |
| `out_prncp_inv` | decimal | No | Outstanding principal for investors |
| `total_pymnt` | decimal | No | Total payments received to date |
| `total_pymnt_inv` | decimal | No | Total payments received by investors |
| `total_rec_prncp` | decimal | No | Total principal received to date |
| `total_rec_int` | decimal | No | Total interest received to date |
| `total_rec_late_fee` | decimal | No | Total late fees received to date |
| `recoveries` | decimal | No | Post-chargeoff gross recovery amount |
| `collection_recovery_fee` | decimal | No | Post-chargeoff collection fees |
| `last_pymnt_d` | string (Mon-YYYY) | No | Month of last payment received |
| `last_pymnt_amnt` | decimal | No | Last total payment amount |
| `next_pymnt_d` | string (Mon-YYYY) | No | Next scheduled payment month |
| `last_credit_pull_d` | string (Mon-YYYY) | No | Most recent credit pull month |
| `last_fico_range_high` | decimal | No | Latest FICO upper bound |
| `last_fico_range_low` | decimal | No | Latest FICO lower bound |

### Joint Application (16 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `annual_inc_joint` | decimal | No | Combined annual income for joint apps |
| `dti_joint` | decimal | No | Combined DTI for joint apps |
| `verification_status_joint` | enum | No | Verification status for joint apps |
| `revol_bal_joint` | decimal | No | Combined revolving balance for joint |
| `sec_app_fico_range_low` | decimal | No | Secondary applicant FICO lower bound |
| `sec_app_fico_range_high` | decimal | No | Secondary applicant FICO upper bound |
| `sec_app_earliest_cr_line` | string (Mon-YYYY) | No | Secondary applicant earliest credit line |
| `sec_app_inq_last_6mths` | integer | No | Secondary applicant inquiries in 6m |
| `sec_app_mort_acc` | integer | No | Secondary applicant mortgage accounts |
| `sec_app_open_acc` | integer | No | Secondary applicant open accounts |
| `sec_app_revol_util` | decimal | No | Secondary applicant revolving util % |
| `sec_app_open_act_il` | integer | No | Secondary applicant active installment |
| `sec_app_num_rev_accts` | integer | No | Secondary applicant revolving accounts |
| `sec_app_chargeoff_within_12_mths` | integer | No | Secondary applicant chargeoffs in 12m |
| `sec_app_collections_12_mths_ex_med` | integer | No | Secondary applicant collections excl. med |
| `sec_app_mths_since_last_major_derog` | integer | No | Secondary applicant months since 90+ delinq |

### Hardship Program (15 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `hardship_flag` | enum | No | Whether borrower is in hardship plan |
| `hardship_type` | string | No | Type of hardship plan offered |
| `hardship_reason` | enum | No | Reason for hardship enrollment |
| `hardship_status` | enum | No | Current hardship plan status |
| `deferral_term` | decimal | No | Deferral term in months |
| `hardship_amount` | decimal | No | Modified payment amount during hardship |
| `hardship_start_date` | string (Mon-YYYY) | No | Hardship plan start date |
| `hardship_end_date` | string (Mon-YYYY) | No | Hardship plan end date |
| `payment_plan_start_date` | string (Mon-YYYY) | No | Payment plan start date |
| `hardship_length` | decimal | No | Length of hardship plan in months |
| `hardship_dpd` | integer | No | Days past due during hardship |
| `hardship_loan_status` | enum | No | Loan status during hardship period |
| `orig_projected_additional_accrued_interest` | decimal | No | Projected additional accrued interest |
| `hardship_payoff_balance_amount` | decimal | No | Payoff balance at hardship start |
| `hardship_last_payment_amount` | decimal | No | Last payment during hardship |

### Debt Settlement (7 fields)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `debt_settlement_flag` | enum | No | Whether debt settlement has occurred |
| `debt_settlement_flag_date` | string (Mon-YYYY) | No | Date settlement flag was set |
| `settlement_status` | enum | No | Current settlement status |
| `settlement_date` | string (Mon-YYYY) | No | Date of settlement agreement |
| `settlement_amount` | decimal | No | Settlement amount in dollars |
| `settlement_percentage` | decimal | No | Settlement as % of owed balance |
| `settlement_term` | integer | No | Settlement term in months |

---

## RejectedApplication Field Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Amount Requested` | decimal | Yes | Requested loan amount in dollars |
| `Application Date` | string (YYYY-MM-DD) | Yes | Date of loan application |
| `Loan Title` | string | No | Free-text loan description |
| `Risk_Score` | decimal | No | FICO-like risk score at application |
| `Debt-To-Income Ratio` | string | No | DTI ratio with % suffix |
| `Zip Code` | string | No | First 3 digits of zip + xx |
| `State` | string | No | 2-letter US state code |
| `Employment Length` | enum | No | Employment length in years |
| `Policy Code` | decimal | Yes | Policy code, always 0.0 |

---

## Data Quality Notes

### AcceptedLoan

- **member_id**: Mostly blank in data from 2016 onward (anonymization)
- **desc**: Free-text field; highly variable, can contain line breaks and special characters
- **emp_title**: High cardinality free-text; many misspellings and variations (e.g., "Manager", "manager", "MANAGER")
- **dates**: `issue_d`, `last_pymnt_d`, `next_pymnt_d`, `earliest_cr_line`, `last_credit_pull_d` stored as `Mon-YYYY` strings, not ISO dates
- **int_rate**: Stored as plain decimal (e.g., 13.56), not with % suffix
- **revol_util**: May contain % suffix in some rows, numeric in others
- **Debt-to-income**: `dti` is plain numeric ratio (e.g., 14.29), no % suffix
- **FICO ranges**: Provided as low/high band (e.g., 670/674), always 4-point spread
- **Blank vs. null**: Empty fields mean "not applicable" or "not yet occurred" (e.g., hardship fields blank when borrower not in hardship)
- **Hardship & settlement**: These field groups are almost entirely blank for loans issued before 2018
- **Joint application fields**: Blank when `application_type = Individual`

### RejectedApplication

- **Debt-To-Income Ratio**: Stored as string with `%` suffix (e.g., `27.56%`)
- **Risk_Score**: FICO-like score; null when not available
- **Policy Code**: Always 0.0 for rejected apps (vs 1.0 for accepted)
- **No loan_id**: Rejected applications have no loan ID -- they were never funded
- **Employment Length**: Same format as accepted (`< 1 year`, `1 year`, ..., `10+ years`) but with more blanks

---

## Relationship Between Entities

AcceptedLoan and RejectedApplication are **independent datasets** with no shared key:

- Accepted loans have `id` (loan ID) and `member_id`
- Rejected applications have neither -- they were never funded
- The two cannot be linked at the applicant level
- `Policy Code = 1` for accepted, `0` for rejected

---

## Key Enum Values

### loan_status (AcceptedLoan)

| Value | Description |
|-------|-------------|
| Fully Paid | Loan fully repaid |
| Charged Off | Loan defaulted, written off |
| Current | Active loan, payments current |
| Late (31-120 days) | Payment 31-120 days late |
| Late (16-30 days) | Payment 16-30 days late |
| In Grace Period | Payment within grace period |
| Default | Loan in default status |
| Issued | Recently issued, no payments yet |

### grade / sub_grade

7 grades (A-G) with 5 sub-grades each (A1-A5 ... G1-G5), 35 total values. A = lowest risk, G = highest risk.

### purpose (AcceptedLoan)

| Value | Description |
|-------|-------------|
| debt_consolidation | Pay off existing debt |
| credit_card | Pay off credit card balances |
| home_improvement | Home renovation/repair |
| major_purchase | Large purchase |
| small_business | Business expenses |
| car | Vehicle purchase |
| medical | Medical expenses |
| moving | Relocation costs |
| vacation | Travel expenses |
| house | Home purchase |
| wedding | Wedding expenses |
| renewable_energy | Solar/green energy |
| educational | Education expenses |
| other | Other purpose |

---

## Schema File Location

```
0-ai-trust/schema/
├── cdr-schema/          # CDR banking entities
├── mlar-schema/         # HMDA mortgage applications
├── linked-schema/       # Unified Customer + Application
├── bpi-schema/          # BPI 2017 loan process events
└── lending-club-schema/ # This file
    ├── lending-club-schema.json
    └── lending-club-schema.md
```