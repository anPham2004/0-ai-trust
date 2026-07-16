# HMDA Modified Loan/Application Register (MLAR) -- Schema Catalog

## Purpose

Schema reference for the HMDA MLAR public dataset filed under the Home Mortgage Disclosure Act. Published by the CFPB/FFIEC. Each record represents one mortgage loan application or origination.

**Data Year:** 2025
**Source File:** `dataset/2025_combined_mlar_header.txt`
**Format:** Pipe-delimited (`|`) flat file, 85 fields per record
**Total Records:** ~13.5 million
**Generated:** 2026-07-16

---

## Entity Summary

| # | Entity | Records | Description |
|---|--------|---------|-------------|
| 1 | MortgageLoanApplication | ~13.5M | Individual mortgage loan application or origination filed under HMDA |

Single flat entity with 85 fields organized into 8 logical groups.

---

## Field Groups

| Group | Fields | Description |
|-------|--------|-------------|
| loan_identification | 7 | Year, LEI, loan type/purpose, preapproval, construction, occupancy |
| property_location | 3 | State, county (FIPS), census tract |
| applicant_demographics | 16 | Ethnicity (5), race (5), sex, age, credit score -- for applicant |
| co_applicant_demographics | 16 | Same set for co-applicant, plus "no co-applicant" codes |
| financial | 13 | Loan amount, income, rate spread, costs, interest rate, DTI, CLTV, property value |
| loan_features | 15 | Lien, HOEPA, balloon, interest-only, negative amortization, term, units, manufactured home |
| processing | 7 | Action taken, purchaser type, denial reasons, submission, payable-to |
| automated_underwriting | 5 | AUS 1-5 (DU, LP, TOTAL, GUS, Other) |

---

## Field Reference

### Loan Identification

| Field | Type | Required | Values/Format |
|-------|------|----------|---------------|
| activity_year | integer | Yes | 4-digit year (e.g. `2025`) |
| lei | string | Yes | 20-char alphanumeric LEI (ISO 17442) |
| loan_type | enum | Yes | 1=Conventional, 2=FHA, 3=VA, 4=USDA/RHS |
| loan_purpose | enum | Yes | 1=Purchase, 2=Improvement, 31=Refinance, 32=Cash-out refi, 4=Other, 5=N/A |
| preapproval | enum | Yes | 1=Requested, 2=Not requested |
| construction_method | enum | Yes | 1=Site-built, 2=Manufactured |
| occupancy_type | enum | Yes | 1=Principal, 2=Second, 3=Investment |

### Property Location

| Field | Type | Required | Values/Format |
|-------|------|----------|---------------|
| state_code | string | No | 2-letter USPS abbreviation; `NA` if not applicable |
| county_code | string | No | 5-digit FIPS code; `NA` if not applicable |
| census_tract | string | No | 11-digit FIPS tract (state+county+tract); `NA` if not applicable |

### Applicant Demographics

| Field | Type | Required | Values/Format |
|-------|------|----------|---------------|
| applicant_ethnicity_1 | enum | Yes | 1=Hispanic, 11=Mexican, 12=Puerto Rican, 13=Cuban, 14=Other Hispanic, 2=Not Hispanic, 3=Not provided, 4=N/A |
| applicant_ethnicity_2-5 | enum | No | Sub-codes of ethnicity_1; blank if single |
| applicant_ethnicity_observed | enum | Yes | 1=Visual/surname, 2=Not visual, 3=N/A |
| applicant_race_1 | enum | Yes | 1=AIAN, 2=Asian, 21-27=Asian sub, 3=Black, 4=NHOPI, 41-44=NHOPI sub, 5=White, 6=Not provided, 7=N/A |
| applicant_race_2-5 | enum | No | Same codes; blank if single race |
| applicant_race_observed | enum | Yes | 1=Visual/surname, 2=Not visual, 3=N/A |
| applicant_sex | enum | Yes | 1=Male, 2=Female, 3=Not provided, 4=N/A, 6=Both selected |
| applicant_sex_observed | enum | Yes | 1=Visual/surname, 2=Not visual, 3=N/A |
| applicant_age | bracket | Yes | `<25`, `25-34`, `35-44`, `45-54`, `55-64`, `65-74`, `>74`, `8888`=N/A |
| applicant_age_above_62 | enum | Yes | `Yes`, `No`, `NA` |
| applicant_credit_scoring_model | enum | Yes | 1-8=specific models, 9=N/A, 11-15=newer models, 1111=Exempt |

### Co-Applicant Demographics

Same structure as applicant. Additional codes: `5`=No co-applicant (ethnicity/sex), `8`=No co-applicant (race), `9999`=No co-applicant (age), `10`=No co-applicant (credit model), `4`=No co-applicant (observed fields).

### Financial

| Field | Type | Required | Values/Format |
|-------|------|----------|---------------|
| loan_amount | integer | Yes | Whole dollars |
| income | integer | No | In $1,000s; can be negative or zero; `NA` if not applicable |
| rate_spread | decimal | No | APR minus APOR in pct points; can be negative; `NA` if not applicable |
| total_loan_costs | decimal | No | Dollar amount; `NA`/`Exempt` |
| total_points_and_fees | decimal | No | Dollar amount; `NA`/`Exempt` |
| origination_charges | decimal | No | Dollar amount; `NA`/`Exempt` |
| discount_points | decimal | No | Dollar amount; `NA`/`Exempt`; blank if none |
| lender_credits | decimal | No | Dollar amount; `NA`/`Exempt`; blank if none |
| interest_rate | decimal | No | Percentage up to 5 decimal places; `NA`/`Exempt` |
| debt_to_income_ratio | string | No | Brackets (`<20%`, `20%-<30%`, `30%-<36%`, `50%-60%`, `>60%`), exact int for 36-49, `NA`/`Exempt` |
| combined_loan_to_value_ratio | decimal | No | Percentage to 3 decimal places; `NA`/`Exempt` |
| property_value | integer | No | Rounded to nearest $10,000; `NA`/`Exempt` |

### Loan Features

| Field | Type | Required | Values/Format |
|-------|------|----------|---------------|
| hoepa_status | enum | Yes | 1=High-cost, 2=Not high-cost, 3=N/A |
| lien_status | enum | Yes | 1=First lien, 2=Subordinate lien |
| loan_term | integer | Yes | Months; `NA`/`Exempt` |
| intro_rate_period | string | No | Months; `NA`/`Exempt` |
| prepayment_penalty_term | string | No | Months; `NA`/`Exempt` |
| balloon_payment | enum | Yes | 1=Yes, 2=No, 1111=Exempt |
| interest_only_payment | enum | Yes | 1=Yes, 2=No, 1111=Exempt |
| negative_amortization | enum | Yes | 1=Yes, 2=No, 1111=Exempt |
| other_non_amortizing_features | enum | Yes | 1=Yes, 2=No, 1111=Exempt |
| reverse_mortgage | enum | Yes | 1=Yes, 2=No, 1111=Exempt |
| open_end_line_of_credit | enum | Yes | 1=Yes, 2=No, 1111=Exempt |
| business_or_commercial_purpose | enum | Yes | 1=Yes, 2=No, 1111=Exempt |
| total_units | enum | Yes | 1-4 exact, `5-24`, `25-49`, `50-99`, `100-149`, `>149` |
| manufactured_home_secured_property_type | enum | Yes | 1=Home+land, 2=Home only, 3=N/A, 1111=Exempt |
| manufactured_home_land_property_interest | enum | Yes | 1=Direct, 2=Indirect, 3=Paid lease, 4=Unpaid lease, 5=N/A, 1111=Exempt |

### Processing

| Field | Type | Required | Values/Format |
|-------|------|----------|---------------|
| action_taken | enum | Yes | 1=Originated, 2=Approved not accepted, 3=Denied, 4=Withdrawn, 5=Incomplete, 6=Purchased, 7=Preapproval denied, 8=Preapproval approved |
| purchaser_type | enum | No | 0=N/A, 1=Fannie, 2=Ginnie, 3=Freddie, 4=Farmer Mac, 5=Private, 6=Bank, 71=Credit union/mortgage co, 9=Other |
| denial_reason_1 | enum | Yes | 1=DTI, 2=Employment, 3=Credit history, 4=Collateral, 5=Insufficient cash, 6=Unverifiable info, 7=Incomplete app, 8=MI denied, 9=Other, 10=N/A, 1111=Exempt |
| denial_reason_2-4 | enum | No | Same codes; blank if not applicable |
| submission_of_application | enum | Yes | 1=Direct, 2=Not direct, 3=N/A, 1111=Exempt |
| initially_payable_to_institution | enum | Yes | 1=Payable to inst, 2=Not payable, 3=N/A, 1111=Exempt |
| multifamily_affordable_units | string | No | Integer count; `NA`/`Exempt` |

### Automated Underwriting

| Field | Type | Required | Values/Format |
|-------|------|----------|---------------|
| aus_1 | enum | Yes | 1=DU, 2=LP/LPA, 3=TOTAL, 4=GUS, 5=Other, 6=N/A, 1111=Exempt |
| aus_2-5 | enum | No | Same codes; blank if single AUS |

---

## Data Quality Notes

### Special Values

| Value | Meaning | Fields |
|-------|---------|--------|
| `NA` | Not applicable or not reported | Financial fields, location, age, DTI, CLTV, etc. |
| `Exempt` | Institution exempt from reporting this field | Financial fields, loan features, AUS |
| `1111` | Exempt (numeric enum form) | Credit model, denial reason, boolean loan features |
| blank (`""`) | Not collected or single value reported | Ethnicity 2-5, race 2-5, denial 2-4, AUS 2-5 |
| `8888` | Not applicable (applicant age) | applicant_age |
| `9999` | No co-applicant | co_applicant_age |

### Code `1111` (Exempt) Pattern

Applies to institutions with fewer than a threshold of covered loans. Fields using `1111`:
- Credit scoring models, denial reasons
- Balloon, interest-only, negative amortization, non-amortizing
- Reverse mortgage, open-end LOC, business purpose
- Manufactured home fields, submission, AUS

### Numeric Fields Stored as Strings

All values in the pipe-delimited file are strings. These fields contain numeric data but may also hold `NA`, `Exempt`, or blank:
- `loan_amount`, `income`, `property_value` -- integers
- `rate_spread`, `interest_rate`, `combined_loan_to_value_ratio` -- decimals
- `total_loan_costs`, `origination_charges`, `discount_points`, `lender_credits` -- dollar amounts
- `loan_term`, `intro_rate_period`, `prepayment_penalty_term` -- integer months

### DTI Mixed Format

`debt_to_income_ratio` uses brackets for low/high ranges (`<20%`, `20%-<30%`, `30%-<36%`, `50%-60%`, `>60%`) but exact integer percentages for the 36-49 range. Both formats appear in the same column.

---

## Sample Records

### Sample 1: Originated Loan

A conventional home purchase in Wisconsin. Applicant (male, 25-34, White, Not Hispanic) with co-applicant. $195K loan on $215K property, 6.125% rate, 30-year term, 95% LTV, DTI 41%. First lien, not high-cost, underwritten via DU.

### Sample 2: Denied Application

A cash-out refinance on an investment property in Illinois. Solo female applicant (45-54). $75K loan on $105K property. Denied for credit history + insufficient cash. No financial terms (NA across cost/rate fields). Not underwritten by AUS.

Full record data in `mlar-schema.json` under `entities.MortgageLoanApplication.samples`.

---

## Relationship to CRM Pipeline

For CRM ingestion, key transformations needed:

| MLAR Field | CRM Mapping |
|-----------|-------------|
| lei | institution_id (join to institution table) |
| applicant_ethnicity/race/sex/age | customer demographics (decode enums) |
| income, loan_amount, property_value | financial profile |
| action_taken | application_status |
| state_code + county_code + census_tract | geographic segmentation |
| interest_rate, rate_spread, loan_term | product attributes |
| denial_reason_1-4 | risk flags / decline analysis |

Key denormalization steps:
1. Decode all numeric enums to human-readable labels
2. Flatten ethnicity_1-5 and race_1-5 into arrays
3. Join LEI to institution name/type via GLEIF registry
4. Convert income from $1,000s to actual dollars
5. Parse DTI mixed format into numeric percentage
