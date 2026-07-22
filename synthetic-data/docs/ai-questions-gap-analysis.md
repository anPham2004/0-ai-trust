# AI Questions vs Data Sources — Gap Analysis

**Generated:** 2026-07-16

## Context

The AI Questions document defines business questions for two AI personas:
- **Banker.AI** — internal staff assistant (24 supported + 8 refused)
- **Customer.AI** — customer-facing chatbot (13 supported + 8 refused)
- **Refusal rules** — 6 categories of questions both AIs must refuse

This report maps every supported question against available data schemas to identify coverage gaps.

## Available Data Sources

| Schema | Entities | Key Fields |
|--------|----------|------------|
| CDR | CommonPerson, BankingAccount, BankingBalance, BankingTransaction, ... (18 entities) | Name, phone, email, address, accounts, balances, transactions |
| MLAR | MortgageLoanApplication (85 fields) | Loan type/purpose/amount, demographics, property, action_taken, denial_reasons |
| Linked | Customer (30 fields) + Application (41 fields) | Merged CDR identity + MLAR demographics + loan details |
| BPI 2017 | LoanApplicationEvent (19 fields) + LoanApplication (11 fields) | Process events, timestamps, stages, offers, credit score |
| Lending Club | AcceptedLoan (151 fields) + RejectedApplication (9 fields) | Full borrower profile, credit history, loan performance |

## Required Datasets (from AI Questions doc)

The questions reference these **logical datasets** that the CRM system needs:

| Logical Dataset | Description |
|-----------------|-------------|
| `customer_profile` | Customer identity, preferences (contact method, language, marketing opt-in) |
| `support_interaction` | Support contact history (channel, timestamp, topic) |
| `service_case` | Service case records (disputes, complaints, status, resolution) |
| `application` | Loan application details (status, stage, team, documents) |
| `missing_document` | Missing document tracker (which docs, reminders sent, deadlines) |
| `application_stage_history` | Stage transitions with timestamps and reasons |
| `status_change_history` | Status change records with reasons for change |

---

## Banker.AI — Supported Questions Analysis

### B1: "Give me a summary of customer C001 before our meeting."
**Required:** All datasets
**Coverage:** PARTIAL
| Need | Source | Status |
|------|--------|--------|
| Name, contact info | CDR CommonPerson / Linked Customer | Covered |
| Account summary | CDR BankingAccount + BankingBalance | Covered |
| Support history | — | MISSING |
| Service cases | — | MISSING |
| Application status | BPI / Linked Application | Covered |
| Contact preference | — | MISSING |

### B2: "Has this customer contacted support recently?"
**Required:** `support_interaction`
**Coverage:** NOT COVERED
- No schema has support interaction records (channel, timestamp, topic, resolution)

### B3: "What issues does this customer frequently raise?"
**Required:** `service_case`
**Coverage:** NOT COVERED
- No schema has service case / complaint categorization data

### B4: "Is this customer dissatisfied?"
**Required:** `service_case`, `support_interaction`
**Coverage:** NOT COVERED
- No satisfaction scores, sentiment, or complaint patterns in any schema

### B5: "Which communication channel should I use?"
**Required:** `customer_profile`
**Coverage:** PARTIAL
- CDR has phone_numbers, email_addresses, physical_addresses (contact INFO exists)
- MISSING: preferred channel flag, communication preference, opt-in/opt-out

### B6: "Which applications are waiting for document verification?"
**Required:** `application` (with stage info)
**Coverage:** PARTIAL
- BPI has process event stages (concept:name) but uses Dutch financial process steps, not generic CRM stages like "Document Verification"
- Linked Application has `action_taken` (final outcome) but no intermediate stages
- MISSING: current stage enum with values like "Document Verification", "Assessment", "Approved"

### B7: "Which customers still have missing documents?"
**Required:** `missing_document`
**Coverage:** NOT COVERED
- No schema tracks required documents, received documents, or document status

### B8: "Show the complete application timeline for this customer."
**Required:** `application_stage_history`, `status_change_history`
**Coverage:** MOSTLY COVERED (via BPI)
- BPI LoanApplicationEvent has full event log with timestamps, activity names, lifecycle transitions
- Can reconstruct complete timeline from BPI events
- NOTE: BPI events are from a Dutch institution; activity names need mapping to English CRM stages

### B9: "Which applications have been inactive for more than 14 days?"
**Required:** `application_stage_history`
**Coverage:** DERIVABLE (via BPI)
- Can compute from max(time:timestamp) per case vs current date
- No other schema has stage timestamps

### B10: "Why was this application returned to the customer?"
**Required:** `status_change_history`
**Coverage:** PARTIAL
- BPI has lifecycle transitions (withdraw, suspend) but no explicit "returned to customer" reason
- Linked has `denial_reasons[]` but only for final denial, not interim returns
- MISSING: return reason field or interim status change reason

### B11: "Which customers need follow-up today?"
**Required:** `application`, `missing_document`
**Coverage:** NOT COVERED
- No follow-up scheduling, reminder dates, or SLA deadline fields in any schema

### B12: "Summarize everything I need before calling this customer."
**Required:** All datasets
**Coverage:** PARTIAL (same gaps as B1 + no talking-point generation data)

### B13: "Does customer C001 have an open case about a missing document?"
**Required:** `missing_document`, `application`, `customer`
**Coverage:** PARTIAL
- Customer identity: Covered (CDR/Linked)
- Missing document case: NOT COVERED

### B14: "What is the current status of application APP001?"
**Required:** `application`
**Coverage:** PARTIAL
- Linked Application has `action_taken` (Originated, Denied, Withdrawn, etc.) — final outcome only
- BPI can derive current process state from latest event
- MISSING: live application status field (Pending, In Review, Approved, etc.)

### B15: "When was the application status last updated?"
**Required:** `application`
**Coverage:** PARTIAL
- BPI: max(time:timestamp) per case gives last update
- Linked: has `created_at` but no `updated_at` on Application entity
- MISSING: explicit last_updated_at timestamp on application

### B16: "Which stage is the application currently in?"
**Required:** `application`
**Coverage:** DERIVABLE (via BPI)
- Latest event's concept:name gives current stage
- Not available in Linked or Lending Club schemas

### B17: "How long has the application existed at its current stage?"
**Required:** `application`
**Coverage:** DERIVABLE (via BPI)
- Compute from timestamp of stage-entry event to now

### B18: "What was the previous stage of application APP001?"
**Required:** `application`
**Coverage:** DERIVABLE (via BPI)
- Second-to-last distinct concept:name in event sequence

### B19: "When did the application move into document review?"
**Required:** `application`
**Coverage:** DERIVABLE (via BPI)
- Filter events for "W_Valideren aanvraag" (validate application) stage → timestamp
- Needs stage name mapping

### B20: "Which documents have been received?"
**Required:** `application`
**Coverage:** NOT COVERED
- No schema tracks individual document receipt/checklist

### B21: "Are submitted documents marked invalid or expired?"
**Required:** `application`
**Coverage:** NOT COVERED
- No schema has document validity status

### B22: "Is the application waiting for customer action or internal review?"
**Required:** `application`
**Coverage:** PARTIAL
- BPI lifecycle:transition (start/complete/suspend) can hint at wait state
- MISSING: explicit ownership indicator (customer vs. internal)

### B23: "Which team currently owns the application?"
**Required:** `application`
**Coverage:** PARTIAL
- BPI has `org:resource` (anonymized staff ID, e.g., User_42) but not team name
- MISSING: team assignment field

### B24: "Prepare a short handover summary for customer C001."
**Required:** All datasets
**Coverage:** PARTIAL (composite of B1 gaps)

---

## Customer.AI — Supported Questions Analysis

### C1: "What is my preferred contact method?"
**Required:** `customer_profile`
**Coverage:** NOT COVERED
- CDR has contact details (phone, email, address) but NO preference flag

### C2: "What language do I prefer for communication?"
**Required:** `customer_profile`
**Coverage:** NOT COVERED
- No language preference field in any schema

### C3: "Am I subscribed to marketing notifications?"
**Required:** `customer_profile`
**Coverage:** NOT COVERED
- No marketing opt-in/opt-out flag in any schema

### C4: "What support cases do I currently have open?"
**Required:** `service_case`
**Coverage:** NOT COVERED

### C5: "What happened to my previous support request?"
**Required:** `service_case`
**Coverage:** NOT COVERED

### C6: "When did I last contact support?"
**Required:** `support_interaction`
**Coverage:** NOT COVERED

### C7: "Can you summarize my recent support history?"
**Required:** `service_case`, `support_interaction`
**Coverage:** NOT COVERED

### C8: "What is the status of my application?"
**Required:** `application`
**Coverage:** PARTIAL
- Linked: `action_taken` (final outcome)
- BPI: derivable from latest event
- MISSING: user-friendly current status label

### C9: "What documents am I still missing?"
**Required:** `missing_document`
**Coverage:** NOT COVERED

### C10: "When did my application move to its current stage?"
**Required:** `application_stage_history`
**Coverage:** DERIVABLE (via BPI)

### C11: "How many reminders have I received?"
**Required:** `missing_document`
**Coverage:** NOT COVERED
- No reminder tracking in any schema

### C12: "Why is my application delayed?"
**Required:** `status_change_history`
**Coverage:** PARTIAL
- BPI can show timeline gaps but has no explicit delay reason
- Linked has `denial_reasons` for final denial only

### C13: "What is the next step in my application?"
**Required:** `application_stage_history`
**Coverage:** PARTIAL
- BPI can infer next step from process model (what typically follows current stage)
- Not explicitly stored

---

## Coverage Summary

### By Question

| ID | Question (short) | Coverage | Primary Gap |
|----|-------------------|----------|-------------|
| **B1** | Customer summary | PARTIAL | No support/service history |
| **B2** | Recent support contact? | NOT COVERED | No `support_interaction` |
| **B3** | Frequent issues? | NOT COVERED | No `service_case` |
| **B4** | Customer dissatisfied? | NOT COVERED | No satisfaction data |
| **B5** | Communication channel? | PARTIAL | No preference flag |
| **B6** | Apps waiting for doc verification? | PARTIAL | No stage field on application |
| **B7** | Customers with missing docs? | NOT COVERED | No `missing_document` |
| **B8** | Application timeline | MOSTLY COVERED | BPI events need stage mapping |
| **B9** | Inactive >14 days? | DERIVABLE | BPI timestamps |
| **B10** | Why returned to customer? | PARTIAL | No return reason |
| **B11** | Follow-up needed today? | NOT COVERED | No follow-up schedule |
| **B12** | Pre-call briefing | PARTIAL | Same as B1 |
| **B13** | Open case for missing doc? | PARTIAL | No `missing_document` |
| **B14** | Current app status? | PARTIAL | Only final outcome, not live status |
| **B15** | Last status update? | PARTIAL | No updated_at on application |
| **B16** | Current stage? | DERIVABLE | From BPI latest event |
| **B17** | Time in current stage? | DERIVABLE | From BPI timestamps |
| **B18** | Previous stage? | DERIVABLE | From BPI event sequence |
| **B19** | When moved to doc review? | DERIVABLE | From BPI timestamps |
| **B20** | Documents received? | NOT COVERED | No document checklist |
| **B21** | Invalid/expired docs? | NOT COVERED | No document validity |
| **B22** | Waiting for customer or internal? | PARTIAL | No ownership indicator |
| **B23** | Which team owns app? | PARTIAL | BPI has staff, not team |
| **B24** | Handover summary | PARTIAL | Composite gaps |
| **C1** | Preferred contact method | NOT COVERED | No preference flag |
| **C2** | Preferred language | NOT COVERED | No language field |
| **C3** | Marketing opt-in? | NOT COVERED | No marketing preference |
| **C4** | Open support cases? | NOT COVERED | No `service_case` |
| **C5** | Previous support outcome? | NOT COVERED | No `service_case` |
| **C6** | Last support contact? | NOT COVERED | No `support_interaction` |
| **C7** | Support history summary | NOT COVERED | No `service_case` or `support_interaction` |
| **C8** | Application status | PARTIAL | Only final outcome |
| **C9** | Missing documents | NOT COVERED | No `missing_document` |
| **C10** | When stage changed? | DERIVABLE | BPI timestamps |
| **C11** | Reminders received? | NOT COVERED | No reminder tracking |
| **C12** | Why delayed? | PARTIAL | No explicit delay reason |
| **C13** | Next step? | PARTIAL | Inferrable from BPI, not explicit |

### By Coverage Level

| Level | Count | Questions |
|-------|-------|-----------|
| MOSTLY COVERED | 1 | B8 |
| DERIVABLE (BPI) | 6 | B9, B16, B17, B18, B19, C10 |
| PARTIAL | 12 | B1, B5, B6, B10, B12, B13, B14, B15, B22, B23, B24, C8, C12, C13 |
| NOT COVERED | 18 | B2, B3, B4, B7, B11, B20, B21, C1, C2, C3, C4, C5, C6, C7, C9, C11 |

**Only 1 of 37 questions is mostly covered. 18 questions (49%) have zero coverage.**

---

## Missing Datasets

These logical datasets do not exist in any current schema and must be created:

### 1. `customer_profile` (preferences extension)

CDR CommonPerson has identity + contact info but lacks preference fields.

| Missing Field | Type | Needed By |
|---------------|------|-----------|
| `preferred_contact_channel` | enum (phone/email/sms/chat) | B5, C1 |
| `preferred_language` | string (ISO 639-1) | C2 |
| `marketing_opt_in` | boolean | C3 |
| `communication_preferences` | object | B5 |
| `satisfaction_score` | decimal | B4 |

**Recommendation:** Add these as fields to the Linked Customer entity.

### 2. `support_interaction`

Entirely missing. No existing schema has support contact records.

| Required Field | Type | Description |
|----------------|------|-------------|
| `interaction_id` | uuid | PK |
| `customer_id` | uuid (FK) | Links to Customer |
| `channel` | enum (phone/email/chat/branch) | Contact channel |
| `timestamp` | datetime | When interaction occurred |
| `topic` | string | Subject/category |
| `resolution` | string | Outcome |
| `agent_id` | string | Staff who handled it |
| `duration_minutes` | integer | Length of interaction |

**Recommendation:** Create as new entity in linked-schema or standalone.

### 3. `service_case`

Entirely missing. No existing schema tracks service cases (disputes, complaints, requests).

| Required Field | Type | Description |
|----------------|------|-------------|
| `case_id` | uuid | PK |
| `customer_id` | uuid (FK) | Links to Customer |
| `case_type` | enum (dispute/complaint/request/inquiry) | Case category |
| `subject` | string | Case subject |
| `status` | enum (open/in_progress/resolved/closed) | Current status |
| `priority` | enum (low/medium/high/critical) | Priority level |
| `created_at` | datetime | Case creation time |
| `resolved_at` | datetime | Resolution time (null if open) |
| `resolution_summary` | string | How it was resolved |
| `assigned_team` | string | Team handling the case |

**Recommendation:** Create as new entity.

### 4. `missing_document`

Entirely missing. No schema tracks document requirements or receipt.

| Required Field | Type | Description |
|----------------|------|-------------|
| `document_id` | uuid | PK |
| `application_id` | uuid (FK) | Links to Application |
| `customer_id` | uuid (FK) | Links to Customer |
| `document_type` | string | Document name (payslip, ID, proof of address) |
| `status` | enum (required/received/invalid/expired/verified) | Document status |
| `requested_at` | datetime | When document was requested |
| `received_at` | datetime | When document was received (null if pending) |
| `expiry_date` | date | Document expiry date (null if N/A) |
| `reminders_sent` | integer | Count of reminders sent |
| `last_reminder_at` | datetime | When last reminder was sent |
| `rejection_reason` | string | Why document was marked invalid |

**Recommendation:** Create as new entity linked to Application.

### 5. `application_stage_history`

Partially covered by BPI LoanApplicationEvent but needs CRM-specific stage model.

| Required Field | Type | Description |
|----------------|------|-------------|
| `history_id` | uuid | PK |
| `application_id` | uuid (FK) | Links to Application |
| `stage` | enum | CRM stage (Submitted/Document Verification/Assessment/Approved/etc.) |
| `entered_at` | datetime | When application entered this stage |
| `exited_at` | datetime | When application left this stage (null if current) |
| `assigned_team` | string | Team responsible at this stage |
| `assigned_to` | string | Individual staff member |
| `sla_deadline` | datetime | SLA deadline for this stage |

**Recommendation:** Either derive from BPI events with stage mapping, or create as new entity.

### 6. `status_change_history`

Partially derivable from BPI but lacks explicit change reasons.

| Required Field | Type | Description |
|----------------|------|-------------|
| `change_id` | uuid | PK |
| `application_id` | uuid (FK) | Links to Application |
| `old_status` | string | Previous status |
| `new_status` | string | New status |
| `changed_at` | datetime | When change occurred |
| `changed_by` | string | Who made the change |
| `reason` | string | Reason for status change |

**Recommendation:** Derive from BPI events with enrichment for reason field.

---

## Refusal Questions — Data Implications

The refusal categories confirm design constraints, not data gaps:

| Category | Implication |
|----------|-------------|
| Sensitive data exposure | Data exists (PII in CDR) but AI must mask/redact — **access control layer needed, not more data** |
| Decision-making | AI should not approve/deny — **model guardrails, not data** |
| Fabricating reasons | AI must cite data, not speculate — **RAG constraint, not data** |
| Data modification | AI is read-only — **permission model, not data** |
| Data quality bypass | AI must respect quality flags — **need quarantine/quality status fields on records** |
| Access control | AI must respect team boundaries — **need RBAC metadata** |
| Regulated advice | AI cannot give financial advice — **compliance guardrails** |

**Data-relevant gaps from refusal rules:**
- Need `data_quality_status` field (quarantined, validated, stale) on key entities
- Need `access_level` or `restricted_team` metadata for row-level security
- Need `record_source_reference` to satisfy "source reference is missing" check

---

## Recommendations

### Priority 1: Create missing CRM entities (blocks 18 questions)
1. **`service_case`** — unblocks B2, B3, B4, C4, C5, C7
2. **`support_interaction`** — unblocks B2, C6, C7
3. **`missing_document`** — unblocks B7, B11, B13, B20, B21, C9, C11

### Priority 2: Extend existing entities (fixes 12 partial questions)
4. **Customer preferences** — add `preferred_channel`, `preferred_language`, `marketing_opt_in` to Customer → unblocks B5, C1, C2, C3
5. **Application stage/status** — add `current_stage`, `current_stage_entered_at`, `assigned_team`, `waiting_on` to Application → unblocks B6, B14, B15, B16, B22, B23, C8

### Priority 3: Create history entities (makes 6 derivable questions explicit)
6. **`application_stage_history`** — explicit stage timeline → improves B8, B9, B16-B19, C10
7. **`status_change_history`** — explicit change log → improves B10, C12, C13

### Data source mapping for new entities

| New Entity | Can Generate From | Strategy |
|------------|-------------------|----------|
| `service_case` | No existing source | Fully synthetic generation needed |
| `support_interaction` | No existing source | Fully synthetic generation needed |
| `missing_document` | No existing source | Synthetic, linked to Application lifecycle |
| Customer preferences | CDR partial | Extend with synthetic preference fields |
| Application stage fields | BPI events | Derive from BPI event log + synthetic CRM stages |
| `application_stage_history` | BPI LoanApplicationEvent | Transform BPI events → CRM stage records |
| `status_change_history` | BPI LoanApplicationEvent | Transform BPI lifecycle transitions → change log |

---

## Post-Generation Coverage

**Status:** Synthetic data generation pipeline implemented (`synthetic-data/run_all.py`)
**Generated:** 2026-07-16

### What Was Built

| Deliverable | Location | Status |
|-------------|----------|--------|
| CRM schema (6 entities) | `ref-schema/crm-schema/crm-schema.json` | DONE |
| CRM schema docs | `ref-schema/crm-schema/crm-schema.md` | DONE |
| CDR generator | `synthetic-data/generate_cdr.py` | DONE |
| BPI generator | `synthetic-data/generate_bpi.py` | DONE |
| Lending Club generator | `synthetic-data/generate_lending_club.py` | DONE |
| CRM preferences generator | `synthetic-data/generate_crm_preferences.py` | DONE |
| CRM interactions generator | `synthetic-data/generate_crm_interactions.py` | DONE |
| CRM documents generator | `synthetic-data/generate_crm_documents.py` | DONE |
| CRM history generator | `synthetic-data/generate_crm_history.py` | DONE |
| Pipeline orchestrator | `synthetic-data/run_all.py` | DONE |

### global_id Linking Key

All 13 tables carry `global_id` (UUID) — the single cross-dataset join key identifying a customer.

```
customers.global_id
  ├── banking_accounts.global_id
  ├── banking_transactions.global_id
  ├── loan_applications.global_id
  │     ├── loan_application_events.global_id
  │     ├── missing_documents.global_id
  │     ├── application_stage_history.global_id
  │     └── status_change_history.global_id
  ├── accepted_loans.global_id
  ├── rejected_applications.global_id
  ├── customer_preferences.global_id
  ├── support_interactions.global_id
  └── service_cases.global_id
```

### Updated Coverage

| Level | Before | After |
|-------|--------|-------|
| MOSTLY COVERED | 1 | 37 |
| DERIVABLE (BPI) | 6 | 0 (now explicit) |
| PARTIAL | 12 | 0 |
| NOT COVERED | 18 | **0** |

**All 37 AI Questions are now answerable once `run_all.py` completes.**

### Per-Question Resolution

| ID | Question | Was | Now | Resolved By |
|----|----------|-----|-----|-------------|
| B1 | Customer summary | PARTIAL | COVERED | All 13 tables joinable via global_id |
| B2 | Recent support contact? | NOT COVERED | COVERED | `support_interactions` |
| B3 | Frequent issues? | NOT COVERED | COVERED | `service_cases.caseType` distribution |
| B4 | Customer dissatisfied? | NOT COVERED | COVERED | `customer_preferences.satisfactionScore` + `service_cases` |
| B5 | Communication channel? | PARTIAL | COVERED | `customer_preferences.preferredContactChannel` |
| B6 | Apps waiting for doc verification? | PARTIAL | COVERED | `application_stage_history.stage` |
| B7 | Customers with missing docs? | NOT COVERED | COVERED | `missing_documents.status=required` |
| B8 | Application timeline | MOSTLY COVERED | COVERED | `application_stage_history` explicit |
| B9 | Inactive >14 days? | DERIVABLE | COVERED | `application_stage_history.exitedAt` null + `enteredAt` |
| B10 | Why returned to customer? | PARTIAL | COVERED | `status_change_history.reason` |
| B11 | Follow-up needed today? | NOT COVERED | COVERED | `missing_documents.remindersSent` + `lastReminderAt` |
| B12 | Pre-call briefing | PARTIAL | COVERED | All tables via global_id join |
| B13 | Open case for missing doc? | PARTIAL | COVERED | `missing_documents` + `service_cases` |
| B14 | Current app status? | PARTIAL | COVERED | `application_stage_history` latest stage |
| B15 | Last status update? | PARTIAL | COVERED | `status_change_history.changedAt` |
| B16 | Current stage? | DERIVABLE | COVERED | `application_stage_history` where exitedAt IS NULL |
| B17 | Time in current stage? | DERIVABLE | COVERED | Current stage `enteredAt` vs now |
| B18 | Previous stage? | DERIVABLE | COVERED | Second-to-last row in `application_stage_history` |
| B19 | When moved to doc review? | DERIVABLE | COVERED | `stage=Document_Verification.enteredAt` |
| B20 | Documents received? | NOT COVERED | COVERED | `missing_documents.status IN (received, verified)` |
| B21 | Invalid/expired docs? | NOT COVERED | COVERED | `missing_documents.status IN (invalid, expired)` |
| B22 | Waiting for customer or internal? | PARTIAL | COVERED | `application_stage_history.assignedTeam` |
| B23 | Which team owns app? | PARTIAL | COVERED | `application_stage_history.assignedTeam` where exitedAt IS NULL |
| B24 | Handover summary | PARTIAL | COVERED | All tables via global_id |
| C1 | Preferred contact method | NOT COVERED | COVERED | `customer_preferences.preferredContactChannel` |
| C2 | Preferred language | NOT COVERED | COVERED | `customer_preferences.preferredLanguage` |
| C3 | Marketing opt-in? | NOT COVERED | COVERED | `customer_preferences.marketingOptIn` |
| C4 | Open support cases? | NOT COVERED | COVERED | `service_cases.status=open` |
| C5 | Previous support outcome? | NOT COVERED | COVERED | `service_cases.resolutionSummary` |
| C6 | Last support contact? | NOT COVERED | COVERED | `support_interactions.timestamp` max per customer |
| C7 | Support history summary | NOT COVERED | COVERED | `support_interactions` + `service_cases` |
| C8 | Application status | PARTIAL | COVERED | `application_stage_history` latest stage |
| C9 | Missing documents | NOT COVERED | COVERED | `missing_documents.status=required` |
| C10 | When stage changed? | DERIVABLE | COVERED | `application_stage_history.enteredAt` |
| C11 | Reminders received? | NOT COVERED | COVERED | `missing_documents.remindersSent` sum per customer |
| C12 | Why delayed? | PARTIAL | COVERED | `status_change_history.reason` |
| C13 | Next step? | PARTIAL | COVERED | `application_stage_history` current stage → known next stage |

### How to Run

```bash
# Prerequisites
pip install faker numpy pandas pyspark databricks-connect

# Configure
cd 0-ai-trust/synthetic-data
# Edit config.py: set CATALOG, SCHEMA, OUTPUT_PATH

# Run pipeline
python3 run_all.py

# Validate FK integrity
python3 -c "
import pandas as pd
customers = pd.read_parquet('output/customers/')
all_ids = set(customers['global_id'])
for table in ['banking_accounts','banking_transactions','loan_applications',
              'customer_preferences','support_interactions','service_cases',
              'missing_documents','application_stage_history','status_change_history',
              'accepted_loans','rejected_applications']:
    df = pd.read_parquet(f'output/{table}/')
    orphans = ~df['global_id'].isin(all_ids)
    status = 'PASS' if orphans.sum() == 0 else f'FAIL ({orphans.sum()} orphans)'
    print(f'{table}: {len(df):,} rows  FK {status}')
"
```
