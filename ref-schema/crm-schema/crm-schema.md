# CRM Schema

## Purpose

This schema documents the six **CRM** entities generated synthetically to simulate customer relationship management data for a financial institution.

- **Source:** Synthetically generated (no real source file)
- **Format:** Parquet, partitioned by `global_id`
- **Period:** Last 12 months from generation date (`START_DATE` → `END_DATE` in `config.py`)
- **Grain:** One row per entity record (one per customer for `CustomerPreferences`; one per interaction, case, document, or history event for all others)
- **Note:** All six entities share `global_id` (UUID) as the cross-dataset linking key to CDR and BPI tables

---

## Entity Summary

| # | Entity | Rows (default) | Description |
|---|--------|---------------|-------------|
| 1 | `CustomerPreferences` | 10,000 | Communication preferences and satisfaction score; 1:1 with CDR customers |
| 2 | `SupportInteraction` | 30,000 | Every customer contact with support staff across all channels |
| 3 | `ServiceCase` | 8,000 | CRM case records for complaints, disputes, requests, and inquiries |
| 4 | `MissingDocument` | 15,000 | Document checklist tracker per loan application |
| 5 | `ApplicationStageHistory` | 25,000 | Stage timeline for loan applications with SLA tracking |
| 6 | `StatusChangeHistory` | 20,000 | Audit log of every application status transition |

---

## Entity: CustomerPreferences

Customer communication preferences and satisfaction score. One record per customer (1:1 with CDR `customers`).

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `globalId` | string (uuid) | Yes | Cross-dataset linking key — matches `global_id` in CDR customers table |
| `customerId` | string | Yes | Unique customer identifier, foreign key to CDR CommonPerson |
| `preferredContactChannel` | enum | Yes | Customer's preferred communication channel |
| `preferredLanguage` | string | Yes | ISO 639-1 language code for preferred communications language |
| `marketingOptIn` | boolean | Yes | Whether customer consents to marketing communications |
| `satisfactionScore` | decimal | No | Overall satisfaction score (1.0–5.0, one decimal place); null if never rated |

### preferredContactChannel Values

| Value | Description |
|-------|-------------|
| `phone` | Voice call |
| `email` | Email message |
| `sms` | SMS text message |
| `chat` | Live chat |

### preferredLanguage Values

| Value | Language |
|-------|----------|
| `en` | English |
| `es` | Spanish |
| `zh` | Chinese |
| `ar` | Arabic |
| `fr` | French |
| `vi` | Vietnamese |

---

## Entity: SupportInteraction

Record of every customer contact with support staff. Multiple records per customer (1:N with CDR `customers`).

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `globalId` | string (uuid) | Yes | Cross-dataset linking key — matches `global_id` in CDR customers table |
| `interactionId` | string (uuid) | Yes | Unique identifier for this support interaction |
| `customerId` | string | Yes | Customer who initiated the contact, FK to CDR CommonPerson |
| `channel` | enum | Yes | Contact channel used |
| `timestamp` | datetime | Yes | When the interaction occurred (ISO 8601 with Z suffix) |
| `topic` | enum | Yes | Subject category of the interaction |
| `resolution` | enum | Yes | Outcome of the interaction |
| `agentId` | string | Yes | Identifier of the staff member who handled the interaction |
| `durationMinutes` | integer | Yes | Length of the interaction in whole minutes (positive integer) |

### channel Values

| Value | Description |
|-------|-------------|
| `phone` | Voice call to support line |
| `email` | Email correspondence |
| `chat` | Live chat session |
| `branch` | In-person branch visit |

### topic Values

| Value | Description |
|-------|-------------|
| `Account_Inquiry` | Questions about account status or details |
| `Application_Status` | Queries about a loan or product application |
| `Document_Upload` | Assistance with submitting required documents |
| `Complaint` | Formal complaint about a product or service |
| `General` | General enquiry not fitting other topics |

### resolution Values

| Value | Description |
|-------|-------------|
| `resolved` | Issue fully resolved during interaction |
| `escalated` | Handed to a senior agent or specialist team |
| `follow_up_required` | Resolution requires follow-up action |
| `unresolved` | Issue could not be resolved |

---

## Entity: ServiceCase

CRM service case record for complaints, disputes, requests, and inquiries. Multiple records per customer (1:N with CDR `customers`).

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `globalId` | string (uuid) | Yes | Cross-dataset linking key — matches `global_id` in CDR customers table |
| `caseId` | string (uuid) | Yes | Unique case identifier |
| `customerId` | string | Yes | Customer this case belongs to, FK to CDR CommonPerson |
| `caseType` | enum | Yes | Category of service case |
| `subject` | string | Yes | Brief description of the case subject |
| `status` | enum | Yes | Current case status |
| `priority` | enum | Yes | Case priority level |
| `createdAt` | datetime | Yes | When the case was created (ISO 8601 with Z suffix) |
| `resolvedAt` | datetime | No | When the case was resolved; null if still open |
| `resolutionSummary` | string | No | Summary of how the case was resolved; null if still open |
| `assignedTeam` | enum | Yes | Team currently responsible for the case |

### caseType Values

| Value | Description |
|-------|-------------|
| `dispute` | Customer disputes a charge, decision, or transaction |
| `complaint` | Formal complaint about service or product |
| `request` | Service or product change request |
| `inquiry` | Information or status inquiry |

### status Values

| Value | Description |
|-------|-------------|
| `open` | Case received, not yet in progress |
| `in_progress` | Actively being worked |
| `resolved` | Resolution reached, pending close confirmation |
| `closed` | Fully closed with no further action |

### priority Values

| Value | Description |
|-------|-------------|
| `low` | Minor impact; resolve within 10 business days |
| `medium` | Moderate impact; resolve within 5 business days |
| `high` | Significant customer impact; resolve within 2 business days |
| `critical` | Regulatory or financial exposure; resolve within 24 hours |

### assignedTeam Values

| Value | Description |
|-------|-------------|
| `Support` | Tier-1 general support team |
| `Complaints` | Dedicated complaints handling team |
| `Escalations` | Senior escalation specialists |
| `Fraud` | Fraud investigation unit |

---

## Entity: MissingDocument

Document checklist tracker per loan application. Multiple records per application (1:N with BPI `loan_applications`).

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `globalId` | string (uuid) | Yes | Cross-dataset linking key — matches `global_id` in CDR customers table |
| `documentId` | string (uuid) | Yes | Unique document record identifier |
| `applicationId` | string | Yes | Loan application requiring this document, FK to BPI LoanApplication |
| `customerId` | string | Yes | Customer who must provide the document, FK to CDR CommonPerson |
| `documentType` | enum | Yes | Type of document required |
| `status` | enum | Yes | Current document status |
| `requestedAt` | datetime | Yes | When the document was first requested (ISO 8601 with Z suffix) |
| `receivedAt` | datetime | No | When the document was received; null if not yet submitted |
| `expiryDate` | date | No | Document expiry date (YYYY-MM-DD); null if not applicable |
| `remindersSent` | integer | Yes | Number of reminder notifications sent (0–10) |
| `lastReminderAt` | datetime | No | Timestamp of the most recent reminder; null if no reminders sent |
| `rejectionReason` | string | No | Reason document was marked invalid; null unless status is `invalid` |

### documentType Values

| Value | Description |
|-------|-------------|
| `payslip` | Recent payslip (income verification) |
| `ID_proof` | Government-issued photo identification |
| `address_proof` | Proof of current residential address |
| `bank_statement` | Recent bank or financial account statement |
| `tax_return` | Most recent lodged tax return |
| `other` | Any other supporting document |

### status Values

| Value | Description |
|-------|-------------|
| `required` | Document requested but not yet received |
| `received` | Document submitted, pending verification |
| `invalid` | Document rejected (see `rejectionReason`) |
| `expired` | Document past its validity date |
| `verified` | Document accepted and verified |

---

## Entity: ApplicationStageHistory

CRM stage timeline for loan applications. Multiple records per application (1:N with BPI `loan_applications`).

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `globalId` | string (uuid) | Yes | Cross-dataset linking key — matches `global_id` in CDR customers table |
| `historyId` | string (uuid) | Yes | Unique history record identifier |
| `applicationId` | string | Yes | Application moving through stages, FK to BPI LoanApplication |
| `stage` | enum | Yes | CRM stage name (see BPI mapping note below) |
| `enteredAt` | datetime | Yes | When the application entered this stage (ISO 8601 with Z suffix) |
| `exitedAt` | datetime | No | When the application left this stage; null if current stage |
| `assignedTeam` | enum | Yes | Team responsible at this stage |
| `assignedTo` | string | Yes | Staff member assigned to this stage (e.g. `User_031`) |
| `slaDeadline` | datetime | Yes | SLA deadline for completing this stage (ISO 8601 with Z suffix) |

### stage Values

| Value | Description |
|-------|-------------|
| `Submitted` | Application received from customer |
| `Document_Verification` | Required documents being collected and verified |
| `Assessment` | Application under assessment by underwriting |
| `Credit_Check` | Credit score and risk evaluation in progress |
| `Approved` | Application fully approved |
| `Conditionally_Approved` | Approved subject to additional conditions |
| `Denied` | Application denied |
| `Withdrawn` | Customer withdrew the application |
| `Cancelled` | Application cancelled by the institution |

**BPI mapping:** `W_Completeren_aanvraag` → `Document_Verification` | `W_Valideren_aanvraag` → `Assessment` | `A_Accepted` → `Approved` | `A_Denied` → `Denied`

### assignedTeam Values

| Value | Description |
|-------|-------------|
| `Intake` | Initial application intake team |
| `Review` | Document review team |
| `Underwriting` | Loan underwriting team |
| `Compliance` | Compliance and risk team |

---

## Entity: StatusChangeHistory

Audit log of every application status transition with change reason. Multiple records per application (1:N with BPI `loan_applications`).

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `globalId` | string (uuid) | Yes | Cross-dataset linking key — matches `global_id` in CDR customers table |
| `changeId` | string (uuid) | Yes | Unique change record identifier |
| `applicationId` | string | Yes | Application that changed status, FK to BPI LoanApplication |
| `oldStatus` | string | Yes | Previous stage or status value |
| `newStatus` | string | Yes | New stage or status value |
| `changedAt` | datetime | Yes | When the status change occurred (ISO 8601 with Z suffix) |
| `changedBy` | string | Yes | Staff member or system that triggered the change |
| `reason` | enum | Yes | Reason for the status change |

### reason Values

| Value | Description |
|-------|-------------|
| `customer_request` | Customer requested the status change |
| `document_rejected` | Required document was rejected, halting progress |
| `credit_fail` | Credit assessment failed, triggering denial |
| `additional_info_required` | More information needed before progression |
| `approved_by_officer` | Loan officer manually approved the application |
| `auto_progression` | System-driven automatic stage progression |
| `withdrawn_by_customer` | Customer voluntarily withdrew the application |

---

## FK Relationships

```
CDR CommonPerson ─────── global_id ──────────────────┐
        │                                             │
        ├──→ CustomerPreferences (1:1)                │
        ├──→ SupportInteraction  (1:N)                │
        └──→ ServiceCase         (1:N)                │
                                                      │
BPI LoanApplication ──── global_id ─────────────────→┤
        │                                             │
        ├──→ MissingDocument          (1:N)           │
        ├──→ ApplicationStageHistory  (1:N)           │
        └──→ StatusChangeHistory      (1:N)           │
                                                      │
Lending Club AcceptedLoan ── global_id ─────────────→┘
Lending Club RejectedApplication ── global_id ──────→┘
```

BPI `LoanApplication.applicationId` is a secondary FK used by `MissingDocument`, `ApplicationStageHistory`, and `StatusChangeHistory` to link within the BPI dataset.

---

## Synthetic Data Distributions

| Entity | Field | Distribution |
|--------|-------|-------------|
| `CustomerPreferences` | `preferredContactChannel` | phone 35%, email 40%, sms 15%, chat 10% |
| `CustomerPreferences` | `preferredLanguage` | en 60%, es 15%, zh 10%, ar 7%, fr 5%, vi 3% |
| `CustomerPreferences` | `marketingOptIn` | True 65%, False 35% |
| `CustomerPreferences` | `satisfactionScore` | Log-normal (μ=1.3 if optIn else 1.0, σ=0.2), clamped 1.0–5.0 |
| `SupportInteraction` | `channel` | phone 40%, email 30%, chat 20%, branch 10% |
| `SupportInteraction` | `topic` | Account_Inquiry 25%, Application_Status 30%, Document_Upload 20%, Complaint 15%, General 10% |
| `SupportInteraction` | `resolution` | resolved 55%, escalated 15%, follow_up_required 20%, unresolved 10% |
| `SupportInteraction` | `durationMinutes` | Log-normal (μ=2.5, σ=0.6), clamped 1–120 |
| `ServiceCase` | `caseType` | dispute 20%, complaint 30%, request 25%, inquiry 25% |
| `ServiceCase` | `status` | open 20%, in_progress 35%, resolved 30%, closed 15% |
| `ServiceCase` | `priority` | low 35%, medium 40%, high 20%, critical 5% |
| `ServiceCase` | `assignedTeam` | Support 50%, Complaints 25%, Escalations 15%, Fraud 10% |
| `MissingDocument` | `documentType` | payslip 30%, ID_proof 25%, address_proof 20%, bank_statement 15%, tax_return 7%, other 3% |
| `MissingDocument` | `status` | required 35%, received 20%, invalid 10%, expired 5%, verified 30% |
| `ApplicationStageHistory` | `stage` | Submitted 15%, Document_Verification 20%, Assessment 18%, Credit_Check 15%, Approved 12%, Conditionally_Approved 8%, Denied 7%, Withdrawn 3%, Cancelled 2% |
| `ApplicationStageHistory` | `assignedTeam` | Intake 20%, Review 30%, Underwriting 35%, Compliance 15% |
| `StatusChangeHistory` | `reason` | auto_progression 30%, approved_by_officer 15%, credit_fail 15%, additional_info_required 15%, document_rejected 10%, customer_request 10%, withdrawn_by_customer 5% |

---

## Data Quality Notes

### Nullable Fields

| Entity | Nullable Fields |
|--------|----------------|
| `CustomerPreferences` | `satisfactionScore` |
| `ServiceCase` | `resolvedAt`, `resolutionSummary` |
| `MissingDocument` | `receivedAt`, `expiryDate`, `lastReminderAt`, `rejectionReason` |
| `ApplicationStageHistory` | `exitedAt` |

### Date Formats

- **Timestamps** (`datetime` fields): ISO 8601 with UTC `Z` suffix — e.g. `2025-11-14T09:23:45.000Z`
- **Dates** (`date` fields): `YYYY-MM-DD` — e.g. `2030-06-15`

### global_id Integrity

- Format: UUID v4 string (e.g. `a1b2c3d4-e5f6-7890-abcd-ef1234567890`)
- FK contract: every `global_id` in a child CRM table **must** exist in the CDR `customers.global_id` column
- The pipeline validation step in `run_all.py` enforces this constraint and reports PASS/FAIL per table

### Null Handling

- Missing optional values are represented as JSON `null` in samples and as Parquet null (not empty string)
- Empty string is never used to represent a missing value
- `rejectionReason` is non-null only when `MissingDocument.status = "invalid"`
- `resolvedAt` and `resolutionSummary` are non-null only when `ServiceCase.status ∈ {resolved, closed}`
