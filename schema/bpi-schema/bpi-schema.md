# BPI Challenge 2017 — Schema Reference

## Purpose

This schema documents the **BPI Challenge 2017** dataset, a process mining event log from a Dutch financial institution's personal loan application process. It covers the full lifecycle of loan applications from creation through acceptance or denial, including intermediate offer creation, fraud assessment, and workflow tasks.

- **Source file:** `bpi_2017_cleaned.csv`
- **Format:** CSV, 19 columns, ~1.2 million rows
- **Period:** 2016–2017
- **Grain:** One row per event; multiple events per case (loan application)

---

## Entity Summary

| Entity | Grain | Row Count | Description |
|--------|-------|-----------|-------------|
| `LoanApplicationEvent` | One row per event | ~1.2M | Raw event log record from process mining |
| `LoanApplication` | One row per case | ~492K (est.) | Case-level aggregated view of a loan application |

---

## Entity: LoanApplicationEvent

The raw event log row. Each row represents a single activity performed by a staff member (or system) on a loan application case.

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Action` | enum | Yes | High-level action: Created, Deleted, Obtained, Released, statechange |
| `org:resource` | string | Yes | Anonymized staff ID (e.g. User_1) who performed the event |
| `concept:name` | enum | Yes | Activity name identifying the process step (24 values) |
| `EventOrigin` | enum | Yes | System that generated the event: Application, Offer, Workflow |
| `EventID` | string | Yes | Unique event identifier (ApplState_*, Offer_*, Application_*) |
| `lifecycle:transition` | enum | Yes | WfMC lifecycle phase: complete, start, schedule, suspend, resume, withdraw, ate_abort |
| `time:timestamp` | datetime | Yes | UTC event timestamp (ISO 8601 with timezone) |
| `case:concept:name` | string | Yes | Case ID linking events to same loan application (Application_XXXXXXXXX) |
| `case:LoanGoal` | enum | Yes | Loan purpose declared by applicant (13 values) |
| `case:ApplicationType` | enum | Yes | Credit type: New credit or Limit raise |
| `case:RequestedAmount` | decimal | Yes | Loan amount requested in euros |
| `OfferID` | string | No | Offer identifier (Offer_XXXXXXXXX); blank for non-offer events |
| `OfferedAmount` | decimal | No | Amount offered in euros; blank for non-offer events |
| `FirstWithdrawalAmount` | decimal | No | First withdrawal amount in euros; blank when not applicable |
| `NumberOfTerms` | decimal | No | Number of repayment terms (stored as float); blank when not applicable |
| `MonthlyCost` | decimal | No | Monthly repayment in euros; blank when not applicable |
| `CreditScore` | decimal | No | Applicant credit score (0–1000+); blank when not assessed |
| `Accepted` | boolean | No | Whether offer was accepted: True, False, or blank |
| `Selected` | boolean | No | Whether offer was selected by customer: True, False, or blank |

### concept:name Values

| Value | Prefix | Description |
|-------|--------|-------------|
| `A_Create Application` | A_ | Application entity created |
| `A_Submitted` | A_ | Application submitted by customer |
| `A_Accepted` | A_ | Application accepted by institution |
| `A_Denied` | A_ | Application denied |
| `A_Cancelled` | A_ | Application cancelled |
| `A_Complete` | A_ | Application processing completed |
| `A_Concept` | A_ | Application in concept/draft state |
| `A_Incomplete` | A_ | Application marked incomplete |
| `A_Pending` | A_ | Application pending review |
| `A_Validating` | A_ | Application under validation |
| `O_Accepted` | O_ | Offer accepted by customer |
| `O_Cancelled` | O_ | Offer cancelled |
| `O_Create Offer` | O_ | New offer created for application |
| `O_Created` | O_ | Offer entity created |
| `O_Refused` | O_ | Offer refused by customer |
| `O_Returned` | O_ | Offer returned for revision |
| `O_Sent (mail and online)` | O_ | Offer sent via mail and online |
| `O_Sent (online only)` | O_ | Offer sent via online channel only |
| `W_Assess potential fraud` | W_ | Workflow task: fraud assessment |
| `W_Call after offers` | W_ | Workflow task: follow-up call after offers |
| `W_Call incomplete files` | W_ | Workflow task: call about incomplete files |
| `W_Complete application` | W_ | Workflow task: complete application processing |
| `W_Handle leads` | W_ | Workflow task: handle new leads |
| `W_Validate application` | W_ | Workflow task: validate application details |

---

## Entity: LoanApplication

Case-level aggregated view. One record per unique `case:concept:name`. Derived from the raw event log by grouping and aggregating events.

### Field Table

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `application_id` | string | Yes | Loan application ID (from case:concept:name) |
| `application_type` | enum | Yes | Credit type: New credit or Limit raise |
| `loan_goal` | enum | Yes | Loan purpose declared by applicant |
| `requested_amount` | decimal | Yes | Amount requested in euros |
| `final_status` | enum | No | Derived from last A_* activity: Accepted, Denied, Cancelled, Complete, Incomplete, Pending |
| `credit_score` | decimal | No | Credit score from offer events; null if never assessed |
| `num_offers` | integer | No | Count of O_Create Offer events for this case |
| `accepted_offer_amount` | decimal | No | OfferedAmount where Accepted=True; null if no offer accepted |
| `monthly_cost` | decimal | No | MonthlyCost where Selected=True; null if no offer selected |
| `number_of_terms` | integer | No | NumberOfTerms where Selected=True; null if no offer selected |
| `total_events` | integer | No | Total event count for this case |

---

## Process Flow

The loan application follows this typical activity sequence:

```
A_Create Application
  └─► A_Submitted
        └─► A_Validating
              ├─► W_Validate application (workflow tasks run in parallel)
              ├─► W_Handle leads
              ├─► W_Assess potential fraud
              └─► A_Accepted
                    └─► O_Create Offer ──► O_Created ──► O_Sent (mail and online)
                          ├─► O_Accepted ──► W_Call after offers ──► A_Complete
                          └─► O_Refused / O_Cancelled ──► (loop: new offer or denial)
              └─► A_Denied / A_Cancelled / A_Incomplete
```

- **A_ prefix** activities are milestones on the application entity.
- **O_ prefix** activities are events on individual offer entities.
- **W_ prefix** activities are human workflow tasks assigned to staff.
- A single application may produce multiple offers (num_offers > 1) before a final decision.

---

## Data Quality Notes

### Blank Values
- `OfferedAmount`, `FirstWithdrawalAmount`, `NumberOfTerms`, `MonthlyCost`, `CreditScore`, `Accepted`, `Selected`, `OfferID` are blank (empty string, not null) for events that do not involve an offer.
- When aggregating to the `LoanApplication` level, treat blank as null.

### Event Ordering
- Events must be ordered by `time:timestamp` within each `case:concept:name` to reconstruct the process trace.
- `lifecycle:transition` value `complete` is the most common; `schedule` and `start` events often precede `complete` for the same logical activity.

### Case vs. Event Level Attributes
- `case:LoanGoal`, `case:ApplicationType`, and `case:RequestedAmount` are **case-level** attributes repeated on every event row for the same case. They are constant per case and should be de-duplicated when building a case-level view.
- `concept:name`, `EventOrigin`, `EventID`, `lifecycle:transition`, `time:timestamp`, `org:resource`, and offer-specific fields (`OfferID`, `OfferedAmount`, etc.) are **event-level** attributes and vary per row.

### Numeric Types
- Numeric fields (`case:RequestedAmount`, `OfferedAmount`, `FirstWithdrawalAmount`, `NumberOfTerms`, `MonthlyCost`, `CreditScore`) are stored as float strings in the CSV (e.g. `"20000.0"`, `"44.0"`). Cast to appropriate numeric types on ingestion.
- `NumberOfTerms` represents a whole number of payment periods but is stored as float.

### Accepted / Selected Semantics
- `Accepted=True` means the customer accepted the offer terms.
- `Selected=True` means the customer chose this specific offer (when multiple were presented). A `Selected` offer is typically also `Accepted`, but not all `Accepted` offers are `Selected`.
