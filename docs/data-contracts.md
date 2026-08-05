# Data Contract Specification

This document defines the contract schema for the Silver and Gold layers of the 0-ai-trust pipeline. Bronze has no data contract -- raw data is poured as-is; its schema is declared in bootstrap SQL and pipeline code. The `source_inventory.yml` serves as the routing manifest.

## Why Only Silver and Gold

| Layer | Role | Contract needed? |
|-------|------|------------------|
| Source | Routing manifest | `source_inventory.yml` (exists) |
| Bronze | Raw immutable landing | No -- schema lives in code (`v001_create_external_objects.sql`, `*_ingestion.py`) |
| Silver | Validated data products | **Yes** -- quality rules, PII classification, AI policy |
| Gold | AI-ready scope gates | **Yes** -- access control, event scope, AI constraints |

## Contract Lifecycle

```
DRAFT -> APPROVED -> ACTIVE -> DEPRECATED
```

- `DRAFT`: under development, not enforced at runtime
- `APPROVED`: reviewed and accepted, can be activated
- `ACTIVE`: enforced by `data_contract_loader.py` (only `APPROVED` and `ACTIVE` pass `require_active=True`)
- `DEPRECATED`: superseded by a newer version, retained for lineage

---

## Silver Contract Schema

One contract per business entity (data product). Silver is the heaviest contract -- it carries quality enforcement, PII classification, freshness SLAs, and AI policy. All rules must be **executable** (`id` + `expression` compiled to Spark SQL by `data_quality_validator.py`).

### Template

```yaml
# ─── Identity ───────────────────────────────────────────────────
product: <table/view name in silver schema>
version: <semver>
contract_status: DRAFT | APPROVED | ACTIVE | DEPRECATED
owner: <domain>-domain
producer: <source-system>-platform
consumers: [<downstream AI/app consumers>]
description: <what this product represents and how it is built>

# ─── Keys ───────────────────────────────────────────────────────
canonical_key: <primary key column>
customer_key: global_id                    # universal FK across all entities
application_key: <optional, if entity links to applications>

# ─── Critical Data Elements ────────────────────────────────────
critical_data_elements: [<columns that must never be null/invalid>]
lifecycle_stage: Activate | Trace | Assess | Monitor | Resolve | Maintain

# ─── Source Lineage ─────────────────────────────────────────────
sources:
  - type: DATABASE | EVENT | FILE
    ingestion_mechanism: DEBEZIUM_CDC | APACHE_KAFKA | S3_MICROBATCH
    bronze_sink: cdc_changes | kafka_events | file_arrivals
    source_dataset: <schema.table or topic name>
    fields: [<fields consumed from this source>]
  - ...                                    # multiple sources per product

freshness_sla: <max acceptable staleness, e.g. "24 hours">
lineage_l1: <human-readable end-to-end lineage path>
lineage_l2_required: true | false          # whether detailed column lineage is needed

known_source_limitations:                  # optional
  - <limitation description>

# ─── Output Schema ──────────────────────────────────────────────
# Every column the Silver product exposes, with type, classification,
# masking strategy, allowed values, and example.
#
# Classifications:
#   INTERNAL      - safe for internal analytics, no special handling
#   CONFIDENTIAL  - PII, must be masked/redacted before AI consumption
#   RESTRICTED    - sensitive business data, internal-only access
#
# Masking strategies (for CONFIDENTIAL/RESTRICTED):
#   hash          - deterministic one-way hash (joinable across tables)
#   redact        - partial or full redaction (e.g. j***@email.com)
#   tokenise      - reversible token (for authorised re-identification)
#   remove        - excluded entirely from downstream output
#
output_schema:
  - name: <column_name>
    type: STRING | INTEGER | BIGINT | BOOLEAN | TIMESTAMP | DOUBLE
    required: true | false
    classification: INTERNAL | CONFIDENTIAL | RESTRICTED
    masking: hash | redact | tokenise | remove    # only for CONFIDENTIAL/RESTRICTED
    allowed_values: [<valid values>]               # only for categorical columns
    example: <representative value>
    description: <what this column means>          # optional, for non-obvious columns
  - ...

# ─── Quality Rules ──────────────────────────────────────────────
# Rules are executable: `expression` is compiled to Spark SQL by
# data_quality_validator.py. Records failing `hard` rules are
# QUARANTINED. Records failing `warn` rules get WARNING status.
#
rules:
  hard:
    - id: <RULE_ID>
      expression: <Spark SQL boolean expression>
      description: <what this rule checks>
    - ...
  warn:
    - id: <RULE_ID>
      expression: <Spark SQL boolean expression>
      description: <what this rule checks>
    - ...

# ─── Tolerances ─────────────────────────────────────────────────
# Maximum acceptable failure rates. Pipeline alerts when breached.
tolerances:
  hard_failure_rate: <float, e.g. 0.01>    # max % of records quarantined
  freshness_breach_rate: <float>           # max % of records stale

# ─── Quality Dimensions ────────────────────────────────────────
# Maps rules to data quality categories for reporting.
quality_dimensions:
  completeness: [<rule IDs checking for nulls/missing>]
  validity: [<rule IDs checking format/range/allowed values>]
  consistency: [<rule IDs checking FK integrity/cross-table>]
  timeliness: [<rule IDs checking freshness/staleness>]
  accuracy: [<rule IDs checking business correctness>]

# ─── AI Policy ──────────────────────────────────────────────────
# What AI consumers (Banker.AI / Customer.AI) can and cannot do
# with this product's data.
ai_policy:
  allowed: [<permitted AI actions>]
  prohibited: [<forbidden AI actions>]

# ─── Zero Trust ─────────────────────────────────────────────────
# Runtime enforcement guarantees for AI consumption safety.
zero_trust:
  quality_status_required: true    # every record carries PASSED/WARNING/QUARANTINED
  source_reference_required: true  # every record traces back to bronze
  quarantined_excluded: true       # QUARANTINED records never reach Gold
  stale_flagged: true              # WARNING records carry known_limitations

# ─── Parameters ─────────────────────────────────────────────────
# Thresholds and reference values used by quality rules.
parameters:
  <parameter_name>: <value>
  ...
```

### Reference: `customers.yml`

```yaml
product: customers
version: 1.1.0
contract_status: DRAFT
owner: customer-domain
producer: customer-master-platform
consumers: [banker-assist, lending-operations]
description: >
  Customer current state from CDC with KYC enrichment.
  Joined from customers + kyc_records bronze CDC payloads.

canonical_key: global_id
customer_key: global_id
critical_data_elements: [global_id, customerId, kyc_status]
lifecycle_stage: Monitor

sources:
  - type: DATABASE
    ingestion_mechanism: DEBEZIUM_CDC
    bronze_sink: cdc_changes
    source_dataset: public.customers
    fields: [global_id, customerId, firstName, lastName, email,
             phoneNumber, gender, age, state, createdAt, lastUpdateTime,
             customerType, prefix, suffix, middleNames,
             occupationCode, phoneNumbers, emailAddresses]
  - type: DATABASE
    ingestion_mechanism: DEBEZIUM_CDC
    bronze_sink: cdc_changes
    source_dataset: public.kyc_records
    fields: [global_id, kycId, verificationStatus, verificationDate,
             riskRating, pepStatus, sanctionsCheck, lastReviewDate,
             nextReviewDate, documentTypes, organisationId,
             abnVerified, asicCheckStatus, beneficialOwnershipVerified]

freshness_sla: 24 hours
lineage_l1: >
  customer-master DB -> Debezium -> Kafka CDC topics -> landing-exporter
  -> S3 bronze/landing/cdc/ -> Auto Loader -> bronze.cdc_changes -> Silver
lineage_l2_required: true

output_schema:
  - name: global_id
    type: STRING
    required: true
    classification: INTERNAL
    example: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    description: "Universal join key across all tables"
  - name: customer_id
    type: STRING
    required: true
    classification: INTERNAL
    example: "CUST-00042"
  - name: first_name
    type: STRING
    required: false
    classification: CONFIDENTIAL
    masking: hash
    example: "Jane"
  - name: last_name
    type: STRING
    required: false
    classification: CONFIDENTIAL
    masking: hash
    example: "Smith"
  - name: email
    type: STRING
    required: false
    classification: CONFIDENTIAL
    masking: redact
    example: "j***@email.com"
  - name: phone_number
    type: STRING
    required: false
    classification: CONFIDENTIAL
    masking: redact
    example: "04XX-XXX-X42"
  - name: gender
    type: STRING
    required: true
    classification: INTERNAL
    allowed_values: [Male, Female, Non_binary]
  - name: age
    type: INTEGER
    required: false
    classification: CONFIDENTIAL
    masking: remove
    description: "Removed from AI-ready output"
  - name: state
    type: STRING
    required: true
    classification: INTERNAL
    allowed_values: [NSW, VIC, QLD, WA, SA, TAS, ACT, NT]
  - name: customer_type
    type: STRING
    required: true
    classification: INTERNAL
    allowed_values: [INDIVIDUAL, BUSINESS, SOLE_TRADER]
  - name: kyc_status
    type: STRING
    required: true
    classification: INTERNAL
    allowed_values: [VERIFIED, PENDING, EXPIRED, FAILED]
  - name: kyc_risk_rating
    type: STRING
    required: false
    classification: RESTRICTED
    allowed_values: [LOW, MEDIUM, HIGH, VERY_HIGH]
    description: "Internal only -- not exposed to Customer.AI"
  - name: pep_status
    type: BOOLEAN
    required: false
    classification: RESTRICTED
    description: "Politically Exposed Person flag"
  - name: sanctions_check
    type: STRING
    required: false
    classification: RESTRICTED
    allowed_values: [CLEAR, MATCH, PENDING]
  - name: created_at
    type: TIMESTAMP
    required: false
    classification: INTERNAL
  - name: last_update_time
    type: TIMESTAMP
    required: false
    classification: INTERNAL

rules:
  hard:
    - id: CUSTOMER_ID_NULL
      expression: "customer_id IS NOT NULL"
      description: "Primary ID must exist"
    - id: GLOBAL_ID_NULL
      expression: "global_id IS NOT NULL"
      description: "Universal FK must exist"
    - id: KYC_MISSING
      expression: "kyc_status IS NOT NULL"
      description: "KYC status required for every customer"
    - id: KYC_STATUS_INVALID
      expression: "kyc_status IN ('VERIFIED','PENDING','EXPIRED','FAILED')"
      description: "KYC must be a known value"
    - id: AGE_MINIMUM
      expression: "age IS NULL OR age >= 18"
      description: "Must be adult"
    - id: CUSTOMER_TYPE_INVALID
      expression: "customer_type IN ('INDIVIDUAL','BUSINESS','SOLE_TRADER')"
      description: "Must be a known type"
  warn:
    - id: CUSTOMER_SNAPSHOT_STALE
      expression: "last_update_time >= date_sub(current_date(), 30)"
      description: "Record updated within 30 days"
    - id: EMAIL_FORMAT_REVIEW
      expression: "email IS NULL OR email RLIKE '^[^@]+@[^@]+\\.[^@]+$'"
      description: "Email format check"
    - id: PHONE_FORMAT_REVIEW
      expression: "phone_number IS NULL OR length(phone_number) >= 8"
      description: "Phone length check"

tolerances:
  hard_failure_rate: 0.01
  freshness_breach_rate: 0.05

quality_dimensions:
  completeness: [CUSTOMER_ID_NULL, GLOBAL_ID_NULL, KYC_MISSING]
  validity: [KYC_STATUS_INVALID, CUSTOMER_TYPE_INVALID, AGE_MINIMUM]
  timeliness: [CUSTOMER_SNAPSHOT_STALE]

ai_policy:
  allowed: [profile_summary, contact_preference, kyc_status_check]
  prohibited: [display_raw_pii, credit_decision, risk_scoring, fraud_assessment]

zero_trust:
  quality_status_required: true
  source_reference_required: true
  quarantined_excluded: true
  stale_flagged: true

parameters:
  allowed_kyc_statuses: [VERIFIED, PENDING, EXPIRED, FAILED]
  freshness_days: 30
  minimum_age: 18
  minimum_created_date: "2010-01-01"
```

---

## Gold Contract Schema

Gold has two contract shapes:

1. **Context contracts** -- define which Silver products and fields are approved for a specific AI use case
2. **Governance registries** -- track CDE ownership and event scope boundaries

### Context Contract Template

```yaml
# ─── Identity ───────────────────────────────────────────────────
context: <use case name>
version: <semver>
contract_status: DRAFT | APPROVED | ACTIVE | DEPRECATED
use_case: <specific scenario this context serves>
description: <what AI experience this context powers>

# ─── Access Control ─────────────────────────────────────────────
# Determines field visibility by audience.
access:
  banker_facing: true | false
  customer_facing: true | false
  internal_only_fields: [<fields visible only to bankers/internal>]
  customer_safe_fields: [<fields safe for customer-facing AI>]

# ─── Source Products ────────────────────────────────────────────
# Which Silver products feed this context. Each must be APPROVED/ACTIVE.
source_products:
  - product: <silver product name>
    min_version: <minimum acceptable semver>
    required_fields: [<columns this context needs>]
    masked_fields: [<columns that must be masked before serving>]
    excluded_fields: [<columns deliberately omitted>]
    conditions: [<when to include this product, optional>]
  - ...

# ─── Event Scope ────────────────────────────────────────────────
# Which semantic event types are in/out of scope for this AI context.
# default_disposition applies to any event type not explicitly listed.
event_scope:
  default_disposition: OUT_OF_SCOPE | IN_SCOPE
  selected: [<event types approved for this context>]
  retained_not_selected: [<event types kept in Silver but not served here>]

# ─── AI Constraints ─────────────────────────────────────────────
# Behavioural boundaries for the AI consumer. Derived from business
# context documents (AI Questions.docx, Banker.AI spec, etc.).
ai_constraints:
  allowed_actions:
    - <action the AI can perform with this context>
    - ...
  prohibited_actions:
    - <action the AI must refuse>
    - ...

# ─── Zero Trust Output Requirements ────────────────────────────
# Runtime enforcement guarantees on the final AI-ready output.
zero_trust:
  quality_status_attached: true    # every record carries quality status
  quarantined_excluded: true       # quarantined records never served
  stale_warning_exposed: true      # WARNING records carry limitation flags
  source_reference_for_verification: true   # AI can cite source
  freshness_max_age: <max context age, e.g. "24 hours">
  unknown_answer_handling: >
    <what AI must do when context is missing, restricted, or unsafe>

# ─── Output Metadata ───────────────────────────────────────────
# Columns appended to every output record for traceability.
output_metadata:
  - contract_name
  - contract_version
  - contract_hash
  - quality_status
  - quality_evaluated_at
  - known_limitations
  - pipeline_run_id
  - source_reference
  - context_refreshed_at
```

### Reference: `banker_assist_context.yml`

```yaml
context: banker_assist
version: 1.0.0
contract_status: DRAFT
use_case: banker_assist_application_status
description: >
  AI-ready context for Banker.AI. Combines customer profile,
  application status, service history, and organisation context.
  Only approved Silver products with PASSED/WARNING quality status.

access:
  banker_facing: true
  customer_facing: false
  internal_only_fields: [kyc_risk_rating, pep_status, sanctions_check]
  customer_safe_fields: [customer_type, state, kyc_status, application_status]

source_products:
  - product: customers
    min_version: "1.0.0"
    required_fields: [global_id, customer_id, customer_type, state, kyc_status]
    masked_fields: [first_name, last_name, email, phone_number]
    excluded_fields: [age]
  - product: loan_applications
    min_version: "1.0.0"
    required_fields: [application_id, global_id, application_status, current_stage]
    excluded_fields: [credit_score]
  - product: service_cases
    min_version: "1.0.0"
    required_fields: [case_id, global_id, case_status, priority]
  - product: organisations
    min_version: "1.0.0"
    required_fields: [organisation_id, legal_name, organisation_type]
    conditions: ["Only when customer_type IN ('BUSINESS','SOLE_TRADER')"]

event_scope:
  default_disposition: OUT_OF_SCOPE
  selected:
    - CustomerCurrentState
    - CustomerKycCurrentState
    - LoanApplicationCurrentState
    - LoanApplicationStatusChanged
    - ApplicationStageChanged
    - LoanDocumentStatusChanged
    - ServiceCaseCurrentState
    - ServiceCaseEventRecorded
  retained_not_selected:
    - PhysicalAddressCurrentState
    - CustomerPreferencesCurrentState
    - SupportInteractionCreated

ai_constraints:
  allowed_actions:
    - customer_summary
    - application_status_lookup
    - missing_document_list
    - service_case_summary
    - handover_preparation
    - stage_timeline
    - next_step_guidance
    - organisation_overview
    - representative_lookup
    - kyb_status_check
  prohibited_actions:
    - credit_decision
    - fraud_assessment
    - risk_prediction
    - display_raw_pii
    - modify_application_status
    - delete_records
    - cross_customer_queries
    - internal_approval_rules
    - regulated_financial_advice

zero_trust:
  quality_status_attached: true
  quarantined_excluded: true
  stale_warning_exposed: true
  source_reference_for_verification: true
  freshness_max_age: 24 hours
  unknown_answer_handling: >
    When context is missing, restricted, or unsafe, the AI must
    refuse explicitly rather than hallucinating an answer.

output_metadata:
  - contract_name
  - contract_version
  - contract_hash
  - quality_status
  - quality_evaluated_at
  - known_limitations
  - pipeline_run_id
  - source_reference
  - context_refreshed_at
```

### CDE Registry Template

Tracks which Silver products contain Critical Data Elements and their governance.

```yaml
# ─── CDE Governance ────────────────────────────────────────────
cde_lifecycle: [Activate, Trace, Assess, Monitor, Resolve, Maintain, Decommission]
grace_system: GRACE

products:
  <product_name>:
    owner: <domain>
    producer: <platform>
    consumers: [<downstream consumers>]
    l1_lineage_required: true | false
  ...
```

### Scope Registry Template

Defines which semantic event types are approved for a specific AI use case.

```yaml
# ─── Scope Gate ─────────────────────────────────────────────────
use_case: <AI use case name>
status: DRAFT | APPROVED | ACTIVE
default_disposition: OUT_OF_SCOPE | IN_SCOPE

selected_event_types:
  - <event types approved for consumption>
  - ...

retained_not_selected_event_types:
  - <event types kept in Silver but not served to this use case>
  - ...
```

---

## How Contracts Connect to Pipeline Code

```
contracts/silver/{subject-area}/*.yml
        |
        v
data_contract_loader.py          -- loads YAML, validates status, computes fingerprint
        |
        v
data_quality_validator.py        -- compiles rules.hard/warn expressions to Spark SQL
        |                           evaluates each row -> PASSED / WARNING / QUARANTINED
        v
pipeline_metadata_builder.py     -- attaches contract_name, contract_version,
                                    contract_hash, known_limitations, quality_evaluated_at
        |
        v
Silver output (validated + classified + metadata-enriched)
        |
        v
contracts/gold/*_context.yml     -- scope gate: which Silver fields reach AI
        |
        v
Gold output (masked + scoped + AI-ready with output_metadata)
```

## Contract File Layout

```
contracts/
├── source/
│   └── source_inventory.yml           # routing manifest (32 datasets)
├── silver/
│   ├── involved-party/                # Subject Area: Involved Party (5)
│   │   ├── ip-individual.yml
│   │   ├── ip-organisation.yml
│   │   ├── ip-party-relationship.yml
│   │   ├── ip-org-relationship.yml
│   │   └── ip-kyc.yml
│   ├── arrangement/                   # Subject Area: Arrangement (4)
│   │   ├── arr-banking-arrangement.yml
│   │   ├── arr-loan.yml
│   │   ├── arr-mortgage.yml
│   │   └── arr-credit-card.yml
│   ├── application/                   # Subject Area: Application (7)
│   │   ├── app-application.yml
│   │   ├── app-stage-history.yml
│   │   ├── app-status-change.yml
│   │   ├── app-missing-document.yml
│   │   ├── app-lifecycle-event.yml
│   │   ├── app-accepted-loan.yml
│   │   └── app-rejected-application.yml
│   └── event/                         # Subject Area: Event (3)
│       ├── evt-service-case.yml
│       ├── evt-support-interaction.yml
│       └── evt-case-event.yml
└── gold/
    ├── banker_assist_context.yml       # context contract
    ├── customer_assist_context.yml     # context contract (future)
    ├── cde_registry.yml               # CDE governance registry
    └── scope_registry.yml             # event scope registry
```

## Summary

| | Silver | Gold (Context) | Gold (Registry) |
|---|---|---|---|
| **Purpose** | Validated data product | AI scope gate | CDE/event governance |
| **Granularity** | Per business entity | Per AI use case | Per governance concern |
| **Schema** | `output_schema` with classification + masking | `source_products` with field inclusion/exclusion | Product ownership list |
| **Quality rules** | Executable `hard`/`warn` with Spark SQL | Inherits from Silver | N/A |
| **PII** | `classification` + `masking` per column | `masked_fields` + `excluded_fields` per product | N/A |
| **AI policy** | `allowed`/`prohibited` actions | `ai_constraints` (full behavioural boundaries) | N/A |
| **Zero Trust** | `quality_status_required`, `quarantined_excluded` | `stale_warning_exposed`, `unknown_answer_handling` | N/A |
| **Tolerances** | `hard_failure_rate`, `freshness_breach_rate` | `freshness_max_age` | N/A |
| **Lineage** | `sources[]`, `lineage_l1` | `source_products[]` with `min_version` | `l1_lineage_required` |
