# Silver Layer Data Contracts

Silver contracts define the validated, schema-enforced, PII-classified output of the Silver layer. Each contract governs one curated product derived from Bronze landing tables, with executable quality rules, masking strategies, and Zero Trust AI policy.

**Format**: DCS v3.1.0 (`kind: DataContract`, `apiVersion: v3.1.0`)

## Contract Inventory

19 contracts across 4 subject areas. **Totals: 296 fields, 149 hard + 22 warn = 171 quality rules.**

### Involved Party (5 contracts)

| Contract | Product | Source | Fields | Rules |
|---|---|---|---|---|
| ip-individual.yml | ip_individual | customers + kyc_records + preferences + addresses (4 CDC) | 27 | 7H/5W |
| ip-organisation.yml | ip_organisation | organisations (1 CDC) | 20 | 7H/2W |
| ip-party-relationship.yml | ip_party_relationship | organisation_party_relationships (1 CDC) | 13 | 8H/1W |
| ip-org-relationship.yml | ip_org_relationship | organisation_relationships (1 CDC) | 11 | 8H/0W |
| ip-kyc.yml | ip_kyc | kyc_records (1 CDC) | 21 | 9H/2W |

### Arrangement (4 contracts)

| Contract | Product | Source | Fields | Rules |
|---|---|---|---|---|
| arr-banking-arrangement.yml | arr_banking_arrangement | banking_accounts + banking_balances (2 CDC) | 16 | 6H/1W |
| arr-loan.yml | arr_loan | loan_accounts (1 CDC) | 17 | 8H/1W |
| arr-mortgage.yml | arr_mortgage | mortgage_accounts (1 CDC) | 14 | 5H/1W |
| arr-credit-card.yml | arr_credit_card | credit_cards (1 CDC) | 15 | 8H/0W |

### Application (7 contracts)

| Contract | Product | Source | Fields | Rules |
|---|---|---|---|---|
| app-application.yml | app_application | loan_applications (1 CDC) | 17 | 11H/1W |
| app-stage-history.yml | app_stage_history | application_stage_history (1 Kafka) | 17 | 10H/2W |
| app-status-change.yml | app_status_change | status_change_history (1 Kafka) | 12 | 12H/0W |
| app-missing-document.yml | app_missing_document | missing_documents (1 CDC) | 17 | 8H/2W |
| app-lifecycle-event.yml | app_lifecycle_event | loan_application_events (1 Kafka) | 13 | 8H/1W |
| app-accepted-loan.yml | app_accepted_loan | accepted_loans (1 S3 file) | 13 | 5H/0W |
| app-rejected-application.yml | app_rejected_application | rejected_applications (1 S3 file) | 10 | 4H/0W |

### Event (3 contracts)

| Contract | Product | Source | Fields | Rules |
|---|---|---|---|---|
| evt-service-case.yml | evt_service_case | service_cases (1 CDC) | 20 | 10H/2W |
| evt-support-interaction.yml | evt_support_interaction | support_interactions (1 Kafka) | 12 | 7H/1W |
| evt-case-event.yml | evt_case_event | service_case_events (1 Kafka) | 11 | 8H/0W |

## Contract Structure

Every Silver contract is a valid DCS v3.1.0 document. Metadata that does not map to standard DCS fields lives under `customProperties` with `g3:` prefixed keys.

```yaml
kind: DataContract
apiVersion: v3.1.0
id: silver.{area}.{name}          # e.g. silver.involved_party.ip_individual
version: 1.0.0
name: {product_name}              # e.g. ip_individual
status: draft
domain: {DOMAIN}                  # CUSTOMER | BANKING | LENDING | SERVICE

description:
  purpose: |
    {business description}

servers:
- server: silver-delta-lake
  type: databricks
  environment: prod

schema:
- name: {product_name}
  logicalType: object
  physicalType: table

  # Quality rules — executable Spark SQL, compiled via F.expr()
  quality:
  - id: {RULE_ID}
    type: sql
    query: "{spark_sql_expression}"
    mustBe: 0
    severity: error          # error → QUARANTINE | warning → ALLOW_WITH_WARNING
    dimension: completeness  # completeness | validity | timeliness
    description: "{description}"
    customProperties:
    - property: g3:action
      value: QUARANTINE       # QUARANTINE | ALLOW_WITH_WARNING

  # Output fields
  properties:
  - name: {field_name}
    logicalType: string       # string | integer | boolean | timestamp | date
    physicalType: STRING      # STRING | INT | BOOLEAN | TIMESTAMP | DATE
    required: true            # present = must not be null; absent = nullable
    classification: INTERNAL  # INTERNAL | CONFIDENTIAL | RESTRICTED
    customProperties:
    - property: g3:sensitivity
      value:
        is_pii: false
        pii_type: CONTACT_INFORMATION  # only when is_pii: true
        handling: redact               # hash | redact | range_bucket | partial_mask | generalise
    - property: g3:domainType
      value: uuid             # uuid | person_name | email | phone | address_component |
                              # amount | date | timestamp | category | boolean_flag |
                              # code | identifier | description | reference_code | counter
    - property: g3:accepted
      value:
        values: [A, B, C]    # only for enum fields

slaProperties:
- property: hardFailureRate
  value: 0.01
- property: freshnessBreachRate
  value: 0.05

customProperties:
- property: g3:sources
  value:
  - type: DATABASE              # DATABASE | EVENT | FILE
    ingestion_mechanism: DEBEZIUM_CDC
    bronze_sink: cdc_changes    # cdc_changes | kafka_events | file_arrivals
    source_dataset: public.{table}
    fields: [{camelCase source fields}]
    cdc:
      supported_operations: [SNAPSHOT, INSERT, UPDATE, DELETE]
      ordering_metadata: [source_lsn, kafka_partition, kafka_offset]

- property: g3:freshnessSla
  value: 24 hours

- property: g3:lineage
  value:
    l1: "{source} -> Bronze -> Silver"
    l2_required: true

- property: g3:knownSourceLimitations
  value:
  - "CDC eventual consistency — records may arrive out of order within partition"

- property: g3:lifecycle
  value:
    owner: {team}-domain
    producer: {platform}
    consumers: [{consumer1}, {consumer2}]
    approver: TBD
    effective_from: null

- property: g3:dataset
  value:
    canonical_key: {pk_field}
    customer_key: {fk_to_customer}
    critical_data_elements: [{cde1}, {cde2}]
    lifecycle_stage: Monitor

- property: g3:qualityDimensions
  value:
    completeness: [{rule_ids}]
    validity: [{rule_ids}]
    timeliness: [{rule_ids}]

- property: g3:aiPolicy
  value:
    allowed: [{use_cases}]
    prohibited: [display_raw_pii, credit_decision, risk_scoring, fraud_assessment]

- property: g3:zeroTrust
  value:
    quality_status_required: true
    source_reference_required: true
    quarantined_excluded: true
    stale_flagged: true
```

## Key Design Decisions

**Contract ID**: `silver.{area}.{name}` pattern (e.g., `silver.involved_party.ip_individual`). The `name` field flows into `pipeline_metadata_builder.py` as `contract_name`.

**Naming**: Subject-area-prefixed (`ip_individual`, `arr_loan`, `app_application`, `evt_service_case`), not source-table-named.

**DCS field types**:

| `logicalType` | `physicalType` | Used for |
|---|---|---|
| `string` | STRING | all string fields |
| `integer` | INT | counters, durations, scores |
| `boolean` | BOOLEAN | flags |
| `timestamp` | TIMESTAMP | timestamps |
| `date` | DATE | dates |

**PII classification mapping** (from Excel modelling):

| Excel Value | `classification` | `is_pii` | Masking Required |
|---|---|---|---|
| Public | INTERNAL | false | No |
| Internal | INTERNAL | false | No |
| Confidential | CONFIDENTIAL | true (personal) / false (non-personal) | Recommended |
| Highly Confidential | RESTRICTED | true | Required |

**Masking strategies** (set in `g3:sensitivity.handling`):
`hash` (one-way token) · `redact` (remove/replace) · `range_bucket` (numeric to range) · `partial_mask` (last N digits) · `generalise` (reduce precision)

**Bronze sink routing** (deterministic from source type):

| Source Type | `bronze_sink` |
|---|---|
| DATABASE | cdc_changes |
| EVENT | kafka_events |
| FILE | file_arrivals |

**Pipeline metadata columns**: Every contract includes 5 pipeline-injected fields with `required: true`, `classification: INTERNAL`:
`pipeline_run_id` · `source_table` · `processed_at` · `dq_status` (PASSED/FAILED/WARNING) · `masking_status` (MASKED/CLEAN)

**Quality rules**: Two severity levels:
- `severity: error` + `g3:action: QUARANTINE` → hard failures, route to quarantine
- `severity: warning` + `g3:action: ALLOW_WITH_WARNING` → soft failures, retain with flag

Every rule is classified in `g3:qualityDimensions` under `completeness`, `validity`, or `timeliness`.

## Source Coverage

19 of 32 datasets from `source_inventory.yml` have Silver contracts. The remaining 13 are either joined into existing contracts (3) or not modelled in the Silver Layer (10).

**Joined as sources** (no separate Silver product):
- `banking_balances` → joined into `arr_banking_arrangement`
- `customer_preferences` → joined into `ip_individual`
- `physical_addresses` → joined into `ip_individual`

**Not modelled** (candidates for future modelling pass):
- Banking: `banking_direct_debits`, `banking_payees`, `banking_products`, `banking_scheduled_payments`, `banking_transactions`
- Energy: `energy_accounts`, `energy_invoices`, `energy_plans`, `energy_service_points`
- Insurance: `insurance_policies`

## Intentional Deviations from Excel Spec

| Contract | Field | Excel Spec | Contract | Reason |
|---|---|---|---|---|
| arr-loan | `organisation_id`, `is_business_arrangement` | Present | Excluded | Cross-table JOIN not implemented at Silver |
| arr-mortgage | `organisation_id`, `is_business_arrangement` | Present | Excluded | Same |
| ip-organisation | `business_name`, `legal_name` | CONFIDENTIAL | INTERNAL | Publicly registered with ASIC |
| ip-individual | `address_suburb`, `address_postcode` | Masking: None | `handling: generalise` | CONFIDENTIAL data requires masking strategy |
| ip-individual | `email_masked`, `phone_masked` | Partial mask pattern | `handling: redact` | "Never expose raw" intent captured by redact |

## Runtime Integration

| File | Reads from Contract | Notes |
|---|---|---|
| `data_contract_loader.py` | `status`, full DCS YAML | Loads from `/Workspace/Shared/0-ai-trust/contracts/`. Rejects non-APPROVED/ACTIVE when `require_active=True`. All 19 are `draft` — load with `require_active=False` until approved. |
| `data_quality_validator.py` | `schema[0].quality[]` with `id`, `query`, `severity` | Compiles `query` to `F.expr()`. Rule severity maps to QUARANTINE / WARNING / PASSED. |
| `pipeline_metadata_builder.py` | `name`, `version`, `g3:knownSourceLimitations` | Writes `contract_name`, `contract_version`, `known_limitations`, `quality_evaluated_at`. |

## Known Dependencies

| Dependency | Impact |
|---|---|
| `contracts/gold/cde_registry.yml` | References old product names. Needs update to subject-area prefixed names. |
| `contracts/gold/scope_registry.yml` | Same. |
| `data_quality_validator.py` | Needs update to read from DCS `schema[0].quality[]` (was `quality_rules.hard[]`/`warn[]`) and `id` (was `rule_id`). |

## Unresolved Questions

1. **arr-loan and arr-mortgage**: Should `organisation_id` / `is_business_arrangement` be added via a JOIN with `banking_accounts` at Silver, or is business/personal segmentation Gold-layer only?
2. **Gold contract update**: When should `cde_registry.yml` and `scope_registry.yml` be updated to reference new contract IDs?
3. **Contract approval**: All 19 are `status: draft`. What approval process moves them to `approved`?
