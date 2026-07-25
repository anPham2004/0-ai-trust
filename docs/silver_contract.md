# Silver Layer Data Contracts

Silver contracts define the validated, schema-enforced, PII-classified output of the Silver layer. Each contract governs one curated product derived from Bronze landing tables, with executable quality rules, masking strategies, and Zero Trust AI policy.

## Contract Inventory

19 contracts across 4 subject areas, matching the Silver Layer data modelling.

### Involved Party (5 contracts)

| Contract | Product | Source | Fields | Rules | Masking |
|---|---|---|---|---|---|
| ip-individual.yml | ip_individual | customers + kyc_records + preferences + addresses (4 CDC sources) | 27 | 7H/5W | 7 fields masked |
| ip-organisation.yml | ip_organisation | organisations (1 CDC source) | 20 | 7H/2W | 2 fields masked |
| ip-party-relationship.yml | ip_party_relationship | organisation_party_relationships (1 CDC source) | 13 | 8H/1W | 0 |
| ip-org-relationship.yml | ip_org_relationship | organisation_relationships (1 CDC source) | 11 | 8H/0W | 0 |
| ip-kyc.yml | ip_kyc | kyc_records (1 CDC source) | 21 | 9H/2W | 3 fields masked |

### Arrangement (4 contracts)

| Contract | Product | Source | Fields | Rules | Masking |
|---|---|---|---|---|---|
| arr-banking-arrangement.yml | arr_banking_arrangement | banking_accounts + banking_balances (2 CDC sources) | 16 | 6H/1W | 2 fields masked |
| arr-loan.yml | arr_loan | loan_accounts (1 CDC source) | 17 | 8H/1W | 2 fields masked |
| arr-mortgage.yml | arr_mortgage | mortgage_accounts (1 CDC source) | 14 | 5H/1W | 1 field masked |
| arr-credit-card.yml | arr_credit_card | credit_cards (1 CDC source) | 15 | 8H/0W | 2 fields masked |

### Application (7 contracts)

| Contract | Product | Source | Fields | Rules | Masking |
|---|---|---|---|---|---|
| app-application.yml | app_application | loan_applications (1 CDC source) | 17 | 11H/1W | 2 fields masked |
| app-stage-history.yml | app_stage_history | application_stage_history (1 Kafka source) | 17 | 10H/2W | 0 |
| app-status-change.yml | app_status_change | status_change_history (1 Kafka source) | 12 | 12H/0W | 0 |
| app-missing-document.yml | app_missing_document | missing_documents (1 CDC source) | 17 | 8H/2W | 0 |
| app-lifecycle-event.yml | app_lifecycle_event | loan_application_events (1 Kafka source) | 13 | 8H/1W | 0 |
| app-accepted-loan.yml | app_accepted_loan | accepted_loans (1 S3 file source) | 13 | 5H/0W | 1 field masked |
| app-rejected-application.yml | app_rejected_application | rejected_applications (1 S3 file source) | 10 | 4H/0W | 0 |

### Event (3 contracts)

| Contract | Product | Source | Fields | Rules | Masking |
|---|---|---|---|---|---|
| evt-service-case.yml | evt_service_case | service_cases (1 CDC source) | 20 | 10H/2W | 0 |
| evt-support-interaction.yml | evt_support_interaction | support_interactions (1 Kafka source) | 12 | 7H/1W | 0 |
| evt-case-event.yml | evt_case_event | service_case_events (1 Kafka source) | 11 | 8H/0W | 0 |

## Contract Structure

Every Silver contract follows the template defined in `dc.md` (lines 60-145):

```
product / version / contract_status / approver / effective_from / owner / producer / consumers
canonical_key / customer_key / keys { primary_key, alternate_keys } / critical_data_elements / lifecycle_stage
sources[] -> { type, ingestion_mechanism, bronze_sink, source_dataset, fields, cdc? }
known_source_limitations[]
freshness_sla / lineage_l1 / lineage_l2_required
output_schema[] -> { name, type, required, nullable, classification, is_pii, pii_type?, logical_type, masking?, allowed_values? }
rules -> { hard[], warn[] } -> each { id, expression, description }
tolerances -> { hard_failure_rate, freshness_breach_rate }
quality_dimensions -> { completeness[], validity[], timeliness[] }
ai_policy -> { allowed[], prohibited[] }
zero_trust -> { quality_status_required, source_reference_required, quarantined_excluded, stale_flagged }
parameters -> contract-specific thresholds
```

### Key Design Decisions

**Product naming**: Subject-area-prefixed (`ip_individual`, `arr_loan`, `app_application`, `evt_service_case`), not source-table-named. The `product` value flows into `pipeline_metadata_builder.py` as `contract_name`.

**PII classification mapping** (from Excel modelling):

| Excel Value | Contract Classification | Masking Required |
|---|---|---|
| Public | INTERNAL | No |
| Internal | INTERNAL | No |
| Confidential | CONFIDENTIAL | Recommended |
| Highly Confidential | RESTRICTED | Required |

**Masking strategies**: `hash` (one-way token), `redact` (remove/replace), `range_bucket` (numeric to range), `partial_mask` (last N digits), `generalise` (reduce precision).

**Bronze sink routing** (deterministic from source_type):

| Source Type | Bronze Sink |
|---|---|
| DATABASE | cdc_changes |
| EVENT | kafka_events |
| FILE | file_arrivals |

**Pipeline metadata columns**: Every contract includes 5 pipeline-injected fields: `pipeline_run_id`, `source_table`, `processed_at`, `dq_status` (PASSED/FAILED/WARNING), `masking_status` (MASKED/CLEAN).

## Source Coverage

19 of 32 datasets from `source_inventory.yml` have Silver contracts. The remaining 13 are either joined into existing contracts (3) or not modelled in the Silver Layer Excel (10):

**Joined as sources** (no separate Silver product):
- `banking_balances` -> joined into `arr_banking_arrangement`
- `customer_preferences` -> joined into `ip_individual`
- `physical_addresses` -> joined into `ip_individual`

**Not modelled** (no contract — candidates for future modelling pass):
- Banking: `banking_direct_debits`, `banking_payees`, `banking_products`, `banking_scheduled_payments`, `banking_transactions`
- Energy: `energy_accounts`, `energy_invoices`, `energy_plans`, `energy_service_points`
- Insurance: `insurance_policies`

## Problems Fixed

### Previous Version (4 flat files)

The old Silver layer had 4 flat contracts (`customers.yml`, `loan_applications.yml`, `organisations.yml`, `service_cases.yml`) with these problems:

| Problem | Impact |
|---|---|
| **Flat `classifications:` map** instead of per-field `output_schema` | No masking strategy per field, no allowed_values, no type information. PII treatment was aspirational, not executable. |
| **Bare string rule IDs** (`- CUSTOMER_ID_NULL`) instead of `{id, expression}` dicts | Rules were labels, not executable. `data_quality_validator.py` requires `expression` to compile Spark SQL — old rules would raise `DataContractError`. |
| **Source-table naming** (`customers`, `loan_applications`) | Coupled contracts to source schema. Silver should model business entities, not source tables. |
| **`dataset:` field name** instead of `source_dataset:` | Inconsistent with dc.md template and `pipeline_metadata_builder.py` expectations. |
| **No bronze_sink declared** | Source routing was implicit. No traceability from contract to Bronze landing table. |
| **4 contracts for 19 products** | Massive under-coverage. 15 of 19 Silver products had no contract at all — no quality rules, no PII classification, no AI policy. |
| **No ai_policy or zero_trust** | No machine-readable guardrails for what AI consumers could do with the data. |
| **organisations.yml had no rules** | Zero quality enforcement for business customer data. |

### New Version (19 contracts) Fixes Applied

| Fix | Details |
|---|---|
| **Per-field output_schema** with classification + masking | Every field has type, required, classification. RESTRICTED fields have masking strategy. CONFIDENTIAL PII fields have masking where appropriate. |
| **Executable rules** with `{id, expression, description}` | All 149 hard rules and 22 warn rules compile to Spark SQL via `data_quality_validator.py`. |
| **Subject-area naming** | Products named by business entity (`ip_individual`, `arr_loan`), not source table. |
| **dc.md field naming** | `source_dataset`, `bronze_sink` per template. |
| **Bronze sink routing** declared | Every contract maps source -> bronze sink deterministically. |
| **Full 19-product coverage** | All Silver Layer Excel modelling products have contracts. |
| **AI policy + Zero Trust** | Every contract has `ai_policy` (allowed + prohibited actions) and `zero_trust` (4-flag enforcement). |
| **Quality dimensions classified** | Every hard rule mapped to completeness, validity, or timeliness dimension. |

### Cross-Validation Fixes (Post-Audit)

| Fix | Contract | Issue | Resolution |
|---|---|---|---|
| **Source dataset naming** | app-stage-history | `nab.application.stage_history` (made-up Kafka topic) | Changed to `application_stage_history` (matches source_inventory.yml) |
| **Source dataset naming** | app-status-change | `nab.application.status_changes` | Changed to `status_change_history` |
| **Source dataset naming** | app-lifecycle-event | `nab.application.lifecycle_events` | Changed to `loan_application_events` |
| **Source dataset naming** | evt-support-interaction | `nab.service.interactions` | Changed to `support_interactions` |
| **Source dataset naming** | evt-case-event | `nab.service.case_events` | Changed to `service_case_events` |
| **Phantom source field** | arr-loan | `organisationId` in source fields but not in physical schema | Removed from source fields |
| **Phantom source field** | arr-mortgage | `organisationId` in source fields but not in physical schema | Removed from source fields |
| **Phantom output fields** | arr-loan | `organisation_id` + `is_business_arrangement` in output_schema but unsourceable | Removed from output_schema |
| **Phantom output fields** | arr-mortgage | `organisation_id` + `is_business_arrangement` in output_schema but unsourceable | Removed from output_schema |
| **CONFIDENTIAL without masking** | ip-individual | `address_suburb`, `address_postcode` had no masking strategy | Added `masking: generalise` |
| **Over-classified** | ip-organisation | `business_name`, `legal_name` marked CONFIDENTIAL | Reclassified to INTERNAL (publicly registered with ASIC) |

## Enrichment (v1.1.0)

Gap analysis against the source `customers.yml` baseline identified 13 structural gaps. v1.1.0 addresses 10 of them.

### Per-Field Metadata (all 296 fields)

| Attribute | Purpose | Coverage |
|---|---|---|
| `nullable` | Separates "field exists" (required) from "value can be null" (nullable) | 296/296 |
| `is_pii` | Programmatic PII marker for compliance automation | 296/296 (24 true) |
| `pii_type` | PII taxonomy: DIRECT_IDENTIFIER, CONTACT_INFORMATION, DEMOGRAPHIC_INFORMATION, EMPLOYMENT_INFORMATION, FINANCIAL_INFORMATION | 24/24 PII fields |
| `logical_type` | Semantic type beyond physical type (uuid, person_name, email, phone, amount, category, etc.) | 296/296 |

### Contract-Level Fields

| Field | Purpose | Coverage |
|---|---|---|
| `keys.primary_key` + `alternate_keys` | Declarative PK/AK for uniqueness (runtime enforcement deferred) | 19/19 |
| `approver` / `effective_from` | Governance lifecycle | 19/19 |
| `cdc.ordering_metadata` | SCD2 deduplication ordering for CDC sources | 12/19 contracts (16 DATABASE source entries) |
| `known_source_limitations` | Pipeline awareness of source constraints | 19/19 |

### Quality Rule Enrichment

| Rule Type | Count | Pattern |
|---|---|---|
| UUID format (hard) | 51 | `<id_field> IS NULL OR <id_field> RLIKE '^[0-9a-f]{8}-...'` |
| Blank-string (hard) | 8 | `<field> IS NULL OR length(trim(<field>)) > 0` |
| G1 false-quarantine fix | 2 moved | EMAIL_MASKED_NULL, PHONE_MASKED_NULL moved from hard to warn in ip-individual |

**Rule totals after enrichment**: 149 hard + 22 warn = 171 (was 92H + 20W = 112).

### Gaps Addressed

| Gap | Status |
|---|---|
| G1 nullable/required conflation | Fixed — nullable added, false-quarantine rules moved to warn |
| G2 PK uniqueness | Declared — keys in contracts, runtime enforcement deferred |
| G3 is_pii/pii_type | Fixed — 24 PII fields tagged with taxonomy |
| G4 logical_type | Fixed — 15-type vocabulary across 296 fields |
| G5 format/regex validation | Fixed — UUID format rules on all ID fields |
| G7 blank-string detection | Fixed — 8 rules on required string business fields |
| G8 approver/effective_from | Fixed — governance fields added |
| G9 CDC ordering metadata | Fixed — ordering metadata on 12 CDC-sourced contracts (16 DATABASE source entries) |
| G13 known_source_limitations | Fixed — limitations declared per contract |

### Gaps Deferred

G6 (accepted specs), G10 (examples), G11 (schema ref), G12 (notes) — Tier 3, optional.

## Runtime Integration

### Framework Files

| File | Reads From Contract | Notes |
|---|---|---|
| `data_contract_loader.py` | `contract_status`, full YAML | Loads from `/Workspace/Shared/0-ai-trust/contracts/`. Rejects non-APPROVED/ACTIVE contracts when `require_active=True`. All 19 are DRAFT — loaded with `require_active=False` until approved. |
| `data_quality_validator.py` | `rules.hard[]`, `rules.warn[]` | Compiles `expression` to `F.expr()`. Produces `quality_status` column: QUARANTINED / WARNING / PASSED. |
| `pipeline_metadata_builder.py` | `product`, `version`, `known_source_limitations` | Writes `contract_name`, `contract_version`, `contract_hash`, `known_limitations`, `quality_evaluated_at`. |

### Silver Pipeline Stubs

All 5 Silver pipeline files are reserved stubs pending contract approval:
- `customer_curated.py`, `organisation_curated.py`, `application_curated.py`, `service_curated.py`, `record_quarantine.py`

Contracts are designed to drive these pipelines when implemented — the `output_schema` defines the target schema, `rules` define the DQ checks, `sources` define the Bronze inputs.

## Known Dependencies (Out of Scope)

| Dependency | Impact |
|---|---|
| `contracts/gold/cde_registry.yml` | References old product names (`customers`, `loan_applications`, `service_cases`, `organisations`). Needs update to new prefixed names. |
| `contracts/gold/scope_registry.yml` | Same — references old event type names. |
| `pipeline_metadata_builder.py` `contract_name` column | Downstream dashboards/filters using old product names will break silently. |

## Unresolved Questions

1. **arr-loan and arr-mortgage business segmentation**: Should these contracts add `loan_applications` as a second source to derive `organisation_id` via JOIN, or is business/personal segmentation only needed at Gold layer?
2. **known_source_limitations**: `pipeline_metadata_builder.py` reads this field but no contract declares it. Should accepted_loans (152 columns, only 8 modelled) or CDC tables (eventual consistency) declare limitations?
3. **Gold contract update**: When should `cde_registry.yml` and `scope_registry.yml` be updated to reference new product names? Same deploy window or separate PR?
