# Bronze → Silver Checklist — `0-ai-trust` Medallion Pipeline

Scope: process the three Bronze sinks (`cdc_changes`, `kafka_events`, `file_arrivals`) into the Silver entities defined by the lineage diagram (Involved Party, Arrangement, Application, Event subject areas).

---

## 1. Understand the shape of the work first

Silver isn't a 1:1 copy of Bronze — it's a **consolidation**. Multiple Bronze/source datasets map into fewer, wider Silver entities across 4 subject areas:

| Subject Area | Silver entities | Bronze-sourced datasets feeding in |
|---|---|---|
| Involved Party | `ip_individual`, `ip_organisation`, `ip_org_party_relationship`, `ip_org_relationship`, `ip_kyc` | customers, customer_preferences, physical_addresses, organisations, organisation_party_relationships, organisation_relationships, kyc_records |
| Arrangement | `arr_banking_arrangement`, `arr_loan`, `arr_mortgage`, `arr_credit_card` | banking_accounts, banking_balances, loan_accounts, mortgage_accounts, credit_cards |
| Application | `app_application`, `app_stage_history`, `app_status_change`, `app_missing_document`, `app_event`, `app_accepted_loan`, `app_rejected_application` | loan_applications + 6 related datasets |
| Event | `evt_service_case`, `evt_support_interaction`, `evt_case_event` | service_cases, support_interactions, service_case_events |

Each has a different **ingestion mechanism** (DATABASE/CDC vs EVENT/Kafka vs FILE), so the Bronze→Silver logic differs by source type, not just by table.

---

## 2. Source-type-specific processing logic

### A. CDC/Database-sourced datasets (23 datasets — customers, accounts, organisations, KYC, etc.)
- Read directly from `bronze.cdc_changes` — Silver owns deriving current-state and history logic itself rather than relying on a pre-built SCD2 view
- Order versions using `source_lsn` (primary) and `kafka_offset` (tiebreaker within the same LSN), not `_ingested_at` or `captured_at`
- Derive current-row selection per PK: latest version by `(source_lsn, kafka_offset)` becomes the current Silver row
- Handle deletes from the `operation` column (`insert`/`update`/`delete`/snapshot state) — decide soft-delete vs hard-delete per entity
- Deduplicate on PK + `source_lsn` (same row can arrive multiple times via Kafka replay/retries)
- Use Delta `MERGE INTO` for the current-state Silver table; if downstream needs point-in-time history, derive SCD2 columns (`valid_from`, `valid_to`, `is_current`) as part of the Silver transform itself, since that logic isn't pre-built upstream

### B. Event-sourced datasets (5 datasets — stage_history, status_change_history, loan_application_events, service_case_events, support_interactions)
- Must stay **append-only** — no merges/updates, only inserts (an event is already historical fact)
- Deduplicate transport-level Kafka duplicates via `bronze.kafka_events_deduplicated` logic (dedupe key = `topic` + `kafka_partition` + `kafka_offset`, or `event_id`)
- Preserve event ordering by `occurred_at` (business time), not `captured_at` (ingestion time)
- Watch for late-arriving events vs. their parent record (e.g. a stage-history event landing before its `loan_applications` row is current)

### C. File-sourced datasets (4 datasets — accepted_loans, rejected_applications, banking_products, energy_plans)
- Parsing/schema enforcement is deferred to Silver — `file_arrivals` stores the source file byte-for-byte, so you own full schema validation here
- `accepted_loans` has a 152-column physical schema collapsed to principal fields in Silver — needs an explicit column-selection/mapping spec
- Catalogue tables (`banking_products`, `organisation_relationships`) have no `global_id` — exempt from FK validation, but still need dedup/versioning if reloaded

---

## 3. Table scoping — what to keep for Silver modelling (before payload parsing)

The lineage diagram is itself a scope decision. Of the 32 Bronze-ingested datasets, only **22** are modelled into Silver entities; the other **10** stay in Bronze only for this modelling phase. This matches the README's principle that "Banking, energy, and insurance records remain in Bronze so that Silver modelling must explicitly select scope rather than inheriting ingestion bias." Confirm/finalize this list *before* investing in payload-parsing schemas (§4) — no point versioning a JSON schema for a dataset that isn't in scope yet.

### Keep — in scope for this Silver modelling round (22 datasets)

Grouped by subject area for entity mapping, but the column that actually decides *how* each dataset gets transformed is **Source Category** — it determines which Bronze table you read from and which load pattern applies.

**Involved Party (7 datasets — all Database/CDC)**

| Dataset | Source Category | Bronze table | Load pattern |
|---|---|---|---|
| `customers` | Database (CDC) | `cdc_changes` | Merge current-state (SCD2 derived in Silver) |
| `customer_preferences` | Database (CDC) | `cdc_changes` | Merge current-state (1:1 with customers) |
| `physical_addresses` | Database (CDC) | `cdc_changes` | Merge current-state |
| `organisations` | Database (CDC) | `cdc_changes` | Merge current-state |
| `organisation_party_relationships` | Database (CDC) | `cdc_changes` | Merge current-state |
| `organisation_relationships` | Database (CDC) | `cdc_changes` | Merge current-state (catalogue — no `global_id`) |
| `kyc_records` | Database (CDC) | `cdc_changes` | Merge current-state (1:1 with customers) |

**Arrangement (5 datasets — all Database/CDC)**

| Dataset | Source Category | Bronze table | Load pattern |
|---|---|---|---|
| `banking_accounts` | Database (CDC) | `cdc_changes` | Merge current-state |
| `banking_balances` | Database (CDC) | `cdc_changes` | Merge current-state (1:1 with accounts) |
| `loan_accounts` | Database (CDC) | `cdc_changes` | Merge current-state |
| `mortgage_accounts` | Database (CDC) | `cdc_changes` | Merge current-state |
| `credit_cards` | Database (CDC) | `cdc_changes` | Merge current-state |

**Application (7 datasets — mixed: Database, Event, and File)**

| Dataset | Source Category | Bronze table | Load pattern |
|---|---|---|---|
| `loan_applications` | Database (CDC) | `cdc_changes` | Merge current-state |
| `missing_documents` | Database (CDC) | `cdc_changes` | Merge current-state |
| `application_stage_history` | Event (Kafka) | `kafka_events` (dedup) | Append-only |
| `status_change_history` | Event (Kafka) | `kafka_events` (dedup) | Append-only |
| `loan_application_events` | Event (Kafka) | `kafka_events` (dedup) | Append-only |
| `accepted_loans` | File | `file_arrivals` | Parse file, no merge (152-col physical schema collapsed to principal fields) |
| `rejected_applications` | File | `file_arrivals` | Parse file, no merge |

**Event (3 datasets — mixed: Database and Event)**

| Dataset | Source Category | Bronze table | Load pattern |
|---|---|---|---|
| `service_cases` | Database (CDC) | `cdc_changes` | Merge current-state |
| `support_interactions` | Event (Kafka) | `kafka_events` (dedup) | Append-only |
| `service_case_events` | Event (Kafka) | `kafka_events` (dedup) | Append-only |

**Scope summary by source category:** 15 of the 22 in-scope datasets are Database/CDC, 5 are Event/Kafka, 2 are File. All Database-sourced datasets read from `bronze.cdc_changes` directly — Silver derives current-state (and SCD2 history, if needed) itself rather than depending on a separate pre-built view. Note that `raw_payload` JSON parsing (§4) only applies to the 20 Database + Event datasets (`cdc_changes` and `kafka_events` rows) — the 2 File datasets (`accepted_loans`, `rejected_applications`) are parsed as structured file records (CSV/Parquet-style column parsing), not JSON payload parsing.

### Exclude — remain Bronze-only, not modelled in this round (10 datasets)

| Dataset | Source Category | Domain |
|---|---|---|
| `banking_direct_debits` | Database (CDC) | Banking (adjacent) |
| `banking_payees` | Database (CDC) | Banking (adjacent) |
| `banking_scheduled_payments` | Database (CDC) | Banking (adjacent) |
| `banking_transactions` | Database (CDC) | Banking (adjacent/transactional) |
| `banking_products` | File | Catalogue |
| `energy_accounts` | Database (CDC) | Energy |
| `energy_service_points` | Database (CDC) | Energy |
| `energy_invoices` | Database (CDC) | Energy |
| `energy_plans` | File | Catalogue |
| `insurance_policies` | Database (CDC) | Insurance |

### Why this matters before parsing payloads
- **Don't build/version JSON schemas for out-of-scope datasets** — that effort is wasted until a future modelling round explicitly brings energy/insurance/transactions/etc. into Silver
- **Filter early in the pipeline, by source table**: `cdc_changes` and `kafka_events` reads should filter on `source_dataset` to just their in-scope rows *before* any `from_json` parsing runs; `file_arrivals` reads should filter to just `accepted_loans`/`rejected_applications` before file parsing — avoids paying parsing cost on data you won't model
- **Source Category, not Subject Area, decides the transform code path** — a `dp.table` (or job) reading from `cdc_changes` needs SCD2/merge logic; one reading from `kafka_events` needs append-only + dedup logic; one reading from `file_arrivals` needs file-schema parsing logic. Two datasets in the same Silver subject area (e.g. `loan_applications` vs `loan_application_events`, both "Application") can require completely different transform code because they come from different sources
- **This is a Silver-modelling boundary, not an ingestion boundary** — all 32 datasets are still ingested to Bronze per the README ("every source is ingested; business relevance is decided only after Silver contracts are approved"); exclusion here doesn't touch the Bronze pipeline
- **Revisit exclusions when scope expands** — `banking_transactions` in particular is a near-certain candidate for a future modelling round (transaction-level Gold use cases), so keep the exclusion list versioned/documented rather than implicit in code

---

## 4. Payload parsing — `raw_payload` (CDC and Kafka events)

Both `bronze.cdc_changes` and `bronze.kafka_events` land the actual business data as a **JSON string** in `raw_payload`. Bronze intentionally does not parse it — that work belongs entirely to Silver. This is a first-class task, not a side-detail:

- **Define an explicit schema per dataset** for `from_json()` — do not rely on schema inference (`schema_of_json` on a sample) in the pipeline itself; infer once at design time, then hard-code/version the `StructType` (or store it in a schema registry / config file per dataset, since this repo is config-driven)
- **CDC payload shape (Debezium-style envelope)**: expect the JSON to carry `before` / `after` states plus operation metadata. Combine this with the `operation` column already exposed by `cdc_changes` (insert/update/delete/snapshot) to decide which side of the envelope (`before` vs `after`) to project into Silver, and whether a row represents a delete tombstone
- **Kafka event payload shape**: parse `raw_payload` per `event_type`/`source_dataset` — different event types on the same topic may carry different payload shapes, so branch parsing logic on `event_type`
- **Flatten nested/array structures** as needed for the target Silver columns (e.g. nested address blocks, arrays of document types)
- **Type casting**: JSON strings carry dates, decimals, and booleans as text/numbers with no guaranteed formatting — explicitly cast to the target Silver types (don't let `from_json` silently null out a field it can't infer)
- **Null-on-parse-failure vs quarantine**: `from_json` returns `null` for malformed JSON or schema mismatches by default — do not let that pass silently. Add an explicit check (e.g. `_corrupt_record` pattern or a post-parse null-check on required fields) and route failures to a quarantine table rather than dropping them
- **Schema drift handling**: source systems evolve — decide a policy for new/missing fields in `raw_payload` versus the hard-coded schema (fail the batch, quarantine the row, or default-fill), and version your per-dataset schema definitions so drift is traceable
- **Never log parsed PII payload contents** — Bronze is Protected data and the Zero Trust boundary explicitly forbids PII/raw payload values in application logs, so parsing/debugging code must not `print`/log field values, only counts or field names
- **Reconcile parsed row counts** back against the manifest record counts and raw `cdc_changes` row counts per `source_dataset`, as a check that parsing didn't silently drop rows

---

## 5. Transformations visible directly in the lineage diagram

- **Field renames/reshaping**: e.g. `full_name ← FirstName + LastName`, `name_token ← derived`
- **PII masking**: `email_masked`, `phone_masked`, `abn_masked`, `acn_masked`, `card_number_masked`, `credit_limit_masked`, `loan_amount_masked`, `requested_amount_masked`, `current_balance_masked` — build a consistent masking/tokenization utility; check whether `name_token` needs to be reversible tokenization vs one-way masking for the rest, per the org's data classification policy
- **Derived business fields**: e.g. `is_business_arrangement ← derived: organisationId IS NOT NULL`, `interest_type ← derived from interestRate`, `is_sla_breached ← derived (now() > slaDeadline AND status IN ('open','in_progress'))`, `duration_days ← derived exitedAt - enteredAt (NULL if current)`, `is_current_stage ← derived exitedAt ISNULL`, `is_invalid_or_expired ← derived: status = REJECTED OR expiryDate < now()`
- **Enrichment joins**: e.g. `ip_kyc.record_type ← derived`, `arr_loan.status` combining multiple source signals

---

## 6. Data quality / integrity work

- Enforce **`global_id` referential integrity**: every non-catalogue row's `global_id` must exist in `ip_individual`/`customers` — build this as a Silver-layer DQ gate (quarantine table for orphans), not a one-off validation script
- Enforce `organisationId` format validation (`ORG-00000`..`ORG-{N-1}`)
- Config-driven expectations per table: nullability, type checks, valid value sets (e.g. `partyRole`, `authorityLevel`, `customerType` enums) — good fit for Delta Live Tables `EXPECT` constraints given the pipeline already runs on `pyspark.pipelines`
- Reconciliation against the micro-batch manifest (record counts, SHA-256 hashes) as a check before promoting Bronze data to Silver
- Tolerances and quarantine references should be config-driven, per the stated Zero Trust boundary ("Silver will implement config-driven DQ, tolerances, quarantine references, and CDE lineage")

---

## 7. Platform/pipeline mechanics (Databricks-specific, this repo)

- Silver tables are declared in the **same** `0-ai-trust-medallion` pipeline as Bronze (Free Edition allows only one active pipeline) — use `pipelines/silver`, with a per-dataset 15-minute `pipelines.trigger.interval`, not a separate triggered pipeline or Job
- Downstream tables must use the shared refresh policy explicitly:
  ```python
  from pyspark import pipelines as dp
  from framework.refresh_policy import downstream_microbatch_spark_conf

  @dp.table(spark_conf=downstream_microbatch_spark_conf())
  def customer_curated():
      ...
  ```
- Idempotency/checkpointing: Silver reads from Bronze via structured streaming — don't `--full-refresh` casually, since external sinks are append-only and a full refresh resets checkpoints without clearing sink data
- Access control / column masking at the Unity Catalog level for PII fields, layered on top of (or instead of) manual masking transforms
- All Silver table data must remain in the team-owned AWS account (no Databricks-managed table storage), consistent with the Bronze pattern
- Testing: unit tests per transformation/parsing function, plus integration tests validating row counts against SCHEMA.md's expected ratios (e.g. `banking_balances` should be exactly 1:1 with `banking_accounts`)

---

## 8. Suggested sequencing

1. Land Bronze correctly first (already done) — raw `cdc_changes` capture, event dedup, file byte-for-byte landing (SCD2/current-state logic is Silver's job, not Bronze's)
2. Lock the 22-dataset scoping decision (§3) — sign off on keep/exclude before any parsing work starts
3. Build and version the per-dataset JSON schemas for `raw_payload` parsing (CDC + Kafka), scoped only to the 22 in-scope datasets — this blocks everything downstream of those sources
4. Build Involved Party Silver entities first (everything else FKs to `global_id`/`organisationId` from here)
5. Then Arrangement and Application in parallel (both depend on Involved Party)
6. Then Event entities last (depend on Application via `applicationId` FK on `service_cases`)
7. Layer in DQ expectations, quarantine routing, and masking as you build each entity — not as a separate pass afterward
