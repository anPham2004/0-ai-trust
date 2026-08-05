# 0 AI Trust

0 AI Trust is a complete data platform demonstration for a banking customer and banker-assistance use case. It shows how heterogeneous operational data is collected, retained as auditable raw history, validated against data contracts, modelled into trusted Silver entities, and exposed through governed Gold and semantic products for AI.

The project deliberately includes unrelated topics such as energy and insurance in the ingestion boundary. This reflects a real enterprise platform: ingestion is source-led and does not decide business scope. Scope, quality, relationships, privacy, and AI eligibility are decided downstream through contracts, modelling, Unity Catalog governance, and semantic views.

The project uses synthetic data only. No real customer names, account numbers, phone numbers, email addresses, or confidential banking information are used. The implementation is designed to be demonstrable on Databricks Free Edition with the source simulator hosted on AWS.

## 1. Executive overview

The platform follows a source-to-serving flow:

~~~text
Operational sources -> AWS S3 Landing -> Bronze -> Silver -> Gold -> Semantic views -> Genie
                         immutable       raw       trusted    serving    governed AI context
~~~

Landing is the replay boundary and keeps source-native envelopes or file content. Bronze is an append-only, source-aligned Delta history. Silver applies contracts, quality rules, dependency checks, deduplication, masking, and canonical modelling. Gold provides dimensions, facts, bridges, and denormalised context products. The semantic layer exposes only audience-appropriate metric views.

## 2. Technology stack

| Concern | Technology | Role |
|---|---|---|
| Source simulation | Python, PostgreSQL, Debezium, Apache Kafka | Generates database changes, business events, invalid records, duplicates, stale records, and relationship violations |
| Cloud boundary | AWS EC2, Amazon S3, IAM, Terraform | Hosts the simulator and provides the landing boundary |
| File ingestion | Databricks Auto Loader | Detects new landing files incrementally and supports schema evolution |
| Processing engine | Apache Spark Structured Streaming | Processes Bronze and downstream micro-batches with checkpoints and watermarks |
| Pipeline API | Spark Declarative Pipelines, using from pyspark import pipelines as dp | Declares streaming tables, materialized views, and AUTO CDC flows |
| Storage | Delta Lake | Provides ACID tables, history, Change Data Feed, and replayable state |
| Governance | Unity Catalog | Controls schemas, grants, lineage, tags, column masks, and storage |
| Contracts | ODCS YAML | Defines schema, ownership, lineage, quality rules, classification, and AI policy |
| Serving | Gold Delta tables and SQL metric views | Publishes marts and AI-ready context |
| AI interface | Databricks Genie | Queries the governed semantic layer |
| Validation | Pytest, contract validators, SQL checks, GitHub Actions | Runs unit, contract, architecture, and live quality checks |

## 3. Dataset topics and source allocation

The project has six topic families. Customer, arrangement, application, and service are the primary banking scope. Energy and insurance are intentionally retained as unrelated enterprise data to demonstrate that Bronze can ingest broad source inventories without leaking out-of-scope data into Silver or AI products.

| Topic family | Example datasets | Ingestion category |
|---|---|---|
| Customer and organisation | customers, organisations, customer references, physical addresses, organisation relationships, party relationships, KYC records | Database CDC |
| Arrangement and banking | banking accounts, loan accounts, mortgage accounts, credit card accounts | Database CDC |
| Application and lending | loan applications, application stage history, status history, missing documents, application events, accepted loans, rejected applications | Database CDC, event, and file |
| Service and support | service cases, support interactions, service case events | Database CDC and event |
| Energy | energy accounts, usage or provider reference data when present in the source inventory | File or event, retained as out-of-scope Bronze data |
| Insurance | policy or claim reference data when present in the source inventory | File or event, retained as out-of-scope Bronze data |

The authoritative routing file is [contracts/source/source_inventory.yml](contracts/source/source_inventory.yml). The complete field inventory and relationships are documented in [SCHEMA.md](SCHEMA.md).

There are exactly three ingestion categories:

~~~text
Database = PostgreSQL transaction-log changes transported through Debezium
Event    = Apache Kafka business events written to the Landing boundary
File     = S3 reference or legacy files delivered in batch or micro-batch
~~~

Kafka, CDC, and file micro-batches are transport mechanisms, not additional business topics.

## 4. End-to-end architecture

### 4.1 Ingestion and Landing

~~~text
Database transaction log -> Debezium -> S3 Landing cdc/<dataset>/
Kafka business event     -> exporter -> S3 Landing event/<dataset>/
Reference or legacy file -> exporter -> S3 Landing file/<dataset>/
Manifest heartbeat                  -> S3 Landing manifests/
~~~

Landing preserves source-native envelopes, operation metadata, offsets, LSNs, checksums, and file content. It is immutable so failed downstream transformations can be replayed without re-querying the source.

### 4.2 Bronze

Bronze uses one source-aligned table per dataset. It parses the transport envelope while retaining the business record and ingestion metadata. It does not apply business masking, current-state merging, or Silver joins.

~~~text
cdc_<dataset>                 database CDC history, append-only, Delta CDF enabled
event_<dataset>               immutable event and history records, Delta CDF enabled
file_<dataset>                source-file records, Delta CDF enabled
control_ingestion_manifests   checksum, count, freshness, LSN, and offset evidence
ingestion_quarantine          malformed transport or rescued-schema records
~~~

Bronze runs continuously with a one-minute trigger. The load type distinguishes initial and incremental records. Source LSN, Kafka offset, commit timestamp, source file, and ingestion timestamp preserve replay and lineage evidence.

### 4.3 Silver

Silver reads Bronze Delta Change Data Feed through readStream. Source contracts validate transport and source schema. Silver contracts apply hard and warning expectations, type conversion, deduplication, masking, canonical modelling, and cross-entity dependency checks.

Mutable CDC entities use create_auto_cdc_flow for SCD2 history. Immutable events and file records use streaming tables with watermark-based deduplication. Joined current-state entities use a controlled snapshot flow. Hard failures go to quarantine.record_failures; missing parents and incomplete lifecycle dependencies go to quarantine.dependency_violations.

Spark evaluates rules over distributed micro-batches. The logic is record-oriented, but it does not loop through records in Python.

### 4.4 Gold and semantic serving

Gold contains dimensions, facts, bridges, and a denormalised subject-context snapshot. The semantic schema contains 20 deployed metric views: 13 for Banker.AI and 7 for Customer.AI.

~~~text
Gold dimensions and facts
  -> Banker.AI views for authorised internal banker use
  -> Customer.AI views with identity-aware self-service scope
  -> Genie system prompt ontology and query-routing rules
~~~

Unity Catalog grants, row filters, and column masks remain authoritative. The Genie prompt is an interpretation and routing layer, not a security boundary.

## 5. Governance, quality, and zero trust

ODCS contracts define dataset identity, ownership, lineage, schema, critical data elements, quality rules, tolerances, classification, and AI policy. Bronze remains source-aligned and does not contain business-quality assertions. Silver is the trusted quality boundary. Gold contracts define serving scope and audience restrictions.

~~~text
Malformed envelope or rescued schema -> bronze.ingestion_quarantine
Record-level hard rule failure        -> quarantine.record_failures
Missing parent or lifecycle failure   -> quarantine.dependency_violations
Warning rule                           -> expectation metrics and warning metadata
~~~

Sensitive fields are masked, tokenised, bucketed, or removed before AI serving. Raw email, phone, TFN, card number, raw payload, risk rating, PEP status, sanctions status, internal notes, and unrestricted operational reasons are excluded from AI-ready context.

## 6. Runtime and deployment

Databricks uses serverless Spark Declarative Pipelines and Unity Catalog. The active resource is 0-ai-trust-medallion; its source directories are pipelines/bronze, pipelines/silver, and pipelines/gold. The project does not use the legacy dlt import, Lakeflow Connect managed ingestion, or direct outbound source connections from Free Edition.

AWS hosts the source simulator and S3 landing boundary. Terraform provisions EC2, IAM, S3, monitoring, and the budget guard. Databricks control-plane metadata and pipeline state remain platform metadata; source and business data are governed through Unity Catalog storage configuration.

Local validation:

~~~bash
python -m pip install -r tests/requirements.txt
python -m pytest tests -q
~~~

Databricks CLI:

~~~bash
databricks auth login --host https://dbc-00affa79-1317.cloud.databricks.com --profile g3-databricks
databricks current-user me --profile g3-databricks
databricks pipelines list-pipelines --profile g3-databricks
~~~

Do not run a full refresh casually. A full refresh resets streaming state and replays immutable Landing data; use it only with reconciliation evidence.

## 7. Repository structure

~~~text
contracts/              ODCS source, Silver, and Gold contracts
dev-tools/datagen/      deterministic synthetic data and reference schemas
source-simulator/       PostgreSQL, Debezium, Kafka, exporter, and dirty-data generator
pipelines/bootstrap/    Unity Catalog DDL migrations
pipelines/bronze/       source-aligned ingestion declarations
pipelines/framework/    contract, DQ, masking, quarantine, and dependency code
pipelines/silver/       canonical Silver model registrations
pipelines/gold/         Gold SQL dimensions, marts, and context products
pipelines/semantic/     Banker.AI and Customer.AI metric views and tags
infrastructure/aws/     Terraform for the AWS simulator boundary
tests/                  architecture, unit, contract, Silver, and Gold tests
docs/                   focused deep dives and visual pipeline documentation
~~~

## 8. Validation results

The local test suite currently reports 108 passed tests. A further 51 live tests are skipped when Databricks runtime credentials are unavailable; these are environment-gated checks rather than silently passing assertions. Contract validation, architecture checks, unit tests, Silver tests, and Gold tests are separated so failures identify the affected boundary.

The deployed semantic layer currently contains 20 metric views, split between 13 Banker.AI views and 7 Customer.AI views. Live evidence must include the latest successful pipeline update, Silver freshness, passed and failed rule counts, quarantined record counts, and representative failure samples. A pipeline object remaining in RUNNING state is not sufficient evidence when Free Edition rejects a newer update because of its daily resource limit.

## 9. Additional documentation

Read this README first. Use the following focused documents only when deeper detail is needed:

- [SCHEMA.md](SCHEMA.md): complete generated dataset inventory, fields, source routing, and relationships.
- [docs/pipeline-architecture.md](docs/pipeline-architecture.md): Bronze, Silver, Gold, CDF, AUTO CDC, and runtime dependencies.
- [docs/data-contracts.md](docs/data-contracts.md): ODCS structure, executable rules, lineage, and AI policy.
- [docs/testing-and-evidence.md](docs/testing-and-evidence.md): local tests, live DQ checks, evidence capture, and CI.
- [docs/genie-system-prompt.md](docs/genie-system-prompt.md): copy-ready ontology and system prompt aligned with deployed semantic views.
- [docs/pipeline-dag.md](docs/pipeline-dag.md): readable dependency map. docs/pipeline-dag.dot is the editable Graphviz source; SVG and PNG are rendered outputs.
