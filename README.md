# 0 AI Trust

This repository simulates a NAB-style data platform for the Banker Assist assignment. Bronze ingestion and the approved 19-entity Silver model are implemented. Gold remains outside the active pipeline until Silver deployment evidence is complete and its former row-level `dq_status` dependency is replaced by the agreed metrics-only quality interface.

## Outcome

Databricks is used for serverless compute, incremental processing, lineage, and Unity Catalog governance. All source files and all Delta table data are stored in the team-owned AWS account:

```text
PostgreSQL transaction log ── Debezium (HVR analogue) ─┐
Apache Kafka business events ──────────────────────────┼─> S3 native landing
Periodic vendor/reference files ───────────────────────┘          │
                                                                  │ Auto Loader
                                                                  v
                                                   External Delta Bronze tables
                                                                  │
                                                   Unity Catalog governance
```

No business data is written to Databricks-managed table storage. Databricks necessarily retains control-plane metadata, pipeline state, lineage, and deployed source code. Those are platform metadata, not source or business table data.

## Unity Catalog and S3 layout

`0-ai-trust` is the catalog. The only project schemas are `bronze`, `silver`, and `gold`. Because the catalog contains a hyphen, SQL must quote it with backticks.

Databricks also exposes the read-only `information_schema` system namespace automatically. It is platform metadata, not a project layer. The automatically created `default` schema is removed by setup.

```text
`0-ai-trust`
├── bronze
│   ├── landing                         external volume
│   ├── cdc_<dataset>                   23 source-aligned streaming tables
│   ├── event_<dataset>                 5 source-aligned streaming tables
│   ├── file_<dataset>                  4 row-level CSV streaming tables
│   ├── ingestion_quarantine            malformed/rescued records
│   └── control_ingestion_manifests     reconciliation control table
├── silver
│   ├── 12 SCD2 streaming tables        mutable CDC-backed entities
│   └── 7 append-only streaming tables  event/history/file entities
└── gold
    ├── 10 external Delta star tables
    └── 10 AI-ready logical views
```

```text
s3://g3-assignment/g3/0-ai-trust/
├── bronze/
│   ├── landing/
│   │   ├── cdc/<dataset>/              nested CDC envelope and record image
│   │   ├── event/<dataset>/            nested event envelope and record
│   │   ├── file/<dataset>/             source files, byte-for-byte
│   │   ├── quarantine/<source>/        malformed transport records
│   │   └── manifests/                  heartbeat and reconciliation receipts
│   └── __managed/                      UC-managed Bronze streaming tables
├── silver/__managed/                   reserved Silver managed root
├── gold/
│   ├── __managed/                      reserved Gold managed root
│   └── tables/<table>/                 future external Delta star tables
└── __managed/                          catalog-level managed metadata
```

The former top-level `landing/` layout is legacy and must not receive new objects.

Dropping an external table removes Unity Catalog metadata but does not delete its S3 files. S3 lifecycle and deletion are therefore an AWS owner responsibility. Direct S3 access bypasses Unity Catalog controls and must remain restricted to infrastructure roles.

## Bronze semantics

The native S3 landing is the immutable, schema-on-read audit trail and is the rerun boundary.

Bronze is logical raw and source-aligned. It structurally parses source records without applying business casts, masking, deduplication, or merge logic:

- `cdc_<dataset>` flattens the Debezium `after` image, or `before` for deletes, and retains operation, LSN, Kafka offset, commit time, and `_load_type`.
- `event_<dataset>` exposes source event fields with event ID, type, correlation, Kafka transport metadata, and `_load_type`.
- `file_<dataset>` parses CSV into rows while leaving source columns as strings. The original CSV remains byte-for-byte in Landing.
- `_rescued_data` prevents schema mismatches from being dropped; affected rows also appear in `ingestion_quarantine`.
- `control_ingestion_manifests` makes checksum, record-count, freshness, LSN, and offset evidence queryable.

All Bronze business tables are append-only. Initial and incremental records share a table and are distinguished by `_load_type`. Silver owns deduplication, current-state merge, SCD, typing, and business validation. Initial database onboarding follows the HVR pattern: complete the initial snapshot, retain its source position, then continue from the same LSN without a gap.

Never run a full refresh casually. It resets streaming state and replays the immutable landing. Use it only for an intentional, audited Bronze rebuild.

Do not rename or change `zero_ai_trust_landing` after Auto Loader has checkpointed it. Managed file events bind continuation state to its queue. A deliberate queue replacement requires a stopped pipeline and a clean replay from immutable landing.

## Source allocation

The 32 generated datasets are intentionally broader than the Banker Assist use case. Banking, energy, and insurance records remain in Bronze so that Silver modelling must explicitly select scope rather than inheriting ingestion bias.

- Database source: 23 mutable authoritative datasets, ingested from PostgreSQL transaction logs through Debezium CDC.
- Event source: 5 true event/history datasets, transported through Apache Kafka.
- File source: 4 legacy/reference/history datasets, delivered as S3 micro-batches.

These are the only three ingestion source categories. CDC, Apache Kafka, and S3 micro-batch are mechanisms used by those categories, not additional source types.

The authoritative routing mapping is `contracts/source/source_inventory.yml`. See [SCHEMA.md](SCHEMA.md) for the complete dataset reference, entity relationships, row counts, and the data carried by each ingestion source. The machine-generated physical schema remains in `dev-tools/datagen/output/master-schema.json`.

The simulator uses Debezium because HVR/Precisely is proprietary. It reproduces the relevant contract: initial snapshot, transaction-log CDC, source LSN, operation type, deletes, and continuous continuation. It must be described as an HVR analogue, not as HVR itself.

## Declarative pipeline terminology

The pipeline uses the successor Spark Declarative Pipelines interface, `from pyspark import pipelines as dp`, Spark Structured Streaming, and Auto Loader. It does not use the legacy `dlt` Python module, Lakeflow Connect managed database connectors, or direct outbound database/Kafka connections from Free Edition.

Databricks renamed Delta Live Tables to Lakeflow Spark Declarative Pipelines. The pipeline runs continuously and owns the Auto Loader and Silver checkpoints. Bronze flows run every minute. Silver reads Bronze Delta CDF with `readStream` and runs in 15-minute micro-batches inside the same pipeline.

Gold is deliberately outside the Spark Declarative Pipeline source glob. Its SQL is not activated by this change because the previous draft expects row-level `dq_status`, which no longer belongs in canonical Silver. Gold quality context must be sourced from aggregate pipeline metrics or a later approved control interface before orchestration is enabled.

## Repository structure

```text
.
├── README.md
├── SCHEMA.md                    dataset schema and ingestion source reference
├── Makefile                    single entry point for common operations
├── .github/workflows/
│   └── sync-databricks.yml     sync develop into the shared Databricks Git Folder
├── contracts/
│   ├── source/                 source routing and ingestion mechanism
│   ├── bronze/                 intentionally empty; raw retention has no business contract
    │   ├── silver/                 19 active entity schema/DQ contracts
│   └── gold/                   CDE classification, scope, masking, and AI consumption
├── dev-tools/
│   └── datagen/
│       ├── requirements.txt
│       ├── scripts/            deterministic data generation
│       ├── reference-schemas/  upstream schema references
│       └── output/             generated schema contract; row files are ignored
├── source-simulator/
│   ├── .env                    local runtime configuration; ignored by Git
│   ├── compose.yaml            EC2 runtime boundary
│   ├── postgres/               authoritative database source
│   ├── bootstrap-loader/       initial bulk load before CDC continuation
│   ├── cdc-connector-init/     Debezium registration
│   ├── activity-generator/     periodic database, Kafka, and file activity
│   ├── landing-exporter/       S3 micro-batch exporter
│   └── credit-monitor/         AWS credit guard
├── pipelines/
│   ├── bootstrap/              versioned Unity Catalog DDL migrations
│   ├── bronze/                 implemented ingestion pipeline
│   ├── silver/                 approved 19-entity incremental model
│   ├── gold-sql/
│   │   ├── star-schema/        idempotent Silver-to-Gold MERGE statements
│   │   └── ai-ready/           role-aware denormalized logical views
│   └── gold/                   intentionally contains no SDP definitions
├── infrastructure/aws/         Terraform for EC2, IAM, S3, and monitoring
└── tests/
    ├── architecture/           repository and ingestion invariants
    ├── unit/                   isolated policy tests
    ├── contracts/              reserved for contract validation
    └── integration/
        ├── silver/             Silver SCD2, DQ, and hand-off acceptance tests
        └── gold/               Gold integrity and AI-ready edge-case tests
```

Naming is deterministic: deployable component directories use `kebab-case`; Python, test, contract, and Terraform identifiers use `snake_case`; SQL migrations use `vNNN_description.sql` and run in lexical order. Do not introduce version suffixes such as `nab-v2` into resource names.

Contract ownership follows the data lifecycle. `source/source_inventory.yml` routes all 32 datasets into ingestion. Bronze intentionally has no transform contract because it retains every native record. Silver owns one active contract per approved entity; hard rules compile to native drop expectations and warning rules compile to metrics-only expectations. Gold owns CDE classification and Banker Assist consumption scope.

There are no PowerShell deployment scripts. Commands below use standard Terraform, Docker, AWS, Databricks, SSH, and Git CLIs and work from any operating system that provides those tools.

## Prerequisites

- AWS CLI profile `g3-aws`
- Databricks CLI profile `g3-databricks`
- Terraform
- Docker for local validation
- OpenSSH
- GNU Make
- Python 3.12+
- Storage credential `zero_ai_trust_storage_credential`; narrowly scoped external locations for catalog/schema managed roots, Bronze landing, and future Gold tables.

Confirm identity before changing infrastructure:

```bash
aws sts get-caller-identity --profile g3-aws
databricks current-user me --profile g3-databricks
```

Check remaining AWS credit:

```bash
aws freetier get-account-plan-state --region us-east-1 --profile g3-aws
```

## Generate and test data

The committed schema is generated from the synthetic data project. Generated row files are local artefacts and are ignored by Git.

```bash
python -m pip install -r dev-tools/datagen/requirements.txt
python dev-tools/datagen/scripts/run_all.py
make test
python -m compileall pipelines source-simulator dev-tools/datagen/scripts
make validate
```

## Provision AWS

Maintain the real, Git-ignored `infrastructure/aws/terraform.tfvars` locally with the administrator `/32` CIDRs, SSH public key, and alert email. Never commit this file.

```bash
export AWS_PROFILE=g3-aws
terraform -chdir=infrastructure/aws init
terraform -chdir=infrastructure/aws fmt -check
terraform -chdir=infrastructure/aws validate
terraform -chdir=infrastructure/aws plan -out=plan.tfplan
terraform -chdir=infrastructure/aws apply plan.tfplan
```

The SNS subscription sends a confirmation email. The remaining-credit alert is inactive until that subscription is confirmed.

Get runtime values:

```bash
terraform -chdir=infrastructure/aws output
```

## Deploy the source simulator

Start the instance if required and obtain its public DNS:

```bash
INSTANCE_ID=$(terraform -chdir=infrastructure/aws output -raw instance_id)
REGION=$(terraform -chdir=infrastructure/aws output -raw region)
aws ec2 start-instances --instance-ids "$INSTANCE_ID" --region "$REGION" --profile g3-aws
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION" --profile g3-aws
HOST=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" --profile g3-aws --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
```

Package only deployable files and upload them:

```bash
tar --exclude='__pycache__' --exclude='*.pyc' --exclude='.env' -czf 0-ai-trust-runtime.tgz source-simulator
scp -i ~/.ssh/g3-assignment 0-ai-trust-runtime.tgz "ubuntu@$HOST:/tmp/"
ssh -i ~/.ssh/g3-assignment "ubuntu@$HOST" 'sudo mkdir -p /opt/0-ai-trust && sudo tar -xzf /tmp/0-ai-trust-runtime.tgz -C /opt/0-ai-trust'
```

`source-simulator/.env` is the single runtime configuration source and is ignored by Git because it contains the real database password and alert ARN. Transfer it separately and restrict it to root:

```bash
scp -i ~/.ssh/g3-assignment source-simulator/.env "ubuntu@$HOST:/tmp/0-ai-trust.env"
ssh -i ~/.ssh/g3-assignment "ubuntu@$HOST" 'sudo install -m 600 -o root -g root /tmp/0-ai-trust.env /opt/0-ai-trust/source-simulator/.env && rm /tmp/0-ai-trust.env'
```

Start the continuously running simulator:

```bash
ssh -i ~/.ssh/g3-assignment "ubuntu@$HOST" 'cd /opt/0-ai-trust/source-simulator && sudo docker compose up -d --build --remove-orphans'
ssh -i ~/.ssh/g3-assignment "ubuntu@$HOST" 'cd /opt/0-ai-trust/source-simulator && sudo docker compose ps'
```

The activity service inserts and updates PostgreSQL applications and publishes meaningful Kafka events every minute. Every hour it drops a new file version. The exporter drains backlog in bounded 5,000-record chunks, then flushes available CDC, Kafka, and file data to S3 every 60 seconds. The continuous Bronze pipeline discovers new landing objects with Auto Loader. Logs contain only operational counts and error types, never payload values or credentials.

Inspect health without printing source payloads:

```bash
ssh -i ~/.ssh/g3-assignment "ubuntu@$HOST" 'cd /opt/0-ai-trust/source-simulator && sudo docker compose logs --tail=50 activity-generator landing-exporter credit-monitor'
```

Emergency stop:

```bash
aws ec2 stop-instances --instance-ids "$INSTANCE_ID" --region "$REGION" --profile g3-aws
```

## Connect DataGrip through SSH

Keep this tunnel open:

```bash
ssh -i ~/.ssh/g3-assignment -N -L 15432:127.0.0.1:5432 "ubuntu@$HOST"
```

DataGrip settings:

```text
Host: 127.0.0.1
Port: 15432
Database: g3_sources
User: g3_app
Password: value from EC2 `/opt/0-ai-trust/source-simulator/.env`
```

PostgreSQL and Kafka Connect ports bind only to EC2 loopback and are not exposed publicly.

## Initialize Unity Catalog

Run `v001_create_external_objects.sql` once through Databricks SQL Editor as the storage owner. `v002_remove_legacy_bronze_objects.sql` removes the superseded shared Bronze model. When replacing the old Silver MVs, stop the pipeline and run `v004_rebuild_silver_streaming_tables.sql` once; it does not touch Bronze or Landing. Do not run `v003_create_gold_external_tables.sql` until Silver deployment evidence is complete.

- catalog `0-ai-trust`
- schemas `bronze`, `silver`, and `gold`
- external landing volume
- 32 source-aligned Bronze streaming tables plus shared quarantine and manifest control tables
- the Bronze schema managed location is the team-owned S3 prefix `bronze/__managed`

Confirm that every physical object resolves to the team-owned S3 hierarchy. Unity Catalog does not allow an explicit `path` on a pipeline streaming table; it places the table under the schema's S3 managed location. A streaming table is reported as `STREAMING_TABLE`; its storage path proves storage ownership:

```sql
SELECT table_schema, table_name, table_type, data_source_format, storage_path
FROM `0-ai-trust`.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;
```

## Configure and run the pipelines

After repository changes are merged into `develop`, the GitHub workflow tests and syncs them into `/Shared/0-ai-trust`. The single `0-ai-trust-medallion` pipeline runs Bronze continuously and processes Silver every 15 minutes. Gold SQL is outside the pipeline source glob and cannot execute accidentally. Its root directory is:

```text
Root folder: /Workspace/Shared/0-ai-trust/pipelines
```

The Bronze flows use Spark Structured Streaming and Auto Loader with a one-minute trigger. Silver uses streaming CDF reads with the shared 15-minute trigger. Twelve mutable entities use native AUTO CDC SCD2 with timestamp `__START_AT` and nullable `__END_AT`; seven immutable entities append incrementally. Hard DQ failures are dropped from Silver and counted in pipeline expectation metrics. No Silver quarantine tables or external Databricks Job are required.

The shared refresh policy is applied explicitly:

```python
from pyspark import pipelines as dp
from framework.refresh_policy import downstream_microbatch_spark_conf

dp.create_streaming_table(
    name="`0-ai-trust`.silver.ip_individual",
    spark_conf=downstream_microbatch_spark_conf(),
)
```

Useful Databricks CLI operations:

```bash
# Confirm the synchronized Git Folder and locate the pipeline/job IDs.
databricks repos get 1237804683921450 --profile g3-databricks
databricks pipelines list-pipelines --profile g3-databricks
databricks jobs list --profile g3-databricks

# Validate all continuous pipeline definitions without writing data.
databricks pipelines start-update <pipeline-id-from-list> \
  --validate-only --profile g3-databricks

# Start continuous processing. Use --full-refresh only for an intentional rebuild.
databricks pipelines start-update <pipeline-id-from-list> \
  --profile g3-databricks

```

Validate representative Bronze tables:

```sql
SELECT _load_type, _operation, COUNT(*)
FROM `0-ai-trust`.bronze.cdc_customers
GROUP BY ALL;

SELECT _load_type, COUNT(*)
FROM `0-ai-trust`.bronze.event_loan_application_events
GROUP BY ALL;

SELECT COUNT(*) FROM `0-ai-trust`.bronze.file_accepted_loans;
SELECT failure_reason, COUNT(*)
FROM `0-ai-trust`.bronze.ingestion_quarantine
GROUP BY failure_reason;
```

## GitHub and Databricks collaboration

GitHub is the source of truth. Each developer works on a feature branch and opens a pull request into `develop`. Do not edit the shared Databricks Git Folder directly.

```text
feature branch -> pull request -> develop -> GitHub Action -> Databricks Git Folder
```

`.github/workflows/sync-databricks.yml` runs when `develop` changes and calls `databricks repos update` for the shared Git Folder `/Shared/0-ai-trust`. It syncs source code only and does not start the pipeline. Configure repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` before enabling it.

## Zero Trust boundaries

- Bronze is Protected data. It is not an AI or Genie source.
- No PII or raw payload values may be written to application logs.
- Every source is ingested; business relevance is decided only after Silver contracts are approved.
- Silver implements config-driven native expectations, SCD2/append history, masking/tokenisation, and L1 lineage. Rejected rows remain replayable from Bronze; aggregate failures are recorded in the pipeline event log.
- Gold SQL implements purpose-bound views, no raw PII projection, quality evidence, known limitations, and AI-ready context. Unity Catalog grants and the `banker-assist-users` group must still be provisioned before deployment.
- AI must never make credit approval or fraud decisions and must not receive raw Highly Confidential fields.

## Gold execution order after Silver is ready

1. Run `pipelines/bootstrap/v003_create_gold_external_tables.sql`.
2. Run the SQL files in `pipelines/gold-sql/star-schema` in this dependency order: party/KYC/organisation dimensions, arrangement/application/document dimensions, application/service facts, then arrangement snapshots.
3. Run every file in `pipelines/gold-sql/ai-ready`; these statements are idempotent logical-view replacements.
4. Execute `tests/integration/gold/test_required_silver_schema.sql` before refresh, then the remaining Gold SQL tests after refresh.
5. Only after a clean manual end-to-end run, configure a non-overlapping 15-minute Gold refresh Job.

The Gold `MERGE` statements are rerunnable. Additive Silver columns are tolerated because projections are explicit. Removing, renaming, or changing the type of a required Silver field is a breaking change detected by the schema test.

## Assignment readiness and remaining gaps

Prepared in this repository: three ingestion mechanisms, immutable Bronze evidence, more than eight contract-defined DQ rules, external Gold star modelling, hybrid AI-ready views, role-aware restricted fields, lineage/quality/version metadata, reconciliation tests, and tests for duplicates, foreign keys, late data, date ordering, idempotency, schema evolution, freshness limitations, and PII leakage.

Still blocked on downstream deployment evidence:
- Unity Catalog grants and membership of `banker-assist-users`;
- identity-to-party entitlements or row filters for customer-specific access, which are absent from the supplied Silver schema;
- sample AI answers, missing/unsafe/refusal cases, and screenshot evidence;
- clean-state end-to-end rerun, measured freshness/reconciliation results, and the final 15-minute Gold Job;
- final runbook values and submitted output extracts.

No Gold SQL, test, migration, grant, Job, or pipeline update is executed by this change.
