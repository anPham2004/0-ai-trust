# 0 AI Trust

This repository simulates a NAB-style data ingestion boundary for the Banker Assist assignment. The current deliverable stops at Bronze. Silver and Gold contain code templates only and must not create datasets until modelling, data-quality rules, PII policies, and AI consumption contracts are approved.

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
│   ├── cdc_changes                     external Delta table
│   ├── cdc_scd2                        logical SCD2 view
│   ├── kafka_events                    external Delta table
│   ├── kafka_events_deduplicated       view
│   └── file_arrivals                   external Delta table
├── silver                              empty template
└── gold                                empty template
```

```text
s3://g3-assignment/g3/0-ai-trust/
├── landing/
│   ├── raw/cdc/                        Debezium envelopes plus topic/key/offset
│   ├── raw/kafka/                      Kafka envelopes plus transport metadata
│   ├── raw/file/                       source files, byte-for-byte
│   └── manifests/
├── bronze/
│   ├── cdc_changes/                    Delta
│   ├── kafka_events/                   Delta
│   └── file_arrivals/                  Delta
├── silver/                             reserved
├── gold/                               reserved
└── __managed/                          guard root; expected to contain no tables
```

Dropping an external table removes Unity Catalog metadata but does not delete its S3 files. S3 lifecycle and deletion are therefore an AWS owner responsibility. Direct S3 access bypasses Unity Catalog controls and must remain restricted to infrastructure roles.

## Bronze semantics

The native S3 landing is the immutable, schema-on-read audit trail and is the rerun boundary.

Bronze adds only transport and ingestion metadata:

- `cdc_changes` retains every snapshot, insert, update, delete, and tombstone envelope. The `cdc_scd2` view orders versions by source LSN and Kafka offset and exposes `valid_from`, `valid_to`, `is_current`, and `is_deleted`.
- `kafka_events` is append-only because a business event is already historical fact. Pretending that an immutable event is a mutable SCD dimension would destroy its meaning.
- `file_arrivals` stores every source file byte-for-byte. Parsing and business schema decisions are deferred to Silver.

This gives mutable database entities SCD Type 2 behaviour without rewriting the protected raw landing. Kafka event history and immutable file facts remain append-only. Initial onboarding follows the HVR pattern: complete the initial snapshot, retain its source position, then continue from the same LSN without a gap.

Never run a full refresh casually. External sinks are append-only and a full refresh resets pipeline checkpoints without clearing sink data. Recovery must either preserve the checkpoint or deliberately rebuild the affected external path from the immutable landing.

Do not rename or change the URL/file-event configuration of an external location after Auto Loader has checkpointed it. Managed file events bind the checkpoint continuation token to the queue. If that configuration must change, first stop the pipeline, make the location change once, temporarily disable `delta.appendOnly`, truncate the three external sinks, restore `delta.appendOnly`, recreate the pipeline checkpoint, and replay from landing. This recovery was tested during setup; changing the queue without resetting the checkpoint raises `CF_MANAGED_FILE_EVENTS_INVALID_CONTINUATION_TOKEN`.

## Source allocation

The 32 generated datasets are intentionally broader than the Banker Assist use case. Banking, energy, and insurance records remain in Bronze so that Silver modelling must explicitly select scope rather than inheriting ingestion bias.

- Database source: 23 mutable authoritative datasets, ingested from PostgreSQL transaction logs through Debezium CDC.
- Event source: 5 true event/history datasets, transported through Apache Kafka.
- File source: 4 legacy/reference/history datasets, delivered as S3 micro-batches.

These are the only three ingestion source categories. CDC, Apache Kafka, and S3 micro-batch are mechanisms used by those categories, not additional source types.

The authoritative routing mapping is `contracts/source_inventory.yml`. See [SCHEMA.md](SCHEMA.md) for the complete dataset reference, entity relationships, row counts, and the data carried by each ingestion source. The machine-generated physical schema remains in `dev-tools/datagen/output/master-schema.json`.

The simulator uses Debezium because HVR/Precisely is proprietary. It reproduces the relevant contract: initial snapshot, transaction-log CDC, source LSN, operation type, deletes, and continuous continuation. It must be described as an HVR analogue, not as HVR itself.

## DLT terminology

The Bronze code deliberately uses the DLT-compatible `dlt` declarative interface, Spark Structured Streaming, and Auto Loader. It does not use Lakeflow Connect, managed database connectors, Lakeflow Jobs, or direct outbound database/Kafka connections from Free Edition.

Databricks renamed the Delta Live Tables product to Lakeflow Spark Declarative Pipelines. Consequently, workspace metadata may display “Lakeflow pipeline” even though the code uses the compatible `dlt` interface. DLT cannot exist as a separate modern workspace resource under its former product name.

## Repository structure

```text
.
├── README.md
├── SCHEMA.md                    dataset schema and ingestion source reference
├── Makefile                    single entry point for common operations
├── databricks.yml
├── .github/workflows/
│   └── deploy.yml              validated Bundle deployment from main
├── contracts/                  shared data contracts, CDEs, and source inventory
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
│   ├── silver/                 modelling template only
│   └── gold/                   modelling template only
├── infrastructure/aws/         Terraform for EC2, IAM, S3, and monitoring
├── resources/
│   ├── pipelines/              declarative pipeline resources
│   └── jobs/                   reserved for scheduled jobs
└── tests/
    ├── architecture/           repository and ingestion invariants
    ├── unit/                   isolated policy tests
    ├── contracts/              reserved for contract validation
    └── integration/            reserved for pipeline smoke tests
```

Naming is deterministic: deployable component directories use `kebab-case`; Python, test, contract, and Terraform identifiers use `snake_case`; Bundle resource files use `<layer>.pipeline.yml`; SQL migrations use `vNNN_description.sql` and run in lexical order. Do not introduce version suffixes such as `nab-v2` into resource names.

There are no PowerShell deployment scripts. Commands below use standard Terraform, Docker, AWS, Databricks, SSH, and Git CLIs and work from any operating system that provides those tools.

## Prerequisites

- AWS CLI profile `g3-aws`
- Databricks CLI profile `g3-databricks`
- Terraform
- Docker for local validation
- OpenSSH
- GNU Make
- Python 3.12+
- The existing S3 storage credential and external location must have read/write access to `s3://g3-assignment/g3/0-ai-trust/`.

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

The activity service updates one existing PostgreSQL application and publishes one Kafka event every five minutes. Every hour it drops a new file version. The exporter writes available CDC, Kafka, and file data to S3 in five-minute micro-batches. Logs contain only operational counts and error types, never payload values or credentials.

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
Database: nab_sources
User: nab_app
Password: value from EC2 `/opt/0-ai-trust/source-simulator/.env`
```

PostgreSQL and Kafka Connect ports bind only to EC2 loopback and are not exposed publicly.

## Initialize Unity Catalog

Run `pipelines/bootstrap/v001_initial_setup.sql` once through Databricks SQL Editor as the storage owner. Future idempotent migrations use the next `vNNN_description.sql` name. The initial migration creates only:

- catalog `0-ai-trust`
- schemas `bronze`, `silver`, and `gold`
- external landing volume
- three external Bronze Delta tables
- two metadata-only views

Confirm that every physical Bronze table is external:

```sql
SELECT table_schema, table_name, table_type, data_source_format, storage_path
FROM `0-ai-trust`.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;
```

## Deploy and run Bronze

Validate the Git commit before deployment:

```bash
databricks bundle validate -t dev -p g3-databricks
databricks bundle plan -t dev -p g3-databricks
databricks bundle deploy -t dev -p g3-databricks
databricks bundle run bronze_ingestion -t dev -p g3-databricks
```

The pipeline is triggered, not continuous. Each update processes all newly landed files using Structured Streaming checkpoints and then stops. This preserves NAB-style micro-batch semantics and avoids unnecessary Free Edition compute usage.

Validate Bronze:

```sql
SELECT COUNT(*) FROM `0-ai-trust`.bronze.cdc_changes;
SELECT COUNT(*) FROM `0-ai-trust`.bronze.kafka_events;
SELECT COUNT(*) FROM `0-ai-trust`.bronze.file_arrivals;

SELECT source_dataset, COUNT(*) AS versions,
       COUNT_IF(is_current) AS current_versions,
       COUNT_IF(is_deleted) AS delete_markers
FROM `0-ai-trust`.bronze.cdc_scd2
GROUP BY source_dataset
ORDER BY source_dataset;
```

## GitHub and Databricks collaboration

GitHub is the source of truth. Each developer links GitHub through the Databricks GitHub App, creates a personal Git Folder and branch, and opens a pull request. Do not share one Git Folder and do not edit deployed Bundle files under `.bundle`.

```text
feature branch -> pull request -> tests/review -> main -> Bundle deploy
```

`.github/workflows/deploy.yml` validates and deploys the Bundle when `main` changes. Configure repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` before enabling it. If the team does not permit a long-lived GitHub token, disable automated deployment and let the designated release manager run `make deploy` with local Databricks OAuth instead. Production GitHub OIDC deployment requires account-level service-principal federation, which Free Edition does not expose.

## Zero Trust boundaries

- Bronze is Protected data. It is not an AI or Genie source.
- No PII or raw payload values may be written to application logs.
- Every source is ingested; business relevance is decided only after Silver contracts are approved.
- Silver will implement config-driven DQ, tolerances, quarantine references, and CDE lineage.
- Gold will implement purpose-bound access, PII masking, quality evidence, known limitations, and AI-ready semantic context.
- AI must never make credit approval or fraud decisions and must not receive raw Highly Confidential fields.

## Current modelling boundary

`pipelines/silver/template.py` and `pipelines/gold/template.py` are intentionally empty templates. Adding tables to either file before modelling approval is a scope violation for the current phase.
