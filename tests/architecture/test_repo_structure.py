import json
import re
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


class ArchitectureTests(unittest.TestCase):
    def test_repository_has_clear_deployable_boundaries(self):
        for directory in ("dev-tools/datagen", "source-simulator", "pipelines", "infrastructure"):
            self.assertTrue((ROOT / directory).is_dir())
        for legacy in ("datagen", "synthetic-data", "simulator", "databricks", "ref-schema"):
            self.assertFalse((ROOT / legacy).exists())

        self.assertFalse((ROOT / "databricks.yml").exists())
        self.assertFalse((ROOT / "resources").exists())

    def test_repository_conventions_are_synchronised(self):
        migrations = list((ROOT / "pipelines/bootstrap").glob("*.sql"))
        self.assertTrue(migrations)
        for migration in migrations:
            self.assertRegex(migration.name, r"^v\d{3}_[a-z0-9_]+\.sql$")

        workflow = (ROOT / ".github/workflows/sync-databricks.yml").read_text(encoding="utf-8")
        self.assertIn("branches: [develop]", workflow)
        self.assertIn("databricks repos update 1237804683921450 --branch develop", workflow)

        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        for target in ("sim-up:", "sim-down:", "test:", "validate:", "bootstrap:"):
            self.assertIn(target, makefile)

    def test_contracts_are_partitioned_by_lifecycle(self):
        expected = {
            "source": {"source_inventory.yml"},
            "bronze": {".gitkeep"},
            "silver": {
                "ip_individual.yml", "ip_organisation.yml",
                "ip_organisation_party_relationship.yml", "ip_organisation_relationship.yml",
                "ip_kyc_kyb_record.yml", "arr_banking_arrangement.yml",
                "arr_loan_arrangement.yml", "arr_mortgage_arrangement.yml",
                "arr_credit_card_arrangement.yml", "app_application.yml",
                "app_application_stage_history.yml", "app_status_change_history.yml",
                "app_document.yml", "app_application_event.yml",
                "app_accepted_loan.yml", "app_rejected_application.yml",
                "evt_service_case.yml", "evt_support_interaction.yml",
                "evt_service_case_event.yml",
            },
            "gold": {"ai_ready_context.yml", "cde_registry.yml", "scope_registry.yml"},
        }
        for layer, filenames in expected.items():
            actual = {path.name for path in (ROOT / "contracts" / layer).iterdir() if path.is_file()}
            self.assertEqual(actual, filenames, layer)

    def test_runtime_configuration_has_no_example_files(self):
        self.assertFalse((ROOT / "source-simulator/.env.example").exists())
        self.assertFalse((ROOT / "infrastructure/aws/terraform.tfvars.example").exists())
        gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn(".env", gitignore)
        self.assertIn("infrastructure/aws/terraform.tfvars", gitignore)

    def test_source_inventory_matches_generated_data(self):
        inventory = yaml.safe_load(
            (ROOT / "contracts/source/source_inventory.yml").read_text(encoding="utf-8")
        )
        generated = json.loads(
            (ROOT / "dev-tools/datagen/output/master-schema.json").read_text(encoding="utf-8")
        )
        tables = [item["table"] for item in inventory["datasets"]]
        self.assertEqual(len(tables), 32)
        self.assertEqual(set(tables), set(generated["tables"]))
        self.assertEqual(generated["total_rows"], 67_180)
        counts = {}
        for item in inventory["datasets"]:
            counts[item["source_type"]] = counts.get(item["source_type"], 0) + 1
        self.assertEqual(counts, {"DATABASE": 23, "EVENT": 5, "FILE": 4})
        mechanisms = {item["ingestion_mechanism"] for item in inventory["datasets"]}
        self.assertEqual(mechanisms, {"DEBEZIUM_CDC", "APACHE_KAFKA", "S3_MICROBATCH"})

    def test_schema_document_matches_generated_physical_schema(self):
        document = (ROOT / "SCHEMA.md").read_text(encoding="utf-8")
        generated = json.loads(
            (ROOT / "dev-tools/datagen/output/master-schema.json").read_text(encoding="utf-8")
        )

        for table, specification in generated["tables"].items():
            heading = re.search(rf"^### {re.escape(table)}\s*$", document, re.MULTILINE)
            self.assertIsNotNone(heading, table)
            next_heading = re.search(r"^#{2,3}\s+", document[heading.end():], re.MULTILINE)
            end = heading.end() + (next_heading.start() if next_heading else len(document))
            section = document[heading.end():end]

            documented = {}
            for line in section.splitlines():
                if not line.startswith("|"):
                    continue
                cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
                if len(cells) < 3 or cells[0] in {"Column", "--------"}:
                    continue
                documented[cells[0].strip("`")] = (cells[1].lower(), cells[2].lower())

            physical = {
                column["name"]: (
                    column["type"].lower(),
                    "yes" if column["nullable"] else "no",
                )
                for column in specification["columns"]
            }
            for column, documented_definition in documented.items():
                self.assertIn(column, physical, f"{table}.{column}")
                self.assertEqual(documented_definition, physical[column], f"{table}.{column}")

            if table == "accepted_loans":
                self.assertEqual(len(physical), 152)
                self.assertIn("*152 total columns.", section)
            else:
                self.assertEqual(set(documented), set(physical), table)

    def test_schema_document_covers_every_ingestion_route(self):
        document = (ROOT / "SCHEMA.md").read_text(encoding="utf-8")
        routing = document.split("## Ingestion Source Allocation", 1)[1].split(
            "## Entity Relationship Diagram", 1
        )[0]
        inventory = yaml.safe_load(
            (ROOT / "contracts/source/source_inventory.yml").read_text(encoding="utf-8")
        )
        for dataset in inventory["datasets"]:
            self.assertIn(f"`{dataset['table']}`", routing)

    def test_source_routing_is_domain_correct(self):
        loader = (ROOT / "source-simulator/bootstrap-loader/loader.py").read_text(encoding="utf-8")
        self.assertIn('"customers": "customer_master"', loader)
        self.assertIn('"loan_applications": "lending_origination"', loader)
        self.assertIn('"banking_transactions": "banking_core"', loader)
        self.assertIn('"service_case_events"', loader)
        self.assertIn('BATCH_TABLES = {"accepted_loans", "rejected_applications"', loader)

    def test_only_three_layer_schemas_are_created(self):
        setup = (ROOT / "pipelines/bootstrap/v001_create_external_objects.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE CATALOG IF NOT EXISTS `0-ai-trust`", setup)
        for layer in ("bronze", "silver", "gold"):
            self.assertIn(f"CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.{layer}", setup)
        self.assertNotIn("nab_bronze", setup)
        self.assertNotIn("nab_silver", setup)
        self.assertNotIn("nab_gold", setup)
        self.assertEqual(setup.count("CREATE SCHEMA IF NOT EXISTS"), 3)

    def test_bronze_storage_is_owned_by_streaming_tables(self):
        setup = (ROOT / "pipelines/bootstrap/v001_create_external_objects.sql").read_text(encoding="utf-8")
        cleanup = (ROOT / "pipelines/bootstrap/v002_remove_legacy_bronze_objects.sql").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("CREATE TABLE", setup)
        self.assertIn("CREATE EXTERNAL VOLUME", setup)
        for legacy in ("cdc_changes", "cdc_scd2", "kafka_events", "file_arrivals"):
            self.assertIn(legacy, cleanup)
        self.assertNotIn("CREATE OR REPLACE VIEW", cleanup)

    def test_declarative_pipeline_uses_source_aligned_streaming_tables(self):
        definitions = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((ROOT / "pipelines/bronze").glob("*.py"))
        )
        support = (ROOT / "pipelines/framework/landing_stream_reader.py").read_text(encoding="utf-8")
        registry = __import__(
            "pipelines.framework.source_dataset_registry",
            fromlist=["CDC_DATASETS", "EVENT_DATASETS", "FILE_DATASETS"],
        )
        self.assertEqual(len(registry.CDC_DATASETS), 23)
        self.assertEqual(len(registry.EVENT_DATASETS), 5)
        self.assertEqual(len(registry.FILE_DATASETS), 4)
        self.assertIn('name=f"cdc_{dataset}"', definitions)
        self.assertIn('name=f"event_{dataset}"', definitions)
        self.assertIn('name=f"file_{dataset}"', definitions)
        self.assertIn('name="ingestion_quarantine"', definitions)
        self.assertIn('name="control_ingestion_manifests"', definitions)
        self.assertNotIn("path=", definitions)
        self.assertNotIn("dp.create_sink(", definitions)
        self.assertNotIn("@dp.append_flow", definitions)
        self.assertNotIn("import dlt", definitions)
        self.assertNotIn("dlt.", definitions)
        self.assertIn("from pyspark import pipelines as dp", definitions)
        self.assertIn('option("cloudFiles.useManagedFileEvents", "true")', support + definitions)
        self.assertIn('option("cloudFiles.format", "json")', support)
        self.assertIn('option("cloudFiles.format", "csv")', support)
        self.assertIn('option("cloudFiles.schemaEvolutionMode", "addNewColumns")', support)
        self.assertIn('option("rescuedDataColumn", "_rescued_data")', support)
        self.assertNotIn('option("cloudFiles.format", "binaryFile")', definitions + support)
        self.assertNotIn("raw_payload", definitions)
        self.assertNotIn("raw_content", definitions)

    def test_landing_exposes_nested_records_for_native_schema_evolution(self):
        exporter = (ROOT / "source-simulator/landing-exporter/exporter.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('record["record"] = envelope.get("after") or envelope.get("before")', exporter)
        self.assertIn('record["record"] = envelope["payload"]', exporter)
        self.assertIn('"payload": parsed', exporter)
        self.assertNotIn('"value": message.value()', exporter)

    def test_silver_defines_the_approved_nineteen_entity_model(self):
        expected = {
            "application_curated.py", "customer_curated.py", "organisation_curated.py",
            "record_quarantine.py", "service_curated.py",
        }
        actual = {path.name for path in (ROOT / "pipelines/silver").glob("*.py")}
        self.assertEqual(actual, expected)
        definitions = "\n".join(
            (ROOT / "pipelines/silver" / filename).read_text(encoding="utf-8")
            for filename in expected
        )
        self.assertEqual(definitions.count('publish_silver_model("'), 19)
        self.assertNotIn("spark.readStream", definitions)

    def test_gold_has_external_star_tables_and_ai_ready_views(self):
        star = sorted((ROOT / "pipelines/gold-sql/star-schema").glob("*.sql"))
        ai_ready = sorted((ROOT / "pipelines/gold-sql/ai-ready").glob("*.sql"))
        self.assertEqual(len(star), 10)
        self.assertEqual(len(ai_ready), 10)

        for path in star:
            sql = path.read_text(encoding="utf-8").upper()
            self.assertIn("MERGE INTO `0-AI-TRUST`.GOLD.", sql)
            self.assertNotIn("CREATE MATERIALIZED VIEW", sql)
            self.assertNotIn("CREATE STREAMING TABLE", sql)

        for path in ai_ready:
            sql = path.read_text(encoding="utf-8").upper()
            self.assertIn("CREATE OR REPLACE VIEW `0-AI-TRUST`.GOLD.AIV_", sql)
            self.assertNotIn("CREATE MATERIALIZED VIEW", sql)

    def test_all_gold_physical_tables_are_external_delta(self):
        setup = (ROOT / "pipelines/bootstrap/v003_create_gold_external_tables.sql").read_text(
            encoding="utf-8"
        )
        self.assertEqual(setup.count("CREATE TABLE IF NOT EXISTS `0-ai-trust`.gold."), 10)
        self.assertEqual(setup.count("USING DELTA"), 10)
        self.assertEqual(
            setup.count("LOCATION 's3://g3-assignment/g3/0-ai-trust/gold/tables/"),
            10,
        )

    def test_ai_ready_views_carry_zero_trust_context(self):
        definitions = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((ROOT / "pipelines/gold-sql/ai-ready").glob("*.sql"))
        )
        for field in (
            "dq_status", "pipeline_run_id", "last_refreshed_at",
            "context_version", "usage_restriction",
        ):
            self.assertIn(field, definitions)
        self.assertIn("is_account_group_member('banker-assist-users')", definitions)
        self.assertNotRegex(definitions.lower(), r"\b(email|phone_number|tfn|card_number)\b")

    def test_periodic_database_kafka_and_file_activity_exists(self):
        activity = (ROOT / "source-simulator/activity-generator/activity.py").read_text(encoding="utf-8")
        self.assertIn("UPDATE lending_origination.loan_applications", activity)
        self.assertIn("INSERT INTO lending_origination.loan_applications", activity)
        self.assertIn('"nab.application.events"', activity)
        self.assertIn("copy_object", activity)
        self.assertIn('"ApplicationSubmitted"', activity)
        self.assertIn('"ApplicationDetailsUpdated"', activity)
        self.assertIn('"LoanApplicationProcessEvent"', activity)
        self.assertNotIn('"periodic_update"', activity)

    def test_compose_uses_apache_kafka_and_credit_guard(self):
        compose = (ROOT / "source-simulator/compose.yaml").read_text(encoding="utf-8")
        self.assertIn("apache/kafka:", compose)
        self.assertIn("quay.io/debezium/connect:", compose)
        self.assertNotIn("redpandadata", compose.lower())
        self.assertIn("AWS_CREDIT_ALERT_THRESHOLD_USD:-80", compose)
        self.assertIn("/dev/tcp/127.0.0.1/9092", compose)
        self.assertNotIn("kafka-topics.sh", compose)
        self.assertIn("mem_limit:", compose)
        self.assertIn("MICROBATCH_INTERVAL_SECONDS:-60", compose)
        self.assertIn("SOURCE_ACTIVITY_INTERVAL_SECONDS:-60", compose)
        self.assertIn("SOURCE_FILE_INTERVAL_SECONDS:-3600", compose)

    def test_downstream_refresh_policy_is_fifteen_minutes(self):
        policy = (ROOT / "pipelines/framework/refresh_policy.py").read_text(encoding="utf-8")
        self.assertIn('DOWNSTREAM_TRIGGER_INTERVAL = "15 minutes"', policy)
        self.assertIn('"pipelines.trigger.interval"', policy)

    def test_landing_manifest_contains_replay_and_freshness_evidence(self):
        exporter = (ROOT / "source-simulator/landing-exporter/exporter.py").read_text(encoding="utf-8")
        for field in (
            "manifest_version", "heartbeat_status", "record_count", "sha256",
            "min_source_lsn", "max_source_lsn", "min_kafka_offset", "max_kafka_offset",
        ):
            self.assertIn(field, exporter)
        self.assertIn('write_grouped_jsonl("event"', exporter)
        self.assertNotIn('write_jsonl("kafka"', exporter)

    def test_repo_has_only_authorized_markdown_documents(self):
        ignored_directories = {".terraform", ".pytest_cache", ".databricks", ".git"}
        markdown = [
            path
            for path in ROOT.rglob("*.md")
            if ignored_directories.isdisjoint(path.parts)
        ]
        self.assertEqual(set(markdown), {ROOT / "README.md", ROOT / "SCHEMA.md"})

    def test_repo_has_no_powershell_automation(self):
        self.assertEqual([], list(ROOT.rglob("*.ps1")))


if __name__ == "__main__":
    unittest.main()
