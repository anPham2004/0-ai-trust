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

        bundle_resource = (ROOT / "resources/pipelines/bronze.pipeline.yml").read_text(encoding="utf-8")
        self.assertIn("../../pipelines/bronze/pipeline.py", bundle_resource)

    def test_repository_conventions_are_synchronised(self):
        bundle = (ROOT / "databricks.yml").read_text(encoding="utf-8")
        self.assertIn("resources/**/*.yml", bundle)

        migrations = list((ROOT / "pipelines/bootstrap").glob("*.sql"))
        self.assertTrue(migrations)
        for migration in migrations:
            self.assertRegex(migration.name, r"^v\d{3}_[a-z0-9_]+\.sql$")

        workflow = (ROOT / ".github/workflows/deploy.yml").read_text(encoding="utf-8")
        self.assertIn("databricks bundle deploy --target dev", workflow)
        self.assertNotIn("databricks repos update", workflow)

        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        for target in ("sim-up:", "sim-down:", "test:", "validate:", "deploy:", "bootstrap:"):
            self.assertIn(target, makefile)

    def test_runtime_configuration_has_no_example_files(self):
        self.assertFalse((ROOT / "source-simulator/.env.example").exists())
        self.assertFalse((ROOT / "infrastructure/aws/terraform.tfvars.example").exists())
        gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn(".env", gitignore)
        self.assertIn("infrastructure/aws/terraform.tfvars", gitignore)

    def test_source_inventory_matches_generated_data(self):
        inventory = yaml.safe_load((ROOT / "contracts/source_inventory.yml").read_text(encoding="utf-8"))
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
            (ROOT / "contracts/source_inventory.yml").read_text(encoding="utf-8")
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
        setup = (ROOT / "pipelines/bootstrap/v001_initial_setup.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE CATALOG IF NOT EXISTS `0-ai-trust`", setup)
        for layer in ("bronze", "silver", "gold"):
            self.assertIn(f"CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.{layer}", setup)
        self.assertNotIn("nab_bronze", setup)
        self.assertNotIn("nab_silver", setup)
        self.assertNotIn("nab_gold", setup)
        self.assertEqual(setup.count("CREATE SCHEMA IF NOT EXISTS"), 3)

    def test_all_bronze_tables_are_external_delta(self):
        setup = (ROOT / "pipelines/bootstrap/v001_initial_setup.sql").read_text(encoding="utf-8")
        for table in ("cdc_changes", "kafka_events", "file_arrivals"):
            marker = f"CREATE TABLE IF NOT EXISTS `0-ai-trust`.bronze.{table}"
            section = setup.split(marker, 1)[1].split(";", 1)[0]
            self.assertIn("USING DELTA", section)
            self.assertIn("LOCATION 's3://g3-assignment/g3/0-ai-trust/bronze/", section)
        self.assertIn("CREATE OR REPLACE VIEW `0-ai-trust`.bronze.cdc_scd2", setup)

    def test_declarative_pipeline_writes_only_external_sinks(self):
        bronze = (ROOT / "pipelines/bronze/pipeline.py").read_text(encoding="utf-8")
        self.assertEqual(bronze.count("dlt.create_sink("), 3)
        self.assertEqual(bronze.count("@dlt.append_flow"), 3)
        self.assertNotIn("@dlt.table", bronze)
        self.assertIn('option("cloudFiles.useManagedFileEvents", "true")', bronze)
        self.assertIn('option("cloudFiles.format", "binaryFile")', bronze)

    def test_silver_and_gold_are_templates_only(self):
        for layer in ("silver", "gold"):
            content = (ROOT / f"pipelines/{layer}/template.py").read_text(encoding="utf-8")
            self.assertNotIn("@dlt.", content)
            self.assertNotIn("CREATE TABLE", content.upper())

    def test_periodic_database_kafka_and_file_activity_exists(self):
        activity = (ROOT / "source-simulator/activity-generator/activity.py").read_text(encoding="utf-8")
        self.assertIn("UPDATE lending_origination.loan_applications", activity)
        self.assertIn('"nab.application.events"', activity)
        self.assertIn("copy_object", activity)

    def test_compose_uses_apache_kafka_and_credit_guard(self):
        compose = (ROOT / "source-simulator/compose.yaml").read_text(encoding="utf-8")
        self.assertIn("apache/kafka:", compose)
        self.assertIn("quay.io/debezium/connect:", compose)
        self.assertNotIn("redpandadata", compose.lower())
        self.assertIn("AWS_CREDIT_ALERT_THRESHOLD_USD:-80", compose)

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
