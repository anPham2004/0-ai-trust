import json
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


class ArchitectureTests(unittest.TestCase):
    def test_repository_has_clear_deployable_boundaries(self):
        for directory in ("datagen", "source-simulator", "pipelines", "infrastructure"):
            self.assertTrue((ROOT / directory).is_dir())
        for legacy in ("synthetic-data", "simulator", "databricks", "ref-schema"):
            self.assertFalse((ROOT / legacy).exists())

        bundle_resource = (ROOT / "resources/bronze.pipeline.yml").read_text(encoding="utf-8")
        self.assertIn("../pipelines/bronze/pipeline.py", bundle_resource)

    def test_source_inventory_matches_generated_data(self):
        inventory = yaml.safe_load((ROOT / "contracts/source_inventory.yml").read_text(encoding="utf-8"))
        generated = json.loads(
            (ROOT / "datagen/output/master-schema.json").read_text(encoding="utf-8")
        )
        tables = [item["table"] for item in inventory["datasets"]]
        self.assertEqual(len(tables), 32)
        self.assertEqual(set(tables), set(generated["tables"]))
        self.assertEqual(generated["total_rows"], 67_180)
        counts = {}
        for item in inventory["datasets"]:
            counts[item["native_source"]] = counts.get(item["native_source"], 0) + 1
        self.assertEqual(counts, {"DEBEZIUM_CDC": 23, "KAFKA": 5, "BATCH_CSV": 4})

    def test_source_routing_is_domain_correct(self):
        loader = (ROOT / "source-simulator/bootstrap-loader/loader.py").read_text(encoding="utf-8")
        self.assertIn('"customers": "customer_master"', loader)
        self.assertIn('"loan_applications": "lending_origination"', loader)
        self.assertIn('"banking_transactions": "banking_core"', loader)
        self.assertIn('"service_case_events"', loader)
        self.assertIn('BATCH_TABLES = {"accepted_loans", "rejected_applications"', loader)

    def test_only_three_layer_schemas_are_created(self):
        setup = (ROOT / "pipelines/bootstrap/setup.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE CATALOG IF NOT EXISTS `0-ai-trust`", setup)
        for layer in ("bronze", "silver", "gold"):
            self.assertIn(f"CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.{layer}", setup)
        self.assertNotIn("nab_bronze", setup)
        self.assertNotIn("nab_silver", setup)
        self.assertNotIn("nab_gold", setup)
        self.assertEqual(setup.count("CREATE SCHEMA IF NOT EXISTS"), 3)

    def test_all_bronze_tables_are_external_delta(self):
        setup = (ROOT / "pipelines/bootstrap/setup.sql").read_text(encoding="utf-8")
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

    def test_repo_has_single_markdown_document(self):
        markdown = [path for path in ROOT.rglob("*.md") if ".terraform" not in path.parts]
        self.assertEqual(markdown, [ROOT / "README.md"])

    def test_repo_has_no_powershell_automation(self):
        self.assertEqual([], list(ROOT.rglob("*.ps1")))


if __name__ == "__main__":
    unittest.main()
