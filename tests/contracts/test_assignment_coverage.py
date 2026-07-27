import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


class AssignmentCoverageTests(unittest.TestCase):
    def test_silver_contracts_define_at_least_eight_quality_rules(self):
        rules = set()
        dimensions = set()
        for path in (ROOT / "contracts/silver").glob("*.yml"):
            contract = yaml.safe_load(path.read_text(encoding="utf-8"))
            for severity in ("hard", "warn"):
                rules.update(contract.get("rules", {}).get(severity, []))
            dimensions.update(contract.get("quality_dimensions", {}))

        self.assertGreaterEqual(len(rules), 8)
        self.assertTrue({"completeness", "consistency", "timeliness", "validity"} <= dimensions)

    def test_each_modelled_domain_has_owner_lineage_and_cdes(self):
        for path in (ROOT / "contracts/silver").glob("*.yml"):
            contract = yaml.safe_load(path.read_text(encoding="utf-8"))
            self.assertTrue(contract.get("owner"), path.name)
            self.assertTrue(contract.get("producer"), path.name)
            self.assertTrue(contract.get("critical_data_elements"), path.name)
            self.assertTrue(contract.get("lineage_l1"), path.name)

    def test_gold_does_not_expose_forbidden_raw_identifiers(self):
        sql = "\n".join(
            path.read_text(encoding="utf-8").lower()
            for path in (ROOT / "pipelines/gold-sql/ai-ready").glob("*.sql")
        )
        for forbidden in ("email", "phone_number", "tfn", "card_number"):
            self.assertNotIn(forbidden, sql)

    def test_ai_ready_contract_matches_every_view(self):
        contract = yaml.safe_load(
            (ROOT / "contracts/gold/ai_ready_context.yml").read_text(encoding="utf-8")
        )
        contracted = {
            view
            for views in contract["outputs"].values()
            for view in views
        }
        implemented = {
            path.stem
            for path in (ROOT / "pipelines/gold-sql/ai-ready").glob("*.sql")
        }
        self.assertEqual(contracted, implemented)
        self.assertEqual(len(contracted), 10)


if __name__ == "__main__":
    unittest.main()
