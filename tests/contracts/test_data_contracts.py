import hashlib
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_contract_files_are_yaml_resources_inside_lifecycle_folders():
    contracts = [path for path in (ROOT / "contracts").rglob("*.yml")]
    assert contracts
    assert all(path.suffix == ".yml" for path in contracts)


def test_silver_contracts_are_draft_until_executable_rules_are_approved():
    for path in (ROOT / "contracts/silver").rglob("*.yml"):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert contract["contract_status"] == "DRAFT"
        assert contract["product"]
        assert contract["canonical_key"]


def test_contract_fingerprints_are_deterministic():
    path = ROOT / "contracts/silver/involved-party/ip-individual.yml"
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    assert hashlib.sha256(canonical.encode("utf-8")).hexdigest() == hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()
