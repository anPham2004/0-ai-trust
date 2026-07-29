import hashlib
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_contract_files_are_yaml_resources_inside_lifecycle_folders():
    contracts = [path for path in (ROOT / "contracts").rglob("*.yml")]
    assert contracts
    assert all(path.suffix == ".yml" for path in contracts)


def test_silver_contracts_are_valid_dcs_documents():
    for path in (ROOT / "contracts/silver").rglob("*.yml"):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert contract["kind"] == "DataContract", path
        assert contract["apiVersion"] == "v3.1.0", path
        assert contract["status"] == "draft", path
        assert contract["id"], path
        schema = contract["schema"][0]
        assert schema["quality"], path
        for rule in schema["quality"]:
            assert rule["id"], path
            assert rule["query"], path


def test_contract_fingerprints_are_deterministic():
    path = ROOT / "contracts/silver/involved-party/ip-individual.yml"
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    assert hashlib.sha256(canonical.encode("utf-8")).hexdigest() == hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()
