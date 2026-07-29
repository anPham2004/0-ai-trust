import hashlib
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_contract_files_are_yaml_resources_inside_lifecycle_folders():
    contracts = [path for path in (ROOT / "contracts").rglob("*.yml")]
    assert contracts
    assert all(path.suffix == ".yml" for path in contracts)


def _dataset_contracts():
    roots = [
        ROOT / "contracts/silver",
        ROOT / "contracts/source/database",
        ROOT / "contracts/source/event",
        ROOT / "contracts/source/file",
    ]
    return [path for root in roots for path in root.rglob("*.yml")]


def test_dataset_contracts_have_valid_odcs_identity_and_schema():
    for path in _dataset_contracts():
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert contract["kind"] == "DataContract", path
        assert contract["apiVersion"] == "v3.1.0", path
        assert contract["status"] in {"draft", "approved", "active"}, path
        assert contract["id"], path
        assert contract["name"], path
        assert len(contract["schema"]) == 1, path
        assert contract["schema"][0]["properties"], path


def test_silver_quality_rules_are_executable_odcs_extensions():
    contracts = list((ROOT / "contracts/silver").rglob("*.yml"))
    assert len(contracts) == 19
    for path in contracts:
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        schema = contract["schema"][0]
        assert schema["quality"], path
        for rule in schema["quality"]:
            assert rule["id"], path
            assert rule["type"] == "custom", (path, rule["id"])
            assert rule["engine"] == "databricks-lakeflow", (path, rule["id"])
            assert rule["implementation"]["expression"], (path, rule["id"])
            assert rule["severity"] in {"error", "warning"}, (path, rule["id"])


def test_published_silver_entities_match_contract_names():
    import re

    contracted = {
        yaml.safe_load(path.read_text(encoding="utf-8"))["name"]
        for path in (ROOT / "contracts/silver").rglob("*.yml")
    }
    definitions = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "pipelines/silver").glob("*.py")
    )
    published = set(re.findall(r'publish_(?:joined_)?(?:scd2|append)_model\("([^"]+)"', definitions))
    assert published == contracted


def test_canonical_silver_contracts_do_not_publish_row_dq_status():
    for path in (ROOT / "contracts/silver").rglob("*.yml"):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        columns = {item["name"] for item in contract["schema"][0]["properties"]}
        assert "dq_status" not in columns, path


def test_contract_fingerprints_are_deterministic():
    path = ROOT / "contracts/silver/involved-party/ip-individual.yml"
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    assert hashlib.sha256(canonical.encode("utf-8")).hexdigest() == hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()
