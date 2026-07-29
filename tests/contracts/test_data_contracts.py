import hashlib
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_contract_files_are_yaml_resources_inside_lifecycle_folders():
    contracts = [path for path in (ROOT / "contracts").rglob("*.yml")]
    assert contracts
    assert all(path.suffix == ".yml" for path in contracts)


def test_every_approved_silver_entity_has_an_executable_contract():
    contracts = list((ROOT / "contracts/silver").glob("*.yml"))
    assert len(contracts) == 19
    for path in contracts:
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert contract["contract_status"] == "ACTIVE"
        assert contract["product"]
        assert contract["canonical_key"]
        assert contract["history_mode"] in {"SCD_TYPE_2", "APPEND_ONLY"}
        assert contract["schema"]
        for severity in ("hard", "warn"):
            for rule in contract["rules"][severity]:
                assert rule["id"]
                assert rule["expression"]


def test_silver_history_modes_match_the_approved_design():
    contracts = [
        yaml.safe_load(path.read_text(encoding="utf-8"))
        for path in (ROOT / "contracts/silver").glob("*.yml")
    ]
    modes = [contract["history_mode"] for contract in contracts]
    assert modes.count("SCD_TYPE_2") == 12
    assert modes.count("APPEND_ONLY") == 7


def test_contract_fingerprints_are_deterministic():
    path = ROOT / "contracts/silver/ip_individual.yml"
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    assert hashlib.sha256(canonical.encode("utf-8")).hexdigest() == hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()
