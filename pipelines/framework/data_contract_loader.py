"""Load versioned ODCS contracts from the workspace Git Folder."""

import hashlib
import json
import os
from pathlib import Path

import yaml


CONTRACT_ROOT = Path("/Workspace/Shared/0-ai-trust/contracts")
PRODUCTION_STATUSES = {"approved", "active"}


class DataContractError(ValueError):
    """Raised when a contract cannot safely drive runtime behaviour."""


def _allowed_statuses() -> set[str]:
    environment = os.getenv("G3_CONTRACT_ENVIRONMENT", "dev").strip().lower()
    return PRODUCTION_STATUSES if environment == "prod" else PRODUCTION_STATUSES | {"draft"}


def load_data_contract(relative_path: str, require_active: bool = True) -> dict:
    path = (CONTRACT_ROOT / relative_path).resolve()
    if CONTRACT_ROOT.resolve() not in path.parents:
        raise DataContractError("Contract path must remain under the contract root")
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise DataContractError(f"Contract must be a mapping: {relative_path}")
    if require_active and contract.get("status") not in _allowed_statuses():
        raise DataContractError(f"Contract is not approved for runtime use: {relative_path}")
    return contract


def load_layer_contract(layer: str, contract_name: str, require_active: bool = True) -> dict:
    """Find one contract by its ODCS ``name`` inside a lifecycle layer."""
    layer_root = (CONTRACT_ROOT / layer).resolve()
    if CONTRACT_ROOT.resolve() not in layer_root.parents:
        raise DataContractError(f"Invalid contract layer: {layer}")

    matches = []
    for path in layer_root.rglob("*.yml"):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(contract, dict) and contract.get("name") == contract_name:
            matches.append((path, contract))

    if len(matches) != 1:
        raise DataContractError(
            f"Expected one {layer} contract named {contract_name}, found {len(matches)}"
        )
    path, contract = matches[0]
    if require_active and contract.get("status") not in _allowed_statuses():
        raise DataContractError(f"Contract is not approved for runtime use: {path}")
    return contract


def contract_fingerprint(contract: dict) -> str:
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
