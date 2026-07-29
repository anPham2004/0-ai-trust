"""Load versioned data contracts from the workspace Git Folder."""

import hashlib
import json
from pathlib import Path

import yaml


CONTRACT_ROOT = Path("/Workspace/Shared/0-ai-trust/contracts")
ACTIVE_STATUSES = {"approved", "active"}  # DCS v3.1.0 uses lowercase status


class DataContractError(ValueError):
    """Raised when a contract cannot safely drive runtime behaviour."""


def load_data_contract(relative_path: str, require_active: bool = True) -> dict:
    path = (CONTRACT_ROOT / relative_path).resolve()
    if CONTRACT_ROOT.resolve() not in path.parents:
        raise DataContractError("Contract path must remain under the contract root")
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise DataContractError(f"Contract must be a mapping: {relative_path}")
    if require_active and contract.get("status") not in ACTIVE_STATUSES:
        raise DataContractError(f"Contract is not approved for runtime use: {relative_path}")
    return contract


def contract_fingerprint(contract: dict) -> str:
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
