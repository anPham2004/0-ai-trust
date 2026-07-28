"""Load versioned data contracts from the workspace Git Folder."""

import hashlib
import json
from pathlib import Path

import yaml


# Single source of truth for all contracts: the workspace Git Folder that mirrors
# this repo's `contracts/` directory, so contract changes go through the same
# code review as pipeline changes rather than being edited ad hoc in production.
CONTRACT_ROOT = Path("/Workspace/Shared/0-ai-trust/contracts")

# Only these statuses mean a human has signed off on the contract. Any other
# status (DRAFT, DEPRECATED, etc.) must not be allowed to drive runtime behaviour.
ACTIVE_STATUSES = {"APPROVED", "ACTIVE"}


class DataContractError(ValueError):
    """Raised when a contract cannot safely drive runtime behaviour."""


def load_data_contract(relative_path: str, require_active: bool = True) -> dict:
    """Read and validate one YAML data contract from the contract root.

    Every check here exists to stop a bad or malicious contract from silently
    changing pipeline behaviour:
      - the resolved path must stay inside CONTRACT_ROOT, so a caller can't pass
        a `relative_path` like "../../secrets.yml" to read files outside contracts/
      - the parsed YAML must be a dict, so a malformed file fails loudly instead
        of causing confusing attribute errors downstream
      - by default the contract must be APPROVED/ACTIVE, so pipelines can't
        accidentally run against a contract that's still a draft or was
        deprecated (callers can opt out via require_active=False, e.g. for
        tooling that needs to inspect draft contracts)
    """
    path = (CONTRACT_ROOT / relative_path).resolve()
    if CONTRACT_ROOT.resolve() not in path.parents:
        raise DataContractError("Contract path must remain under the contract root")
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise DataContractError(f"Contract must be a mapping: {relative_path}")
    if require_active and contract.get("contract_status") not in ACTIVE_STATUSES:
        raise DataContractError(f"Contract is not approved for runtime use: {relative_path}")
    return contract


def contract_fingerprint(contract: dict) -> str:
    """Hash a contract into a short, stable ID for lineage and audit trails.

    Serialising with sort_keys + fixed separators before hashing means the same
    contract content always produces the same fingerprint regardless of key
    order or formatting, so this value can be safely stamped onto output rows
    (e.g. by pipeline_metadata_builder) to prove exactly which version of the
    contract validated them.
    """
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
