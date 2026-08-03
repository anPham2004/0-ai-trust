"""Unit tests for the contract loader: fingerprinting, path safety, status gating.

``contract_fingerprint`` previously had only a tautological test (hash the
same JSON twice, compare it to itself) — this replaces that with a real
content-drift check. The path-traversal guards and draft/approved status
gating in ``load_data_contract`` / ``load_layer_contract`` had no direct test
at all before this file.
"""

from pathlib import Path

import pytest

import framework.data_contract_loader as data_contract_loader
from framework.data_contract_loader import DataContractError, contract_fingerprint

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def repo_contract_root(monkeypatch):
    """Point the loader at this repo's real contracts/ directory.

    In production ``CONTRACT_ROOT`` is the deployed Databricks Git Folder
    path (``/Workspace/Shared/0-ai-trust/contracts``), which mirrors this
    repo's ``contracts/`` directory 1:1.
    """
    monkeypatch.setattr(data_contract_loader, "CONTRACT_ROOT", ROOT / "contracts")


class TestContractFingerprint:
    def test_same_content_hashes_the_same_even_with_different_dict_identity(self):
        contract = {"id": "x", "version": "1.0.0"}
        assert contract_fingerprint(contract) == contract_fingerprint(dict(contract))

    def test_different_content_hashes_differently(self):
        assert contract_fingerprint({"id": "x"}) != contract_fingerprint({"id": "y"})

    def test_key_order_does_not_affect_the_hash(self):
        assert contract_fingerprint({"a": 1, "b": 2}) == contract_fingerprint({"b": 2, "a": 1})


class TestLoadDataContract:
    def test_loads_a_real_contract_by_relative_path(self):
        contract = data_contract_loader.load_data_contract("silver/involved-party/ip-individual.yml")
        assert contract["name"] == "ip_individual"

    def test_rejects_path_traversal_outside_the_contract_root(self):
        with pytest.raises(DataContractError):
            data_contract_loader.load_data_contract("../README.md")

    def test_rejects_a_contract_not_approved_for_runtime_use_in_prod(self, tmp_path, monkeypatch):
        monkeypatch.setattr(data_contract_loader, "CONTRACT_ROOT", tmp_path)
        monkeypatch.setenv("G3_CONTRACT_ENVIRONMENT", "prod")
        (tmp_path / "draft.yml").write_text("status: draft\nname: x\n", encoding="utf-8")
        with pytest.raises(DataContractError):
            data_contract_loader.load_data_contract("draft.yml")

    def test_allows_a_draft_contract_outside_prod(self, tmp_path, monkeypatch):
        monkeypatch.setattr(data_contract_loader, "CONTRACT_ROOT", tmp_path)
        monkeypatch.setenv("G3_CONTRACT_ENVIRONMENT", "dev")
        (tmp_path / "draft.yml").write_text("status: draft\nname: x\n", encoding="utf-8")
        assert data_contract_loader.load_data_contract("draft.yml")["status"] == "draft"


class TestLoadLayerContract:
    def test_finds_exactly_one_contract_by_odcs_name(self):
        contract = data_contract_loader.load_layer_contract("silver", "ip_individual")
        assert contract["name"] == "ip_individual"

    def test_unknown_contract_name_raises(self):
        with pytest.raises(DataContractError):
            data_contract_loader.load_layer_contract("silver", "does_not_exist")

    def test_rejects_a_layer_path_outside_the_contract_root(self):
        with pytest.raises(DataContractError):
            data_contract_loader.load_layer_contract("../etc", "x")
