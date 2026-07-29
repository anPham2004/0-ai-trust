#!/usr/bin/env python3
"""Migrate custom data contracts to ODCS v3.1.0 format.

Transforms contracts in contracts/source/ and contracts/silver/ from the
project's custom schema to the Open Data Contract Standard v3.1.0, preserving
all business logic, quality rules, sensitivity metadata, and ingestion config.

Usage:
    python3 scripts/migrate-contracts-to-odcs.py
    python3 scripts/migrate-contracts-to-odcs.py --dry-run
"""

import argparse
import copy
import sys
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# YAML helpers -- preserve key order, avoid anchors/aliases
# ---------------------------------------------------------------------------

class _OrderedDumper(yaml.SafeDumper):
    """Dump dicts in insertion order, disable aliases."""

    def ignore_aliases(self, data):
        return True


def _dict_representer(dumper, data):
    return dumper.represent_mapping("tag:yaml.org,2002:map", data.items())


_OrderedDumper.add_representer(dict, _dict_representer)


def _str_representer(dumper, data):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_OrderedDumper.add_representer(str, _str_representer)


def dump_yaml(data):
    return yaml.dump(data, Dumper=_OrderedDumper, default_flow_style=False,
                     sort_keys=False, allow_unicode=True, width=120)


def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Quality rule mapping
# ---------------------------------------------------------------------------

# Simple rule types that map to ODCS library metrics
LIBRARY_RULE_MAP = {
    "NOT_NULL": ("nullValues", "completeness"),
    "UNIQUE": ("duplicateValues", "uniqueness"),
    "FORMAT": ("invalidValues", "conformity"),
    "FORMAT_IF_PRESENT": ("invalidValues", "conformity"),
    "REGEX": ("invalidValues", "conformity"),
    "REGEX_IF_PRESENT": ("invalidValues", "conformity"),
    "ACCEPTED_VALUES": ("invalidValues", "conformity"),
    "ACCEPTED_VALUES_IF_PRESENT": ("invalidValues", "conformity"),
}

# Dimension mapping for custom rules
DIMENSION_MAP = {
    "NON_BLANK_IF_PRESENT": "completeness",
    "NOT_NULL_IF_PRESENT": "completeness",
    "ANY_PRESENT": "completeness",
    "ALL_NOT_NULL": "completeness",
    "RANGE": "conformity",
    "RANGE_IF_PRESENT": "conformity",
    "PARSEABLE_TIMESTAMP_IF_PRESENT": "conformity",
    "PARSEABLE_DECIMAL_IF_PRESENT": "conformity",
    "PARSEABLE_INTEGER_IF_PRESENT": "conformity",
    "PARSEABLE_DATE_IF_PRESENT": "conformity",
    "MAX_LENGTH_IF_PRESENT": "conformity",
    "VALID_JSON_ARRAY_IF_PRESENT": "conformity",
    "BOOLEAN": "conformity",
    "COLUMN_COMPARISON": "consistency",
    "COLUMN_EQUALITY": "consistency",
    "REFERENCE_PAIR_CONSISTENT": "consistency",
    "FOREIGN_KEY_IF_PRESENT": "consistency",
    "REFERENCE_SET_IF_PRESENT": "consistency",
    "REFERENCE_TIMESTAMP_COMPARISON": "consistency",
    "PREFIX_TO_VALUE_MAPPING": "consistency",
    "PREFIX_MATCH": "consistency",
    "CONDITIONAL_ALL_NULL_UNLESS_PREFIX": "consistency",
    "CONDITIONAL_NOT_NULL_BY_PREFIX": "consistency",
    "CONDITIONAL_ACCEPTED_VALUES": "consistency",
    "CONDITIONAL_VALUE_MAP": "consistency",
    "NUMERIC_COLUMN_COMPARISON": "consistency",
    "NUMERIC_RATIO_RANGE": "consistency",
    "PARENT_DECLARED_CHILD_COUNT_MATCH": "consistency",
    "MONOTONIC_EVENT_TIME_PER_GROUP": "timeliness",
    "TIMESTAMP_HOUR_POPULATION_RATIO": "timeliness",
    "TIMESTAMP_NOT_IN_FUTURE": "timeliness",
    "DATE_NOT_IN_FUTURE": "timeliness",
    "REGEX_NUMERIC_COMPONENT_RANGE_IF_PRESENT": "conformity",
    "MASKED_SUFFIX_MATCH": "consistency",
    "CONDITIONAL_NUMERIC_CONSISTENCY": "consistency",
    "REFERENCE_CATALOGUE_PENDING": "conformity",
    "DOCUMENTED_CROSS_LAYER_ALIGNMENT": "consistency",
    # Dataset-level file rules
    "EXACT_COLUMN_COUNT": "completeness",
    "AUTHORITATIVE_SCHEMA_MATCH": "conformity",
    "RECORD_COUNT_EXPECTATION": "completeness",
    "PROHIBITED_RAW_FIELDS_IN_AI_OUTPUT": "conformity",
    # Event envelope rules
    "UNIQUE_EVENT_ENVELOPE_POSITION": "uniqueness",
    "ENVELOPE_FIELD_NOT_NULL": "completeness",
    # File rules
    "FILE_SIZE_MIN": "completeness",
    "DUPLICATE_FILE_HASH": "uniqueness",
    "FILE_MODIFICATION_AGE": "timeliness",
    "FILE_PATH_MATCH": "consistency",
    "SHA256_MATCH": "consistency",
    "RECORD_COUNT_MATCH": "consistency",
    "MANIFEST_VALUE": "consistency",
}


def _map_severity(sev):
    return sev.lower() if sev else "error"


def _map_field_quality_rule(rule):
    """Convert a field-level quality rule to ODCS quality format."""
    rule_type = rule.get("type", "")
    rule_id = rule.get("rule_id", "")
    severity = _map_severity(rule.get("severity"))
    action = rule.get("action", "")
    desc = rule.get("description", "")
    params = rule.get("parameters", {})

    odcs_rule = {"id": rule_id}

    if rule_type in LIBRARY_RULE_MAP:
        metric, dimension = LIBRARY_RULE_MAP[rule_type]
        odcs_rule["type"] = "library"
        odcs_rule["metric"] = metric
        # Add pattern/values arguments for invalidValues
        if metric == "invalidValues":
            args = {}
            if "regex" in params:
                args["pattern"] = params["regex"]
            elif "format" in params:
                args["format"] = params["format"]
            elif "values" in params:
                args["values"] = params["values"]
            if args:
                odcs_rule["arguments"] = args
        odcs_rule["mustBe"] = 0
        odcs_rule["severity"] = severity
        odcs_rule["dimension"] = dimension
    else:
        dimension = DIMENSION_MAP.get(rule_type, "conformity")
        odcs_rule["type"] = "custom"
        odcs_rule["engine"] = "g3-pipeline"
        impl = {"ruleType": rule_type}
        if params:
            impl["parameters"] = params
        odcs_rule["implementation"] = impl
        odcs_rule["severity"] = severity
        odcs_rule["dimension"] = dimension

    if desc:
        odcs_rule["description"] = desc

    # Preserve action in customProperties
    if action:
        odcs_rule["customProperties"] = [
            {"property": "g3:action", "value": action}
        ]

    return odcs_rule


def _map_dataset_quality_rule(rule):
    """Convert a dataset-level quality rule to ODCS quality format."""
    rule_type = rule.get("type", "")
    rule_id = rule.get("rule_id", "")
    severity = _map_severity(rule.get("severity"))
    action = rule.get("action", "")
    desc = rule.get("description", "")
    params = rule.get("parameters", {})
    columns = rule.get("columns", [])

    odcs_rule = {"id": rule_id}

    # A few dataset rules can map to library
    if rule_type == "UNIQUE" and columns:
        odcs_rule["type"] = "library"
        odcs_rule["metric"] = "duplicateValues"
        odcs_rule["mustBe"] = 0
        odcs_rule["severity"] = severity
        odcs_rule["dimension"] = "uniqueness"
    elif rule_type == "NOT_NULL" and columns:
        odcs_rule["type"] = "library"
        odcs_rule["metric"] = "nullValues"
        odcs_rule["mustBe"] = 0
        odcs_rule["severity"] = severity
        odcs_rule["dimension"] = "completeness"
    else:
        dimension = DIMENSION_MAP.get(rule_type, "consistency")
        odcs_rule["type"] = "custom"
        odcs_rule["engine"] = "g3-pipeline"
        impl = {"ruleType": rule_type}
        if params:
            impl["parameters"] = params
        if columns:
            impl["columns"] = columns
        odcs_rule["implementation"] = impl
        odcs_rule["severity"] = severity
        odcs_rule["dimension"] = dimension

    if desc:
        odcs_rule["description"] = desc

    cps = []
    if action:
        cps.append({"property": "g3:action", "value": action})
    if columns and rule_type in ("UNIQUE", "NOT_NULL"):
        cps.append({"property": "g3:columns", "value": columns})
    if cps:
        odcs_rule["customProperties"] = cps

    return odcs_rule


# ---------------------------------------------------------------------------
# Logical type mapping
# ---------------------------------------------------------------------------

def _map_logical_type(data_type, logical_type):
    """Map current data_type + logical_type to ODCS logicalType."""
    lt = (logical_type or "").lower()
    dt = (data_type or "").lower()

    if lt in ("timestamp",):
        return "timestamp"
    if lt in ("date",):
        return "date"
    if lt in ("boolean_flag", "boolean"):
        return "boolean"
    if dt in ("int32", "int64", "integer"):
        return "integer"
    if dt in ("float", "double", "decimal"):
        return "number"
    if dt in ("bool", "boolean"):
        return "boolean"
    if dt.upper() == "BOOLEAN":
        return "boolean"
    if dt.upper() == "TIMESTAMP":
        return "timestamp"
    if dt.upper() == "DATE":
        return "date"
    return "string"


def _build_logical_type_options(field):
    """Build logicalTypeOptions from accepted constraints."""
    accepted = field.get("accepted", {})
    if not accepted or not isinstance(accepted, dict):
        return None

    opts = {}
    fmt = accepted.get("format")
    if fmt:
        opts["format"] = fmt.lower() if fmt in ("UUID",) else fmt

    regex = accepted.get("regex")
    if regex:
        opts["pattern"] = regex

    min_len = accepted.get("min_length")
    max_len = accepted.get("max_length")
    if min_len is not None:
        opts["minLength"] = min_len
    if max_len is not None:
        opts["maxLength"] = max_len

    mn = accepted.get("min")
    mx = accepted.get("max")
    if mn is not None:
        opts["minimum"] = mn
    if mx is not None:
        opts["maximum"] = mx

    return opts if opts else None


# ---------------------------------------------------------------------------
# Property (field) migration
# ---------------------------------------------------------------------------

def _migrate_property(field, primary_keys):
    """Convert a single field to an ODCS property."""
    name = field["name"]
    data_type = field.get("data_type", "string")
    logical_type = field.get("logical_type", "")

    prop = {"name": name}
    prop["logicalType"] = _map_logical_type(data_type, logical_type)
    prop["physicalType"] = data_type

    # required = !nullable
    nullable = field.get("nullable", True)
    if not nullable:
        prop["required"] = True

    # Primary key
    if name in primary_keys:
        prop["primaryKey"] = True
        prop["primaryKeyPosition"] = primary_keys.index(name) + 1

    # Classification from sensitivity
    sensitivity = field.get("sensitivity", {})
    classification = sensitivity.get("classification")
    if classification:
        prop["classification"] = classification

    # logicalTypeOptions
    lto = _build_logical_type_options(field)
    if lto:
        prop["logicalTypeOptions"] = lto

    # Examples
    example = field.get("example")
    if example is not None:
        prop["examples"] = [example]

    # Quality rules
    qr = field.get("quality_rules", [])
    if qr:
        prop["quality"] = [_map_field_quality_rule(r) for r in qr]

    # customProperties for sensitivity details and domain type
    cps = []
    sens_cp = {}
    if sensitivity.get("is_pii") is not None:
        sens_cp["is_pii"] = sensitivity["is_pii"]
    if sensitivity.get("pii_type"):
        sens_cp["pii_type"] = sensitivity["pii_type"]
    if sensitivity.get("handling"):
        sens_cp["handling"] = sensitivity["handling"]
    if sens_cp:
        cps.append({"property": "g3:sensitivity", "value": sens_cp})

    if logical_type:
        cps.append({"property": "g3:domainType", "value": logical_type})

    # Preserve accepted values that didn't map to logicalTypeOptions
    accepted = field.get("accepted", {})
    if isinstance(accepted, dict):
        leftover = {k: v for k, v in accepted.items()
                    if k not in ("format", "regex", "min_length", "max_length", "min", "max")}
        if leftover:
            cps.append({"property": "g3:accepted", "value": leftover})

    if cps:
        prop["customProperties"] = cps

    return prop


# ---------------------------------------------------------------------------
# Source contract migration
# ---------------------------------------------------------------------------

def _detect_source_type(contract):
    """Detect DATABASE / EVENT / FILE from source.category."""
    source = contract.get("source", {})
    return source.get("category", "UNKNOWN")


def _server_type_for_source(source_type, contract):
    """Map source type to ODCS server type."""
    if source_type == "DATABASE":
        st = contract.get("source", {}).get("system_type", "").lower()
        if "postgres" in st:
            return "postgresql"
        return "custom"
    if source_type == "EVENT":
        return "kafka"
    if source_type == "FILE":
        return "s3"
    return "custom"


def _build_server(source_type, contract):
    """Build the ODCS servers array."""
    server_type = _server_type_for_source(source_type, contract)
    source = contract.get("source", {})
    name = contract.get("dataset", {}).get("name", "unknown")

    desc_map = {
        "DATABASE": f"PostgreSQL CDC source via Debezium for {name}",
        "EVENT": f"Kafka event stream for {name}",
        "FILE": f"S3 file drop for {name}",
    }

    return [{"server": f"source-{server_type}",
             "type": server_type,
             "description": desc_map.get(source_type, f"Source for {name}"),
             "environment": "prod"}]


def migrate_source_contract(contract, source_type):
    """Transform a source contract to ODCS v3.1.0."""
    dataset = contract.get("dataset", {})
    lifecycle = contract.get("lifecycle", {})
    keys = contract.get("keys", {})
    source = contract.get("source", {})
    ingestion = contract.get("ingestion", {})
    schema_section = contract.get("schema", {})
    fields = schema_section.get("fields", []) if isinstance(schema_section, dict) else []

    primary_keys = keys.get("primary_key", [])

    # -- Top-level ODCS fields --
    odcs = {}
    odcs["kind"] = "DataContract"
    odcs["apiVersion"] = "v3.1.0"
    odcs["id"] = contract.get("contract_id", "")
    odcs["version"] = contract.get("contract_version", "1.0.0")
    odcs["name"] = dataset.get("name", "")
    odcs["status"] = (lifecycle.get("status", "DRAFT") or "DRAFT").lower()
    odcs["domain"] = dataset.get("domain", "")

    # Description
    desc_text = dataset.get("description", "")
    if desc_text:
        odcs["description"] = {"purpose": desc_text}

    # Servers
    odcs["servers"] = _build_server(source_type, contract)

    # -- Schema --
    schema_obj = {
        "name": dataset.get("name", ""),
        "logicalType": "object",
        "physicalType": "table",
    }

    # physicalName from source_object or event_name
    phys_name = source.get("source_object") or source.get("event_name") or dataset.get("name", "")
    schema_obj["physicalName"] = phys_name

    # Relationships (foreign keys)
    fks = keys.get("foreign_keys", [])
    if fks:
        rels = []
        for fk in fks:
            cols = fk.get("columns", [])
            ref = fk.get("reference", {})
            ref_dataset = ref.get("dataset", "")
            ref_cols = ref.get("columns", [])
            if cols and ref_dataset and ref_cols:
                from_refs = [f"{dataset.get('name', '')}.{c}" for c in cols]
                to_refs = [f"{ref_dataset}.{c}" for c in ref_cols]
                rel = {"type": "foreignKey", "from": from_refs, "to": to_refs}
                if fk.get("nullable") is not None:
                    rel["customProperties"] = [
                        {"property": "g3:nullable", "value": fk["nullable"]}
                    ]
                rels.append(rel)
        if rels:
            schema_obj["relationships"] = rels

    # Dataset-level quality rules
    ds_qr = contract.get("dataset_quality_rules", [])
    if ds_qr:
        schema_obj["quality"] = [_map_dataset_quality_rule(r) for r in ds_qr]

    # Properties
    props = [_migrate_property(f, primary_keys) for f in fields]
    if props:
        schema_obj["properties"] = props

    # Schema-level customProperties for schema authority (FILE contracts)
    schema_cps = []
    if isinstance(schema_section, dict):
        for key in ("contract_mode", "expected_column_count", "authoritative_schema_required",
                     "authoritative_schema_ref", "full_field_catalogue_status",
                     "unknown_column_action", "missing_authoritative_column_action",
                     "missing_column_action", "incompatible_type_change_action", "notes"):
            if key in schema_section and key != "fields":
                schema_cps.append({"property": f"g3:schema:{key}", "value": schema_section[key]})
    # documented_financial_field_families (accepted_loans)
    if isinstance(schema_section, dict) and "documented_financial_field_families" in schema_section:
        schema_cps.append({
            "property": "g3:schema:documentedFinancialFieldFamilies",
            "value": schema_section["documented_financial_field_families"]
        })
    if schema_cps:
        schema_obj["customProperties"] = schema_cps

    odcs["schema"] = [schema_obj]

    # -- Contract-level customProperties --
    cps = []

    # g3:source
    source_cp = {}
    for k in ("category", "system_type", "source_object", "authoritative",
              "event_name", "topic_name", "append_only", "authoritative_for",
              "delivery_mode", "catalogue", "contains_global_id",
              "customer_fk_validation_exempt"):
        if k in source:
            source_cp[k] = source[k]
    if source_cp:
        cps.append({"property": "g3:source", "value": source_cp})

    # g3:ingestion
    if ingestion:
        cps.append({"property": "g3:ingestion", "value": ingestion})

    # g3:lifecycle
    lc_cp = {}
    for k in ("owner", "approver", "effective_from", "approval_blockers"):
        if k in lifecycle:
            lc_cp[k] = lifecycle[k]
    if lc_cp:
        cps.append({"property": "g3:lifecycle", "value": lc_cp})

    # g3:dataset
    ds_cp = {}
    for k in ("expected_rows_scale_1", "physical_schema_ref",
              "expected_physical_column_count"):
        if k in dataset:
            ds_cp[k] = dataset[k]
    if ds_cp:
        cps.append({"property": "g3:dataset", "value": ds_cp})

    # g3:alternateKeys
    alt_keys = keys.get("alternate_keys", [])
    if alt_keys:
        cps.append({"property": "g3:alternateKeys", "value": alt_keys})

    # g3:eventDeduplicationKey
    dedup_key = keys.get("event_deduplication_key")
    if dedup_key:
        cps.append({"property": "g3:eventDeduplicationKey", "value": dedup_key})

    # g3:relationshipNotes (FILE)
    rel_notes = keys.get("relationship_notes")
    if rel_notes:
        cps.append({"property": "g3:relationshipNotes", "value": rel_notes})

    # g3:manifest (FILE)
    manifest = contract.get("manifest")
    if manifest:
        cps.append({"property": "g3:manifest", "value": manifest})

    # g3:eventQualityRules (EVENT)
    event_qr = contract.get("event_quality_rules")
    if event_qr:
        cps.append({"property": "g3:eventQualityRules",
                     "value": [_map_dataset_quality_rule(r) for r in event_qr]})

    # g3:derivedQualityChecks (EVENT)
    derived_qc = contract.get("derived_quality_checks")
    if derived_qc:
        cps.append({"property": "g3:derivedQualityChecks", "value": derived_qc})

    # g3:fileQualityRules (FILE)
    file_qr = contract.get("file_quality_rules")
    if file_qr:
        cps.append({"property": "g3:fileQualityRules",
                     "value": [_map_dataset_quality_rule(r) for r in file_qr]})

    # g3:notes
    notes = contract.get("notes")
    if notes:
        cps.append({"property": "g3:notes", "value": notes})

    if cps:
        odcs["customProperties"] = cps

    return odcs


# ---------------------------------------------------------------------------
# Silver contract migration
# ---------------------------------------------------------------------------

def _map_silver_quality_rule(rule):
    """Convert a silver Spark SQL quality rule to ODCS format."""
    return {
        "id": rule.get("rule_id", ""),
        "type": "sql",
        "query": rule.get("expression", ""),
        "mustBe": 0,
        "severity": _map_severity(rule.get("severity")),
        "dimension": "completeness" if "NULL" in rule.get("rule_id", "") else "conformity",
        "description": rule.get("description", ""),
        "customProperties": [
            {"property": "g3:action", "value": rule.get("action", "QUARANTINE")}
        ],
    }


def migrate_silver_contract(contract):
    """Transform a silver contract to ODCS v3.1.0."""
    dataset = contract.get("dataset", {})
    lifecycle = contract.get("lifecycle", {})
    keys = contract.get("keys", {})
    schema_section = contract.get("schema", {})
    fields = schema_section.get("fields", []) if isinstance(schema_section, dict) else []

    primary_keys = keys.get("primary_key", [])

    # -- Top-level ODCS fields --
    odcs = {}
    odcs["kind"] = "DataContract"
    odcs["apiVersion"] = "v3.1.0"
    odcs["id"] = contract.get("contract_id", "")
    odcs["version"] = contract.get("contract_version", "1.0.0")
    odcs["name"] = dataset.get("name", "")
    odcs["status"] = (lifecycle.get("status", "DRAFT") or "DRAFT").lower()
    odcs["domain"] = dataset.get("domain", "")

    # Description
    desc_text = dataset.get("description", "")
    if desc_text:
        odcs["description"] = {"purpose": desc_text}

    # Servers
    odcs["servers"] = [{
        "server": "silver-delta-lake",
        "type": "databricks",
        "description": f"Silver Delta Lake table for {dataset.get('name', '')}",
        "environment": "prod",
    }]

    # -- Schema --
    schema_obj = {
        "name": dataset.get("name", ""),
        "logicalType": "object",
        "physicalType": "table",
    }

    # Quality rules (Spark SQL)
    quality_rules = contract.get("quality_rules", {})
    hard_rules = quality_rules.get("hard", []) if isinstance(quality_rules, dict) else []
    warn_rules = quality_rules.get("warn", []) if isinstance(quality_rules, dict) else []
    all_rules = hard_rules + warn_rules
    if all_rules:
        schema_obj["quality"] = [_map_silver_quality_rule(r) for r in all_rules]

    # Properties
    props = [_migrate_property(f, primary_keys) for f in fields]
    if props:
        schema_obj["properties"] = props

    odcs["schema"] = [schema_obj]

    # -- slaProperties (tolerances) --
    tolerances = contract.get("tolerances", {})
    if tolerances:
        sla = []
        if "hard_failure_rate" in tolerances:
            sla.append({
                "property": "hardFailureRate",
                "value": tolerances["hard_failure_rate"],
                "description": "Max proportion of hard-rule failures before pipeline halts",
            })
        if "freshness_breach_rate" in tolerances:
            sla.append({
                "property": "freshnessBreachRate",
                "value": tolerances["freshness_breach_rate"],
                "description": "Max proportion of freshness breaches before alert",
            })
        if sla:
            odcs["slaProperties"] = sla

    # -- Contract-level customProperties --
    cps = []

    # g3:sources
    sources = contract.get("sources")
    if sources:
        cps.append({"property": "g3:sources", "value": sources})

    # g3:freshnessSla
    freshness = contract.get("freshness_sla")
    if freshness:
        cps.append({"property": "g3:freshnessSla", "value": freshness})

    # g3:lineage
    lineage_cp = {}
    l1 = contract.get("lineage_l1")
    l2 = contract.get("lineage_l2_required")
    if l1:
        lineage_cp["l1"] = l1
    if l2 is not None:
        lineage_cp["l2_required"] = l2
    if lineage_cp:
        cps.append({"property": "g3:lineage", "value": lineage_cp})

    # g3:knownSourceLimitations
    lims = contract.get("known_source_limitations")
    if lims:
        cps.append({"property": "g3:knownSourceLimitations", "value": lims})

    # g3:lifecycle
    lc_cp = {}
    for k in ("owner", "producer", "consumers", "approver", "effective_from"):
        if k in lifecycle:
            lc_cp[k] = lifecycle[k]
    if lc_cp:
        cps.append({"property": "g3:lifecycle", "value": lc_cp})

    # g3:dataset
    ds_cp = {}
    for k in ("canonical_key", "customer_key", "critical_data_elements", "lifecycle_stage"):
        if k in dataset:
            ds_cp[k] = dataset[k]
    if ds_cp:
        cps.append({"property": "g3:dataset", "value": ds_cp})

    # g3:qualityDimensions
    qd = contract.get("quality_dimensions")
    if qd:
        cps.append({"property": "g3:qualityDimensions", "value": qd})

    # g3:aiPolicy
    ai = contract.get("ai_policy")
    if ai:
        cps.append({"property": "g3:aiPolicy", "value": ai})

    # g3:zeroTrust
    zt = contract.get("zero_trust")
    if zt:
        cps.append({"property": "g3:zeroTrust", "value": zt})

    # g3:parameters
    params = contract.get("parameters")
    if params:
        cps.append({"property": "g3:parameters", "value": params})

    # g3:notes
    notes = contract.get("notes")
    if notes:
        cps.append({"property": "g3:notes", "value": notes})

    if cps:
        odcs["customProperties"] = cps

    return odcs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def find_source_contracts(base):
    """Find all source contract YAML files."""
    contracts = []
    for subdir in ("database", "event", "file"):
        d = base / "source" / subdir
        if d.exists():
            contracts.extend(sorted(d.rglob("*.yml")))
    return contracts


def find_silver_contracts(base):
    """Find all silver contract YAML files."""
    d = base / "silver"
    if d.exists():
        return sorted(d.rglob("*.yml"))
    return []


def main():
    parser = argparse.ArgumentParser(description="Migrate contracts to ODCS v3.1.0")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print migration summary without writing files")
    args = parser.parse_args()

    base = Path(__file__).resolve().parent.parent / "contracts"

    # Source contracts
    source_files = find_source_contracts(base)
    silver_files = find_silver_contracts(base)

    print(f"Found {len(source_files)} source contracts, {len(silver_files)} silver contracts")

    errors = []
    migrated = 0

    for f in source_files:
        try:
            contract = load_yaml(f)
            if not contract:
                print(f"  SKIP (empty): {f.relative_to(base)}")
                continue
            source_type = _detect_source_type(contract)
            odcs = migrate_source_contract(contract, source_type)

            if args.dry_run:
                qr_count = sum(len(p.get("quality", [])) for p in odcs["schema"][0].get("properties", []))
                ds_qr = len(odcs["schema"][0].get("quality", []))
                print(f"  OK {f.relative_to(base)} -> {source_type}, {len(odcs['schema'][0].get('properties', []))} props, {qr_count} field QR, {ds_qr} dataset QR")
            else:
                output = dump_yaml(odcs)
                with open(f, "w") as fh:
                    fh.write(output)
                print(f"  MIGRATED: {f.relative_to(base)}")
            migrated += 1
        except Exception as e:
            errors.append((str(f.relative_to(base)), str(e)))
            print(f"  ERROR: {f.relative_to(base)}: {e}")

    for f in silver_files:
        try:
            contract = load_yaml(f)
            if not contract:
                print(f"  SKIP (empty): {f.relative_to(base)}")
                continue
            odcs = migrate_silver_contract(contract)

            if args.dry_run:
                qr_count = len(odcs["schema"][0].get("quality", []))
                print(f"  OK {f.relative_to(base)} -> SILVER, {len(odcs['schema'][0].get('properties', []))} props, {qr_count} QR")
            else:
                output = dump_yaml(odcs)
                with open(f, "w") as fh:
                    fh.write(output)
                print(f"  MIGRATED: {f.relative_to(base)}")
            migrated += 1
        except Exception as e:
            errors.append((str(f.relative_to(base)), str(e)))
            print(f"  ERROR: {f.relative_to(base)}: {e}")

    print(f"\nMigrated: {migrated}, Errors: {len(errors)}")
    if errors:
        for path, err in errors:
            print(f"  {path}: {err}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
