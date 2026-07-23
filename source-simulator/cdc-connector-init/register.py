import json
import os
import time

import requests


CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
CONNECTOR_NAME = "nab-postgres-cdc"


def connector_config() -> dict:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": os.environ["POSTGRES_USER"],
        "database.password": os.environ["POSTGRES_PASSWORD"],
        "database.dbname": os.environ["POSTGRES_DB"],
        "topic.prefix": "nab-cdc",
        "plugin.name": "pgoutput",
        "slot.name": "nab_debezium",
        "publication.name": "nab_debezium_pub",
        "publication.autocreate.mode": "filtered",
        "schema.include.list": (
            "customer_master,lending_origination,case_management,banking_core,energy_core,"
            "insurance_core,lending_servicing,organisation_master"
        ),
        "table.include.list": (
            "customer_master.customers,customer_master.kyc_records,"
            "customer_master.physical_addresses,customer_master.customer_preferences,"
            "lending_origination.loan_applications,lending_origination.missing_documents,"
            "case_management.service_cases,"
            "banking_core.banking_accounts,banking_core.banking_balances,"
            "banking_core.banking_direct_debits,banking_core.banking_payees,"
            "banking_core.banking_scheduled_payments,banking_core.credit_cards,"
            "banking_core.banking_transactions,"
            "energy_core.energy_accounts,energy_core.energy_invoices,"
            "energy_core.energy_service_points,insurance_core.insurance_policies,"
            "lending_servicing.loan_accounts,lending_servicing.mortgage_accounts,"
            "organisation_master.organisations,"
            "organisation_master.organisation_party_relationships,"
            "organisation_master.organisation_relationships"
        ),
        "snapshot.mode": "initial",
        "time.precision.mode": "isostring",
        "tombstones.on.delete": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }


def wait_for_connect() -> None:
    for _ in range(60):
        try:
            if requests.get(f"{CONNECT_URL}/connector-plugins", timeout=3).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("Kafka Connect did not become ready")


def main() -> None:
    wait_for_connect()
    payload = {"name": CONNECTOR_NAME, "config": connector_config()}
    response = requests.post(f"{CONNECT_URL}/connectors", json=payload, timeout=15)
    if response.status_code == 409:
        response = requests.put(
            f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
            json=payload["config"],
            timeout=15,
        )
    response.raise_for_status()
    print(json.dumps({"event": "debezium_connector_ready", "connector": CONNECTOR_NAME}))


if __name__ == "__main__":
    main()
