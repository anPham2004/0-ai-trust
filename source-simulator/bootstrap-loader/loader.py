import hashlib
import json
import os
from datetime import date, datetime
from decimal import Decimal

import pyarrow.dataset as ds
import pyarrow.fs as pafs
import psycopg
from psycopg import sql
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


DATABASE_URL = os.environ["DATABASE_URL"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
S3_BUCKET = os.environ["S3_BUCKET"]
BOOTSTRAP_PREFIX = os.getenv("BOOTSTRAP_PREFIX", "g3/bootstrap/nab").strip("/")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
SOURCE_NAMESPACE = "nab"
EXPECTED_RECORD_COUNT = 67_180

TABLES = [
    "accepted_loans", "application_stage_history", "banking_accounts", "banking_balances",
    "banking_direct_debits", "banking_payees", "banking_products", "banking_scheduled_payments",
    "banking_transactions", "credit_cards", "customer_preferences", "customers", "energy_accounts",
    "energy_invoices", "energy_plans", "energy_service_points", "insurance_policies", "kyc_records",
    "loan_accounts", "loan_application_events", "loan_applications", "missing_documents",
    "mortgage_accounts", "organisation_party_relationships", "organisation_relationships",
    "organisations", "physical_addresses", "rejected_applications", "service_case_events",
    "service_cases", "status_change_history", "support_interactions",
]

PRIMARY_KEYS = {
    "accepted_loans": "loanId", "application_stage_history": "historyId",
    "banking_accounts": "accountId", "banking_balances": "accountId",
    "banking_direct_debits": "directDebitId", "banking_payees": "payeeId",
    "banking_products": "productId", "banking_scheduled_payments": "scheduledPaymentId",
    "banking_transactions": "transactionId", "credit_cards": "creditCardId",
    "customer_preferences": "global_id", "customers": "global_id",
    "energy_accounts": "energyAccountId", "energy_invoices": "invoiceNumber",
    "energy_plans": "planId", "energy_service_points": "servicePointId",
    "insurance_policies": "policyId", "kyc_records": "kycId", "loan_accounts": "loanId",
    "loan_application_events": "eventId", "loan_applications": "applicationId",
    "missing_documents": "documentId", "mortgage_accounts": "mortgageId",
    "organisation_party_relationships": "relationshipId",
    "organisation_relationships": "relationshipId",
    "organisations": "organisationId", "physical_addresses": "addressId",
    "rejected_applications": "loanId", "service_case_events": "eventId",
    "service_cases": "caseId",
    "status_change_history": "changeId", "support_interactions": "interactionId",
}

# NAB-style routing: mutable authoritative state is database/CDC, true event history is Kafka,
# and legacy/reference/history feeds remain files.
BATCH_TABLES = {"accepted_loans", "rejected_applications", "banking_products", "energy_plans"}
KAFKA_EVENT_TABLES = {
    "application_stage_history", "loan_application_events", "status_change_history",
    "support_interactions", "service_case_events",
}
CDC_SCHEMAS = {
    "customers": "customer_master", "kyc_records": "customer_master",
    "physical_addresses": "customer_master", "customer_preferences": "customer_master",
    "loan_applications": "lending_origination", "service_cases": "case_management",
    "missing_documents": "lending_origination",
    "banking_accounts": "banking_core", "banking_balances": "banking_core",
    "banking_direct_debits": "banking_core", "banking_payees": "banking_core",
    "banking_scheduled_payments": "banking_core", "credit_cards": "banking_core",
    "banking_transactions": "banking_core",
    "energy_accounts": "energy_core", "energy_invoices": "energy_core",
    "energy_service_points": "energy_core", "insurance_policies": "insurance_core",
    "loan_accounts": "lending_servicing", "mortgage_accounts": "lending_servicing",
    "organisations": "organisation_master",
    "organisation_party_relationships": "organisation_master",
    "organisation_relationships": "organisation_master",
}
assert set(TABLES) == BATCH_TABLES | KAFKA_EVENT_TABLES | set(CDC_SCHEMAS)

TOPICS = {
    "application_stage_history": "nab.application.events",
    "loan_application_events": "nab.application.events",
    "status_change_history": "nab.application.events",
    "support_interactions": "nab.customer-service.events",
    "service_case_events": "nab.customer-service.events",
}
EVENT_TYPES = {
    "application_stage_history": "ApplicationStageChanged",
    "loan_application_events": "LoanApplicationProcessEvent",
    "status_change_history": "LoanApplicationStatusChanged",
    "support_interactions": "SupportInteractionCreated",
    "service_case_events": "ServiceCaseEventRecorded",
}


def json_value(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_value(item) for item in value]
    return value


def read_all() -> dict[str, list[dict]]:
    filesystem = pafs.S3FileSystem(region=AWS_REGION)
    result = {}
    for table in TABLES:
        dataset = ds.dataset(
            f"{S3_BUCKET}/{BOOTSTRAP_PREFIX}/{table}", filesystem=filesystem,
            format="parquet", exclude_invalid_files=True,
        )
        result[table] = [json_value(row) for row in dataset.to_table().to_pylist()]
        print(json.dumps({"event": "bootstrap_dataset_read", "dataset": table,
                          "record_count": len(result[table])}), flush=True)
    return result


def postgres_type(rows: list[dict], column: str) -> str:
    value = next((row.get(column) for row in rows if row.get(column) is not None), None)
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    return "TEXT"


def upsert_source_replica(cursor, schema: str, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    columns = list(rows[0])
    primary_key = PRIMARY_KEYS[table]
    definitions = [
        sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(postgres_type(rows, column)))
        for column in columns
    ]
    definitions.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(primary_key)))
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
    )
    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(table), sql.SQL(", ").join(definitions)
        )
    )
    updates = [column for column in columns if column != primary_key]
    query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}").format(
        sql.Identifier(schema), sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        sql.Identifier(primary_key),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
            for column in updates
        ),
    )
    cursor.executemany(query, [[row.get(column) for column in columns] for row in rows])


def source_fingerprint(data: dict[str, list[dict]]) -> str:
    digest = hashlib.sha256()
    for table in sorted(data):
        digest.update(table.encode())
        for row in sorted(data[table], key=lambda item: str(item.get(PRIMARY_KEYS[table]) or "")):
            digest.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
    return digest.hexdigest()


def prepare_control_tables() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS integration")
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS integration.bootstrap_state (
                     source_fingerprint TEXT PRIMARY KEY,
                     completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"""
            )
        conn.commit()


def already_bootstrapped(fingerprint: str) -> bool:
    prepare_control_tables()
    with psycopg.connect(DATABASE_URL) as conn:
        return conn.execute(
            "SELECT EXISTS (SELECT 1 FROM integration.bootstrap_state WHERE source_fingerprint = %s)",
            (fingerprint,),
        ).fetchone()[0]


def seed_postgres(data: dict[str, list[dict]]) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cursor:
            for table, schema in CDC_SCHEMAS.items():
                upsert_source_replica(cursor, schema, table, data[table])
            cursor.executemany(
                """INSERT INTO integration.bootstrap_audit(dataset, source_record_count)
                   VALUES (%s,%s) ON CONFLICT (dataset) DO UPDATE SET
                   source_record_count=EXCLUDED.source_record_count, loaded_at=clock_timestamp()""",
                [(name, len(rows)) for name, rows in data.items()],
            )
        conn.commit()


def occurred_at(row: dict):
    for field in ("timestamp", "eventTimestamp", "changedAt", "enteredAt", "requestedAt", "createdAt"):
        if row.get(field) is not None:
            return row[field]
    return None


def seed_kafka(data: dict[str, list[dict]]) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    futures = admin.create_topics([NewTopic(topic, 1, 1) for topic in sorted(set(TOPICS.values()))])
    for future in futures.values():
        try:
            future.result()
        except Exception:
            pass
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True})
    count = 0
    for table in sorted(KAFKA_EVENT_TABLES):
        for row in data[table]:
            key = str(row.get(PRIMARY_KEYS[table]) or "")
            envelope = {
                "event_id": hashlib.sha256(f"{SOURCE_NAMESPACE}|{table}|{key}".encode()).hexdigest(),
                "event_type": EVENT_TYPES[table], "event_version": "1.0",
                "occurred_at": occurred_at(row), "producer": table,
                "correlation_id": row.get("global_id") or row.get("applicationId") or key,
                "source_dataset": table, "load_type": "INITIAL", "payload": row,
            }
            producer.produce(TOPICS[table], key=key.encode(),
                             value=json.dumps(envelope, separators=(",", ":")).encode())
            producer.poll(0)
            count += 1
    undelivered = producer.flush(120)
    if undelivered:
        raise RuntimeError(f"Kafka bootstrap timed out for {undelivered} event(s)")
    print(json.dumps({"event": "kafka_bootstrap_complete", "record_count": count}), flush=True)


def mark_complete(fingerprint: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(
            "INSERT INTO integration.bootstrap_state(source_fingerprint) VALUES (%s) ON CONFLICT DO NOTHING",
            (fingerprint,),
        )
        conn.commit()


def main() -> None:
    data = read_all()
    record_count = sum(map(len, data.values()))
    if len(data) != len(TABLES) or record_count != EXPECTED_RECORD_COUNT:
        raise RuntimeError(
            f"bootstrap audit gate failed: datasets={len(data)} records={record_count}; "
            f"expected datasets={len(TABLES)} records={EXPECTED_RECORD_COUNT}"
        )
    fingerprint = source_fingerprint(data)
    if already_bootstrapped(fingerprint):
        print(json.dumps({"event": "bootstrap_already_complete", "source_fingerprint": fingerprint}), flush=True)
        return
    seed_postgres(data)
    seed_kafka(data)
    mark_complete(fingerprint)
    print(json.dumps({"event": "bootstrap_complete", "source_namespace": SOURCE_NAMESPACE,
                      "source_fingerprint": fingerprint,
                      "dataset_count": len(data), "record_count": record_count,
                      "cdc_record_count": sum(len(data[t]) for t in CDC_SCHEMAS),
                      "kafka_record_count": sum(len(data[t]) for t in KAFKA_EVENT_TABLES),
                      "file_record_count": sum(len(data[t]) for t in BATCH_TABLES)}), flush=True)


if __name__ == "__main__":
    main()
