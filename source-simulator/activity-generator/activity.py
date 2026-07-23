"""Generate low-volume operational changes from already bootstrapped source records."""

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import psycopg
from confluent_kafka import Producer


DATABASE_URL = os.environ["DATABASE_URL"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
S3_BUCKET = os.environ["S3_BUCKET"]
SOURCE_BATCH_PREFIX = os.getenv("SOURCE_BATCH_PREFIX", "g3/source/nab/batch").strip("/")
INTERVAL = int(os.getenv("SOURCE_ACTIVITY_INTERVAL_SECONDS", "300"))
FILE_EVERY = int(os.getenv("SOURCE_FILE_EVERY_CYCLES", "12"))
S3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))


def now() -> datetime:
    return datetime.now(timezone.utc)


def safe_log(event: str, **metrics) -> None:
    print(json.dumps({"event": event, "timestamp": now().isoformat(), **metrics}), flush=True)


def update_database(cycle: int) -> tuple[str, str | None]:
    with psycopg.connect(DATABASE_URL) as connection:
        row = connection.execute(
            'SELECT "applicationId", "global_id" FROM lending_origination.loan_applications '
            'ORDER BY "applicationId" OFFSET %s LIMIT 1',
            (cycle % 100,),
        ).fetchone()
        if row is None:
            raise RuntimeError("No bootstrapped loan application is available")
        connection.execute(
            'UPDATE lending_origination.loan_applications SET "lastUpdatedAt" = %s '
            'WHERE "applicationId" = %s',
            (now().isoformat(), row[0]),
        )
        connection.commit()
        return row[0], row[1]


def insert_database_record(cycle: int) -> tuple[str, str | None]:
    """Insert a new source-owned application so CDC demonstrates create semantics."""
    application_id = f"SIM-{now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(DATABASE_URL) as connection:
        row = connection.execute(
            'INSERT INTO lending_origination.loan_applications '
            '("global_id", "applicationId", "customerId", "loanGoal", "applicationType", '
            '"requestedAmount", "creditScore", "finalOutcome", "submittedAt", '
            '"lastUpdatedAt", "numOffers", "acceptedOfferAmount", "monthlyCost", '
            '"numberOfTerms", "totalEvents", "organisationId") '
            'SELECT "global_id", %s, "customerId", "loanGoal", "applicationType", '
            '"requestedAmount", "creditScore", %s, %s, %s, "numOffers", '
            '"acceptedOfferAmount", "monthlyCost", "numberOfTerms", "totalEvents", '
            '"organisationId" '
            'FROM lending_origination.loan_applications '
            'ORDER BY "applicationId" OFFSET %s LIMIT 1 '
            'RETURNING "applicationId", "global_id"',
            (
                application_id,
                "Pending",
                now().isoformat(),
                now().isoformat(),
                cycle % 100,
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("No source application is available to seed an incremental insert")
        connection.commit()
        return row[0], row[1]


def publish_event(cycle: int, application_id: str, global_id: str | None) -> None:
    timestamp = now().isoformat()
    event_id = hashlib.sha256(f"periodic|{cycle}|{application_id}|{timestamp}".encode()).hexdigest()
    payload = {
        "event_id": event_id,
        "event_type": "LoanApplicationActivityObserved",
        "event_version": "1.0",
        "occurred_at": timestamp,
        "producer": "activity-generator",
        "correlation_id": application_id,
        "source_dataset": "loan_application_events",
        "payload": {
            "eventId": event_id,
            "applicationId": application_id,
            "global_id": global_id,
            "timestamp": timestamp,
            "eventOrigin": "source-simulator",
            "action": "periodic_update",
        },
    }
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True})
    producer.produce(
        "nab.application.events",
        key=application_id.encode(),
        value=json.dumps(payload, separators=(",", ":")).encode(),
    )
    producer.flush(30)


def publish_file(cycle: int) -> bool:
    if cycle % FILE_EVERY:
        return False
    source_prefix = f"{SOURCE_BATCH_PREFIX}/banking_products/"
    response = S3.list_objects_v2(Bucket=S3_BUCKET, Prefix=source_prefix)
    source = next(
        (item["Key"] for item in response.get("Contents", []) if item["Key"].endswith(".csv")),
        None,
    )
    if source is None:
        return False
    timestamp = now().strftime("%Y%m%dT%H%M%SZ")
    destination = f"{source_prefix}banking_products-{timestamp}.csv"
    S3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": source},
        Key=destination,
        ServerSideEncryption="AES256",
    )
    return True


def main() -> None:
    cycle = 0
    while True:
        try:
            update_database(cycle)
            application_id, global_id = insert_database_record(cycle)
            publish_event(cycle, application_id, global_id)
            file_created = publish_file(cycle)
            safe_log("source_activity_complete", cycle=cycle, database_inserts=1, database_updates=1,
                     kafka_events=1, file_arrivals=int(file_created))
            cycle += 1
        except Exception as exc:
            safe_log("source_activity_failed", error_type=type(exc).__name__)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
