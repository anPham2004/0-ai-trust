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
INTERVAL = int(os.getenv("SOURCE_ACTIVITY_INTERVAL_SECONDS", "60"))
FILE_INTERVAL = int(os.getenv("SOURCE_FILE_INTERVAL_SECONDS", "3600"))
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


def process_event(
    cycle: int,
    application_id: str,
    global_id: str | None,
    lifecycle_transition: str,
    action: str,
) -> tuple[str, bytes, bytes]:
    """Build a meaningful event using the same payload shape as bootstrap history."""
    timestamp = now().isoformat()
    event_id = hashlib.sha256(
        f"{cycle}|{application_id}|{lifecycle_transition}|{timestamp}".encode()
    ).hexdigest()
    event_payload = {
        "global_id": global_id,
        "eventId": event_id,
        "applicationId": application_id,
        "conceptName": "LoanApplication",
        "lifecycleTransition": lifecycle_transition,
        "timestamp": timestamp,
        "orgResource": "source-simulator",
        "action": action,
        "eventOrigin": "source-simulator",
        "offerId": None,
        "offeredAmount": None,
        "firstWithdrawalAmount": None,
        "numberOfTerms": None,
        "monthlyCost": None,
        "creditScore": None,
        "accepted": None,
        "selected": None,
    }
    envelope = {
        "event_id": event_id,
        "event_type": "LoanApplicationProcessEvent",
        "event_version": "1.0",
        "occurred_at": timestamp,
        "producer": "activity-generator",
        "correlation_id": application_id,
        "source_dataset": "loan_application_events",
        "load_type": "INCREMENTAL",
        "payload": event_payload,
    }
    return application_id, application_id.encode(), json.dumps(
        envelope, separators=(",", ":")
    ).encode()


def publish_events(events: list[tuple[str, bytes, bytes]]) -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True})
    for _, key, value in events:
        producer.produce("nab.application.events", key=key, value=value)
        producer.poll(0)
    undelivered = producer.flush(30)
    if undelivered:
        raise RuntimeError(f"Kafka delivery timed out for {undelivered} event(s)")


def publish_file() -> bool:
    """Create at most one immutable source file per interval, including across restarts."""
    source_prefix = f"{SOURCE_BATCH_PREFIX}/banking_products/"
    response = S3.list_objects_v2(Bucket=S3_BUCKET, Prefix=source_prefix)
    objects = [item for item in response.get("Contents", []) if item["Key"].endswith(".csv")]
    generated = [
        item
        for item in objects
        if item["Key"].removeprefix(source_prefix).startswith("banking_products-")
    ]
    if generated and (now() - max(item["LastModified"] for item in generated)).total_seconds() < FILE_INTERVAL:
        return False
    source = next(
        (item["Key"] for item in sorted(objects, key=lambda item: item["LastModified"])),
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
            updated_application_id, updated_global_id = update_database(cycle)
            inserted_application_id, inserted_global_id = insert_database_record(cycle)
            events = [
                process_event(
                    cycle,
                    inserted_application_id,
                    inserted_global_id,
                    "ApplicationSubmitted",
                    "create",
                ),
                process_event(
                    cycle,
                    updated_application_id,
                    updated_global_id,
                    "ApplicationDetailsUpdated",
                    "update",
                ),
            ]
            publish_events(events)
            file_created = publish_file()
            safe_log("source_activity_complete", cycle=cycle, database_inserts=1, database_updates=1,
                     kafka_events=len(events), file_arrivals=int(file_created))
            cycle += 1
        except Exception as exc:
            safe_log("source_activity_failed", error_type=type(exc).__name__)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
