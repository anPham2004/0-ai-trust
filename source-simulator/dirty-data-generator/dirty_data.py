"""Emit one deterministic invalid source row at a time for DQ/quarantine demos.

The row is source-owned PostgreSQL data, so Debezium captures it through the
same CDC path as clean activity.  It deliberately violates the Silver
``LOAN_GOAL_NULL`` hard rule while keeping its other keys and timestamps valid.
"""

import os
import time
import uuid
from datetime import datetime, timezone

import psycopg


DATABASE_URL = os.environ["DATABASE_URL"]
INTERVAL_SECONDS = int(os.getenv("DIRTY_DATA_INTERVAL_SECONDS", "60"))
INITIAL_DELAY_SECONDS = int(os.getenv("DIRTY_DATA_INITIAL_DELAY_SECONDS", "30"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def emit_invalid_application(cycle: int) -> str:
    application_id = f"SIM-{utc_now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(DATABASE_URL) as connection:
        row = connection.execute(
            'SELECT "global_id", "customerId", "applicationType", "requestedAmount", '
            '"creditScore", "submittedAt", "numOffers", "acceptedOfferAmount", '
            '"monthlyCost", "numberOfTerms", "totalEvents", "organisationId" '
            'FROM lending_origination.loan_applications '
            'WHERE "loanGoal" IS NOT NULL AND "applicationId" NOT LIKE %s '
            'ORDER BY "applicationId" OFFSET %s LIMIT 1',
            ("SIM-%", cycle % 100),
        ).fetchone()
        if row is None:
            raise RuntimeError("No clean application is available for the DQ fixture")

        connection.execute(
            'INSERT INTO lending_origination.loan_applications '
            '("global_id", "applicationId", "customerId", "loanGoal", "applicationType", '
            '"requestedAmount", "creditScore", "finalOutcome", "submittedAt", '
            '"lastUpdatedAt", "numOffers", "acceptedOfferAmount", "monthlyCost", '
            '"numberOfTerms", "totalEvents", "organisationId") '
            'VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (
                row[0], application_id, row[1], row[2], row[3], row[4], "Pending",
                row[5], utc_now().isoformat(), row[6], row[7], row[8], row[9], row[10], row[11],
            ),
        )
        connection.commit()
    return application_id


def main() -> None:
    time.sleep(INITIAL_DELAY_SECONDS)
    cycle = 0
    while True:
        try:
            application_id = emit_invalid_application(cycle)
            print(
                f"{{\"event\":\"dirty_record_emitted\",\"dataset\":\"loan_applications\","
                f"\"record_key\":\"{application_id}\",\"failed_rule\":\"LOAN_GOAL_NULL\"}}",
                flush=True,
            )
            cycle += 1
        except Exception as exc:
            print(
                f"{{\"event\":\"dirty_record_failed\",\"error_type\":\"{type(exc).__name__}\"}}",
                flush=True,
            )
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
