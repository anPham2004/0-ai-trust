"""Fail live DQ CI when Silver is not recent enough for the current pipeline run."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from databricks import sql


MAX_LAG_MINUTES = int(os.getenv("DATABRICKS_MAX_SILVER_LAG_MINUTES", "30"))


def main() -> int:
    required = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        print(f"Missing Databricks connection variables: {', '.join(missing)}", file=sys.stderr)
        return 1

    connection = sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_timestamp(), MAX(processed_at)
                FROM `0-ai-trust`.silver.app_application
                """
            )
            now_value, processed_value = cursor.fetchone()
    finally:
        connection.close()

    if processed_value is None:
        print("Silver app_application has no processed_at value", file=sys.stderr)
        return 1

    if now_value.tzinfo is None:
        now_value = now_value.replace(tzinfo=timezone.utc)
    if processed_value.tzinfo is None:
        processed_value = processed_value.replace(tzinfo=timezone.utc)

    lag_minutes = (now_value - processed_value).total_seconds() / 60
    print(
        f"Silver freshness: processed_at={processed_value.isoformat()}, "
        f"now={now_value.isoformat()}, lag_minutes={lag_minutes:.2f}"
    )
    if lag_minutes > MAX_LAG_MINUTES:
        print(
            f"Silver is older than the {MAX_LAG_MINUTES} minute CI threshold",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
