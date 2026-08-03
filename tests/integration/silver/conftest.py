"""Live-workspace plumbing for the `live_dq` Silver integration suite.

These tests execute the existing assert_true() SQL scripts against an
already-deployed, already-running Silver pipeline — they need a real SQL
warehouse, not fixture data. Absent those three env vars, every `live_dq`
test is skipped automatically (not errored), so the default `pytest tests -q`
run (used locally and by the push-triggered sync-databricks.yml CI job, where
no live pipeline necessarily exists yet) stays green. The scheduled
`dq-check.yml` workflow sets these env vars and is where these tests actually
execute.
"""

import os

import pytest

REQUIRED_ENV_VARS = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")


def pytest_collection_modifyitems(config, items):
    if all(os.getenv(var) for var in REQUIRED_ENV_VARS):
        return
    skip_live = pytest.mark.skip(
        reason=(
            "DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN not set; "
            "live_dq tests require an already-deployed, already-running Silver pipeline."
        )
    )
    for item in items:
        if "live_dq" in item.keywords:
            item.add_marker(skip_live)


@pytest.fixture(scope="module")
def live_sql_cursor():
    from databricks import sql  # imported lazily: only needed when live_dq tests actually run

    connection = sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        connection.close()
