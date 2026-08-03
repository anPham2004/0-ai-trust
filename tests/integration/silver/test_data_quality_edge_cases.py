"""Runs test_data_quality_edge_cases.sql as real, CI-collectible pytest cases.

The .sql file remains the single source of truth for the assertions — this
just splits it into individual assert_true() statements and executes each
against a live SQL warehouse, so a DQ regression in canonical Silver shows up
as a normal pytest failure (with the exact failing predicate) instead of a
README instruction nobody runs.
"""

import re
from pathlib import Path

import pytest

SQL_PATH = Path(__file__).with_name("test_data_quality_edge_cases.sql")


def _statements() -> list[str]:
    lines = [
        line for line in SQL_PATH.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("--")
    ]
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


def _id(statement: str) -> str:
    match = re.search(r"assert_true\([^']*'([^']+)'", statement)
    return match.group(1) if match else statement[:60]


STATEMENTS = _statements()


@pytest.mark.live_dq
@pytest.mark.parametrize("statement", STATEMENTS, ids=[_id(statement) for statement in STATEMENTS])
def test_data_quality_edge_case(statement, live_sql_cursor):
    live_sql_cursor.execute(statement)
