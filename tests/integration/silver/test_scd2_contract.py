"""Runs test_scd2_contract.sql as real, CI-collectible pytest cases.

The .sql file remains the single source of truth for the assertions — this
just splits it into individual assert_true() statements and executes each
against a live SQL warehouse. See test_data_quality_edge_cases.py for the
same pattern applied to the DQ edge-case suite.
"""

import re
from pathlib import Path

import pytest

SQL_PATH = Path(__file__).with_name("test_scd2_contract.sql")


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
def test_scd2_contract_assertion(statement, live_sql_cursor):
    live_sql_cursor.execute(statement)
