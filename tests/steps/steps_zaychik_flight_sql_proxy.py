# Copyright (c) 2026 Kenneth Stott
# Canary: 38658ef4-d6d0-4d66-a533-61095f2238fa
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-144 — Zaychik Flight SQL → Trino JDBC translation."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pytest_bdd import given, scenarios, then, when

import provisa.executor.trino_flight as trf


@pytest.fixture
def shared_data():
    return {}


@given("a Flight SQL client submits a query")
def given_flight_sql_client_submits(shared_data):
    shared_data["sql"] = "SELECT * FROM orders WHERE region = @1 AND amount > @2"
    shared_data["params"] = ["EU", 100]


@when("Zaychik receives the request")
def when_zaychik_receives(shared_data):
    # Zaychik binds Flight SQL parameters into the SQL text forwarded to Trino (JDBC).
    shared_data["translated"] = trf._substitute_params(shared_data["sql"], shared_data["params"])


@then("it translates the Flight SQL protocol to Trino JDBC and returns results as Arrow batches")
def then_translates_and_returns_arrow(shared_data):
    translated = shared_data["translated"]
    assert "@1" not in translated and "@2" not in translated
    assert "'EU'" in translated and "100" in translated

    # The ADBC Flight SQL cursor yields an Arrow table, which the proxy returns as batches.
    conn = MagicMock()
    cursor = conn.cursor.return_value
    expected = pa.table({"id": [1, 2]})
    cursor.fetch_arrow_table.return_value = expected
    result = trf.execute_trino_flight_arrow(conn, shared_data["sql"], shared_data["params"])
    assert isinstance(result, pa.Table)
    cursor.execute.assert_called_once()
    executed_sql = cursor.execute.call_args[0][0]
    assert "@1" not in executed_sql  # params were substituted before hitting Trino


scenarios("../features/REQ-144.feature")
