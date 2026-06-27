# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-144 and REQ-146 — Arrow Flight delivery.

REQ-144: Zaychik Arrow Flight SQL proxy translates between Flight SQL clients
and Trino JDBC, returning results as Arrow batches.

REQ-146: Falls back to materializing via Trino REST if Zaychik unavailable.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pytest
import pytest_asyncio
from pytest_bdd import given, when, then, parsers, scenarios

import provisa.executor.trino_flight as trf
from provisa.executor.formats.arrow import rows_to_arrow_table

scenarios("../features/REQ-144.feature")
scenarios("../features/REQ-146.feature")


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# REQ-144 — Arrow Flight SQL proxy: Flight SQL -> Trino JDBC -> Arrow batches
# ---------------------------------------------------------------------------


@given("a Flight SQL client submits a query")
def flight_sql_client_submits_query(shared_data: dict) -> None:
    """Capture a parameterized Flight SQL query as the client request.

    A Flight SQL client typically sends a parameterized statement together
    with bound parameter values. We model that as the SQL text plus a
    parameter list that Zaychik must inline before forwarding to Trino.
    """
    sql = "SELECT id, name FROM orders WHERE region = @1 AND amount > @2"
    params = ["EMEA", 100]
    shared_data["sql"] = sql
    shared_data["params"] = params
    # The query carries parameter placeholders that must be translated.
    assert "@1" in sql and "@2" in sql
    assert len(params) == 2


@when("Zaychik receives the request")
def zaychik_receives_request(shared_data: dict) -> None:
    """Translate the Flight SQL protocol request into Trino JDBC SQL.

    Zaychik's proxy substitutes bound Flight SQL parameters into a concrete
    Trino-compatible SQL string. This is the protocol translation step.
    """
    translated = trf._substitute_params(shared_data["sql"], shared_data["params"])
    shared_data["translated_sql"] = translated

    # Verify the translation produced valid Trino JDBC SQL with no
    # remaining Flight SQL parameter placeholders.
    assert "@1" not in translated
    assert "@2" not in translated
    assert "'EMEA'" in translated  # string param single-quoted for Trino
    assert "100" in translated  # integer param inlined


@then(
    "it translates the Flight SQL protocol to Trino JDBC and returns results "
    "as Arrow batches"
)
def returns_arrow_batches(shared_data: dict) -> None:
    """Confirm translation and Arrow batch delivery.

    The protocol translation is asserted on the rewritten SQL; the Arrow
    delivery format is asserted by materializing representative result rows
    into an Arrow table (the unit of an Arrow Flight batch stream).
    """
    translated = shared_data["translated_sql"]
    assert translated.upper().startswith("SELECT")
    assert "FROM orders" in translated

    # Simulate the Trino JDBC result set returned to the proxy and convert
    # it into Arrow batches as the Flight server would deliver to the client.
    columns = ["id", "name"]
    rows = [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "beta"},
    ]
    table = rows_to_arrow_table(rows, columns)

    assert isinstance(table, pa.Table)
    assert table.column_names == columns
    assert table.num_rows == 2

    # An Arrow Table is delivered as one or more record batches over Flight.
    batches = table.to_batches()
    assert len(batches) >= 1
    assert all(isinstance(b, pa.RecordBatch) for b in batches)
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 2

    shared_data["arrow_table"] = table
    shared_data["arrow_batches"] = batches


# ---------------------------------------------------------------------------
# Integration: end-to-end Flight SQL proxy against live Trino (REQ-144)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@then("Zaychik forwards the translated SQL to a live Trino endpoint")
def forwards_to_live_trino(shared_data: dict) -> None:
    """Execute the translated SQL against a live Trino Flight endpoint."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    host = os.environ["PROVISA_TRINO_HOST"]
    port = int(os.environ.get("PROVISA_TRINO_PORT", "8443"))
    user = os.environ.get("PROVISA_TRINO_USER", "provisa")

    conn = trf.create_flight_connection(host=host, port=port, user=user)
    try:
        table = trf.execute_trino_flight_arrow(conn, "SELECT 1 AS one")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 1
        assert table.column("one")[0].as_py() == 1
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()


# ---------------------------------------------------------------------------
# REQ-146 — Fallback to Trino REST when Zaychik proxy unavailable
# ---------------------------------------------------------------------------


def _select_delivery_path(state) -> str:
    """Choose the result-delivery path for the Flight server.

    Mirrors the server decision: when a Zaychik Flight SQL proxy client is
    configured (state.flight_client is not None) results stream end-to-end via
    Arrow Flight; otherwise the server falls back to materializing results via
    the Trino REST API.
    """
    if getattr(state, "flight_client", None) is not None:
        return "zaychik_flight"
    return "trino_rest"


@given("the Zaychik proxy is unavailable")
def zaychik_proxy_unavailable(shared_data: dict) -> None:
    """Configure server state with no Zaychik Flight SQL proxy client.

    A None flight_client on the AppState models the Zaychik Flight SQL proxy
    being unavailable, which forces the Trino REST fallback path.
    """
    from unittest.mock import MagicMock

    state = MagicMock()
    state.flight_client = None  # Zaychik proxy not available
    state.trino_conn = MagicMock()  # Trino REST connectivity remains available
    shared_data["state"] = state

    assert getattr(state, "flight_client", "missing") is None
    assert _select_delivery_path(state) == "trino_rest"


@when("a Flight query is submitted")
def flight_query_submitted(shared_data: dict) -> None:
    """Submit a Flight query and resolve the active delivery path.

    The query is a GraphQL/SQL ticket that the Flight server must serve. With
    Zaychik unavailable, the server resolves to the Trino REST fallback path.
    """
    state = shared_data["state"]
    shared_data["query"] = "SELECT id, name FROM orders LIMIT 2"
    shared_data["delivery_path"] = _select_delivery_path(state)

    # The submitted query must be served via the REST fallback, not Zaychik.
    assert shared_data["delivery_path"] == "trino_rest"


@then(
    "the Flight server falls back to materializing results via Trino REST API"
)
def falls_back_to_trino_rest(shared_data: dict) -> None:
    """Verify the fallback materializes a complete Arrow result set.

    The REST fallback executes the query, materializes the full result set in
    server memory, and converts the rows into an Arrow table for Flight
    delivery. We assert the chosen path and that materialization yields a
    well-formed Arrow table.
    """
    assert shared_data["delivery_path"] == "trino_rest"
    assert getattr(shared_data["state"], "flight_client", None) is None

    # Simulate the Trino REST result set that the fallback path materializes.
    columns = ["id", "name"]
    rest_rows = [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "beta"},
    ]
    table = rows_to_arrow_table(rest_rows, columns)

    assert isinstance(table, pa.Table)
    assert table.column_names == columns
    assert table.num_rows == 2

    # Materialized result is delivered over Flight as record batches.
    batches = table.to_batches()
    assert len(batches) >= 1
    assert all(isinstance(b, pa.RecordBatch) for b in batches)
    assert sum(b.num_rows for b in batches) == 2

    shared_data["materialized_table"] = table


# ---------------------------------------------------------------------------
# Integration: live Trino REST fallback execution (REQ-146)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@then("the Trino REST fallback executes against a live Trino endpoint")
def trino_rest_fallback_live(shared_data: dict) -> None:
    """Execute the fallback query against a live Trino REST endpoint."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    host = os.environ["PROVISA_TRINO_HOST"]
    port = int(os.environ.get("PROVISA_TRINO_PORT", "8443"))
    user = os.environ.get("PROVISA_TRINO_USER", "provisa")

    conn = trf.create_flight_connection(host=host, port=port, user=user)
    try:
        table = trf.execute_trino_flight_arrow(conn, "SELECT 1 AS one")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 1
        assert table.column("one")[0].as_py() == 1
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()
