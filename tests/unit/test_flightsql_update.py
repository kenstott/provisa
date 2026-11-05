# Copyright (c) 2026 Kenneth Stott
# Canary: 1ad19c15-1b75-4ac3-9718-c4bd6b55369a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL statement and prepared statement updates (REQ-2175), through the ADBC Flight SQL
driver against a server on a loopback port: each statement governed as the caller and run through
the one pipeline, answered its affected count. Before, ExecuteUpdate failed NOT_IMPLEMENTED
("CommandStatementUpdate is not served"). The pipeline is stubbed here to show what it is asked."""

# Requirements: REQ-2175

from __future__ import annotations

import types

import adbc_driver_flightsql.dbapi as flightsql
import adbc_driver_manager
import pyarrow as pa
import pytest

from provisa.api.flight.server import ProvisaFlightServer
from provisa.executor.result import QueryResult


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port whose pipeline governs each statement into a plan
    naming it and runs the plan, answered one affected row per ``VALUES`` row. All three record
    what they are asked, and the role each statement was governed as."""
    governed: list[tuple[str, str]] = []
    verified: list[str] = []
    ran: list[str] = []

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        request.setdefault("role", "anonymous")
        return body()

    async def _govern(sql, role_id, state, **kwargs):  # noqa: ARG001  # the pipeline's signature
        governed.append((sql, role_id))
        if sql.startswith("SELECT"):
            return QueryResult(rows=[(1,), (2,)], column_names=["n"])  # a registered function's
        return types.SimpleNamespace(sql=sql)

    async def _describe(sql, role_id, *, as_of=None):  # noqa: ARG001  # pgwire's signature
        return types.SimpleNamespace(shape=None, governed=None)  # a write has no result columns

    def _verify(plan):
        verified.append(plan.sql)

    async def _execute(plan, state):  # noqa: ARG001  # the pipeline's signature
        ran.append(plan.sql)
        rows = plan.sql.partition("VALUES")[2].count("(")
        return QueryResult(rows=[], column_names=[], rowcount=rows)

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    monkeypatch.setattr("provisa.pgwire._pipeline.govern_batch_final_plan_with_fn", _govern)
    monkeypatch.setattr("provisa.pgwire._pipeline.require_governed_plan", _verify)
    monkeypatch.setattr("provisa.pgwire._pipeline.describe_pgwire_statement", _describe)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(
            uri=f"grpc://127.0.0.1:{server.port}", governed=governed, verified=verified, ran=ran
        )
    finally:
        server.shutdown()


def _connect(uri: str):
    return flightsql.connect(
        uri, db_kwargs={"adbc.flight.sql.rpc.call_header.x-provisa-role": "seller"}
    )


def test_a_statement_update_is_governed_as_the_caller_and_answered_its_count(served):
    statement = "INSERT INTO sales.orders (id) VALUES (1), (2)"
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.adbc_statement.set_sql_query(statement)
        assert cur.adbc_statement.execute_update() == 2
    assert served.governed == [(statement, "seller")]
    assert served.verified == served.ran == [statement]


def test_a_prepared_update_runs_once_per_parameter_row_answered_their_total(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.adbc_statement.set_sql_query("INSERT INTO sales.orders (id, note) VALUES ($1, $2)")
        cur.adbc_statement.prepare()
        cur.adbc_statement.bind(
            pa.record_batch({"0": pa.array([7, 8], pa.int64()), "1": pa.nulls(2)})
        )
        assert cur.adbc_statement.execute_update() == 2
    assert served.ran == [
        "INSERT INTO sales.orders (id, note) VALUES (7, NULL)",
        "INSERT INTO sales.orders (id, note) VALUES (8, NULL)",
    ]
    assert {role for _, role in served.governed} == {"seller"}


def test_a_prepared_update_with_no_parameters_runs_as_it_is(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.adbc_statement.set_sql_query("DELETE FROM sales.orders WHERE id = 2")
        cur.adbc_statement.prepare()
        assert cur.adbc_statement.execute_update() == 0
    assert served.ran == ["DELETE FROM sales.orders WHERE id = 2"]


def test_a_statement_answered_rows_counts_them(served):
    """A registered function's call is answered its rows by the pipeline, never planned."""
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.adbc_statement.set_sql_query("SELECT archive_orders()")
        assert cur.adbc_statement.execute_update() == 2
    assert served.ran == []


def test_a_refused_statement_is_the_callers_error(served, monkeypatch):
    async def _refuse(sql, role_id, state, **kwargs):  # noqa: ARG001  # the pipeline's signature
        raise PermissionError("seller may not delete from sales.orders")

    monkeypatch.setattr("provisa.pgwire._pipeline.govern_batch_final_plan_with_fn", _refuse)
    with (
        _connect(served.uri) as conn,
        conn.cursor() as cur,
        pytest.raises(adbc_driver_manager.Error, match="may not delete from sales.orders"),
    ):
        cur.adbc_statement.set_sql_query("DELETE FROM sales.orders")
        cur.adbc_statement.execute_update()
