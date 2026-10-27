# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL prepared statements (REQ-2175): create, close, bound parameters through DoPut and
the statement's schema, through the ADBC Flight SQL driver against a server on a loopback port.

The ADBC DB-API prepares every statement it runs. Before, CreatePreparedStatement answered an
empty stream ("EOF ... Prepare"), and a bound parameter fell back to bulk ingest ("must set
IngestTargetTable"). The governed pipeline and pgwire's Describe are stubbed here to show what
they are asked: a prepared statement runs as its bound statement, described as the caller."""

# Requirements: REQ-2175

from __future__ import annotations

import types

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight.server import ProvisaFlightServer


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


_SHAPE = [("id", "integer"), ("name", "varchar(20)")]


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port. Its pipeline answers each statement with one row
    naming it; Describe types every statement as ``_SHAPE``. Both record what they are asked, and
    the caller each ran as."""
    asked: list[dict] = []
    described: list[tuple[str, str]] = []

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        request.setdefault("role", "anonymous")
        return body()

    def _pipeline(self, request, ticket):  # noqa: ARG001  # the native ticket's signature
        asked.append(dict(request))
        return flight.RecordBatchStream(pa.table({"statement": [request["query"]]}))

    async def _describe(sql, role_id, *, as_of=None):  # noqa: ARG001  # pgwire's signature
        described.append((sql, role_id))
        return types.SimpleNamespace(shape=_SHAPE, governed=None)

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    monkeypatch.setattr(ProvisaFlightServer, "_do_get_on_loop", _pipeline)
    monkeypatch.setattr("provisa.pgwire._pipeline.describe_pgwire_statement", _describe)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(
            uri=f"grpc://127.0.0.1:{server.port}", asked=asked, described=described
        )
    finally:
        server.shutdown()


def _connect(uri: str):
    return flightsql.connect(
        uri, db_kwargs={"adbc.flight.sql.rpc.call_header.x-provisa-role": "seller"}
    )


def test_a_prepared_statement_runs_through_the_pipeline_as_the_caller(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM sales.orders")
        assert cur.fetchall() == [("SELECT id FROM sales.orders",)]
    assert served.asked == [{"query": "SELECT id FROM sales.orders", "role": "seller"}]
    assert served.described == [("SELECT id FROM sales.orders", "seller")]


def test_a_bound_parameter_is_written_into_the_statement_as_a_literal(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM sales.orders WHERE region = $1 AND id > $2", ("it's", 7))
        cur.fetchall()
    (request,) = served.asked
    assert request["query"] == "SELECT id FROM sales.orders WHERE region = 'it''s' AND id > 7"


def test_several_parameter_rows_are_one_statement_their_results_in_turn(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.adbc_prepare("SELECT $1")
        cur.adbc_statement.bind(pa.record_batch({"0": [1, 2]}))
        handle, _ = cur.adbc_statement.execute_query()
        pa.RecordBatchReader._import_from_c(handle.address).read_all()
    (request,) = served.asked
    assert request["query"] == "(SELECT 1)\nUNION ALL\n(SELECT 2)"


def test_the_execute_schema_is_the_described_one(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        schema = cur.adbc_execute_schema("SELECT id, name FROM sales.orders")
    assert schema == pa.schema([("id", pa.int32()), ("name", pa.utf8())])
    assert served.asked == []  # described, never run


def test_closing_a_prepared_statement_is_answered(served):
    with _connect(served.uri) as conn:
        cur = conn.cursor()
        cur.adbc_prepare("SELECT 1")
        cur.close()  # ClosePreparedStatement
    assert served.asked == []
