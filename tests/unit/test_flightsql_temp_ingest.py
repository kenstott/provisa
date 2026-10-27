# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL's temporary ingest (REQ-2175, REQ-615): a temporary table is the one object a
session may create, so ``temporary=True`` lands the rows in a temporary table of the caller's
Flight session -- the session's own TempSession, as a pgwire session's -- which that session's
statements read and whose GetObjects lists it (schema ``pg_temp``), no other session sees, and
which ends with the session. Through the ADBC Flight SQL driver with its cookie middleware on,
against a server on a loopback port; the temporary store's write face is stubbed to show what it
is given."""

# Requirements: REQ-2175, REQ-615

from __future__ import annotations

import types

import adbc_driver_flightsql.dbapi as flightsql
import adbc_driver_manager
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight import flightsql_catalog
from provisa.api.flight.server import ProvisaFlightServer
from provisa.compiler import temp_tables


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


def _catalog(state, role):  # noqa: ARG001  # role_catalog's signature
    return flightsql_catalog.RoleCatalog("provisa", [], {}, [], {})


@pytest.fixture
def served(monkeypatch):
    """A server whose temporary store keeps each landed table's rows in memory, and whose
    pipeline answers a statement with the temporary tables of the session it ran in."""
    landed: dict[tuple[str, str], tuple[list, list]] = {}
    ended: list[str] = []
    ran: list[tuple[str, frozenset]] = []

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        request.setdefault("role", "seller")
        return ProvisaFlightServer._in_flight_session(self, request, body)

    async def _reland(state, session, name, columns, rows):  # noqa: ARG001  # temp_exec's
        landed[(session.id, name)] = (columns, rows)
        if name not in session.tables:
            session.tables[name] = temp_tables.TempTable(name, columns)
        session.stored = True

    async def _held(state, session, name):  # noqa: ARG001  # temp_exec's
        return landed.get((session.id, name), ([], []))[1]

    async def _end(session, state):  # noqa: ARG001  # temp_exec's
        ended.append(session.id)
        session.tables.clear()

    def _pipeline(self, request, ticket):  # noqa: ARG001  # the governed ticket body's signature
        ran.append((request["query"], temp_tables.names()))
        return flight.RecordBatchStream(pa.table({"n": [1]}))

    async def _describe(sql, role_id, *, as_of=None):  # noqa: ARG001  # pgwire's signature
        return types.SimpleNamespace(shape=[("n", "integer")], governed=None)

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    monkeypatch.setattr(ProvisaFlightServer, "_do_get_inner", _pipeline)
    monkeypatch.setattr(flightsql_catalog, "role_catalog", _catalog)
    monkeypatch.setattr("provisa.pgwire.temp_exec.reland", _reland)
    monkeypatch.setattr("provisa.pgwire.temp_exec.held_rows", _held)
    monkeypatch.setattr("provisa.pgwire.temp_exec.end", _end)
    monkeypatch.setattr("provisa.pgwire._pipeline.describe_pgwire_statement", _describe)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(
            uri=f"grpc://127.0.0.1:{server.port}", landed=landed, ended=ended, ran=ran
        )
    finally:
        server.shutdown()


def _connect(uri: str):
    return flightsql.connect(
        uri,
        db_kwargs={
            "adbc.flight.sql.rpc.call_header.x-provisa-role": "seller",
            "adbc.flight.sql.rpc.with_cookie_middleware": "true",
        },
    )


_ROWS = pa.table({"idx": pa.array([1, 2], pa.int64()), "value": ["a", None]})


def _tables(conn) -> list[tuple[str, str]]:
    return [
        (schema["db_schema_name"], table["table_name"])
        for catalog in conn.adbc_get_objects(depth="tables").read_all().to_pylist()
        for schema in catalog["catalog_db_schemas"]
        for table in schema["db_schema_tables"]
    ]


def test_a_temporary_ingest_lands_in_the_sessions_own_table(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        assert cur.adbc_ingest("scratch", _ROWS, mode="create", temporary=True) == 2
        assert cur.adbc_ingest("scratch", _ROWS.slice(1), mode="append", temporary=True) == 1
        cur.execute("SELECT idx FROM scratch")
        cur.fetchall()
        assert _tables(conn) == [("pg_temp", "scratch")]
    ((columns, rows),) = served.landed.values()
    assert columns == [("idx", "bigint"), ("value", "text")]
    assert rows == [(1, "a"), (2, None), (2, None)]
    assert served.ran == [("SELECT idx FROM scratch", frozenset({"scratch"}))]
    assert len(served.ended) == 1  # the session ended with the connection, and its table


def test_another_session_does_not_see_it(served):
    with _connect(served.uri) as first, _connect(served.uri) as second:
        with first.cursor() as cur:
            cur.adbc_ingest("scratch", _ROWS, mode="create", temporary=True)
        assert _tables(second) == []
        assert _tables(first) == [("pg_temp", "scratch")]


def test_a_temporary_table_is_created_once_and_may_be_replaced(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.adbc_ingest("scratch", _ROWS, mode="create", temporary=True)
        with pytest.raises(adbc_driver_manager.Error, match="already exists"):
            cur.adbc_ingest("scratch", _ROWS, mode="create", temporary=True)
        cur.adbc_ingest("scratch", _ROWS.slice(0, 1), mode="replace", temporary=True)
        with pytest.raises(adbc_driver_manager.Error, match="no temporary table 'other'"):
            cur.adbc_ingest("other", _ROWS, mode="append", temporary=True)
    ((_, rows),) = served.landed.values()
    assert rows == [(1, "a")]
