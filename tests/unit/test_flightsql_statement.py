# Copyright (c) 2026 Kenneth Stott
# Canary: 07a2e3cd-fbbe-4c9d-8b8f-b99d42ff35a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Arrow Flight SQL protocol on Provisa's Flight endpoint: a statement query and SqlInfo
(REQ-2175), through the ADBC Flight SQL driver against a server on a loopback port.

A statement's rows are the native ticket's governed pipeline (``_do_get_on_loop``) run on the
statement, with the credential and role the call's headers carry; the pipeline is stubbed here to
show what it is asked. Before, a Flight SQL command reached the native path-descriptor code and
failed "'NoneType' object is not iterable" (a command descriptor has no path)."""

# Requirements: REQ-2175

from __future__ import annotations

import importlib.metadata
import types

import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi as flightsql
import adbc_driver_manager
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


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port whose governed pipeline answers every statement
    with one row naming it, recording the request it was asked to run."""
    asked: list[dict] = []

    def _pipeline(self, request, ticket):  # noqa: ARG001  # the native ticket's signature
        asked.append(dict(request))
        return flight.RecordBatchStream(pa.table({"statement": [request["query"]]}))

    monkeypatch.setattr(ProvisaFlightServer, "_do_get_on_loop", _pipeline)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(uri=f"grpc://127.0.0.1:{server.port}", asked=asked)
    finally:
        server.shutdown()


def _connect(uri: str, token: str | None = None, role: str | None = None):
    kwargs: dict[str, str] = {}
    if token is not None:
        kwargs["adbc.flight.sql.authorization_header"] = f"Bearer {token}"
    if role is not None:
        kwargs["adbc.flight.sql.rpc.call_header.x-provisa-role"] = role
    return flightsql.connect(uri, db_kwargs=kwargs)


def _query(uri: str, sql: str, **db_kwargs: str) -> pa.Table:
    """``sql`` run as a statement query (ExecuteQuery, no Prepare): CommandStatementQuery."""
    with (
        adbc_driver_flightsql.connect(uri, db_kwargs=db_kwargs) as db,
        adbc_driver_manager.AdbcConnection(db) as conn,
        adbc_driver_manager.AdbcStatement(conn) as stmt,
    ):
        stmt.set_sql_query(sql)
        stream, _ = stmt.execute_query()
        return pa.RecordBatchReader._import_from_c(stream.address).read_all()


def test_a_statement_query_is_run_by_the_governed_pipeline_as_the_caller(served):
    rows = _query(
        served.uri,
        "SELECT id FROM sales.orders",
        **{
            "adbc.flight.sql.authorization_header": "Bearer sam",
            "adbc.flight.sql.rpc.call_header.x-provisa-role": "seller",
        },
    )
    assert rows.to_pylist() == [{"statement": "SELECT id FROM sales.orders"}]
    (request,) = served.asked
    assert request == {"query": "SELECT id FROM sales.orders", "token": "sam", "role": "seller"}


def test_a_statement_without_a_credential_asks_as_no_one(served):
    _query(served.uri, "SELECT 1")
    (request,) = served.asked
    assert request == {"query": "SELECT 1"}


def test_sql_info_names_provisa_and_its_versions(served):
    with _connect(served.uri) as conn:
        info = conn.adbc_get_info()
    assert info["vendor_name"] == "Provisa"
    assert info["vendor_version"] == importlib.metadata.version("provisa")
    assert info["vendor_arrow_version"] == pa.__version__


def test_an_empty_statement_is_refused_by_name(served):
    with pytest.raises(adbc_driver_manager.Error, match="needs a statement"):
        _query(served.uri, "  ")
    assert served.asked == []


def test_a_native_json_ticket_still_reaches_the_native_path(served):
    client = flight.connect(served.uri)
    try:
        rows = client.do_get(flight.Ticket(b'{"query": "SELECT 2", "role": "seller"}')).read_all()
    finally:
        client.close()
    assert rows.to_pylist() == [{"statement": "SELECT 2"}]
    assert served.asked == [{"query": "SELECT 2", "role": "seller"}]
