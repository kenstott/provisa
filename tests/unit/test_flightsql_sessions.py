# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL session options a client sets (REQ-2175): the session's current schema, kept per
session under the session cookie Flight defines (``arrow_flight_session_id``), bound to the caller
who set it, through the ADBC Flight SQL driver with its cookie middleware on, against a server on
a loopback port. Before, SetSessionOptions was not served."""

# Requirements: REQ-2175

from __future__ import annotations

import types

import adbc_driver_flightsql.dbapi as flightsql
import adbc_driver_manager
import pyarrow.flight as flight
import pytest

from provisa.api.flight import flightsql_catalog
from provisa.api.flight.server import ProvisaFlightServer
from provisa.api.flight.sessions import SessionStore
from provisa.audit.context import audit_identity_scope
from provisa.proto import Flight_pb2


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


def _catalog(state, role):  # noqa: ARG001  # role_catalog's signature
    """A role served one table, in the sales schema."""
    return flightsql_catalog.RoleCatalog(
        "provisa", [flightsql_catalog._Table("provisa", "sales", "orders", 16385)], {}, [], {}
    )


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port, each call running as the user its bearer token
    names (``anonymous`` without one) and the role its x-provisa-role header names."""

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        request.setdefault("role", "seller")
        with audit_identity_scope(str(request.get("token", "anonymous")), "flight"):
            return body()

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    monkeypatch.setattr(flightsql_catalog, "role_catalog", _catalog)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(uri=f"grpc://127.0.0.1:{server.port}", server=server)
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


def test_a_session_sets_its_current_schema_and_reads_it_back(served):
    with _connect(served.uri) as conn:
        assert conn.adbc_current_db_schema == "public"
        conn.adbc_current_db_schema = "sales"
        assert conn.adbc_current_db_schema == "sales"
        conn.adbc_current_db_schema = "public"
        assert conn.adbc_current_db_schema == "public"
        conn.adbc_current_catalog = "provisa"  # the one catalog it is served
        assert conn.adbc_current_catalog == "provisa"


def test_another_session_keeps_its_own(served):
    with _connect(served.uri) as first, _connect(served.uri) as second:
        first.adbc_current_db_schema = "sales"
        assert second.adbc_current_db_schema == "public"
        assert first.adbc_current_db_schema == "sales"


def test_a_schema_or_catalog_the_role_is_not_served_is_not_found(served):
    with _connect(served.uri) as conn:
        with pytest.raises(adbc_driver_manager.Error, match="thisschemadoesnotexist") as schema:
            conn.adbc_current_db_schema = "thisschemadoesnotexist"
        assert schema.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND
        with pytest.raises(adbc_driver_manager.Error, match="elsewhere") as catalog:
            conn.adbc_current_catalog = "elsewhere"
        assert catalog.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND
        assert conn.adbc_current_db_schema == "public"


def test_an_option_provisa_does_not_have_is_refused_by_name(served):
    with (
        _connect(served.uri) as conn,
        pytest.raises(adbc_driver_manager.Error, match=r"'color' \(invalid name\)"),
    ):
        conn.adbc_connection.set_options(**{"adbc.flight.sql.session.option.color": "red"})


def test_closing_the_connection_closes_its_session(served):
    with _connect(served.uri) as conn:
        conn.adbc_current_db_schema = "sales"
        assert len(served.server._sessions) == 1
    assert len(served.server._sessions) == 0


def test_a_session_cookie_presented_by_another_caller_is_refused(served):
    with _connect(served.uri) as conn:
        conn.adbc_current_db_schema = "sales"
        (session_id,) = served.server._sessions.ids()
        client = flight.connect(served.uri)
        try:
            options = flight.FlightCallOptions(
                headers=[
                    (b"authorization", b"Bearer mallory"),
                    (b"cookie", f"arrow_flight_session_id={session_id}".encode()),
                ]
            )
            body = Flight_pb2.GetSessionOptionsRequest().SerializeToString()
            with pytest.raises(flight.FlightUnauthorizedError, match="another caller"):
                list(client.do_action(flight.Action("GetSessionOptions", body), options))
        finally:
            client.close()
        assert conn.adbc_current_db_schema == "sales"


def test_the_store_is_bounded_and_forgets_its_oldest_session():
    store = SessionStore(limit=2)
    owner = ("u", None)
    for session_id, schema in (("first", "a"), ("second", "b"), ("third", "c")):
        store.update(session_id, owner, {"schema": schema})
    assert store.options("first", owner) is None  # forgotten: the client is served the defaults
    assert store.options("second", owner) == {"schema": "b"}
    assert len(store) == 2


def test_the_store_refuses_a_session_to_another_owner():
    store = SessionStore()
    store.update("s", ("alice", "acme"), {"schema": "sales"})
    for other in (("mallory", "acme"), ("alice", "globex")):
        with pytest.raises(PermissionError, match="another caller"):
            store.options("s", other)
        with pytest.raises(PermissionError, match="another caller"):
            store.update("s", other, {"schema": "public"})
    assert store.options("s", ("alice", "acme")) == {"schema": "sales"}
