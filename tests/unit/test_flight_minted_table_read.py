# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e8a3f-27d4-4b96-a0e2-9f3d6b71c845
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2270: a table argument read over Arrow Flight SQL with the token minted for the call.

A real ProvisaFlightServer on a loopback port, read by ``provisa_services.TableRef.read`` (the
client a Python remote host such as the Spark connector's uses). The token is admitted as the
call's role and no other, in the call's org, with its caller's session variables, audited as its
caller; only a statement reading the call's tables runs, and only while the call is live. The
governed pipeline itself is stubbed to report what it was asked to run as; that the rows come
through it under a row rule is tests/integration/test_spark_connector_e2e.py."""

# Requirements: REQ-2226, REQ-2270

from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight.server import ProvisaFlightServer
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import minted_call_tokens
from provisa.pgwire.minted import MintedCall, MintedTokenStore, read_statement

pytestmark = pytest.mark.unit

ORDERS = "sales.orders"
_SERVICES = (
    Path(__file__).resolve().parents[2]
    / "provisa"
    / "python"
    / "services"
    / "provisa_services"
    / "__init__.py"
)


def _services() -> types.ModuleType:
    """The services module a code source's worker imports, loaded from its file."""
    spec = importlib.util.spec_from_file_location("provisa_services_under_test", _SERVICES)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


services = _services()


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}
    tracked_functions: dict = {}
    tracked_webhooks: dict = {}

    def __init__(self, admin_db: Database) -> None:
        self.admin_db = admin_db


def _call(**changes) -> MintedCall:
    fields = {
        "call_id": "call-7",
        "role_id": "analyst",
        "members": ("analyst",),
        "qualifiers": (),
        "user_id": "alice",
        "org_id": "org1",
        "tables": (ORDERS,),
        "session_vars": {"provisa.region": "emea"},
        "entitlements": None,
    }
    return MintedCall(**{**fields, **changes})


@pytest.fixture
def served(tmp_path, monkeypatch):
    """A Flight server whose governed pipeline answers with what it was asked to run as: the role,
    the audited principal, the org and the caller's session variables."""
    from provisa.audit.context import current_audit_identity
    from provisa.core.request_context import current_org, current_session_vars

    engine = create_engine_from_url(f"sqlite:///{tmp_path / 'cp.db'}", pool_size=5, max_overflow=0)
    minted_call_tokens.create(engine)
    db = Database(engine, name="cp")
    asked: list[dict] = []

    def _pipeline(self, request, ticket):  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]  # _do_get_inner signature
        identity = current_audit_identity()
        assert identity is not None
        seen = {
            "query": request["query"],
            "role": request["role"],
            "user": identity.user_id,
            "surface": identity.surface,
            "org": current_org.get(),
            "region": current_session_vars.get().get("provisa.region"),
        }
        asked.append(seen)
        return flight.RecordBatchStream(pa.table({k: [v] for k, v in seen.items()}))

    monkeypatch.setattr(ProvisaFlightServer, "_do_get_inner", _pipeline)
    server = ProvisaFlightServer(_State(db), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(
            uri=f"grpc://127.0.0.1:{server.port}",
            store=MintedTokenStore(db),
            asked=asked,
            server=server,
        )
    finally:
        server.shutdown()


def _ref(uri: str, token: str, *, call: str = "call-7", sql: str | None = None):
    return services.TableRef(
        ORDERS,
        ("id",),
        None,
        sql if sql is not None else read_statement(ORDERS, ["id"]),
        services.TableFlight(uri=uri, token=token, call=call),
    )


def test_a_table_argument_is_read_as_the_calls_role_audited_as_its_caller(served):
    token = asyncio.run(served.store.mint(_call()))
    rows = _ref(served.uri, token).read().to_pylist()
    assert rows == [
        {
            "query": 'SELECT "id" FROM sales.orders',
            "role": "analyst",
            "user": "alice",
            "surface": "flight",
            "org": "org1",
            "region": "emea",
        }
    ]


def test_a_wrong_token_is_refused_before_the_pipeline(served):
    asyncio.run(served.store.mint(_call()))
    with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
        _ref(served.uri, "provisa_call_not-the-token").read()
    assert served.asked == []


def test_another_calls_token_is_refused(served):
    token = asyncio.run(served.store.mint(_call()))
    with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
        _ref(served.uri, token, call="call-8").read()
    assert served.asked == []


def test_a_statement_outside_the_calls_tables_is_refused(served):
    token = asyncio.run(served.store.mint(_call()))
    with pytest.raises(flight.FlightUnauthorizedError):
        _ref(served.uri, token, sql="SELECT id FROM sales.customers").read()
    assert served.asked == []


def test_a_write_is_refused(served):
    token = asyncio.run(served.store.mint(_call()))
    with pytest.raises(flight.FlightUnauthorizedError, match="only reads"):
        _ref(served.uri, token, sql="DELETE FROM sales.orders").read()
    assert served.asked == []


def test_the_token_ends_with_its_call(served):
    token = asyncio.run(served.store.mint(_call()))
    asyncio.run(served.store.retire(token))
    with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
        _ref(served.uri, token).read()
    assert served.asked == []


def test_a_minted_token_reads_no_catalog_descriptor(served):
    token = asyncio.run(served.store.mint(_call()))
    options = flight.FlightCallOptions(
        headers=[(b"authorization", f"Bearer {token}".encode()), (b"x-provisa-call", b"call-7")]
    )
    client = flight.connect(served.uri)
    try:
        with pytest.raises(flight.FlightUnauthorizedError, match="only runs statements"):
            client.get_flight_info(flight.FlightDescriptor.for_path("sales", "orders"), options)
    finally:
        client.close()


def test_a_table_given_over_postgresql_is_not_read_over_flight():
    connection = services.TableConnection("h", 1, "call-7", "t", "provisa", "require")
    ref = services.TableRef(ORDERS, ("id",), connection, read_statement(ORDERS, ["id"]))
    with pytest.raises(ValueError, match="given over PostgreSQL"):
        ref.read()


def test_the_server_says_whether_it_serves_tls(served):
    assert served.server.tls is False


# -- REQ-2226: the listener minted for one call of a local worker ------------------------------


@pytest.fixture
def one_call(served, tmp_path):
    """A listener minted for call-7 over the same state and pipeline as ``served``."""
    token = asyncio.run(served.store.mint(_call()))
    server = ProvisaFlightServer(
        served.server._state, location="grpc://127.0.0.1:0", one_call=("call-7", token)
    )
    try:
        yield types.SimpleNamespace(uri=f"grpc://127.0.0.1:{server.port}", token=token)
    finally:
        server.shutdown()


def test_the_calls_listener_reads_its_table_as_the_calls_role(served, one_call):
    rows = _ref(one_call.uri, one_call.token).read().to_pylist()
    assert [(r["role"], r["user"], r["org"]) for r in rows] == [("analyst", "alice", "org1")]


def test_the_calls_listener_refuses_another_calls_token(served, one_call):
    other = asyncio.run(served.store.mint(_call(call_id="call-8")))
    with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
        _ref(one_call.uri, other, call="call-8").read()
    assert served.asked == []


def test_the_calls_listener_refuses_a_call_with_no_token(served, one_call):
    client = flight.connect(one_call.uri)
    try:
        descriptor = flight.FlightDescriptor.for_command(
            services._statement_command(read_statement(ORDERS, ["id"]))
        )
        with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
            client.get_flight_info(descriptor)
    finally:
        client.close()
    assert served.asked == []


def test_the_calls_listener_answers_no_rpc_but_a_statements_read(one_call):
    options = flight.FlightCallOptions(
        headers=[
            (b"authorization", f"Bearer {one_call.token}".encode()),
            (b"x-provisa-call", b"call-7"),
        ]
    )
    client = flight.connect(one_call.uri)
    try:
        with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
            list(client.list_flights(options=options))
        with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
            list(client.do_action(flight.Action("healthcheck", b""), options))
    finally:
        client.close()


def test_the_calls_listener_ends_with_its_token(served, one_call):
    asyncio.run(served.store.retire(one_call.token))
    with pytest.raises(flight.FlightUnauthenticatedError, match="minted token refused"):
        _ref(one_call.uri, one_call.token).read()
    assert served.asked == []
