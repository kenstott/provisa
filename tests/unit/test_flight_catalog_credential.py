# Copyright (c) 2026 Kenneth Stott
# Canary: 4e1d8b37-9a25-4c6f-b0e2-7f3c5a9d1e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow Flight's catalog is listed to a credential, as its role (REQ-1263, REQ-127).

``list_flights``, ``get_flight_info`` and ``get_schema`` carry no ticket. With authentication on
they used to answer the whole catalog — every table, column and command — to a caller presenting
no credential, and a catalog ticket answered it whatever role its credential held. Now each needs
a credential (the call's ``authorization`` header; the ticket's token) and lists what the
authorized role is served: one role its own tables, columns and commands, a set of held roles
their union. A deployment that authenticates nobody lists the whole catalog, as before.
"""

# Requirements: REQ-1263, REQ-127, REQ-128, REQ-1156, REQ-1620

from __future__ import annotations

import json
import types

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight import catalog as flight_catalog
from provisa.api.flight import server as flight_server
from provisa.api.flight.server import ProvisaFlightServer
from provisa.auth.models import AuthIdentity

_AUTH_CONFIG = {"provider": "oidc", "default_role": "seller", "role_mapping": []}
_META = "meta:hr_reader+seller"

# table id → (domain, table, columns)
_TABLES = {
    1: ("sales", "orders", ["id", "region", "margin"]),
    2: ("hr", "staff", ["id", "salary"]),
}
# role → table id → the columns it is served
_SERVED = {
    "seller": {1: ["id", "region"]},
    "hr_reader": {2: ["id", "salary"]},
    _META: {1: ["id", "region"], 2: ["id", "salary"]},
}
_COMMANDS = {
    "seller": ["order_count"],
    "hr_reader": ["staff_count"],
    _META: ["order_count", "staff_count"],
    None: ["order_count", "staff_count"],
}


class _Conn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def fetch(self, sql: str):
        if "FROM registered_tables" in sql:
            return [
                {
                    "id": table_id,
                    "domain_id": domain,
                    "table_name": name,
                    "description": "",
                    "modeling_role": None,
                    "modeling_history": None,
                }
                for table_id, (domain, name, _) in _TABLES.items()
            ]
        if "FROM relationships" in sql:
            return []
        return [
            {
                "table_id": table_id,
                "column_name": column,
                "alias": None,
                "data_type": "varchar",
                "description": "",
                "is_primary_key": False,
            }
            for table_id, (_, _, columns) in _TABLES.items()
            for column in columns
        ]


def _context(served: dict[int, list[str]]):
    return types.SimpleNamespace(
        tables={f"t{table_id}": types.SimpleNamespace(table_id=table_id) for table_id in served},
        physical_to_sql={(tid, col): col for tid, cols in served.items() for col in cols},
    )


class _State:
    def __init__(self, *, auth: bool, multitenancy: bool = False):
        self.auth_config = _AUTH_CONFIG if auth else None
        self.auth_middleware_active = auth
        self.multitenancy = multitenancy
        self.org_id = "default"
        self.admin_db = None
        self.model_db = types.SimpleNamespace(acquire=lambda: _Conn())
        self.engine_conn = None
        self.roles = {r: {"id": r} for r in _SERVED}
        self.contexts = {role: _context(served) for role, served in _SERVED.items()}
        self.meta_roles = {_META: ("hr_reader", "seller")}
        self.metrics = {
            "net": types.SimpleNamespace(description="net", ai_context=None, visible_to=["*"]),
            "pay": types.SimpleNamespace(
                description="pay", ai_context=None, visible_to=["hr_reader"]
            ),
        }


class _Call:
    """A call's context: the headers it arrived with, as the server's middleware keeps them."""

    def __init__(self, token: str | None = None, role: str | None = None):
        headers: dict[str, list[str]] = {}
        if token is not None:
            headers["authorization"] = [f"Bearer {token}"]
        if role is not None:
            headers["x-provisa-role"] = [role]
        self._headers = headers

    def get_middleware(self, key: str):
        assert key == "headers"
        return types.SimpleNamespace(headers=self._headers)


def _server(monkeypatch, *, auth: bool = True, multitenancy: bool = False) -> ProvisaFlightServer:
    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    srv._state = _State(auth=auth, multitenancy=multitenancy)

    async def _validate(state, token):  # noqa: ARG001  # signature mirrors the real validator
        if token == "sam":
            return _identity(["seller"])
        if token == "both":
            return _identity(["seller", "hr_reader"])
        raise ValueError("no such credential")

    monkeypatch.setattr(flight_server, "_validate_flight_credential", _validate)
    monkeypatch.setattr(
        "provisa.security.meta_role.ensure_meta_role",
        lambda state, members: "meta:" + "+".join(sorted(set(members))),
    )
    monkeypatch.setattr(
        "provisa.api.data.action_exec.list_visible_commands",
        lambda state, role: [
            {
                "name": n,
                "domain": "sales",
                "description": "",
                "kind": "query",
                "set_returning": True,
                "arguments": [],
            }
            for n in _COMMANDS[role]
        ],
    )
    return srv


def _identity(roles: list[str]) -> AuthIdentity:
    return AuthIdentity(
        user_id="u-1",
        email=None,
        display_name=None,
        roles=roles,
        raw_claims={},
        active_org_id=None,
    )


def _listed(srv, call) -> set[tuple[str, ...]]:
    return {
        tuple(p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path)
        for info in srv.list_flights(call, b"")
    }


def _columns(srv, call, domain: str, table: str) -> list[str]:
    return srv.get_schema(call, flight.FlightDescriptor.for_path(domain, table)).schema.names


def _catalog_ticket(srv, **body) -> list[dict]:
    stream = srv.do_get(None, flight.Ticket(json.dumps(body).encode()))
    return _rows(stream)


def _rows(stream) -> list[dict]:
    """The rows a RecordBatchStream was built from (the catalog stream wraps one table)."""
    return _CAPTURED.pop()


_CAPTURED: list[list[dict]] = []


@pytest.fixture(autouse=True)
def _capture_streams(monkeypatch):
    """The catalog ticket's answer, as rows: the stream is built from one Arrow table."""
    _CAPTURED.clear()

    def _stream(table, *args, **kwargs):  # noqa: ARG001
        _CAPTURED.append(table.to_pylist())
        return "stream"

    monkeypatch.setattr(flight_server, "record_batch_stream", _stream)
    monkeypatch.setattr(flight_server, "_report_table", lambda table: None)


_ORDERS, _STAFF = ("sales", "orders"), ("hr", "staff")
_ORDER_COUNT, _STAFF_COUNT = (
    ("commands", "sales", "order_count"),
    ("commands", "sales", "staff_count"),
)


# --- no credential ---------------------------------------------------------------------------------


def test_every_catalog_call_without_a_credential_is_refused(monkeypatch):
    srv = _server(monkeypatch)
    descriptor = flight.FlightDescriptor.for_path(*_ORDERS)
    for call in (
        lambda: list(srv.list_flights(_Call(), b"")),
        lambda: srv.get_flight_info(_Call(), descriptor),
        lambda: srv.get_schema(_Call(), descriptor),
        lambda: srv.do_get(None, flight.Ticket(b"{}")),
        lambda: list(srv.list_flights(_Call(token="forged"), b"")),
    ):
        with pytest.raises(flight.FlightUnauthenticatedError):
            call()
    assert _CAPTURED == []


# --- a role is listed what it is served -----------------------------------------------------------


def test_a_role_is_listed_only_its_own_tables_columns_and_commands(monkeypatch):
    srv = _server(monkeypatch)
    sam = _Call(token="sam")
    assert _listed(srv, sam) == {_ORDERS, _ORDER_COUNT}
    assert _columns(srv, sam, *_ORDERS) == ["id", "region"]  # not `margin`
    assert srv.get_flight_info(sam, flight.FlightDescriptor.for_path(*_ORDERS)).schema.names == [
        "id",
        "region",
    ]
    for call in (
        lambda: srv.get_schema(sam, flight.FlightDescriptor.for_path(*_STAFF)),
        lambda: srv.get_flight_info(sam, flight.FlightDescriptor.for_path(*_STAFF)),
        lambda: srv.get_flight_info(sam, flight.FlightDescriptor.for_path(*_STAFF_COUNT)),
        lambda: srv.get_flight_info(sam, flight.FlightDescriptor.for_path("metrics", "pay")),
    ):
        with pytest.raises(flight.FlightServerError, match="not found"):
            call()
    # A metric granted to every role is found.
    srv.get_flight_info(sam, flight.FlightDescriptor.for_path("metrics", "net"))


def test_a_catalog_ticket_lists_its_credentials_role(monkeypatch):
    srv = _server(monkeypatch)
    rows = _catalog_ticket(srv, token="sam")
    assert {(r["schema_name"], r["table_name"]) for r in rows} == {_ORDERS}
    columns = _catalog_ticket(srv, token="sam", domain="sales", table="orders")
    assert [c["column_name"] for c in columns] == ["id", "region"]
    with pytest.raises(flight.FlightServerError, match="not found"):
        _catalog_ticket(srv, token="sam", domain="hr", table="staff")


def test_a_held_set_is_listed_the_union(monkeypatch):
    srv = _server(monkeypatch)
    both = _Call(token="both", role="seller,hr_reader")
    assert _listed(srv, both) == {_ORDERS, _STAFF, _ORDER_COUNT, _STAFF_COUNT}
    assert _columns(srv, both, *_STAFF) == ["id", "salary"]
    srv.get_flight_info(both, flight.FlightDescriptor.for_path("metrics", "pay"))
    rows = _catalog_ticket(srv, token="both", role="hr_reader,seller")
    assert {(r["schema_name"], r["table_name"]) for r in rows} == {_ORDERS, _STAFF}
    # One of the held roles, named alone, is that role.
    assert _listed(srv, _Call(token="both", role="hr_reader")) == {_STAFF, _STAFF_COUNT}


def test_a_role_the_credential_does_not_hold_is_refused(monkeypatch):
    srv = _server(monkeypatch)
    descriptor = flight.FlightDescriptor.for_path(*_STAFF)
    for role in ("hr_reader", "seller,hr_reader", _META):
        call = _Call(token="sam", role=role)
        for rpc in (
            lambda: list(srv.list_flights(call, b"")),
            lambda: srv.get_flight_info(call, descriptor),
            lambda: srv.get_schema(call, descriptor),
            lambda: srv.do_get(
                None, flight.Ticket(json.dumps({"token": "sam", "role": role}).encode())
            ),
        ):
            with pytest.raises(flight.FlightUnauthorizedError):
                rpc()
    assert _CAPTURED == []


# --- a deployment that authenticates nobody: as before -------------------------------------------


def test_with_authentication_off_the_whole_catalog_is_listed_without_a_credential(monkeypatch):
    srv = _server(monkeypatch, auth=False)
    assert _listed(srv, _Call()) == {_ORDERS, _STAFF, _ORDER_COUNT, _STAFF_COUNT}
    assert _columns(srv, _Call(), *_ORDERS) == ["id", "region", "margin"]
    # A role named by an unauthenticated call narrows nothing: there is no identity to hold it.
    assert _listed(srv, _Call(role="seller")) == {_ORDERS, _STAFF, _ORDER_COUNT, _STAFF_COUNT}
    rows = _catalog_ticket(srv, role="seller")
    assert {(r["schema_name"], r["table_name"]) for r in rows} == {_ORDERS, _STAFF}
    srv.get_flight_info(_Call(), flight.FlightDescriptor.for_path("metrics", "pay"))


# --- multitenancy: the ticketless calls name no org -----------------------------------------------


@pytest.mark.parametrize("auth", [True, False])
def test_under_multitenancy_the_ticketless_calls_are_refused_as_before(monkeypatch, auth):
    srv = _server(monkeypatch, auth=auth, multitenancy=True)
    call = _Call(token="sam") if auth else _Call()
    descriptor = flight.FlightDescriptor.for_path(*_ORDERS)
    for rpc in (
        lambda: list(srv.list_flights(call, b"")),
        lambda: srv.get_flight_info(call, descriptor),
        lambda: srv.get_schema(call, descriptor),
    ):
        with pytest.raises(flight.FlightServerError, match="names no org under multitenancy"):
            rpc()


# --- the visibility the catalog is narrowed by ----------------------------------------------------


def test_a_role_with_no_data_surface_is_listed_nothing(monkeypatch):
    srv = _server(monkeypatch)
    assert flight_catalog.role_visibility(srv._state, "platform_admin") == {}
    assert flight_catalog.build_catalog_tables(srv._state, "platform_admin") == []


# --- over the wire: a real Flight server and client -----------------------------------------------
#
# The cases above call the server's methods; these go through pyarrow's own RPC layer, which is
# where a refusal becomes (or fails to become) a typed error for the client, and where the
# handler thread's bindings are whatever the server set up — nothing a direct call shares.


@pytest.fixture
def wire(monkeypatch):
    """A ProvisaFlightServer bound to a loopback port over the model above, auth on."""
    monkeypatch.undo()  # the autouse stream capture: the wire needs real record-batch streams
    probe = _server(monkeypatch)  # installs the credential validator and the command list
    bound: list[str | None] = []

    def _ensure(state, members):
        from provisa.core.request_context import current_org

        bound.append(current_org.get(None))
        return "meta:" + "+".join(sorted(set(members)))

    monkeypatch.setattr("provisa.security.meta_role.ensure_meta_role", _ensure)
    server = ProvisaFlightServer(probe._state, location="grpc://127.0.0.1:0")
    client = flight.connect(f"grpc://127.0.0.1:{server.port}")
    try:
        yield types.SimpleNamespace(client=client, org_at_meta_role=bound, state=probe._state)
    finally:
        client.close()
        server.shutdown()


def _options(token: str | None = None, role: str | None = None) -> flight.FlightCallOptions:
    headers: list[tuple[bytes, bytes]] = []
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode()))
    if role is not None:
        headers.append((b"x-provisa-role", role.encode()))
    return flight.FlightCallOptions(headers=headers)


def _wire_listed(client, options) -> set[tuple[str, ...]]:
    return {
        tuple(p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path)
        for info in client.list_flights(b"", options)
    }


def test_over_the_wire_a_role_is_listed_its_catalog_and_reads_its_schema(wire):
    sam = _options("sam")
    assert _wire_listed(wire.client, sam) == {_ORDERS, _ORDER_COUNT}
    descriptor = flight.FlightDescriptor.for_path(*_ORDERS)
    assert wire.client.get_schema(descriptor, sam).schema.names == ["id", "region"]
    assert wire.client.get_flight_info(descriptor, sam).schema.names == ["id", "region"]


def test_over_the_wire_the_org_is_bound_when_a_set_becomes_its_meta_role(wire):
    """Acting as a set makes its meta-role in the org's runtime — a tenant-data path. It ran
    before the org was bound, so every catalog call naming a set failed 'No active org bound'."""
    both = _options("both", role="seller,hr_reader")
    assert _wire_listed(wire.client, both) == {_ORDERS, _STAFF, _ORDER_COUNT, _STAFF_COUNT}
    wire.client.get_flight_info(flight.FlightDescriptor.for_path(*_STAFF), both)
    wire.client.get_schema(flight.FlightDescriptor.for_path(*_STAFF), both)
    rows = wire.client.do_get(
        flight.Ticket(json.dumps({"token": "both", "role": "seller,hr_reader"}).encode())
    ).read_all()
    assert set(rows.column("table_name").to_pylist()) == {"orders", "staff"}
    assert wire.org_at_meta_role == ["default"] * 4, wire.org_at_meta_role


def test_over_the_wire_a_refusal_reaches_the_client_as_what_it_is(wire):
    descriptor = flight.FlightDescriptor.for_path(*_STAFF)
    # No credential, or a rejected one: unauthenticated.
    for options in (_options(), _options("forged")):
        with pytest.raises(flight.FlightUnauthenticatedError):
            list(wire.client.list_flights(b"", options))
        with pytest.raises(flight.FlightUnauthenticatedError):
            wire.client.get_flight_info(descriptor, options)
    with pytest.raises(flight.FlightUnauthenticatedError):
        wire.client.do_get(flight.Ticket(b"{}")).read_all()
    # A valid credential asking for a role it does not hold: permission denied, by name.
    unheld = _options("sam", role="hr_reader")
    with pytest.raises(flight.FlightUnauthorizedError, match="hr_reader"):
        list(wire.client.list_flights(b"", unheld))
    with pytest.raises(flight.FlightUnauthorizedError, match="hr_reader"):
        wire.client.get_flight_info(descriptor, unheld)
    with pytest.raises(flight.FlightUnauthorizedError, match="hr_reader"):
        wire.client.do_get(
            flight.Ticket(json.dumps({"token": "sam", "role": "hr_reader"}).encode())
        ).read_all()
    # What the role is not served is not found.
    with pytest.raises(flight.FlightServerError, match="not found"):
        wire.client.get_flight_info(descriptor, _options("sam"))


def test_over_the_wire_get_schema_refuses_with_the_reason(wire):
    """GetSchema is refused on the same terms. pyarrow's server binding does not carry a Flight
    error's kind for this one RPC (24.0: `_get_schema` reports any exception as 'Unknown
    error'), so the client sees an ArrowException — with the server's reason in it."""
    descriptor = flight.FlightDescriptor.for_path(*_STAFF)
    for options, reason in (
        (_options(), "a bearer credential is required"),
        (_options("forged"), "credential rejected"),
        (_options("sam", role="hr_reader"), "'hr_reader' is not assigned"),
        (_options("sam"), "Table not found: hr.staff"),
    ):
        with pytest.raises(pa.ArrowException, match=reason):
            wire.client.get_schema(descriptor, options)


# --- the same catalog over HTTP (REQ-128) -----------------------------------------------------------


async def _http_listing(monkeypatch, state, role: str | None) -> dict[tuple[str, ...], pa.Schema]:
    """path → Arrow schema, as GET /data/catalog answers a request running as ``role``."""
    import base64

    import provisa.api.app as app_mod
    from provisa.api.data.endpoint_dev import catalog_endpoint

    monkeypatch.setattr(app_mod, "state", state)
    request = types.SimpleNamespace(state=types.SimpleNamespace(role=role))
    body = await catalog_endpoint(request)  # type: ignore[arg-type]
    return {
        tuple(entry["path"]): pa.ipc.read_schema(pa.py_buffer(base64.b64decode(entry["schema"])))
        for entry in body["tables"]
    }


def _flight_tables(client, options) -> dict[tuple[str, ...], pa.Schema]:
    """path → Arrow schema of the TABLES the Flight listing gives (commands and metrics, at
    longer or prefixed paths, are not catalog tables)."""
    listed = {
        tuple(p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path): info.schema
        for info in client.list_flights(b"", options)
    }
    return {path: schema for path, schema in listed.items() if path in {_ORDERS, _STAFF}}


@pytest.mark.parametrize(
    "options, role",
    [
        (("sam", None), "seller"),
        (("both", "hr_reader"), "hr_reader"),
        (("both", "seller,hr_reader"), _META),
    ],
)
async def test_the_http_catalog_is_the_flight_listing_field_for_field(
    wire, monkeypatch, options, role
):
    """One builder, two transports: the same tables, and for each the same fields — names,
    types, nullability and the description / key metadata they carry."""
    over_flight = _flight_tables(wire.client, _options(*options))
    over_http = await _http_listing(monkeypatch, wire.state, role)
    assert over_flight, "the role is served something"
    assert set(over_http) == set(over_flight)
    for path, schema in over_flight.items():
        assert over_http[path].equals(schema, check_metadata=True), path
        assert [f.metadata for f in over_http[path]] == [f.metadata for f in schema], path


async def test_the_http_catalog_lists_the_whole_catalog_when_nobody_is_authenticated(monkeypatch):
    """As the Flight listing does: with no auth provider there is no role to narrow by."""
    srv = _server(monkeypatch, auth=False)
    listed = await _http_listing(monkeypatch, srv._state, "seller")
    assert set(listed) == {_ORDERS, _STAFF}
    assert [f.name for f in listed[_ORDERS]] == list(_TABLES[1][2])


async def test_the_http_catalog_needs_the_role_the_request_runs_as(monkeypatch):
    from provisa.api.errors import ApiError

    srv = _server(monkeypatch)
    with pytest.raises(ApiError) as refused:
        await _http_listing(monkeypatch, srv._state, None)
    assert refused.value.status_code == 422


def test_the_catalog_route_is_schema_metadata_in_high_security_mode():
    from provisa.security import high_security

    assert "/data/catalog" in high_security._METADATA_PREFIXES
