# Copyright (c) 2026 Kenneth Stott
# Canary: f6d2c7de-7fe7-44ee-9839-bf9a487092ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a caller discovers over gRPC is what its role is served, as its model stands now
(REQ-045, REQ-051, REQ-1266).

One server, any number of organisations and roles. Server reflection describes a caller the
service of the role it runs as, in its organisation -- the proto that role downloads, the tables
and columns its GraphQL schema has. A call is decoded and answered with its organisation's own
messages. A change to a role or to a model is described and served by the next call."""

# Requirements: REQ-045, REQ-051, REQ-1266

from __future__ import annotations


import grpc
import pytest
from google.protobuf import descriptor_pb2
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

from provisa.core.request_context import current_org
from provisa.grpc import server as grpc_server
from provisa.grpc.descriptor import OrgDescriptor, bound_descriptor
from tests.port_lease import lease_port

pytestmark = pytest.mark.unit


def _proto(table: str, column: str) -> str:
    return f"""syntax = "proto3";
package provisa.v1;
message {table} {{ int32 {column} = 1; }}
message {table}Request {{ int32 limit = 1; }}
service ProvisaService {{ rpc Query{table} ({table}Request) returns (stream {table}); }}
"""


class _Deployment:
    """A server process's state: each organisation's service is its own, read as the bound
    organisation's (what AppState.wire_proto and .grpc_descriptor are)."""

    multitenancy = True
    security_high = False
    # Each organisation declares no derived role: an unsecured call's role resolves to itself
    # (derived_roles.unsecured_principal, REQ-1952).
    settings_overrides: dict = {"derived_roles": []}

    def __init__(self, protos: dict[str, str], roles: dict[str, dict[str, str]] | None = None):
        self.protos = protos  # organisation -> the proto its wire is encoded with
        self.roles = roles or {}  # organisation -> role -> the proto that role is served
        self._compiled: dict[str, OrgDescriptor] = {}
        self._role_compiled: dict[str, dict] = {}

    @property
    def proto_files(self) -> dict[str, str]:
        return self.roles.get(current_org.get(), {})

    @property
    def grpc_role_descriptors(self) -> dict:
        return self._role_compiled.setdefault(current_org.get(), {})

    @property
    def wire_proto(self) -> str | None:
        return self.protos.get(current_org.get())

    @property
    def grpc_descriptor(self):
        return self._compiled.get(current_org.get())

    @grpc_descriptor.setter
    def grpc_descriptor(self, value) -> None:
        self._compiled[current_org.get()] = value


@pytest.fixture
def deployment(monkeypatch):
    """A real server on a loopback port over two organisations with different models. Each
    table's query handler is stood in for: it answers one row carrying the request's limit, in
    the bound organisation's own message."""
    state = _Deployment(
        {"acme": _proto("Orders", "id"), "globex": _proto("Refunds", "ref")},
        {
            "acme": {"analyst": _proto("Orders", "id")},
            "globex": {"analyst": _proto("Refunds", "ref")},
        },
    )

    async def _built(_org_id):
        return None

    async def _rows(self, request, context, type_name, role_id):
        message = getattr(self._pb2, type_name)
        (field,) = message.DESCRIPTOR.fields
        yield message(**{field.name: request.limit})

    monkeypatch.setattr("provisa.api.app.ensure_serving_runtime", _built)
    monkeypatch.setattr("provisa.grpc.auth.auth_active", lambda _state: False)
    monkeypatch.setattr(grpc_server, "_allow_unsecured_reflection", lambda: True)
    monkeypatch.setattr(grpc_server.ProvisaServicer, "_handle_query_bound", _rows)
    monkeypatch.setattr(grpc_server.ProvisaServicer, "_meter_msg", lambda self, msg: msg)
    # A leased port: below the kernel's ephemeral range, so no other bind(0) or outbound
    # connection on the machine is handed it (tests/port_lease.py).
    port = lease_port()
    server = grpc_server.start_grpc_server(port, state)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield state, channel
    finally:
        channel.close()
        server.stop(grace=0).wait()


def _as(org: str, role: str = "analyst") -> tuple:
    return (("x-provisa-org", org), ("x-provisa-role", role))


def _reflected(channel, metadata: tuple) -> descriptor_pb2.FileDescriptorProto:
    stub = reflection_pb2_grpc.ServerReflectionStub(channel)
    request = reflection_pb2.ServerReflectionRequest(
        file_containing_symbol="provisa.v1.ProvisaService"
    )
    (response,) = stub.ServerReflectionInfo(iter([request]), metadata=metadata)
    # The file, and after it the files it imports (the well-known types a service names).
    files = [
        descriptor_pb2.FileDescriptorProto.FromString(raw)
        for raw in response.file_descriptor_response.file_descriptor_proto
    ]
    (file,) = [f for f in files if f.package == "provisa.v1"]
    return file


def _described(channel, org: str, role: str = "analyst") -> descriptor_pb2.FileDescriptorProto:
    """The Provisa service as reflection describes it to a caller of ``org`` acting as ``role``."""
    return _reflected(channel, _as(org, role))


def _query(channel, org: str, table: str, limit: int) -> list[bytes]:
    call = channel.unary_stream(
        f"/provisa.v1.ProvisaService/Query{table}",
        request_serializer=lambda raw: raw,
        response_deserializer=lambda raw: raw,
    )
    request = bound_for(org, table)(limit=limit).SerializeToString()
    return list(call(request, metadata=_as(org)))


def bound_for(org: str, table: str):
    """The request class of ``table`` as ``org``'s own client would have generated it."""
    protos = {"acme": _proto("Orders", "id"), "globex": _proto("Refunds", "ref")}
    return getattr(OrgDescriptor(protos.get(org) or _proto(table, "id")), f"{table}Request")


def test_two_organisations_are_never_described_each_others_service(deployment):
    _state, channel = deployment
    acme, globex = _described(channel, "acme"), _described(channel, "globex")
    assert [m.name for m in acme.message_type] == ["Orders", "OrdersRequest"]
    assert [m.name for m in globex.message_type] == ["Refunds", "RefundsRequest"]
    assert [m.name for m in acme.service[0].method] == ["QueryOrders"]
    assert [m.name for m in globex.service[0].method] == ["QueryRefunds"]


def test_a_call_is_decoded_and_answered_with_its_organisations_own_messages(deployment):
    state, channel = deployment
    (row,) = _query(channel, "acme", "Orders", 7)
    assert OrgDescriptor(state.protos["acme"]).Orders.FromString(row).id == 7
    (row,) = _query(channel, "globex", "Refunds", 9)
    assert OrgDescriptor(state.protos["globex"]).Refunds.FromString(row).ref == 9


def test_a_method_of_another_organisations_service_is_not_this_callers(deployment):
    _state, channel = deployment
    call = channel.unary_stream(
        "/provisa.v1.ProvisaService/QueryRefunds",
        request_serializer=lambda raw: raw,
        response_deserializer=lambda raw: raw,
    )
    with pytest.raises(grpc.RpcError) as refused:
        list(call(b"", metadata=_as("acme")))
    assert refused.value.code() == grpc.StatusCode.UNIMPLEMENTED


def test_a_call_that_names_no_organisation_is_described_and_served_nothing(deployment):
    _state, channel = deployment
    stub = reflection_pb2_grpc.ServerReflectionStub(channel)
    request = reflection_pb2.ServerReflectionRequest(
        file_containing_symbol="provisa.v1.ProvisaService"
    )
    with pytest.raises(grpc.RpcError) as refused:
        list(stub.ServerReflectionInfo(iter([request])))
    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
    call = channel.unary_stream(
        "/provisa.v1.ProvisaService/QueryOrders",
        request_serializer=lambda raw: raw,
        response_deserializer=lambda raw: raw,
    )
    with pytest.raises(grpc.RpcError) as refused:
        list(call(b"", metadata=(("x-provisa-role", "analyst"),)))
    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED


def test_a_change_to_an_organisations_model_is_served_by_the_next_call(deployment):
    state, channel = deployment
    assert [m.name for m in _described(channel, "acme").service[0].method] == ["QueryOrders"]
    # Its schema build writes the new service, for the wire and for the role: a table added.
    state.protos["acme"] = state.roles["acme"]["analyst"] = _proto("Orders", "id").replace(
        "service ProvisaService {",
        "message Returns { int32 id = 1; }\nmessage ReturnsRequest { int32 limit = 1; }\n"
        "service ProvisaService { rpc QueryReturns (ReturnsRequest) returns (stream Returns);",
    )
    described = _described(channel, "acme")
    assert sorted(m.name for m in described.service[0].method) == ["QueryOrders", "QueryReturns"]
    call = channel.unary_stream(
        "/provisa.v1.ProvisaService/QueryReturns",
        request_serializer=lambda raw: raw,
        response_deserializer=lambda raw: raw,
    )
    compiled = OrgDescriptor(state.protos["acme"])
    (row,) = call(compiled.ReturnsRequest(limit=4).SerializeToString(), metadata=_as("acme"))
    assert compiled.Returns.FromString(row).id == 4
    # The other organisation's service is as it was.
    assert [m.name for m in _described(channel, "globex").service[0].method] == ["QueryRefunds"]


def test_a_service_is_compiled_once_for_the_text_it_was_written_as():
    state = _Deployment({"acme": _proto("Orders", "id")})
    token = current_org.set("acme")
    try:
        first = bound_descriptor(state)
        assert bound_descriptor(state) is first
        state.protos["acme"] = _proto("Orders", "number")
        assert bound_descriptor(state) is not first
    finally:
        current_org.reset(token)
    other = current_org.set("nobody")
    try:
        assert bound_descriptor(state) is None
    finally:
        current_org.reset(other)


_TIMESTAMP_ROW = '''
from datetime import datetime, timezone

from provisa.grpc.descriptor import OrgDescriptor

orders = OrgDescriptor("""syntax = "proto3";
package provisa.v1;
import "google/protobuf/timestamp.proto";
message Orders { int32 id = 1; google.protobuf.Timestamp created_at = 2; }
message OrdersRequest { int32 limit = 1; }
service ProvisaService { rpc QueryOrders (OrdersRequest) returns (stream Orders); }
""").Orders
from provisa.grpc.server import _proto_value  # after the organisation's service is compiled

when = datetime(2026, 10, 12, 3, 4, 5, tzinfo=timezone.utc)
field = orders.DESCRIPTOR.fields_by_name["created_at"]
row = orders(id=1, created_at=_proto_value(field, when)).SerializeToString()
assert orders.FromString(row).created_at.ToDatetime(tzinfo=timezone.utc) == when
'''


def test_a_timestamp_column_is_answered_with_the_organisations_own_timestamp():
    """A row's datetime is set into the Timestamp of the organisation's own service, whose pool
    is not the default one (a DIRECT read of a TIMESTAMP column). Run in a fresh interpreter: an
    organisation's service compiled before the server module is loaded is the order a running
    server meets (a model edit, a new organisation), and the one upb failed in with
    "error return without exception set" (suite 38198316000, core 1/6)."""
    import subprocess
    import sys
    from pathlib import Path

    done = subprocess.run(
        [sys.executable, "-c", _TIMESTAMP_ROW],
        cwd=Path(__file__).resolve().parents[2],  # this checkout's provisa
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert done.returncode == 0, done.stderr


# --- what a role is described is what that role is served -----------------------------------------


def test_a_role_is_described_its_own_proto_not_the_organisations_union(deployment):
    """The wire carries every table of the organisation; a role is described its own."""
    state, channel = deployment
    union = _proto("Orders", "id").replace(
        "service ProvisaService {",
        "message Ledger { int32 id = 1; }\nmessage LedgerRequest { int32 limit = 1; }\n"
        "service ProvisaService { rpc QueryLedger (LedgerRequest) returns (stream Ledger);",
    )
    state.protos["acme"] = union
    state.roles["acme"] = {"analyst": _proto("Orders", "id"), "auditor": union}
    assert [m.name for m in _described(channel, "acme").message_type] == [
        "Orders",
        "OrdersRequest",
    ]
    assert sorted(m.name for m in _described(channel, "acme", "auditor").service[0].method) == [
        "QueryLedger",
        "QueryOrders",
    ]


def test_a_role_with_no_service_and_a_call_naming_no_role_are_described_nothing(deployment):
    _state, channel = deployment
    for metadata, status in (
        (_as("acme", "nobody"), grpc.StatusCode.NOT_FOUND),
        ((("x-provisa-org", "acme"),), grpc.StatusCode.UNAUTHENTICATED),
    ):
        with pytest.raises(grpc.RpcError) as refused:
            _reflected(channel, metadata)
        assert refused.value.code() == status


def test_a_change_to_a_role_is_described_by_the_next_call(deployment, monkeypatch):
    """A meta-role is a role built from the roles its caller holds; when its members change its
    surface is rebuilt, its proto with it, and the next reflection call describes that."""
    from provisa.security import meta_role

    state, channel = deployment
    # The caller names a and b; it acts as their meta-role (REQ-1952), whose surface this
    # deployment stands in for below.
    monkeypatch.setattr(meta_role, "ensure_meta_role", lambda _s, m: meta_role.meta_role_id(m))
    state.roles["acme"]["meta:a+b"] = _proto("Orders", "id")
    assert [m.name for m in _described(channel, "acme", "a,b").message_type] == [
        "Orders",
        "OrdersRequest",
    ]
    state.roles["acme"]["meta:a+b"] = _proto("Orders", "id").replace(
        "message Orders { int32 id = 1; }", "message Orders { int32 id = 1; string note = 2; }"
    )
    orders = _described(channel, "acme", "a,b").message_type[0]
    assert [f.name for f in orders.field] == ["id", "note"]
    # Another role of the same organisation is described as before.
    assert [f.name for f in _described(channel, "acme").message_type[0].field] == ["id"]


@pytest.fixture
def built(monkeypatch):
    """A real server over the real state after the real role build: two roles of one
    organisation, each reaching one domain."""
    from tests.unit.test_empty_domain_access_reaches_nothing import _built_roles

    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")
    state = _built_roles(
        monkeypatch,
        [
            {
                "id": "sales_reader",
                "description": "Test role.",
                "capabilities": ["usage"],
                "domain_access": ["sales"],
            },
            {
                "id": "finance_reader",
                "description": "Test role.",
                "capabilities": ["usage"],
                "domain_access": ["finance"],
            },
        ],
    )
    monkeypatch.setattr("provisa.grpc.auth.auth_active", lambda _state: False)
    monkeypatch.setattr(grpc_server, "_allow_unsecured_reflection", lambda: True)
    monkeypatch.setattr(state, "security_high", False, raising=False)
    # A leased port: below the kernel's ephemeral range, so no other bind(0) or outbound
    # connection on the machine is handed it (tests/port_lease.py).
    port = lease_port()
    server = grpc_server.start_grpc_server(port, state)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield state, channel
    finally:
        channel.close()
        server.stop(grace=0).wait()


def _table_fields(schema) -> list[str]:
    """The root fields a role's Query type serves its tables through: every field but the
    semantic-address resolver every schema carries (REQ-1365), which reads no one table."""
    from provisa.compiler.schema_gen import RESOLVE_FIELD

    return sorted(f for f in schema.query_type.fields if f != RESOLVE_FIELD)


def _service(file: descriptor_pb2.FileDescriptorProto) -> tuple[set[str], dict[str, list[str]]]:
    methods = {m.name for s in file.service for m in s.method}
    return methods, {m.name: [f.name for f in m.field] for m in file.message_type}


@pytest.mark.parametrize(
    ("role", "has", "has_not"),
    [("sales_reader", "Orders", "Ledger"), ("finance_reader", "Ledger", "Orders")],
)
def test_reflection_lists_exactly_what_the_roles_own_proto_lists(built, role, has, has_not):
    """The role's proto is what its build made for it beside its GraphQL schema, and what it
    downloads (GET /data/proto/{role}). Reflection describes that file: every method and every
    message field of it, and no table of a domain the role does not reach."""
    from provisa.grpc.schema_gen import compile_descriptor_set

    state, channel = built
    described = _reflected(channel, (("x-provisa-role", role),))
    files = descriptor_pb2.FileDescriptorSet.FromString(
        compile_descriptor_set(state.proto_files[role])
    )
    (own,) = [f for f in files.file if f.package == "provisa.v1"]
    assert _service(described) == _service(own)
    methods, messages = _service(described)
    assert f"Query{has}" in methods and has in messages
    assert f"Query{has_not}" not in methods and has_not not in messages
    # The table its GraphQL schema offers is the one its service queries.
    (field,) = _table_fields(state.schemas[role])
    assert f"Query{field.capitalize()}" in methods
    # The wire, which no caller is described, carries both.
    assert {"QueryOrders", "QueryLedger"} <= _service(
        [
            f
            for f in descriptor_pb2.FileDescriptorSet.FromString(
                compile_descriptor_set(state.wire_proto)
            ).file
            if f.package == "provisa.v1"
        ][0]
    )[0]


def test_a_meta_role_is_described_what_its_members_are_served_together(built, monkeypatch):
    """A caller holding two roles acts as their meta-role, built on first use with a surface of
    its own -- GraphQL schema and proto alike. It is described that surface with no restart."""
    from provisa.security.meta_role import ensure_meta_role

    state, channel = built
    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(state._active_runtime(), name, {}, raising=False)
    meta = ensure_meta_role(state, ["sales_reader", "finance_reader"])
    # The caller names the roles it holds; the server acts as their meta-role (REQ-1952).
    as_set = (("x-provisa-role", "sales_reader,finance_reader"),)
    methods, messages = _service(_reflected(channel, as_set))
    assert {"QueryOrders", "QueryLedger"} <= methods
    assert _table_fields(state.schemas[meta]) == ["ledger", "orders"]
    # Each member alone is still described its own.
    alone, _ = _service(_reflected(channel, (("x-provisa-role", "sales_reader"),)))
    assert "QueryLedger" not in alone


# --- a role's service is what it may call, and its own proto reads the wire -----------------------


def _wider(proto: str) -> str:
    """``proto`` with a column and a table only a wider role is served."""
    return proto.replace(
        "message Orders { int32 id = 1; }", "message Orders { int32 id = 1; int32 cost = 2; }"
    ).replace(
        "service ProvisaService {",
        "message Ledger { int32 id = 1; }\nmessage LedgerRequest { int32 limit = 1; }\n"
        "service ProvisaService { rpc QueryLedger (LedgerRequest) returns (stream Ledger);",
    )


def test_a_method_only_another_roles_service_has_is_unimplemented_for_this_role(deployment):
    state, channel = deployment
    union = _wider(_proto("Orders", "id"))
    state.protos["acme"] = union
    state.roles["acme"] = {"analyst": _proto("Orders", "id"), "auditor": union}
    compiled = OrgDescriptor(union)
    call = channel.unary_stream(
        "/provisa.v1.ProvisaService/QueryLedger",
        request_serializer=lambda raw: raw,
        response_deserializer=lambda raw: raw,
    )
    request = compiled.LedgerRequest(limit=2).SerializeToString()
    (row,) = call(request, metadata=_as("acme", "auditor"))  # its service has the method
    assert compiled.Ledger.FromString(row).id == 2
    with pytest.raises(grpc.RpcError) as refused:
        list(call(request, metadata=_as("acme", "analyst")))  # and this role's has not
    assert refused.value.code() == grpc.StatusCode.UNIMPLEMENTED


def test_a_client_built_from_its_roles_proto_round_trips_against_the_wire(deployment, monkeypatch):
    """Field numbers are shared across every role's proto and the wire's: a role's proto is a
    subset that reads the same bytes. The request a role's client encodes is decoded by the
    server, and the answer -- encoded with the organisation's wire message, which carries a
    column this role's proto does not name -- is decoded by the role's client."""
    state, channel = deployment
    state.protos["acme"] = _wider(_proto("Orders", "id"))

    async def _rows(self, request, context, type_name, role_id):
        yield self._pb2.Orders(id=request.limit, cost=99)  # the wire's own message

    monkeypatch.setattr(grpc_server.ProvisaServicer, "_handle_query_bound", _rows)
    client = OrgDescriptor(state.roles["acme"]["analyst"])  # what the role downloaded
    call = channel.unary_stream(
        "/provisa.v1.ProvisaService/QueryOrders",
        request_serializer=lambda message: message.SerializeToString(),
        response_deserializer=client.Orders.FromString,
    )
    (row,) = call(client.OrdersRequest(limit=5), metadata=_as("acme"))
    assert row.id == 5
    assert [f.name for f in row.DESCRIPTOR.fields] == ["id"]  # its proto names no other column


def test_a_model_edit_that_rebuilds_a_meta_role_is_described_by_the_next_call(built, monkeypatch):
    """A caller is acting as a meta-role when the model changes: a table one of its members
    reads is given another name. The rebuild rebuilds the members, then the meta-role from them
    (``rebuild_meta_roles``), its proto with its surface -- and the next reflection call, on
    the same running server, describes the table by its new name."""
    import tests.unit.test_empty_domain_access_reaches_nothing as model
    from provisa.security.meta_role import ensure_meta_role, rebuild_meta_roles

    state, channel = built
    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(state._active_runtime(), name, {}, raising=False)
    roles = [dict(state.roles[r]) for r in ("sales_reader", "finance_reader")]
    meta = ensure_meta_role(state, ["sales_reader", "finance_reader"])
    as_meta = (("x-provisa-role", "sales_reader,finance_reader"),)  # acts as ``meta``
    before, _messages = _service(_reflected(channel, as_meta))
    assert {"QueryOrders", "QueryLedger"} <= before and "QueryPurchases" not in before

    # The model edit: ``orders`` is served as ``purchases``.
    tables = model._tables()
    (orders,) = [t for t in tables if t["table_name"] == "orders"]
    orders["alias"] = "purchases"
    monkeypatch.setattr(model, "_tables", lambda: tables)
    model._built_roles(monkeypatch, roles)
    rebuild_meta_roles(state, {r["id"] for r in roles})

    after, messages = _service(_reflected(channel, as_meta))
    assert "QueryPurchases" in after and "QueryOrders" not in after
    assert "Purchases" in messages and "QueryLedger" in after
    assert _table_fields(state.schemas[meta]) == ["ledger", "purchases"]
    # ... and so is the member that reads the table, while the other member is as it was.
    member, _ = _service(_reflected(channel, (("x-provisa-role", "sales_reader"),)))
    assert "QueryPurchases" in member and "QueryOrders" not in member
    other, _ = _service(_reflected(channel, (("x-provisa-role", "finance_reader"),)))
    assert "QueryLedger" in other and "QueryPurchases" not in other


# --- a role's service calls what its GraphQL root offers -----------------------------------------


@pytest.fixture
def meta_reach(monkeypatch):
    """The real role build: one role that reaches only ``sales``, one that reaches the meta
    (catalog) domain as well."""
    from tests.unit.test_empty_domain_access_reaches_nothing import _built_roles

    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")
    return _built_roles(
        monkeypatch,
        [
            {
                "id": "sales_reader",
                "description": "Test role.",
                "capabilities": ["usage"],
                "domain_access": ["sales"],
            },
            {"id": "cataloguer", "capabilities": ["usage"], "domain_access": ["sales", "meta"]},
        ],
    )


def _rpcs(proto: str) -> set[str]:
    import re

    return set(re.findall(r"rpc (\w+)\(", proto))


def test_a_table_only_traversed_to_has_no_rpc_as_it_has_no_root_field(meta_reach):
    """Every role's surface holds the catalog tables, to be read through each type's ``_meta``.
    A role that does not reach the catalog domain has no root field for them in its GraphQL
    Query type -- and no RPC for them in its service, whose Query message already says so."""
    state = meta_reach
    without, with_reach = state.proto_files["sales_reader"], state.proto_files["cataloguer"]
    assert _table_fields(state.schemas["sales_reader"]) == ["orders"]
    assert {r for r in _rpcs(without) if "Meta" in r} == set()
    assert {"QueryOrders", "QueryOrdersBatch"} <= _rpcs(without)
    assert "message RegisteredTablesMeta {" in without, "its message stays: it is traversed to"

    roots = sorted(state.schemas["cataloguer"].query_type.fields)
    assert "registeredTablesMeta" in roots and "orders" in roots
    assert {"QueryRegisteredTablesMeta", "QueryRegisteredTablesMetaBatch"} <= _rpcs(with_reach)
    # The wire every role's bytes are encoded with has the method, for the role that has it.
    assert "QueryRegisteredTablesMeta" in _rpcs(state.wire_proto)


def test_the_generic_command_rpc_is_every_roles(meta_reach):
    """REQ-1156: commands are invocable on every surface; the generic entry is always there."""
    for role in ("sales_reader", "cataloguer"):
        assert "CallCommand" in _rpcs(meta_reach.proto_files[role])


def test_every_roles_service_resolves_a_semantic_address(meta_reach):
    """REQ-1365, REQ-1929: resolve is in every role's GraphQL schema, so its RPC is in every
    role's service, generated from the same rule."""
    for role in ("sales_reader", "cataloguer"):
        assert "resolve" in meta_reach.schemas[role].query_type.fields
        assert "Resolve" in _rpcs(meta_reach.proto_files[role])


def test_every_roles_resolve_takes_as_of_and_as_recorded(meta_reach):
    """REQ-1163: the RPC's request carries what the GraphQL field's arguments do."""
    for role in ("sales_reader", "cataloguer"):
        proto = meta_reach.proto_files[role]
        assert "string as_of = 2;" in proto and "string as_recorded = 3;" in proto


_RESOLVE = """syntax = "proto3";
package provisa.v1;
message Orders { int32 id = 1; }
message OrdersRequest { int32 limit = 1; }
message ResolveAddressRequest { string uri = 1; string as_of = 2; string as_recorded = 3; }
message ResolveAddressResponse { string result_json = 1; }
service ProvisaService {
  rpc QueryOrders (OrdersRequest) returns (stream Orders);
  rpc Resolve (ResolveAddressRequest) returns (ResolveAddressResponse);
}
"""


def test_a_resolve_call_is_answered_by_the_one_resolve_function(deployment, monkeypatch):
    """The RPC hands the address, the role and the bound organisation to the resolve every other
    surface calls, and answers what it found as JSON. An as_of or as_recorded the call does not give is none."""
    import json

    from provisa.api.data import resolve as resolve_mod

    state, channel = deployment
    state.protos["acme"] = _RESOLVE
    state.roles["acme"]["analyst"] = _RESOLVE
    calls: list[tuple] = []

    async def _resolve(_state, role_id, uri, *, org_id, as_of=None, as_recorded=None):
        calls.append((role_id, uri, org_id, as_of, as_recorded))
        return {"uri": uri, "row": {"id": 7}}

    monkeypatch.setattr(resolve_mod, "resolve", _resolve)
    messages = OrgDescriptor(_RESOLVE)
    call = channel.unary_unary(
        "/provisa.v1.ProvisaService/Resolve",
        request_serializer=lambda message: message.SerializeToString(),
        response_deserializer=messages.ResolveAddressResponse.FromString,
    )
    uri = "provisa://acme/sales/tables/orders/pk=7"
    answer = call(messages.ResolveAddressRequest(uri=uri), metadata=_as("acme"))
    assert json.loads(answer.result_json) == {"uri": uri, "row": {"id": 7}}
    assert calls == [("analyst", uri, "acme", None, None)]
