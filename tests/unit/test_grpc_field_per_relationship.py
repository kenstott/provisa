# Copyright (c) 2026 Kenneth Stott
# Canary: 059301d4-4bea-47a4-99ea-767585ad576f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: each relationship is its own field of the gRPC ``{Type}`` message, named by its GraphQL
relationship name, so a ``read_mask`` path names the relationship itself and every relationship to
one table is readable; field numbers of fields that already exist stay as they were (REQ-1929)."""

# Requirements: REQ-1929, REQ-1903

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

# The proxy binds the pipeline's functions when it is imported. Imported first under _run's
# patches, it would keep their mocks for every later test in the process.
import provisa.api.data.endpoint_grpc_proxy  # noqa: F401
from provisa.executor.result import QueryResult
from tests.grpc_model import serving_state


def _schema_input():
    """orders has two many-to-one relationships to customers: ``customer`` (by customer_id) and
    ``referrer`` (by referrer_id, a GraphQL alias); customers has ``orders`` (one-to-many)."""
    from provisa.compiler import naming as _naming
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput

    def col(name, data_type):
        return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)

    def table(tid, name, cols):
        return {
            "id": tid,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": name,
            "columns": [{"column_name": c, "visible_to": ["admin"]} for c in cols],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }

    _naming.configure(gql="snake")
    return SchemaInput(
        tables=[
            table(1, "customers", ["id", "name"]),
            table(2, "orders", ["id", "customer_id", "referrer_id"]),
        ],
        relationships=[
            {
                "id": "r-orders",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "id",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
            },
            {
                "id": "r-customer",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
            {
                "id": "r-referrer",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "referrer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
                "graphql_alias": "referrer",
            },
        ],
        column_types={
            1: [col("id", "integer"), col("name", "text")],
            2: [col("id", "integer"), col("customer_id", "integer"), col("referrer_id", "integer")],
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )


def _model():
    from provisa.compiler.context import build_context
    from provisa.compiler.schema_gen import generate_schema

    si = _schema_input()
    return generate_schema(si), build_context(si)


def _proto(allocator=None) -> str:
    from provisa.grpc.proto_gen import generate_proto

    return generate_proto(_schema_input(), allocator, authoritative=allocator is not None)


def _fields(descriptor) -> dict[str, tuple[int, str | None, bool]]:
    """name -> (number, message type name or None, repeated) of a message's fields."""
    return {
        f.name: (f.number, f.message_type.name if f.message_type else None, f.is_repeated)
        for f in descriptor.fields
    }


class TestProtoDeclaresEachRelationship:
    def test_both_relationships_to_one_table_are_fields_named_by_relationship(self):
        from provisa.grpc.descriptor import OrgDescriptor

        orders = _fields(OrgDescriptor(_proto()).Orders.DESCRIPTOR)
        assert orders["customer"][1:] == ("Customers", False)
        assert orders["referrer"][1:] == ("Customers", False)
        assert "customers" not in orders

    def test_one_to_many_is_a_repeated_field_named_by_relationship(self):
        from provisa.grpc.descriptor import OrgDescriptor

        customers = _fields(OrgDescriptor(_proto()).Customers.DESCRIPTOR)
        assert customers["orders"][1:] == ("Orders", True)

    def test_existing_field_numbers_are_kept_and_new_fields_numbered_after_them(self):
        """A build before this change numbered Orders' columns and one relation field named for
        the related table (``customers``). Those numbers stay with their fields; the
        relationship fields take numbers after every number used, never a retired one."""
        from provisa.grpc.descriptor import OrgDescriptor
        from provisa.grpc.field_numbering import FieldNumberAllocator

        before = [
            ("2", "row", "customer_id", 1, False),
            ("2", "row", "id", 2, False),
            ("2", "row", "referrer_id", 3, False),
            ("2", "row", "customers", 4, False),
            ("1", "row", "id", 1, False),
            ("1", "row", "name", 2, False),
            ("1", "row", "orders", 3, False),
        ]
        allocator = FieldNumberAllocator(before)
        pb2 = OrgDescriptor(_proto(allocator))
        orders = _fields(pb2.Orders.DESCRIPTOR)
        customers = _fields(pb2.Customers.DESCRIPTOR)

        assert {n: orders[n][0] for n in ("customer_id", "id", "referrer_id")} == {
            "customer_id": 1,
            "id": 2,
            "referrer_id": 3,
        }
        assert {orders["customer"][0], orders["referrer"][0]} == {5, 6}
        assert {n: customers[n][0] for n in ("id", "name", "orders")} == {
            "id": 1,
            "name": 2,
            "orders": 3,
        }
        # The relation field no longer declared is retired: its number is never handed out again.
        retired = {(t, n): (num, rm) for t, _ns, n, num, rm in allocator.pending_rows()}
        assert retired[("2", "customers")] == (4, True)

    def test_numbers_are_stable_across_rebuilds(self):
        """A rebuild from the persisted numbers (a restart) declares the same message."""
        from provisa.grpc.field_numbering import FieldNumberAllocator

        first = FieldNumberAllocator([])
        text = _proto(first)
        assert _proto(FieldNumberAllocator(first.pending_rows())) == text


class TestReadMaskNamesTheRelationship:
    def test_each_relationship_path_lowers_to_its_own_relationship(self):
        from provisa.grpc.query_ir import grpc_table_to_graphql, resolve_read_mask

        schema, ctx = _model()
        mask = resolve_read_mask(ctx, "Orders", ["id", "customer.name", "referrer.name"])
        assert mask is not None
        assert [(r.field, r.graphql_field) for r in mask.relations] == [
            ("customer", "customer"),
            ("referrer", "referrer"),
        ]
        lowered = grpc_table_to_graphql(ctx, schema, "Orders", 10, None, mask)
        assert lowered is not None
        assert "customer { name }" in lowered[0]
        assert "referrer { name }" in lowered[0]


# --- The servicer and the proxy answer each relationship's own rows --------------------------------


def _compiled_columns(model, paths):
    from provisa.compiler.parser import parse_query
    from provisa.compiler.sql_gen import compile_query
    from provisa.grpc.query_ir import grpc_table_to_graphql, resolve_read_mask

    schema, ctx = model
    lowered = grpc_table_to_graphql(
        ctx, schema, "Orders", 10, None, resolve_read_mask(ctx, "Orders", paths)
    )
    assert lowered is not None
    (compiled,) = compile_query(parse_query(schema, lowered[0], {}, ctx=ctx), ctx, {})
    return [c.column for c in compiled.columns]


_PATHS = ["id", "customer.name", "referrer.name"]
_ROWS = [(7, json.dumps({"name": "Ann"}), json.dumps({"name": "Bob"}))]


@pytest.mark.asyncio
async def test_servicer_fills_each_relationship_with_its_own_row():
    from provisa.grpc.descriptor import OrgDescriptor
    from provisa.grpc.server import ProvisaServicer
    from provisa.transpiler.router import Route

    model = _model()
    pb2 = OrgDescriptor(_proto())
    servicer = ProvisaServicer(serving_state(model), pb2)
    servicer._emit_trailing_metadata = MagicMock()
    servicer._meter_msg = lambda msg: msg
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata = MagicMock(return_value=[("x-provisa-role", "admin")])
    request = SimpleNamespace(
        limit=10,
        offset=0,
        batch_rows=0,
        read_mask=SimpleNamespace(paths=_PATHS),
        HasField=lambda _name: False,
        DESCRIPTOR=SimpleNamespace(fields_by_name={}),
    )
    result = QueryResult(rows=_ROWS, column_names=_compiled_columns(model, _PATHS))
    plan = SimpleNamespace(
        route=Route.CACHE, source_id=None, cache_hit=None, warnings=[], materialize=None
    )
    with (
        patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled",
            new_callable=AsyncMock,
            return_value=plan,
        ),
        patch(
            "provisa.pgwire._pipeline._execute_plan", new_callable=AsyncMock, return_value=result
        ),
    ):
        sent = [m async for m in servicer._handle_query(request, context, "Orders")]

    context.abort.assert_not_awaited()
    (message,) = [pb2.Orders.FromString(m) for m in sent]
    assert message.id == 7
    assert message.customer.name == "Ann"
    assert message.referrer.name == "Bob"


class _ProxyRequest:
    """The Starlette request ``grpc_proxy`` reads: a JSON body, headers, the auth layer's state."""

    def __init__(self, body: dict, headers: dict[str, str]) -> None:
        self._body = body
        self.headers = headers
        self.state = SimpleNamespace()

    async def json(self) -> dict:
        return self._body


@pytest.mark.asyncio
async def test_proxy_nests_each_relationship_under_its_own_name():
    from provisa.api.data import endpoint_grpc_proxy as proxy

    model = _model()
    result = QueryResult(rows=_ROWS, column_names=_compiled_columns(model, _PATHS))
    with (
        patch("provisa.api.app.state", serving_state(model)),
        patch.object(
            proxy,
            "_govern_and_route_compiled",
            AsyncMock(return_value=SimpleNamespace(warnings=[])),
        ),
        patch.object(proxy, "_execute_plan", AsyncMock(return_value=result)),
    ):
        response = await proxy.grpc_proxy(
            "Orders",
            _ProxyRequest({"read_mask": {"paths": _PATHS}}, {"x-provisa-role": "admin"}),  # type: ignore[arg-type]
        )

    assert json.loads(bytes(response.body)) == [
        {"id": 7, "customer": {"name": "Ann"}, "referrer": {"name": "Bob"}}
    ]


def test_reflection_describes_each_relationship_field():
    """Server reflection, over the role's compiled pool as the servicer serves it, lists both
    relationship fields of Orders."""
    from google.protobuf import descriptor_pb2
    from grpc_reflection.v1alpha import reflection, reflection_pb2

    from provisa.grpc.descriptor import SERVICE_NAME, OrgDescriptor

    pool = OrgDescriptor(_proto()).pool
    servicer = reflection.ReflectionServicer((SERVICE_NAME, reflection.SERVICE_NAME), pool=pool)
    request = reflection_pb2.ServerReflectionRequest(file_containing_symbol="provisa.v1.Orders")
    (response,) = list(servicer.ServerReflectionInfo(iter([request]), MagicMock()))
    described = {}
    for raw in response.file_descriptor_response.file_descriptor_proto:
        file = descriptor_pb2.FileDescriptorProto.FromString(raw)
        for message in file.message_type:
            described[message.name] = {f.name: f.type_name for f in message.field}
    assert described["Orders"]["customer"] == ".provisa.v1.Customers"
    assert described["Orders"]["referrer"] == ".provisa.v1.Customers"


# --- A relationship's field is its GraphQL name in proto snake_case; a collision is refused --------


def _with_aliases(**aliases: str):
    """The model with ``graphql_alias`` set on relationships by id (``r_customer`` is r-customer)."""
    si = _schema_input()
    for rel in si.relationships:
        alias = aliases.get(rel["id"].replace("-", "_"))
        if alias is not None:
            rel["graphql_alias"] = alias
    return si


class TestProtoFieldTranslation:
    def test_a_camel_case_relationship_name_is_a_snake_case_field_and_readable(self):
        from provisa.compiler.context import build_context
        from provisa.grpc.descriptor import OrgDescriptor
        from provisa.grpc.proto_gen import generate_proto
        from provisa.grpc.query_ir import resolve_read_mask

        si = _with_aliases(r_referrer="referredBy")
        orders = _fields(OrgDescriptor(generate_proto(si)).Orders.DESCRIPTOR)
        assert orders["referred_by"][1:] == ("Customers", False)
        assert "referredBy" not in orders
        mask = resolve_read_mask(build_context(si), "Orders", ["referred_by.name"])
        assert mask is not None
        assert [(r.field, r.graphql_field) for r in mask.relations] == [
            ("referred_by", "referredBy")
        ]

    def test_two_relationships_translating_to_one_field_are_refused_naming_both(self):
        from provisa.grpc.proto_gen import ProtoFieldCollision, generate_proto

        si = _with_aliases(r_customer="buyer_of", r_referrer="buyerOf")
        with pytest.raises(ProtoFieldCollision) as refused:
            generate_proto(si)
        message = str(refused.value)
        assert "'buyer_of'" in message and "'buyerOf'" in message
        assert "r-customer" in message and "r-referrer" in message
        assert refused.value.params["field"] == "buyer_of"
        assert refused.value.params["type"] == "Orders"

    def test_a_relationship_translating_to_a_column_field_is_refused_naming_both(self):
        from provisa.grpc.proto_gen import ProtoFieldCollision, generate_proto

        si = _with_aliases(r_referrer="referrerId")
        with pytest.raises(ProtoFieldCollision) as refused:
            generate_proto(si)
        message = str(refused.value)
        assert "column 'referrer_id'" in message
        assert "relationship 'referrerId' (r-referrer)" in message
