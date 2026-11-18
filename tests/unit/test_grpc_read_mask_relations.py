# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: a gRPC row read's ``read_mask`` names relationship paths, and each row comes back with
the relationship's message filled, as the GraphQL it is derived from nests it (REQ-1929).

A path's first segment is a relation field the ``{Type}`` message declares (its name is the
related table's proto field name, proto_gen); the rest is a path into the related table, resolved
by the same rules. The request lowers to a GraphQL selection nesting the relationship, so it is
governed as that GraphQL is, and the servicer writes the relation field: one message for a
many-to-one, one per related row for a one-to-many."""

# Requirements: REQ-1929, REQ-803

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


def _model(customer_visible_to=("admin",), domain_access=("*",)):
    """``(schema, ctx)``: orders (many-to-one ``customer``) and customers (one-to-many
    ``orders``). In the proto, the relation fields are named for the related table: Orders has
    ``customers``, Customers has ``orders``."""
    from provisa.compiler import naming as _naming
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    def col(name, data_type):
        return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)

    def table(tid, name, domain, cols, visible_to=("admin",)):
        return {
            "id": tid,
            "source_id": "sales-pg",
            "domain_id": domain,
            "schema_name": "public",
            "table_name": name,
            "columns": [{"column_name": c, "visible_to": list(visible_to)} for c in cols],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }

    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=[
            table(1, "customers", "crm", ["id", "name", "prefs"], customer_visible_to),
            table(2, "orders", "sales", ["id", "customer_id", "note"]),
        ],
        relationships=[
            {
                "id": "r1",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "id",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
            },
            {
                "id": "r2",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
        ],
        column_types={
            1: [col("id", "integer"), col("name", "text"), col("prefs", "jsonb")],
            2: [col("id", "integer"), col("customer_id", "integer"), col("note", "text")],
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": list(domain_access)},
        domains=[{"id": "sales", "description": "Sales"}, {"id": "crm", "description": "CRM"}],
        source_types={"sales-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


def _mask(model, type_name, paths):
    from provisa.grpc.query_ir import resolve_read_mask

    return resolve_read_mask(model[1], type_name, paths)


def _text(model, type_name, paths) -> str:
    from provisa.grpc.query_ir import grpc_table_to_graphql

    schema, ctx = model
    lowered = grpc_table_to_graphql(
        ctx, schema, type_name, 10, None, _mask(model, type_name, paths)
    )
    assert lowered is not None
    return lowered[0]


def _compiled(model, type_name, paths):
    from provisa.compiler.parser import parse_query
    from provisa.compiler.sql_gen import compile_query

    schema, ctx = model
    text = _text(model, type_name, paths)
    (compiled,) = compile_query(parse_query(schema, text, {}, ctx=ctx), ctx, {})
    return compiled


class TestRelationPathLowering:
    def test_many_to_one_path_nests_the_relationship_under_its_proto_name(self):
        text = _text(_model(), "Orders", ["id", "customers.name"])
        assert "customers: customer { name }" in text
        assert text.index("id") < text.index("customers:")

    def test_one_to_many_path_nests_the_relationship(self):
        text = _text(_model(), "Customers", ["id", "orders.note"])
        assert "orders { note }" in text

    def test_relation_named_alone_selects_every_readable_column_of_it(self):
        text = _text(_model(), "Orders", ["customers"])
        assert "customers: customer { id name prefs }" in text

    def test_paths_into_one_relation_merge_in_its_column_order(self):
        text = _text(_model(), "Orders", ["customers.name", "customers.id"])
        assert "customers: customer { id name }" in text

    def test_lowered_text_compiles_to_a_statement_reading_the_related_table(self):
        compiled = _compiled(_model(), "Orders", ["id", "customers.name"])
        assert "customers" in compiled.sql
        assert [c.field_name for c in compiled.columns if c.nested_in is None][:1] == ["id"]

    def test_json_sub_path_inside_a_relation_restricts_its_value(self):
        mask = _mask(_model(), "Orders", ["customers.prefs.theme"])
        (relation,) = mask.relations
        assert relation.field == "customers"
        assert relation.mask.columns == ("prefs",)

    def test_relation_only_mask_selects_no_root_column(self):
        mask = _mask(_model(), "Orders", ["customers.name"])
        assert mask.columns == ()


class TestRelationPathRefusals:
    @pytest.mark.parametrize(
        "path",
        [
            "customers.no_such_column",
            "customer.name",  # the GraphQL field name; the proto field is ``customers``
            "customers.name.x",  # name is not JSON-valued
            "customers.",
        ],
    )
    def test_path_not_readable_through_the_relation_is_refused_by_name(self, path):
        from provisa.grpc.query_ir import ReadMaskError

        with pytest.raises(ReadMaskError) as failure:
            _mask(_model(), "Orders", [path])
        assert failure.value.path == path
        assert path in str(failure.value)

    def test_relation_to_a_table_the_role_cannot_see_is_refused_by_name(self):
        from provisa.grpc.query_ir import ReadMaskError

        model = _model(domain_access=("sales",))
        with pytest.raises(ReadMaskError) as failure:
            _mask(model, "Orders", ["customers.name"])
        assert failure.value.path == "customers.name"

    def test_column_of_the_related_table_the_role_cannot_read_is_refused_by_name(self):
        from provisa.grpc.query_ir import ReadMaskError

        model = _model(customer_visible_to=())
        with pytest.raises(ReadMaskError) as failure:
            _mask(model, "Orders", ["customers.name"])
        assert failure.value.path == "customers.name"


# --- The servicer fills the relation field ---------------------------------------------------------

_PROTO = (
    'syntax = "proto3"; package provisa.v1;'
    " message Customers { optional int64 id = 1; optional string name = 2;"
    " optional string prefs = 3; repeated Orders orders = 4; }"
    " message Orders { optional int64 customer_id = 1; optional int64 id = 2;"
    " optional string note = 3; Customers customers = 4; }"
    " message OrdersBatch { repeated Orders rows = 1; }"
    " message CustomersBatch { repeated Customers rows = 1; }"
)


def _request(paths, batch_rows=0):
    return SimpleNamespace(
        limit=10,
        offset=0,
        batch_rows=batch_rows,
        read_mask=SimpleNamespace(paths=paths),
        HasField=lambda _name: False,
        DESCRIPTOR=SimpleNamespace(fields_by_name={}),
    )


def _servicer(model):
    from provisa.grpc.descriptor import OrgDescriptor
    from provisa.grpc.server import ProvisaServicer

    pb2 = OrgDescriptor(_PROTO)
    servicer = ProvisaServicer(serving_state(model), pb2)
    servicer._emit_trailing_metadata = MagicMock()
    servicer._meter_msg = lambda msg: msg
    return servicer, pb2


def _context():
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata = MagicMock(return_value=[("x-provisa-role", "admin")])
    return context


def _result(model, type_name, paths, rows):
    """A result in the layout of the statement the request compiles to."""
    compiled = _compiled(model, type_name, paths)
    return QueryResult(rows=rows, column_names=[c.column for c in compiled.columns])


async def _run(servicer, request, context, result, handler="_handle_query", type_name="Orders"):
    from provisa.transpiler.router import Route

    plan = SimpleNamespace(
        route=Route.CACHE, source_id=None, cache_hit=None, warnings=[], materialize=None
    )
    with (
        patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled",
            new_callable=AsyncMock,
            return_value=plan,
        ) as govern,
        patch(
            "provisa.pgwire._pipeline._execute_plan", new_callable=AsyncMock, return_value=result
        ),
    ):
        sent = [m async for m in getattr(servicer, handler)(request, context, type_name)]
    return govern, sent


class TestServicerFillsRelations:
    @pytest.mark.asyncio
    async def test_many_to_one_fills_the_singular_relation_message(self):
        model = _model()
        paths = ["id", "customers.name"]
        result = _result(model, "Orders", paths, [(7, json.dumps({"name": "Ann"})), (8, None)])
        servicer, pb2 = _servicer(model)
        govern, sent = await _run(servicer, _request(paths), _context(), result)

        assert "customers" in govern.await_args.args[0]
        first, second = [pb2.Orders.FromString(m) for m in sent]
        assert first.id == 7 and first.customers.name == "Ann"
        assert not first.HasField("note")
        assert second.id == 8 and not second.HasField("customers")

    @pytest.mark.asyncio
    async def test_one_to_many_fills_one_message_per_related_row(self):
        model = _model()
        paths = ["id", "orders.note"]
        result = _result(
            model,
            "Customers",
            paths,
            [(1, json.dumps([{"note": "a"}, {"note": "b"}])), (2, json.dumps([]))],
        )
        servicer, pb2 = _servicer(model)
        _govern, sent = await _run(
            servicer, _request(paths), _context(), result, type_name="Customers"
        )

        first, second = [pb2.Customers.FromString(m) for m in sent]
        assert first.id == 1 and [o.note for o in first.orders] == ["a", "b"]
        assert second.id == 2 and len(second.orders) == 0

    @pytest.mark.asyncio
    async def test_json_sub_path_inside_a_relation_restricts_the_value(self):
        model = _model()
        paths = ["customers.prefs.theme"]
        prefs = {"theme": "dark", "lang": "en"}
        result = _result(model, "Orders", paths, [(json.dumps({"prefs": prefs}),)])
        servicer, pb2 = _servicer(model)
        _govern, sent = await _run(servicer, _request(paths), _context(), result)

        (message,) = [pb2.Orders.FromString(m) for m in sent]
        assert json.loads(message.customers.prefs) == {"theme": "dark"}

    @pytest.mark.asyncio
    async def test_batch_rpc_carries_the_nested_rows(self):
        model = _model()
        paths = ["id", "customers.name"]
        result = _result(
            model,
            "Orders",
            paths,
            [(1, json.dumps({"name": "A"})), (2, json.dumps({"name": "B"})), (3, None)],
        )
        servicer, pb2 = _servicer(model)
        _govern, sent = await _run(
            servicer, _request(paths, batch_rows=2), _context(), result, "_handle_query_batch"
        )

        batches = [pb2.OrdersBatch.FromString(m) for m in sent]
        assert [len(b.rows) for b in batches] == [2, 1]
        rows = [r for b in batches for r in b.rows]
        assert [(r.id, r.customers.name) for r in rows] == [(1, "A"), (2, "B"), (3, "")]
        assert not rows[2].HasField("customers")

    @pytest.mark.asyncio
    async def test_unreadable_relation_path_aborts_before_governing(self):
        model = _model()
        servicer, _pb2 = _servicer(model)
        context = _context()
        result = QueryResult(rows=[], column_names=["id"])
        govern, _sent = await _run(servicer, _request(["customers.nope"]), context, result)

        govern.assert_not_awaited()
        code, details = context.abort.await_args.args
        assert code == grpc.StatusCode.INVALID_ARGUMENT
        assert "customers.nope" in details


# --- The HTTP gRPC proxy answers the same nested rows ----------------------------------------------


class _ProxyRequest:
    """The Starlette request ``grpc_proxy`` reads: a JSON body, headers, the auth layer's state."""

    def __init__(self, body: dict, headers: dict[str, str]) -> None:
        self._body = body
        self.headers = headers
        self.state = SimpleNamespace()

    async def json(self) -> dict:
        return self._body


@pytest.mark.asyncio
async def test_proxy_row_read_nests_the_masked_relations():
    from provisa.api.data import endpoint_grpc_proxy as proxy

    model = _model()
    paths = ["id", "customers.prefs.theme"]
    prefs = {"theme": "dark", "lang": "en"}
    result = _result(model, "Orders", paths, [(7, json.dumps({"prefs": prefs})), (8, None)])
    plan = SimpleNamespace(warnings=[])
    with (
        patch("provisa.api.app.state", serving_state(model)),
        patch.object(proxy, "_govern_and_route_compiled", AsyncMock(return_value=plan)) as govern,
        patch.object(proxy, "_execute_plan", AsyncMock(return_value=result)),
    ):
        body = {"read_mask": {"paths": paths}}
        response = await proxy.grpc_proxy(
            "Orders",
            _ProxyRequest(body, {"x-provisa-role": "admin"}),  # type: ignore[arg-type]
        )

    assert "customers" in govern.await_args.args[0]
    assert json.loads(response.body) == [
        {"id": 7, "customers": {"prefs": {"theme": "dark"}}},
        {"id": 8, "customers": None},
    ]
