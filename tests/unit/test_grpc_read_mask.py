# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f4a82-7c1e-4d39-9a5b-2e8d7c3f1a96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: a gRPC request's ``read_mask`` selects the columns of the semantic SELECT (REQ-803).

One read_mask semantics for the native servicer and the HTTP gRPC proxy, implemented once in
``provisa.grpc.query_ir``: ``resolve_read_mask`` validates the paths against the columns the role
can read, the SELECT names only the masked columns, and a dotted sub-path into a JSON-valued
column restricts that column's value."""

# Requirements: REQ-803

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_types import TableMeta
from provisa.grpc.query_ir import grpc_table_to_semantic_sql

_COLUMNS = [
    ("order_id", "integer"),
    ("amount", "decimal(10,2)"),
    ("region", "varchar"),
    ("status", "varchar"),
    ("order_meta", "jsonb"),
]
_META = {"source_id": "s1", "created_by": "u1", "tags": {"a": 1, "b": 2}}


def _ctx():
    meta = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg1",
        catalog_name="pg1",
        schema_name="public",
        table_name="orders",
    )
    return SimpleNamespace(tables={"orders": meta}, aggregate_columns={1: list(_COLUMNS)})


def _mask(paths: list[str]):
    from provisa.grpc.query_ir import resolve_read_mask

    return resolve_read_mask(_ctx(), "Orders", paths)


def _sql(paths: list[str], limit: int = 10, filter_msg=None) -> str:
    lowered = grpc_table_to_semantic_sql(_ctx(), "Orders", limit, filter_msg, _mask(paths))
    assert lowered is not None
    return lowered[0]


def _selected(sql: str) -> list[str]:
    select = sqlglot.parse_one(sql, read="postgres")
    assert isinstance(select, exp.Select)
    return [e.alias_or_name for e in select.expressions]


def _where_columns(sql: str) -> list[str]:
    where = sqlglot.parse_one(sql, read="postgres").args.get("where")
    return [c.name for c in where.find_all(exp.Column)] if where is not None else []


class TestSemanticSqlReadMask:
    def test_mask_selects_only_the_masked_columns(self):
        assert _selected(_sql(["order_id", "amount"])) == ["order_id", "amount"]

    def test_mask_order_does_not_change_the_statement(self):
        """The select list follows the table's column order, so one mask is one statement."""
        assert _sql(["amount", "order_id"]) == _sql(["order_id", "amount"])

    def test_repeated_path_selects_the_column_once(self):
        assert _selected(_sql(["status", "status"])) == ["status"]

    def test_empty_mask_is_no_mask(self):
        assert _mask([]) is None
        assert _selected(_sql([])) == [c for c, _t in _COLUMNS]

    def test_absent_mask_selects_every_column(self):
        lowered = grpc_table_to_semantic_sql(_ctx(), "Orders", 10)
        assert lowered is not None
        assert _selected(lowered[0]) == [c for c, _t in _COLUMNS]

    def test_json_sub_path_selects_its_column(self):
        """The sub-path is applied to the value; the projection names the column itself."""
        assert _selected(_sql(["order_id", "order_meta.source_id"])) == ["order_id", "order_meta"]

    def test_filter_on_a_column_outside_the_mask_still_filters(self):
        filter_msg = MagicMock()
        filter_msg.DESCRIPTOR.fields = [SimpleNamespace(name="region")]
        filter_msg.HasField = lambda name: name == "region"
        filter_msg.region = "east"

        sql = _sql(["order_id"], filter_msg=filter_msg)
        assert _selected(sql) == ["order_id"]
        assert _where_columns(sql) == ["region"]


class TestReadMaskValidation:
    def test_unknown_path_raises_naming_the_path(self):
        from provisa.grpc.query_ir import ReadMaskError

        with pytest.raises(ReadMaskError, match="no_such_field") as failure:
            _mask(["order_id", "no_such_field"])
        assert failure.value.path == "no_such_field"

    def test_sub_path_under_an_unknown_column_raises_naming_the_path(self):
        from provisa.grpc.query_ir import ReadMaskError

        with pytest.raises(ReadMaskError, match=r"no_such_field\.source_id") as failure:
            _mask(["no_such_field.source_id"])
        assert failure.value.path == "no_such_field.source_id"

    def test_sub_path_into_a_column_that_is_not_json_raises_naming_the_path(self):
        from provisa.grpc.query_ir import ReadMaskError

        with pytest.raises(ReadMaskError, match=r"amount\.cents") as failure:
            _mask(["amount.cents"])
        assert failure.value.path == "amount.cents"

    @pytest.mark.parametrize("path", ["customers", "customers.name"])
    def test_relation_field_is_not_a_readable_column(self, path):
        """``Query{Type}`` reads the table's own columns; it never populates a relation field, so
        a path naming one is rejected like any other path that is not a readable column."""
        from provisa.grpc.query_ir import ReadMaskError

        ctx = _ctx()
        ctx.joins = {("Orders", "customers"): SimpleNamespace()}
        from provisa.grpc.query_ir import resolve_read_mask

        with pytest.raises(ReadMaskError) as failure:
            resolve_read_mask(ctx, "Orders", [path])
        assert failure.value.path == path

    @pytest.mark.parametrize("path", ["", "order_meta.", ".source_id", "order_meta..a"])
    def test_malformed_path_raises(self, path):
        from provisa.grpc.query_ir import ReadMaskError

        with pytest.raises(ReadMaskError):
            _mask([path])


class TestJsonSubPaths:
    """``restrictions`` lines the mask up with a result's columns: one entry per column, ``None``
    where the whole value is returned; ``restrict_json`` applies an entry to that column's value."""

    def _restricted(self, paths: list[str], value):
        from provisa.grpc.query_ir import restrict_json

        (restriction,) = _mask(paths).restrictions(["order_meta"])
        assert restriction is not None
        return restrict_json(value, restriction)

    def test_whole_columns_carry_no_restriction(self):
        mask = _mask(["order_id", "order_meta"])
        assert mask.restrictions(["order_id", "order_meta"]) == [None, None]

    def test_restrictions_follow_the_result_column_order_and_casing(self):
        mask = _mask(["order_id", "order_meta.source_id"])
        first, second = mask.restrictions(["ORDER_META", "order_id"])
        assert first is not None and second is None

    def test_sub_path_keeps_only_that_key_of_json_text(self):
        """A source driver hands a json/jsonb column back as text; it stays text."""
        out = self._restricted(["order_meta.source_id"], json.dumps(_META))
        assert isinstance(out, str)
        assert json.loads(out) == {"source_id": "s1"}

    def test_sub_path_keeps_only_that_key_of_a_decoded_object(self):
        assert self._restricted(["order_meta.source_id"], dict(_META)) == {"source_id": "s1"}

    def test_several_sub_paths_keep_each_key(self):
        out = self._restricted(["order_meta.source_id", "order_meta.created_by"], dict(_META))
        assert out == {"source_id": "s1", "created_by": "u1"}

    def test_deeper_sub_path_restricts_the_nested_object(self):
        assert self._restricted(["order_meta.tags.a"], dict(_META)) == {"tags": {"a": 1}}

    def test_sub_path_applies_to_each_object_of_a_list(self):
        out = self._restricted(["order_meta.name"], [{"name": "a", "x": 1}, {"name": "b"}])
        assert out == [{"name": "a"}, {"name": "b"}]

    def test_whole_column_path_wins_over_its_sub_path(self):
        for paths in (
            ["order_meta", "order_meta.source_id"],
            ["order_meta.source_id", "order_meta"],
        ):
            assert _mask(paths).restrictions(["order_meta"]) == [None]

    def test_null_value_stays_null(self):
        assert self._restricted(["order_meta.source_id"], None) is None


def test_two_masks_never_share_a_kept_plan_key():
    """The compiled stage keys its kept plan on the statement text (pgwire.governed_plan); the
    mask is in that text, so two masks are two keys."""
    from provisa.pgwire.governed_plan import plan_key

    state = SimpleNamespace(schema_boot_id="boot", schema_version=1)
    keys = {
        plan_key(state, "compiled", "admin", _sql(paths))
        for paths in (["order_id", "amount"], ["region", "status"], [])
    }
    assert len(keys) == 3


def _request(paths: list[str]):
    return SimpleNamespace(
        limit=10, read_mask=SimpleNamespace(paths=paths), HasField=lambda _name: False
    )


def _servicer():
    from google.protobuf.descriptor import FieldDescriptor

    from provisa.grpc.server import ProvisaServicer

    fields = [
        SimpleNamespace(name="order_id", message_type=None, type=FieldDescriptor.TYPE_INT64),
        SimpleNamespace(name="order_meta", message_type=None, type=FieldDescriptor.TYPE_STRING),
    ]
    descriptor = SimpleNamespace(fields=fields, fields_by_name={f.name: f for f in fields})
    msg_cls = MagicMock(DESCRIPTOR=descriptor)
    pb2 = SimpleNamespace(Orders=msg_cls)
    state = SimpleNamespace(contexts={"admin": _ctx()}, multitenancy=False)
    servicer = ProvisaServicer(state, pb2, MagicMock())
    servicer._emit_license_nag = MagicMock()
    servicer._meter_msg = lambda msg: msg
    return servicer, msg_cls


def _context():
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata = MagicMock(return_value=[("x-provisa-role", "admin")])
    return context


async def _run(servicer, request, context, result):
    from provisa.transpiler.router import Route

    plan = SimpleNamespace(route=Route.CACHE, source_id=None, cache_hit=None)
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
        _ = [m async for m in servicer._handle_query(request, context, "Orders", "orders")]
    return govern


class TestServicerReadMask:
    @pytest.mark.asyncio
    async def test_request_mask_reaches_the_governed_statement(self):
        """The servicer hands the pipeline a SELECT of the masked columns only."""
        servicer, _msg_cls = _servicer()
        result = SimpleNamespace(column_names=["order_id", "amount"], rows=[])
        govern = await _run(servicer, _request(["order_id", "amount"]), _context(), result)

        govern.assert_awaited_once()
        assert _selected(govern.await_args.args[0]) == ["order_id", "amount"]

    @pytest.mark.asyncio
    async def test_json_sub_path_restricts_the_message_value(self):
        servicer, msg_cls = _servicer()
        result = SimpleNamespace(
            column_names=["order_id", "order_meta"], rows=[[7, json.dumps(_META)]]
        )
        request = _request(["order_id", "order_meta.source_id"])
        govern = await _run(servicer, request, _context(), result)

        assert _selected(govern.await_args.args[0]) == ["order_id", "order_meta"]
        msg_cls.assert_called_once()
        kwargs = msg_cls.call_args.kwargs
        assert kwargs["order_id"] == 7
        assert json.loads(kwargs["order_meta"]) == {"source_id": "s1"}

    @pytest.mark.asyncio
    async def test_unknown_path_aborts_invalid_argument_before_governing(self):
        servicer, _msg_cls = _servicer()
        context = _context()
        result = SimpleNamespace(column_names=["order_id"], rows=[])
        govern = await _run(servicer, _request(["order_id", "no_such_field"]), context, result)

        govern.assert_not_awaited()
        context.abort.assert_awaited_once()
        code, details = context.abort.await_args.args
        assert code == grpc.StatusCode.INVALID_ARGUMENT
        assert "no_such_field" in details
