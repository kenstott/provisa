# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3d9e41-2a6c-4f85-b1d0-5c8e2a9f4d17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: a gRPC filter may only name a column the requesting role can read (REQ-1860, #131).

The native server serves one wire proto whose ``{Type}Filter`` message carries every column, so
a role can SET a filter field for a column hidden from it. The lowering must reject that field
by name before any statement is built: a predicate on a hidden column returns only the rows
matching it, so the row count would confirm or refute the hidden value."""

# Requirements: REQ-1860

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from provisa.compiler.sql_types import TableMeta
from provisa.grpc.query_ir import (
    grpc_table_to_group_by_graphql_text,
    grpc_table_to_semantic_sql,
)

# The columns THIS role can read; ``amount`` exists on the table and in the wire proto's filter
# message, but is not visible to the role.
_READABLE = [("order_id", "integer"), ("region", "varchar"), ("status", "varchar")]


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
    return SimpleNamespace(tables={"orders": meta}, aggregate_columns={1: list(_READABLE)})


def _filter(**set_fields):
    """A wire-proto ``OrdersFilter``: every column a field, ``set_fields`` the ones the client set."""
    fields = [SimpleNamespace(name=n) for n in ("order_id", "amount", "region", "status")]
    return SimpleNamespace(
        DESCRIPTOR=SimpleNamespace(fields=fields),
        HasField=lambda name: name in set_fields,
        **set_fields,
    )


class TestLoweringRejectsAHiddenColumn:
    def test_query_filter_on_a_hidden_column_raises_naming_the_field(self):
        from provisa.grpc.query_ir import FilterError

        with pytest.raises(FilterError, match="amount") as failure:
            grpc_table_to_semantic_sql(_ctx(), "Orders", 10, _filter(amount=10.5))
        assert failure.value.field == "amount"

    def test_group_by_filter_on_a_hidden_column_raises_naming_the_field(self):
        from provisa.grpc.query_ir import FilterError

        with pytest.raises(FilterError, match="amount") as failure:
            grpc_table_to_group_by_graphql_text(
                _ctx(), "Orders", ["region"], funcs=["count"], filter_msg=_filter(amount=10.5)
            )
        assert failure.value.field == "amount"

    def test_a_hidden_column_is_rejected_even_beside_readable_ones(self):
        from provisa.grpc.query_ir import FilterError

        with pytest.raises(FilterError, match="amount"):
            grpc_table_to_semantic_sql(
                _ctx(), "Orders", 10, _filter(region="east", amount=10.5, status="open")
            )

    def test_the_error_does_not_depend_on_the_value(self):
        """Nothing about the hidden value is observable: the same error for any value."""
        from provisa.grpc.query_ir import FilterError

        messages = set()
        for value in (10.5, 999.0, 0.0):
            with pytest.raises(FilterError) as failure:
                grpc_table_to_semantic_sql(_ctx(), "Orders", 10, _filter(amount=value))
            messages.add(str(failure.value))
        assert len(messages) == 1

    def test_filter_on_readable_columns_still_lowers(self):
        sql = grpc_table_to_semantic_sql(_ctx(), "Orders", 10, _filter(region="east", order_id=3))
        assert sql is not None
        assert 'WHERE "order_id" = 3 AND "region" = \'east\'' in sql

        text = grpc_table_to_group_by_graphql_text(
            _ctx(), "Orders", ["region"], funcs=["count"], filter_msg=_filter(status="open")
        )
        assert text is not None
        assert 'where: { status: { eq: "open" } }' in text


def _servicer():
    from provisa.grpc.server import ProvisaServicer

    descriptor = SimpleNamespace(fields=[], fields_by_name={})
    pb2 = SimpleNamespace(
        Orders=MagicMock(DESCRIPTOR=descriptor),
        OrdersGroupByRow=MagicMock(),
        OrdersAggregateResult=MagicMock(),
    )
    state = SimpleNamespace(
        contexts={"analyst": _ctx()}, schemas={"analyst": MagicMock()}, multitenancy=False
    )
    return ProvisaServicer(state, pb2, MagicMock())


def _context():
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata = MagicMock(return_value=[("x-provisa-role", "analyst")])
    return context


def _assert_rejected(context, govern) -> None:
    govern.assert_not_awaited()  # nothing was governed, routed or run
    context.abort.assert_awaited_once()
    code, details = context.abort.await_args.args
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert "amount" in details


class TestServicerRejectsBeforeAnyStatement:
    @pytest.mark.asyncio
    async def test_query_aborts_invalid_argument(self):
        context = _context()
        request = SimpleNamespace(
            limit=10, filter=_filter(amount=10.5), HasField=lambda name: name == "filter"
        )
        with patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled", new_callable=AsyncMock
        ) as govern:
            _ = [m async for m in _servicer()._handle_query(request, context, "Orders", "orders")]
        _assert_rejected(context, govern)

    @pytest.mark.asyncio
    async def test_group_by_aborts_invalid_argument(self):
        context = _context()
        request = SimpleNamespace(
            by=["region"],
            funcs=["count"],
            filter=_filter(amount=10.5),
            HasField=lambda name: name == "filter",
        )
        with (
            patch(
                "provisa.pgwire._pipeline._govern_and_route_compiled", new_callable=AsyncMock
            ) as govern,
            patch("provisa.compiler.parser.parse_query") as parse,
        ):
            _ = [m async for m in _servicer()._handle_query_group_by(request, context, "Orders")]
        parse.assert_not_called()  # rejected before the query text is even parsed
        _assert_rejected(context, govern)
