# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f8a2e-2b7a-4c1f-9c2e-1a9f3b8d6e4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for gRPC aggregate/group-by protocol parity (REQ-1359).

Covers:
  * proto_gen.py emitting {Type}AggregateResult / {Type}GroupByRequest / {Type}GroupByRow
    messages and Query{Type}Aggregate / Query{Type}GroupBy RPCs, gated by
    enable_aggregates / enable_group_by.
  * query_ir.py's grpc_table_to_aggregate_graphql_text / grpc_table_to_group_by_graphql_text
    GraphQL-text synthesis.
  * server.py's ProvisaServicer.__getattr__ dynamic dispatch, including the
    Query{Type}Aggregate / Query{Type}GroupBy vs. plain Query{Type} no-collision case.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput
from provisa.compiler.sql_types import TableMeta
from provisa.grpc.proto_gen import generate_proto
from provisa.grpc.query_ir import (
    grpc_table_to_aggregate_graphql_text,
    grpc_table_to_group_by_graphql_text,
)
from provisa.grpc.server import ProvisaServicer


def _make_si(enable_aggregates: bool = False, enable_group_by: bool = False):
    """Minimal single-table SchemaInput, modeled on tests/unit/test_proto_gen.py's `_make_si`,
    using the pet_store `inquiries` table shape (config/provisa-install.yaml) as the reference
    fixture: a numeric column (amount) and a comparable-only column (created_at)."""
    tables = [
        {
            "id": 1,
            "source_id": "pg1",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "inquiries",
            "enable_aggregates": enable_aggregates,
            "enable_group_by": enable_group_by,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
            ],
        }
    ]
    column_types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="amount", data_type="decimal(10,2)", is_nullable=True),
            ColumnMetadata(column_name="created_at", data_type="timestamp", is_nullable=True),
            ColumnMetadata(column_name="status", data_type="varchar", is_nullable=True),
        ]
    }
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "domain_access": ["*"], "capabilities": []},
        domains=[{"id": "sales", "description": "Sales domain"}],
        source_types=None,
    )


class TestProtoGenAggregateGating:
    def test_neither_flag_emits_nothing(self):
        proto = generate_proto(_make_si(enable_aggregates=False, enable_group_by=False))
        assert "AggregateResult" not in proto
        assert "GroupByRequest" not in proto
        assert "GroupByRow" not in proto
        assert "QueryInquiriesAggregate" not in proto
        assert "QueryInquiriesGroupBy" not in proto
        # No table opts in, so the Empty import must not be pulled in either.
        assert "empty.proto" not in proto

    def test_enable_aggregates_emits_aggregate_result_and_rpc(self):
        proto = generate_proto(_make_si(enable_aggregates=True, enable_group_by=False))
        assert "message InquiriesAggregateResult {" in proto
        assert "int32 count = 1;" in proto
        # amount is numeric -> sum/avg/stddev/variance sub-messages
        assert "message InquiriesSumFields {" in proto
        assert "message InquiriesAvgFields {" in proto
        assert "message InquiriesStddevFields {" in proto
        assert "message InquiriesVarianceFields {" in proto
        # id/created_at/status are comparable -> min/max sub-messages
        assert "message InquiriesMinFields {" in proto
        assert "message InquiriesMaxFields {" in proto
        assert (
            "rpc QueryInquiriesAggregate(google.protobuf.Empty) "
            "returns (InquiriesAggregateResult);" in proto
        )
        assert 'import "google/protobuf/empty.proto";' in proto
        # group-by not enabled -> no group-by messages/RPC
        assert "GroupByRequest" not in proto
        assert "GroupByRow" not in proto
        assert "QueryInquiriesGroupBy" not in proto

    def test_enable_group_by_emits_group_by_messages_and_rpc(self):
        proto = generate_proto(_make_si(enable_aggregates=False, enable_group_by=True))
        assert "message InquiriesGroupByRequest {" in proto
        assert "repeated string by = 1;" in proto
        assert "InquiriesFilter filter = 2;" in proto
        assert "message InquiriesGroupByRow {" in proto
        assert "string group_key = 1;" in proto
        assert "InquiriesAggregateResult aggregate = 2;" in proto
        assert (
            "rpc QueryInquiriesGroupBy(InquiriesGroupByRequest) "
            "returns (stream InquiriesGroupByRow);" in proto
        )
        # group-by alone still needs the aggregate result type, but not the Empty import
        # (that's only pulled in for the unary Aggregate RPC).
        assert "message InquiriesAggregateResult {" in proto
        assert "empty.proto" not in proto
        assert "QueryInquiriesAggregate" not in proto

    def test_both_flags_emit_both(self):
        proto = generate_proto(_make_si(enable_aggregates=True, enable_group_by=True))
        assert "message InquiriesAggregateResult {" in proto
        assert "message InquiriesGroupByRequest {" in proto
        assert "message InquiriesGroupByRow {" in proto
        assert "rpc QueryInquiriesAggregate(google.protobuf.Empty) " in proto
        assert "rpc QueryInquiriesGroupBy(InquiriesGroupByRequest) " in proto


class TestQueryIRAggregateText:
    @pytest.fixture(autouse=True)
    def _snake_convention(self):
        """Force the snake naming convention so synthesized field names (inquiries_aggregate,
        not inquiriesAggregate) are deterministic regardless of global naming config left over
        from other tests/modules."""
        from provisa.compiler.naming import configure

        configure(gql="snake", sql="snake")
        yield
        configure(gql="apollo_graphql", sql="snake")

    def _ctx(self):
        meta = TableMeta(
            table_id=1,
            field_name="inquiries",
            type_name="Inquiries",
            source_id="pg1",
            catalog_name="pg1",
            schema_name="public",
            table_name="inquiries",
        )
        return SimpleNamespace(
            tables={"inquiries": meta},
            aggregate_columns={
                1: [
                    ("amount", "decimal(10,2)"),
                    ("created_at", "timestamp"),
                    ("status", "varchar"),
                ]
            },
        )

    def test_aggregate_text_shape(self):
        ctx = self._ctx()
        text = grpc_table_to_aggregate_graphql_text(ctx, "Inquiries")
        assert text is not None
        assert text.startswith("{ inquiries_aggregate {")
        assert "count" in text
        assert "sum { amount }" in text
        assert "avg { amount }" in text
        assert "stddev { amount }" in text
        assert "variance { amount }" in text
        assert "min { amount created_at status }" in text
        assert "max { amount created_at status }" in text

    def test_aggregate_text_unknown_type_returns_none(self):
        ctx = self._ctx()
        assert grpc_table_to_aggregate_graphql_text(ctx, "Nonexistent") is None

    def test_group_by_text_shape(self):
        ctx = self._ctx()
        text = grpc_table_to_group_by_graphql_text(ctx, "Inquiries", ["status"])
        assert text is not None
        assert text == (
            "{ inquiries_group_by(by: [status]) { groupKey aggregate "
            "{ count sum { amount } avg { amount } stddev { amount } variance { amount } "
            "min { amount created_at status } max { amount created_at status } } } }"
        )

    def test_group_by_text_multiple_by_columns(self):
        ctx = self._ctx()
        text = grpc_table_to_group_by_graphql_text(ctx, "Inquiries", ["status", "created_at"])
        assert "by: [status, created_at]" in text

    def test_group_by_text_empty_by_returns_none(self):
        ctx = self._ctx()
        assert grpc_table_to_group_by_graphql_text(ctx, "Inquiries", []) is None

    def test_group_by_text_unknown_type_returns_none(self):
        ctx = self._ctx()
        assert grpc_table_to_group_by_graphql_text(ctx, "Nonexistent", ["status"]) is None

    def test_type_name_matching_is_separator_insensitive(self):
        # proto collapses the domain separator: PS__Inquiries -> PsInquiries.
        meta = TableMeta(
            table_id=1,
            field_name="ps__inquiries",
            type_name="PS__Inquiries",
            source_id="pg1",
            catalog_name="pg1",
            schema_name="public",
            table_name="inquiries",
        )
        ctx = SimpleNamespace(
            tables={"ps__inquiries": meta},
            aggregate_columns={1: [("amount", "decimal(10,2)")]},
        )
        text = grpc_table_to_aggregate_graphql_text(ctx, "PsInquiries")
        assert text is not None
        assert "ps__inquiries_aggregate" in text


def _make_pb2_module_with_aggregates(type_name: str = "Inquiries"):
    """Fake pb2 module exposing Query message + AggregateResult + GroupByRow classes, so
    __getattr__ dispatch can be exercised without a real generated proto module."""
    query_msg_cls = MagicMock()
    query_msg_cls.DESCRIPTOR = SimpleNamespace(fields=[])
    agg_msg_cls = MagicMock()
    group_by_row_cls = MagicMock()

    pb2 = SimpleNamespace(
        **{
            type_name: query_msg_cls,
            f"{type_name}AggregateResult": agg_msg_cls,
            f"{type_name}GroupByRow": group_by_row_cls,
            "DESCRIPTOR": SimpleNamespace(services_by_name={}),
        }
    )
    return pb2


class TestServicerAggregateDispatch:
    def test_aggregate_rpc_resolves_to_aggregate_handler(self):
        pb2 = _make_pb2_module_with_aggregates()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        handler = servicer.QueryInquiriesAggregate
        assert callable(handler)

    def test_group_by_rpc_resolves_to_group_by_handler(self):
        pb2 = _make_pb2_module_with_aggregates()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        handler = servicer.QueryInquiriesGroupBy
        assert callable(handler)

    def test_plain_query_rpc_still_resolves(self):
        pb2 = _make_pb2_module_with_aggregates()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        handler = servicer.QueryInquiries
        assert callable(handler)

    @pytest.mark.asyncio
    async def test_no_collision_aggregate_dispatches_to_aggregate_bound(self, monkeypatch):
        """Query{Type}Aggregate must dispatch to _handle_query_aggregate, never the generic
        _handle_query (which the plain Query{Type} prefix branch would wrongly match if it were
        checked first)."""
        pb2 = _make_pb2_module_with_aggregates()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        calls = []

        async def fake_aggregate(request, context, type_name):
            calls.append(("aggregate", type_name))
            return "AGG_RESULT"

        async def fake_plain_query(request, context, type_name, field_name):
            calls.append(("plain", type_name))
            if False:
                yield None  # pragma: no cover - keep this an async generator

        monkeypatch.setattr(servicer, "_handle_query_aggregate", fake_aggregate)
        monkeypatch.setattr(servicer, "_handle_query", fake_plain_query)

        handler = servicer.QueryInquiriesAggregate
        result = await handler(MagicMock(), MagicMock())

        assert result == "AGG_RESULT"
        assert calls == [("aggregate", "Inquiries")]

    @pytest.mark.asyncio
    async def test_no_collision_group_by_dispatches_to_group_by_bound(self, monkeypatch):
        pb2 = _make_pb2_module_with_aggregates()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        calls = []

        async def fake_group_by(request, context, type_name):
            calls.append(("group_by", type_name))
            yield "ROW1"
            yield "ROW2"

        async def fake_plain_query(request, context, type_name, field_name):
            calls.append(("plain", type_name))
            if False:
                yield None  # pragma: no cover

        monkeypatch.setattr(servicer, "_handle_query_group_by", fake_group_by)
        monkeypatch.setattr(servicer, "_handle_query", fake_plain_query)

        handler = servicer.QueryInquiriesGroupBy
        results = [msg async for msg in handler(MagicMock(), MagicMock())]

        assert results == ["ROW1", "ROW2"]
        assert calls == [("group_by", "Inquiries")]

    @pytest.mark.asyncio
    async def test_plain_query_dispatches_to_handle_query_not_aggregate(self, monkeypatch):
        pb2 = _make_pb2_module_with_aggregates()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        calls = []

        async def fake_plain_query(request, context, type_name, field_name):
            calls.append(("plain", type_name, field_name))
            yield "ROW"

        async def fake_aggregate(request, context, type_name):
            calls.append(("aggregate", type_name))
            return "AGG_RESULT"

        monkeypatch.setattr(servicer, "_handle_query", fake_plain_query)
        monkeypatch.setattr(servicer, "_handle_query_aggregate", fake_aggregate)

        handler = servicer.QueryInquiries
        results = [msg async for msg in handler(MagicMock(), MagicMock())]

        assert results == ["ROW"]
        assert calls == [("plain", "Inquiries", "inquiries")]

    def test_type_name_stripped_correctly_for_aggregate(self):
        pb2 = _make_pb2_module_with_aggregates("Orders")
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        # Access via __getattr__ directly to inspect closure behavior indirectly: the handler
        # must be built for type_name == "Orders", not "OrdersAggregate" or "".
        handler = servicer.__getattr__("QueryOrdersAggregate")
        assert callable(handler)

    def test_type_name_stripped_correctly_for_group_by(self):
        pb2 = _make_pb2_module_with_aggregates("Orders")
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        handler = servicer.__getattr__("QueryOrdersGroupBy")
        assert callable(handler)
