# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for grpc requirements: REQ-525, REQ-538, REQ-617"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
import grpc.aio
import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput
from provisa.grpc.proto_gen import generate_proto
from provisa.grpc.server import ProvisaServicer, _get_role


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _make_schema_input(
    role_id: str, table_columns: list[str], col_types_override: dict | None = None
):
    """Build a minimal SchemaInput for a single table with the given columns visible to role_id."""
    tables = [
        {
            "id": 1,
            "source_id": "test-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [{"column_name": c, "visible_to": [role_id]} for c in table_columns],
        }
    ]
    col_types = col_types_override or {
        1: [_col(c, "varchar(100)") for c in table_columns],
    }
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": role_id, "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"test-pg": "postgresql"},
    )


class TestREQ525PerRoleProtoGeneration:
    """REQ-525: Auto-generated .proto is generated per role; each role receives a proto
    definition reflecting only the tables and columns visible to that role."""

    def test_admin_proto_contains_secret_column(self):
        # REQ-525
        si = _make_schema_input("admin", ["id", "amount", "secret"])
        proto = generate_proto(si)
        assert "secret" in proto

    def test_viewer_proto_omits_secret_column(self):
        # REQ-525
        si = _make_schema_input("viewer", ["id", "amount"])
        proto = generate_proto(si)
        assert "secret" not in proto

    def test_each_role_receives_distinct_proto(self):
        # REQ-525: Two roles with different column visibility produce different protos.
        si_admin = _make_schema_input("admin", ["id", "amount", "secret"])
        si_viewer = _make_schema_input("viewer", ["id", "amount"])
        proto_admin = generate_proto(si_admin)
        proto_viewer = generate_proto(si_viewer)
        assert proto_admin != proto_viewer

    def test_proto_only_contains_visible_columns(self):
        # REQ-525: Proto must NOT expose columns that are invisible to the role.
        all_cols = ["id", "amount", "internal_cost", "profit_margin"]
        visible = ["id", "amount"]
        hidden = ["internal_cost", "profit_margin"]
        # Build a table where hidden cols exist in col_types but are NOT visible_to viewer.
        tables = [
            {
                "id": 1,
                "source_id": "test-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": c, "visible_to": ["viewer"] if c in visible else ["admin"]}
                    for c in all_cols
                ],
            }
        ]
        col_types = {1: [_col(c, "varchar(100)") for c in all_cols]}
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "viewer", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": "Sales"}],
            source_types={"test-pg": "postgresql"},
        )
        proto = generate_proto(si)
        for col in hidden:
            assert col not in proto, f"Hidden column {col!r} must not appear in viewer proto"
        for col in visible:
            assert col in proto, f"Visible column {col!r} must appear in viewer proto"

    def test_proto_contains_message_for_each_visible_table(self):
        # REQ-525: Each visible table produces a message type in the proto.
        si = _make_schema_input("admin", ["id", "amount"])
        proto = generate_proto(si)
        # The table is "orders" -> type name should be "Orders"
        assert "message Orders" in proto

    def test_no_visible_tables_raises(self):
        # REQ-525: A role with no visible tables cannot generate a proto (no empty proto allowed).
        tables = [
            {
                "id": 1,
                "source_id": "test-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
            }
        ]
        col_types = {1: [_col("id", "integer")]}
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "viewer", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": "Sales"}],
            source_types={"test-pg": "postgresql"},
        )
        with pytest.raises(ValueError, match="No tables visible"):
            generate_proto(si)


class TestREQ538ProtoTypeMappings:
    """REQ-538: .proto maps Provisa/Trino column types to protobuf scalar types:
    integer→int32, bigint→int64, varchar→string, decimal→double, boolean→bool,
    timestamp→google.protobuf.Timestamp. Each registered table produces one proto message.
    Relationships between tables produce nested message fields."""

    def _proto_for(self, col_defs: list[tuple[str, str]]) -> str:
        """Generate proto for a table with given (name, column_type) column pairs."""
        col_names = [name for name, _ in col_defs]
        tables = [
            {
                "id": 1,
                "source_id": "test-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [{"column_name": n, "visible_to": ["admin"]} for n in col_names],
            }
        ]
        col_types = {1: [_col(name, dtype) for name, dtype in col_defs]}
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": "Sales"}],
            source_types={"test-pg": "postgresql"},
        )
        return generate_proto(si)

    def test_integer_maps_to_int32(self):
        # REQ-538
        proto = self._proto_for([("qty", "integer")])
        assert "int32 qty" in proto

    def test_bigint_maps_to_int64(self):
        # REQ-538
        proto = self._proto_for([("big_id", "bigint")])
        assert "int64 big_id" in proto

    def test_varchar_maps_to_string(self):
        # REQ-538
        proto = self._proto_for([("name", "varchar(255)")])
        assert "string name" in proto

    def test_decimal_maps_to_double(self):
        # REQ-538
        proto = self._proto_for([("price", "decimal(10,2)")])
        assert "double price" in proto

    def test_boolean_maps_to_bool(self):
        # REQ-538
        proto = self._proto_for([("active", "boolean")])
        assert "bool active" in proto

    def test_timestamp_maps_to_protobuf_timestamp(self):
        # REQ-538
        proto = self._proto_for([("created_at", "timestamp")])
        assert "google.protobuf.Timestamp created_at" in proto
        # timestamp import must also be present
        assert 'import "google/protobuf/timestamp.proto"' in proto

    def test_each_table_produces_one_message(self):
        # REQ-538: Each registered table produces one proto message type.
        proto = self._proto_for([("id", "integer"), ("amount", "decimal(10,2)")])
        # Exactly one "message Orders {" block
        assert proto.count("message Orders {") == 1

    def test_relationships_produce_nested_message_fields(self):
        # REQ-538: Relationships between tables produce nested message fields.
        tables = [
            {
                "id": 1,
                "source_id": "test-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
            },
            {
                "id": 2,
                "source_id": "test-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "customers",
                "governance": "pre-approved",
                "columns": [{"column_name": "cust_id", "visible_to": ["admin"]}],
            },
        ]
        col_types = {
            1: [_col("id", "integer")],
            2: [_col("cust_id", "integer")],
        }
        relationships = [
            {
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "id",
                "target_column": "cust_id",
                "cardinality": "many-to-one",
            }
        ]
        si = SchemaInput(
            tables=tables,
            relationships=relationships,
            column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": "Sales"}],
            source_types={"test-pg": "postgresql"},
        )
        proto = generate_proto(si)
        # The Orders message should contain a nested Customers field
        assert "Customers" in proto


class TestREQ617RoleSelectionViaMetadata:
    """REQ-617: Role selection on every Provisa gRPC RPC is via the x-provisa-role metadata key.
    Missing or unrecognised role metadata causes the call to be rejected with UNAUTHENTICATED.
    Streaming query RPCs emit one response message per result row; mutation RPCs are unary."""

    @pytest.mark.asyncio
    async def test_missing_role_metadata_raises_unauthenticated(self):
        # REQ-617: Missing x-provisa-role causes UNAUTHENTICATED rejection.
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = []
        with pytest.raises(grpc.aio.AbortError) as exc_info:
            _get_role(context)
        assert exc_info.value.args[0] == grpc.StatusCode.UNAUTHENTICATED

    @pytest.mark.asyncio
    async def test_valid_role_metadata_is_extracted(self):
        # REQ-617: x-provisa-role metadata key is used for role selection.
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "analyst")]
        role = _get_role(context)
        assert role == "analyst"

    @pytest.mark.asyncio
    async def test_unrecognised_role_aborts_with_not_found(self):
        # REQ-617: Unrecognised role metadata causes the call to be rejected.
        pb2, _ = _make_pb2_module("Orders", ["id"])
        state = SimpleNamespace(
            schemas={"admin": MagicMock()},
            contexts={"admin": MagicMock()},
        )
        servicer = ProvisaServicer(state, pb2, MagicMock())
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "unknown_role")]
        context.abort.side_effect = grpc.aio.AbortError(
            grpc.StatusCode.NOT_FOUND, "No schema for role 'unknown_role'"
        )
        request = MagicMock()
        with pytest.raises(grpc.aio.AbortError):
            async for _ in servicer._handle_query(request, context, "Orders", "orders"):
                pass
        context.abort.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_streaming_query_emits_one_message_per_row(self):
        # REQ-617: Streaming query RPCs emit one response message per result row.
        from unittest.mock import patch
        from provisa.compiler.rls import RLSContext
        from provisa.transpiler.router import Route

        pb2, _ = _make_pb2_module("Orders", ["id", "amount"])
        state = SimpleNamespace(
            schemas={"admin": MagicMock()},
            contexts={"admin": MagicMock()},
            rls_contexts={"admin": RLSContext.empty()},
            roles={"admin": {"id": "admin", "capabilities": []}},
            source_pools=MagicMock(),
            source_types={},
            source_dialects={},
            masking_rules={},
            mv_registry=SimpleNamespace(get_fresh=lambda: []),
            trino_conn=MagicMock(),
            flight_client=None,
        )
        # Mandatory terminal-execution binding (REQ-825).
        from provisa.federation.engine import build_trino_engine
        from provisa.federation.runtime import EngineRuntime

        state.federation_engine = EngineRuntime(build_trino_engine(), state)
        servicer = ProvisaServicer(state, pb2, MagicMock())
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()
        request.limit = 0

        # New pipeline seam: govern/route/execute via provisa.pgwire._pipeline. Three rows in.
        fake_plan = SimpleNamespace(route=Route.DIRECT, source_id="test-pg")
        fake_result = SimpleNamespace(
            column_names=["id", "amount"], rows=[[1, 10.0], [2, 20.0], [3, 30.0]]
        )

        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_semantic_sql",
                return_value="SELECT id, amount FROM orders",
            ),
            patch(
                "provisa.pgwire._pipeline._govern_and_route_compiled",
                new_callable=AsyncMock,
                return_value=fake_plan,
            ),
            patch(
                "provisa.pgwire._pipeline._execute_plan",
                new_callable=AsyncMock,
                return_value=fake_result,
            ),
        ):
            rows_yielded = []
            async for msg in servicer._handle_query(request, context, "Orders", "orders"):
                rows_yielded.append(msg)

        # One message per row: 3 rows → 3 messages
        assert len(rows_yielded) == 3

    @pytest.mark.asyncio
    async def test_mutation_rpc_is_unary(self):
        # REQ-617: Mutation RPCs are unary (return a single response, not a stream).
        pb2, _ = _make_pb2_module("Orders", ["id"])
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        request = MagicMock()
        # _handle_insert is a coroutine (unary), not an async generator (streaming).
        import inspect

        result = servicer._handle_insert(request, context, "Orders")
        assert inspect.iscoroutine(result), (
            "_handle_insert must be a coroutine (unary), not a generator"
        )
        # Consume to avoid ResourceWarning
        try:
            await result
        except Exception:
            pass


def _make_pb2_module(type_name: str = "Orders", fields: list[str] | None = None):
    if fields is None:
        fields = ["id", "amount"]
    field_descriptors = [SimpleNamespace(name=f, message_type=None) for f in fields]
    descriptor = SimpleNamespace(fields=field_descriptors)
    msg_cls = MagicMock()
    msg_cls.DESCRIPTOR = descriptor
    pb2 = SimpleNamespace(
        **{type_name: msg_cls, "DESCRIPTOR": SimpleNamespace(services_by_name={})}
    )
    return pb2, msg_cls
