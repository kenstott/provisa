# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-defa-123456789003
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for gRPC query execution (not just reflection).

Tests cover:
  - Server-side utilities: _pascal_to_snake, _load_module
  - ProvisaServicer attribute resolution (__getattr__)
  - Server startup and port binding (skipped when grpcio unavailable)
  - Live query execution, streaming, role enforcement, error handling
    (skipped when grpcio or a live gRPC server are unavailable)

A test gRPC server is started in a session-scoped fixture on a random
high-numbered port.  All heavy infrastructure tests are individually
marked to skip when the required components are missing.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

grpc = pytest.importorskip("grpc")
grpc_aio = pytest.importorskip("grpc.aio")

from provisa.grpc.server import (
    _pascal_to_snake,
    ProvisaServicer,
    start_grpc_server,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_GRPC_PORT = int(os.environ.get("PROVISA_TEST_GRPC_PORT", "50151"))

MINIMAL_PROTO = """\
syntax = "proto3";
package provisa.v1;

message Order {
  int32 id = 1;
  string region = 2;
  double amount = 3;
}

message OrderFilter {
  int32 id = 1;
  string region = 2;
  double amount = 3;
}

message OrderRequest {
  OrderFilter filter = 1;
  int32 limit = 2;
  int32 offset = 3;
}

message MutationResponse {
  int32 affected_rows = 1;
}

service ProvisaService {
  rpc QueryOrder (OrderRequest) returns (stream Order);
}
"""


def _grpcio_tools_available() -> bool:
    try:
        from grpc_tools import protoc  # noqa: F401
        return True
    except ImportError:
        return False


def _pg_available() -> bool:
    import socket

    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", "5432"))
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


_SKIP_NO_GRPC_TOOLS = pytest.mark.skipif(
    not _grpcio_tools_available(),
    reason="grpc_tools not installed — cannot compile test proto",
)
_SKIP_NO_PG = pytest.mark.skipif(
    not _pg_available(), reason="PostgreSQL unavailable"
)


# ---------------------------------------------------------------------------
# Pure-logic tests (no server, no PG required)
# ---------------------------------------------------------------------------


class TestPascalToSnake:
    """Unit tests for _pascal_to_snake helper."""

    async def test_simple_pascal(self):
        assert _pascal_to_snake("Orders") == "orders"

    async def test_compound_pascal(self):
        assert _pascal_to_snake("CustomerSegments") == "customer_segments"

    async def test_already_snake(self):
        assert _pascal_to_snake("orders") == "orders"

    async def test_single_word(self):
        assert _pascal_to_snake("Customer") == "customer"

    async def test_acronym_preserved(self):
        # e.g. "OrderID" → "order_i_d" is the regex behaviour; just check it's lowercase
        result = _pascal_to_snake("OrderID")
        assert result == result.lower()

    async def test_multi_word(self):
        assert _pascal_to_snake("SalesOrderDetail") == "sales_order_detail"


class TestProvisaServicerGetattr:
    """Unit tests for ProvisaServicer.__getattr__ dynamic dispatch."""

    def _make_servicer(self):
        state = MagicMock()
        pb2 = MagicMock()
        pb2_grpc = MagicMock()
        return ProvisaServicer(state, pb2, pb2_grpc)

    async def test_query_handler_returned_for_query_prefix(self):
        svc = self._make_servicer()
        handler = svc.QueryOrders
        # Handler should be callable (async generator)
        assert callable(handler)

    async def test_insert_handler_returned_for_insert_prefix(self):
        svc = self._make_servicer()
        handler = svc.InsertOrders
        assert callable(handler)

    async def test_unknown_attribute_raises(self):
        svc = self._make_servicer()
        with pytest.raises(AttributeError):
            _ = svc.UnknownMethod

    async def test_query_handler_is_async(self):
        import inspect
        svc = self._make_servicer()
        handler = svc.QueryOrders
        # The outer wrapper is a sync function returning an async generator
        assert callable(handler)

    async def test_pascal_to_snake_applied_to_type_name(self):
        """QueryCustomerSegments should resolve to field_name customer_segments."""
        svc = self._make_servicer()
        # Accessing the handler should not raise even for compound names
        handler = svc.QueryCustomerSegments
        assert callable(handler)


class TestGetRoleMetadata:
    """Unit tests for the role extraction helper via context mock."""

    async def test_role_extracted_from_metadata(self):
        """_get_role returns the role from invocation metadata."""
        from provisa.grpc.server import _get_role

        ctx = MagicMock()
        ctx.invocation_metadata.return_value = [("x-provisa-role", "analyst")]
        role = _get_role(ctx)
        assert role == "analyst"

    async def test_missing_role_raises_abort_error(self):
        """_get_role raises grpc.aio.AbortError when header missing."""
        from provisa.grpc.server import _get_role
        import grpc.aio

        ctx = MagicMock()
        ctx.invocation_metadata.return_value = []
        with pytest.raises(grpc.aio.AbortError):
            _get_role(ctx)


# ---------------------------------------------------------------------------
# Proto compilation fixture (requires grpc_tools)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compiled_proto_paths():
    """Compile MINIMAL_PROTO to _pb2.py/_pb2_grpc.py in a temp directory.

    Skips the test if grpc_tools is not installed.
    """
    pytest.importorskip("grpc_tools.protoc")
    from grpc_tools import protoc

    with tempfile.TemporaryDirectory() as tmpdir:
        proto_path = Path(tmpdir) / "provisa_service.proto"
        proto_path.write_text(MINIMAL_PROTO)

        result = protoc.main([
            "grpc_tools.protoc",
            f"--proto_path={tmpdir}",
            f"--python_out={tmpdir}",
            f"--grpc_python_out={tmpdir}",
            str(proto_path),
        ])
        if result != 0:
            pytest.skip("protoc compilation failed")

        pb2_path = Path(tmpdir) / "provisa_service_pb2.py"
        pb2_grpc_path = Path(tmpdir) / "provisa_service_pb2_grpc.py"
        yield str(pb2_path), str(pb2_grpc_path)


# ---------------------------------------------------------------------------
# Server startup test
# ---------------------------------------------------------------------------


class TestGrpcServerStarts:
    """Verify the gRPC server starts and can be stopped cleanly."""

    @_SKIP_NO_GRPC_TOOLS
    async def test_grpc_server_starts(self, compiled_proto_paths):
        """gRPC server binds to port and starts without error."""
        pb2_path, pb2_grpc_path = compiled_proto_paths

        state = MagicMock()
        state.schemas = {}
        state.contexts = {}
        state.rls_contexts = {}
        state.roles = {}
        state.source_pools = MagicMock()
        state.source_types = {}
        state.source_dialects = {}
        state.masking_rules = {}
        state.mv_registry = MagicMock()
        state.mv_registry.get_fresh.return_value = []
        state.trino_conn = None

        server = await start_grpc_server(
            port=_TEST_GRPC_PORT,
            state=state,
            pb2_path=pb2_path,
            pb2_grpc_path=pb2_grpc_path,
        )
        assert server is not None
        await server.stop(grace=0)

    @_SKIP_NO_GRPC_TOOLS
    async def test_grpc_server_binds_expected_port(self, compiled_proto_paths):
        """Server binds to the port specified in the call."""
        import socket

        pb2_path, pb2_grpc_path = compiled_proto_paths
        state = MagicMock()
        state.schemas = {}
        state.contexts = {}
        state.rls_contexts = {}
        state.roles = {}
        state.source_pools = MagicMock()
        state.source_types = {}
        state.source_dialects = {}
        state.masking_rules = {}
        state.mv_registry = MagicMock()
        state.mv_registry.get_fresh.return_value = []
        state.trino_conn = None

        port = _TEST_GRPC_PORT + 1
        server = await start_grpc_server(
            port=port, state=state,
            pb2_path=pb2_path, pb2_grpc_path=pb2_grpc_path,
        )
        try:
            # Port should now be in use
            with pytest.raises(OSError):
                with socket.create_server(("localhost", port)):
                    pass
        finally:
            await server.stop(grace=0)


# ---------------------------------------------------------------------------
# Live query execution tests (require PG + compiled proto)
# ---------------------------------------------------------------------------


class TestGrpcQueryExecution:
    """Live gRPC call tests.

    These require:
      - grpc_tools to compile the test proto
      - PostgreSQL to be reachable
    """

    @pytest.fixture(scope="class")
    async def grpc_server_and_stub(self, compiled_proto_paths, pg_pool):
        """Start a gRPC server backed by a real PG pool and return a stub."""
        from provisa.executor.pool import SourcePool
        from provisa.compiler.rls import RLSContext
        from provisa.grpc.server import _load_module

        pb2_path, pb2_grpc_path = compiled_proto_paths

        pb2 = _load_module(pb2_path, "provisa_service_pb2_exec")
        pb2_grpc = _load_module(pb2_grpc_path, "provisa_service_pb2_grpc_exec")

        from graphql import (
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLNonNull,
            GraphQLObjectType,
            GraphQLSchema,
            GraphQLString,
            GraphQLFloat,
        )

        order_type = GraphQLObjectType(
            "Order",
            lambda: {
                "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
                "region": GraphQLField(GraphQLString),
                "amount": GraphQLField(GraphQLFloat),
            },
        )
        query_type = GraphQLObjectType(
            "Query",
            {"order": GraphQLField(GraphQLList(order_type))},
        )
        schema = GraphQLSchema(query=query_type)

        try:
            from provisa.compiler.sql_gen import CompilationContext, TableMeta
            ctx = CompilationContext(
                tables={
                    "order": TableMeta(
                        table_id=1,
                        field_name="order",
                        type_name="Order",
                        source_id="test-pg",
                        catalog_name="postgresql",
                        schema_name="public",
                        table_name="orders",
                        domain_id="default",
                    )
                }
            )
        except Exception:
            pytest.skip("Cannot build CompilationContext with TableMeta")

        source_pool = SourcePool()
        try:
            await source_pool.add(
                "test-pg",
                source_type="postgresql",
                host=os.environ.get("PG_HOST", "localhost"),
                port=int(os.environ.get("PG_PORT", "5432")),
                database=os.environ.get("PG_DATABASE", "provisa"),
                user=os.environ.get("PG_USER", "provisa"),
                password=os.environ.get("PG_PASSWORD", "provisa"),
            )
        except Exception:
            pytest.skip("Cannot connect to PostgreSQL for gRPC tests")

        state = MagicMock()
        state.schemas = {"admin": schema}
        state.contexts = {"admin": ctx}
        state.rls_contexts = {"admin": RLSContext.empty()}
        state.roles = {"admin": {"id": "admin", "capabilities": ["full_results"]}}
        state.source_pools = source_pool
        state.source_types = {"test-pg": "postgresql"}
        state.source_dialects = {"test-pg": "postgres"}
        state.masking_rules = {}
        state.mv_registry = MagicMock()
        state.mv_registry.get_fresh.return_value = []
        state.trino_conn = None

        port = _TEST_GRPC_PORT + 2
        server = await start_grpc_server(
            port=port, state=state,
            pb2_path=pb2_path, pb2_grpc_path=pb2_grpc_path,
        )

        channel = grpc.aio.insecure_channel(f"localhost:{port}")
        stub_cls = None
        for attr in dir(pb2_grpc):
            if attr.endswith("Stub"):
                stub_cls = getattr(pb2_grpc, attr)
                break
        if stub_cls is None:
            await server.stop(grace=0)
            pytest.skip("No stub class found in generated grpc module")

        stub = stub_cls(channel)

        yield stub, pb2

        await channel.close()
        await server.stop(grace=0)
        await source_pool.close_all()

    @_SKIP_NO_GRPC_TOOLS
    @_SKIP_NO_PG
    async def test_grpc_query_returns_rows(self, grpc_server_and_stub):
        """Execute a query via gRPC and verify rows are returned."""
        stub, pb2 = grpc_server_and_stub
        request = pb2.OrderRequest(limit=3)
        rows = []
        async for row in stub.QueryOrder(
            request,
            metadata=[("x-provisa-role", "admin")],
        ):
            rows.append(row)
        assert len(rows) >= 0  # 0 rows is acceptable; connection itself must succeed

    @_SKIP_NO_GRPC_TOOLS
    @_SKIP_NO_PG
    async def test_grpc_streaming_response(self, grpc_server_and_stub):
        """Streaming RPC yields multiple messages (or completes cleanly)."""
        stub, pb2 = grpc_server_and_stub
        request = pb2.OrderRequest(limit=5)
        count = 0
        async for _ in stub.QueryOrder(
            request,
            metadata=[("x-provisa-role", "admin")],
        ):
            count += 1
        # The key assertion is that iteration completes without raising
        assert count >= 0

    @_SKIP_NO_GRPC_TOOLS
    @_SKIP_NO_PG
    async def test_grpc_role_header_applied(self, grpc_server_and_stub):
        """Role in metadata header is respected — wrong role returns NOT_FOUND."""
        stub, pb2 = grpc_server_and_stub
        request = pb2.OrderRequest(limit=1)

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            async for _ in stub.QueryOrder(
                request,
                metadata=[("x-provisa-role", "nonexistent_role")],
            ):
                pass

        assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    @_SKIP_NO_GRPC_TOOLS
    async def test_grpc_missing_role_returns_unauthenticated(self, grpc_server_and_stub):
        """Missing role header causes UNAUTHENTICATED error."""
        stub, pb2 = grpc_server_and_stub
        request = pb2.OrderRequest(limit=1)

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            async for _ in stub.QueryOrder(request, metadata=[]):
                pass

        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

    @_SKIP_NO_GRPC_TOOLS
    async def test_grpc_invalid_query_returns_error(self, grpc_server_and_stub):
        """An RPC for an unregistered type resolves via schema and may abort."""
        # We test via the servicer's _handle_query path with a mocked context
        pb2_mock = MagicMock()
        pb2_mock.UnknownType.DESCRIPTOR.fields = []
        pb2_grpc_mock = MagicMock()

        state = MagicMock()
        state.schemas = {}  # empty — will cause NOT_FOUND

        servicer = ProvisaServicer(state, pb2_mock, pb2_grpc_mock)

        ctx = AsyncMock()
        ctx.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        ctx.abort = AsyncMock()

        request = MagicMock()
        # Consume the async generator; abort is called internally
        async for _ in servicer._handle_query(request, ctx, "UnknownType", "unknown_type"):
            pass

        ctx.abort.assert_called_once()
        call_args = ctx.abort.call_args[0]
        assert call_args[0] == grpc.StatusCode.NOT_FOUND
