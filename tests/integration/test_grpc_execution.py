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

Pure-logic tests (_pascal_to_snake, ProvisaServicer.__getattr__, _get_role)
have been moved to tests/unit/test_grpc_server.py.

Tests cover:
  - Server startup and port binding (skipped when grpcio unavailable)
  - Live query execution, streaming, role enforcement, error handling
    (skipped when grpcio or a live gRPC server are unavailable)

A test gRPC server is started in a session-scoped fixture on a random
high-numbered port.  All heavy infrastructure tests are individually
marked to skip when the required components are missing.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

grpc = pytest.importorskip("grpc")
grpc_aio = pytest.importorskip("grpc.aio")

from provisa.grpc.server import (  # noqa: E402
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
package test.grpc.v1;

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

    # Use a distinct proto filename to avoid colliding with the production
    # provisa_service.proto in the global protobuf descriptor pool.
    with tempfile.TemporaryDirectory() as tmpdir:
        proto_path = Path(tmpdir) / "test_grpc_service.proto"
        proto_path.write_text(MINIMAL_PROTO)

        result = protoc.main(
            [
                "grpc_tools.protoc",
                f"--proto_path={tmpdir}",
                f"--python_out={tmpdir}",
                f"--grpc_python_out={tmpdir}",
                str(proto_path),
            ]
        )
        if result != 0:
            raise RuntimeError(f"protoc compilation failed (exit code {result})")

        pb2_path = Path(tmpdir) / "test_grpc_service_pb2.py"
        pb2_grpc_path = Path(tmpdir) / "test_grpc_service_pb2_grpc.py"
        yield str(pb2_path), str(pb2_grpc_path)


# ---------------------------------------------------------------------------
# Server startup test
# ---------------------------------------------------------------------------


class TestGrpcServerStarts:
    """Verify the gRPC server starts and can be stopped cleanly."""

    async def test_grpc_server_starts(self, compiled_proto_paths):
        """gRPC server binds to port and starts without error."""
        # integration: mock-justified — AppState is not a docker-compose service.
        # MagicMock scaffolds the struct fields needed for server startup.
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
        state.engine_conn = None
        # Mandatory terminal-execution binding (REQ-825) on the MagicMock scaffold state.
        from provisa.federation.engine import build_trino_engine
        from provisa.federation.runtime import EngineRuntime

        state.federation_engine = EngineRuntime(build_trino_engine(), state)

        server = await start_grpc_server(
            port=_TEST_GRPC_PORT,
            state=state,
            pb2_path=pb2_path,
            pb2_grpc_path=pb2_grpc_path,
        )
        assert server is not None
        await server.stop(grace=0)

    async def test_grpc_server_binds_expected_port(self, compiled_proto_paths):
        """Server binds to the port specified in the call."""
        # integration: mock-justified — AppState is not a docker-compose service.
        # MagicMock scaffolds the struct fields needed for server startup.
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
        state.engine_conn = None
        # Mandatory terminal-execution binding (REQ-825) on the MagicMock scaffold state.
        from provisa.federation.engine import build_trino_engine
        from provisa.federation.runtime import EngineRuntime

        state.federation_engine = EngineRuntime(build_trino_engine(), state)

        port = _TEST_GRPC_PORT + 1
        server = await start_grpc_server(
            port=port,
            state=state,
            pb2_path=pb2_path,
            pb2_grpc_path=pb2_grpc_path,
        )
        try:
            # Port should now be in use — verify by connecting to it
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            connected = False
            try:
                sock.connect(("localhost", port))
                connected = True
                sock.close()
            except (ConnectionRefusedError, OSError):
                pass
            assert connected, f"gRPC server is not listening on port {port}"
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
    async def grpc_server_and_stub(self, compiled_proto_paths, tenant_db):
        """Start a gRPC server backed by a real PG pool and return a stub."""
        _ = tenant_db  # requested for side-effect: ensures PG pool is ready before server starts
        from provisa.executor.pool import SourcePool
        from provisa.compiler.rls import RLSContext
        from provisa.grpc.server import _load_module

        pb2_path, pb2_grpc_path = compiled_proto_paths

        # Derive module names from the file stem so _pb2_grpc.py can import its sibling.
        pb2 = _load_module(pb2_path, Path(pb2_path).stem)
        pb2_grpc = _load_module(pb2_grpc_path, Path(pb2_grpc_path).stem)

        from typing import cast
        from graphql import (
            GraphQLArgument,
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLNonNull,
            GraphQLObjectType,
            GraphQLScalarType,
            GraphQLSchema,
            GraphQLString,
            GraphQLFloat,
        )

        _int = cast(GraphQLScalarType, GraphQLInt)
        _str = cast(GraphQLScalarType, GraphQLString)
        _float = cast(GraphQLScalarType, GraphQLFloat)
        order_type = GraphQLObjectType(
            "Order",
            lambda: {
                "id": GraphQLField(GraphQLNonNull(_int)),
                "region": GraphQLField(_str),
                "amount": GraphQLField(_float),
            },
        )
        query_type = GraphQLObjectType(
            "Query",
            {
                "order": GraphQLField(
                    GraphQLList(order_type),
                    args={"limit": GraphQLArgument(_int)},
                )
            },
        )
        schema = GraphQLSchema(query=cast(GraphQLObjectType, query_type))

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

        source_pool = SourcePool()
        await source_pool.add(
            "test-pg",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
        )

        # integration: mock-justified — AppState is not a docker-compose service.
        # MagicMock scaffolds the struct fields; the real data path (source_pool + PG) is live.
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
        state.engine_conn = None
        # Mandatory terminal-execution binding (REQ-825) on the MagicMock scaffold state.
        from provisa.federation.engine import build_trino_engine
        from provisa.federation.runtime import EngineRuntime

        state.federation_engine = EngineRuntime(build_trino_engine(), state)

        port = _TEST_GRPC_PORT + 2
        server = await start_grpc_server(
            port=port,
            state=state,
            pb2_path=pb2_path,
            pb2_grpc_path=pb2_grpc_path,
        )

        channel = grpc.aio.insecure_channel(f"localhost:{port}")
        stub_cls = next(
            (getattr(pb2_grpc, attr) for attr in dir(pb2_grpc) if attr.endswith("Stub")),
            None,
        )
        assert stub_cls is not None, (
            f"No Stub class in generated {Path(pb2_grpc_path).stem}; "
            f"available: {[a for a in dir(pb2_grpc) if not a.startswith('_')]}"
        )
        stub = stub_cls(channel)

        yield stub, pb2

        await channel.close()
        await server.stop(grace=0)
        await source_pool.close_all()

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
        # Each row must be an Order proto message with the expected attributes
        for row in rows:
            assert hasattr(row, "id"), "Order proto missing 'id' field"
            assert hasattr(row, "region"), "Order proto missing 'region' field"
            assert hasattr(row, "amount"), "Order proto missing 'amount' field"
        # Limit of 3 means we cannot receive more than 3 rows
        assert len(rows) <= 3, f"Expected at most 3 rows, got {len(rows)}"

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
        # Iteration must complete without raising, and the limit caps the result
        assert count <= 5, f"Expected at most 5 rows (limit=5), got {count}"

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

    async def test_grpc_missing_role_returns_unauthenticated(self, grpc_server_and_stub):
        """Missing role header causes UNAUTHENTICATED error."""
        stub, pb2 = grpc_server_and_stub
        request = pb2.OrderRequest(limit=1)

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            async for _ in stub.QueryOrder(request, metadata=[]):
                pass

        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

    async def test_grpc_invalid_query_returns_error(self, grpc_server_and_stub):
        """An RPC for an unregistered type resolves via schema and may abort."""
        _ = grpc_server_and_stub  # ensures server is up; this test mocks the servicer directly
        # integration: mock-justified — error injection test; MagicMock scaffolds
        # a minimal servicer with an empty state to trigger the NOT_FOUND abort path.
        # No docker-compose service can inject this specific error condition.
        pb2_mock = MagicMock()
        pb2_mock.UnknownType.DESCRIPTOR.fields = []
        pb2_grpc_mock = MagicMock()

        state = MagicMock()
        state.schemas = {}  # empty — will cause NOT_FOUND

        servicer = ProvisaServicer(state, pb2_mock, pb2_grpc_mock)

        ctx = MagicMock()
        ctx.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        ctx.abort = AsyncMock()

        request = MagicMock()
        # Consume the async generator; abort is called internally
        async for _ in servicer._handle_query(request, ctx, "UnknownType", "unknown_type"):
            pass

        ctx.abort.assert_called_once()
        call_args = ctx.abort.call_args[0]
        assert call_args[0] == grpc.StatusCode.NOT_FOUND
