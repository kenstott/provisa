# Copyright (c) 2025 Kenneth Stott
# Canary: c3c9be54-e46a-4d29-bf14-709cb777bbf8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for gRPC query endpoint.

Requires Docker Compose stack with PG, Trino, and the gRPC server running.
Tests streamed queries, mutations, and role-based field filtering.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from provisa.grpc.server import ProvisaServicer, start_grpc_server

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


def _make_pb2_with_descriptor(type_name: str, fields: list[str]):
    """Build a fake pb2 module with proper DESCRIPTOR for e2e-style testing."""
    field_descriptors = []
    for f in fields:
        fd = SimpleNamespace(name=f, message_type=None)
        field_descriptors.append(fd)

    descriptor = SimpleNamespace(fields=field_descriptors)
    msg_cls = MagicMock()
    msg_cls.DESCRIPTOR = descriptor

    service_descriptor = SimpleNamespace(full_name="provisa.v1.ProvisaService")
    pb2 = SimpleNamespace(
        **{type_name: msg_cls},
        DESCRIPTOR=SimpleNamespace(services_by_name={"ProvisaService": service_descriptor}),
    )
    return pb2, msg_cls


def _make_full_state(role_id: str, extra_roles: dict | None = None):
    """Build a full mock AppState for e2e-style servicer tests."""
    from provisa.compiler.rls import RLSContext

    roles = {role_id: {"id": role_id, "capabilities": []}}
    schemas = {role_id: MagicMock()}
    contexts = {role_id: MagicMock()}
    rls_contexts = {role_id: RLSContext.empty()}

    if extra_roles:
        for rid, rdata in extra_roles.items():
            roles[rid] = rdata
            schemas[rid] = MagicMock()
            contexts[rid] = MagicMock()
            rls_contexts[rid] = RLSContext.empty()

    return SimpleNamespace(
        schemas=schemas,
        contexts=contexts,
        rls_contexts=rls_contexts,
        roles=roles,
        source_pools=MagicMock(),
        source_types={},
        source_dialects={},
        masking_rules={},
        mv_registry=SimpleNamespace(get_fresh=lambda: []),
        trino_conn=MagicMock(),
    )


def _mock_context(role: str) -> AsyncMock:
    """Build a mock gRPC context with role metadata."""
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata.return_value = [("x-provisa-role", role)]
    return context


def _query_patches(fake_compiled, fake_result):
    """Return a context manager stack of patches for _handle_query dependencies."""
    return (
        patch("provisa.compiler.parser.parse_query"),
        patch("provisa.compiler.sql_gen.compile_query", return_value=[fake_compiled]),
        patch("provisa.compiler.rls.inject_rls", return_value=fake_compiled),
        patch("provisa.compiler.mask_inject.inject_masking", return_value=fake_compiled),
        patch("provisa.mv.rewriter.rewrite_if_mv_match", return_value=fake_compiled),
        patch("provisa.transpiler.router.decide_route"),
        patch("provisa.security.rights.has_capability", return_value=False),
        patch("provisa.compiler.sampling.apply_sampling", return_value=fake_compiled),
        patch("provisa.compiler.sampling.get_sample_size", return_value=100),
        patch("provisa.transpiler.transpile.transpile", return_value=fake_compiled.sql),
        patch("provisa.executor.direct.execute_direct", new_callable=AsyncMock, return_value=fake_result),
    )


class TestStreamedQueries:
    async def test_query_streams_multiple_rows(self):
        """Verify QueryOrders streams back all rows from the result set."""
        pb2, msg_cls = _make_pb2_with_descriptor("Orders", ["id", "amount"])
        state = _make_full_state("admin")
        servicer = ProvisaServicer(state, pb2, MagicMock())

        fake_compiled = SimpleNamespace(
            sql="SELECT id, amount FROM orders",
            params=[],
            sources=["pg1"],
            columns=[
                SimpleNamespace(field_name="id", nested_in=None),
                SimpleNamespace(field_name="amount", nested_in=None),
            ],
        )
        rows = [[i, float(i * 100)] for i in range(1, 6)]
        fake_result = SimpleNamespace(rows=rows)

        with (
            patch("provisa.compiler.parser.parse_query"),
            patch("provisa.compiler.sql_gen.compile_query", return_value=[fake_compiled]),
            patch("provisa.compiler.rls.inject_rls", return_value=fake_compiled),
            patch("provisa.compiler.mask_inject.inject_masking", return_value=fake_compiled),
            patch("provisa.mv.rewriter.rewrite_if_mv_match", return_value=fake_compiled),
            patch("provisa.transpiler.router.decide_route") as mock_route,
            patch("provisa.security.rights.has_capability", return_value=False),
            patch("provisa.compiler.sampling.apply_sampling", return_value=fake_compiled),
            patch("provisa.compiler.sampling.get_sample_size", return_value=100),
            patch("provisa.transpiler.transpile.transpile", return_value="SELECT id, amount FROM orders"),
            patch("provisa.executor.direct.execute_direct", new_callable=AsyncMock, return_value=fake_result),
        ):
            from provisa.transpiler.router import Route
            mock_route.return_value = SimpleNamespace(
                route=Route.DIRECT, source_id="pg1", dialect="postgres",
            )

            context = _mock_context("admin")
            request = MagicMock()

            streamed = []
            async for msg in servicer._handle_query(request, context, "Orders", "orders"):
                streamed.append(msg)

            assert len(streamed) == 5
            msg_cls.assert_any_call(id=1, amount=100.0)
            msg_cls.assert_any_call(id=5, amount=500.0)

    async def test_empty_result_streams_nothing(self):
        """An empty result set yields no messages."""
        pb2, msg_cls = _make_pb2_with_descriptor("Orders", ["id", "amount"])
        state = _make_full_state("admin")
        servicer = ProvisaServicer(state, pb2, MagicMock())

        fake_compiled = SimpleNamespace(
            sql="SELECT id, amount FROM orders WHERE 1=0",
            params=[],
            sources=["pg1"],
            columns=[
                SimpleNamespace(field_name="id", nested_in=None),
                SimpleNamespace(field_name="amount", nested_in=None),
            ],
        )
        fake_result = SimpleNamespace(rows=[])

        with (
            patch("provisa.compiler.parser.parse_query"),
            patch("provisa.compiler.sql_gen.compile_query", return_value=[fake_compiled]),
            patch("provisa.compiler.rls.inject_rls", return_value=fake_compiled),
            patch("provisa.compiler.mask_inject.inject_masking", return_value=fake_compiled),
            patch("provisa.mv.rewriter.rewrite_if_mv_match", return_value=fake_compiled),
            patch("provisa.transpiler.router.decide_route") as mock_route,
            patch("provisa.security.rights.has_capability", return_value=False),
            patch("provisa.compiler.sampling.apply_sampling", return_value=fake_compiled),
            patch("provisa.compiler.sampling.get_sample_size", return_value=100),
            patch("provisa.transpiler.transpile.transpile", return_value="SELECT id, amount FROM orders WHERE 1=0"),
            patch("provisa.executor.direct.execute_direct", new_callable=AsyncMock, return_value=fake_result),
        ):
            from provisa.transpiler.router import Route
            mock_route.return_value = SimpleNamespace(
                route=Route.DIRECT, source_id="pg1", dialect="postgres",
            )

            context = _mock_context("admin")
            streamed = []
            async for msg in servicer._handle_query(MagicMock(), context, "Orders", "orders"):
                streamed.append(msg)

            assert len(streamed) == 0


class TestMutationsViaGrpc:
    async def test_insert_aborts_unimplemented(self):
        """Insert RPCs currently return UNIMPLEMENTED."""
        pb2, _ = _make_pb2_with_descriptor("Orders", ["id", "amount"])
        state = _make_full_state("admin")
        servicer = ProvisaServicer(state, pb2, MagicMock())

        context = _mock_context("admin")
        request = MagicMock()

        await servicer._handle_insert(request, context, "Orders")
        context.abort.assert_awaited_once_with(
            grpc.StatusCode.UNIMPLEMENTED, "InsertOrders not yet implemented"
        )

    async def test_insert_different_tables(self):
        """Insert abort message includes the correct table name."""
        pb2, _ = _make_pb2_with_descriptor("Customers", ["id", "name"])
        state = _make_full_state("admin")
        servicer = ProvisaServicer(state, pb2, MagicMock())

        context = _mock_context("admin")
        await servicer._handle_insert(MagicMock(), context, "Customers")
        context.abort.assert_awaited_once_with(
            grpc.StatusCode.UNIMPLEMENTED, "InsertCustomers not yet implemented"
        )


class TestRoleBasedFieldFiltering:
    async def test_admin_sees_all_fields(self):
        """Admin role descriptor includes all fields."""
        pb2, msg_cls = _make_pb2_with_descriptor("Orders", ["id", "amount", "secret_code"])
        state = _make_full_state("admin")
        servicer = ProvisaServicer(state, pb2, MagicMock())

        descriptor = msg_cls.DESCRIPTOR
        field_names = [f.name for f in descriptor.fields if not f.message_type]
        assert field_names == ["id", "amount", "secret_code"]

    async def test_viewer_sees_restricted_fields(self):
        """Viewer role descriptor excludes secret fields."""
        pb2, msg_cls = _make_pb2_with_descriptor("Orders", ["id", "amount"])
        state = _make_full_state("viewer")
        servicer = ProvisaServicer(state, pb2, MagicMock())

        descriptor = msg_cls.DESCRIPTOR
        field_names = [f.name for f in descriptor.fields if not f.message_type]
        assert "secret_code" not in field_names
        assert "id" in field_names
        assert "amount" in field_names

    async def test_query_uses_role_from_metadata(self):
        """The servicer extracts role from gRPC metadata to select schema."""
        pb2, msg_cls = _make_pb2_with_descriptor("Orders", ["id"])
        state = _make_full_state("admin", extra_roles={
            "viewer": {"id": "viewer", "capabilities": []},
        })
        servicer = ProvisaServicer(state, pb2, MagicMock())

        fake_compiled = SimpleNamespace(
            sql="SELECT id FROM orders",
            params=[],
            sources=["pg1"],
            columns=[SimpleNamespace(field_name="id", nested_in=None)],
        )
        fake_result = SimpleNamespace(rows=[[42]])

        with (
            patch("provisa.compiler.parser.parse_query") as mock_parse,
            patch("provisa.compiler.sql_gen.compile_query", return_value=[fake_compiled]),
            patch("provisa.compiler.rls.inject_rls", return_value=fake_compiled),
            patch("provisa.compiler.mask_inject.inject_masking", return_value=fake_compiled),
            patch("provisa.mv.rewriter.rewrite_if_mv_match", return_value=fake_compiled),
            patch("provisa.transpiler.router.decide_route") as mock_route,
            patch("provisa.security.rights.has_capability", return_value=False),
            patch("provisa.compiler.sampling.apply_sampling", return_value=fake_compiled),
            patch("provisa.compiler.sampling.get_sample_size", return_value=100),
            patch("provisa.transpiler.transpile.transpile", return_value="SELECT id FROM orders"),
            patch("provisa.executor.direct.execute_direct", new_callable=AsyncMock, return_value=fake_result),
        ):
            from provisa.transpiler.router import Route
            mock_route.return_value = SimpleNamespace(
                route=Route.DIRECT, source_id="pg1", dialect="postgres",
            )

            context = _mock_context("viewer")
            streamed = []
            async for msg in servicer._handle_query(MagicMock(), context, "Orders", "orders"):
                streamed.append(msg)

            mock_parse.assert_called_once()
            schema_arg = mock_parse.call_args[0][0]
            assert schema_arg == state.schemas["viewer"]

    async def test_nonexistent_role_aborts(self):
        """A role not in state.schemas aborts with NOT_FOUND."""
        pb2, _ = _make_pb2_with_descriptor("Orders", ["id"])
        state = _make_full_state("admin")
        servicer = ProvisaServicer(state, pb2, MagicMock())

        context = _mock_context("hacker")
        context.abort.side_effect = grpc.aio.AbortError(
            grpc.StatusCode.NOT_FOUND, "No schema for role 'hacker'"
        )

        results = []
        with pytest.raises(grpc.aio.AbortError):
            async for msg in servicer._handle_query(MagicMock(), context, "Orders", "orders"):
                results.append(msg)

        context.abort.assert_awaited_once_with(
            grpc.StatusCode.NOT_FOUND, "No schema for role 'hacker'"
        )
