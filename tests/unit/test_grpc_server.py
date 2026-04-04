# Copyright (c) 2025 Kenneth Stott
# Canary: df862196-ab91-4bcc-9e17-7cc756cf8134
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for gRPC server servicer logic."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from provisa.grpc.server import ProvisaServicer, _pascal_to_snake, _get_role


class TestPascalToSnake:
    def test_simple(self):
        assert _pascal_to_snake("Orders") == "orders"

    def test_two_words(self):
        assert _pascal_to_snake("CustomerSegments") == "customer_segments"

    def test_single_char(self):
        assert _pascal_to_snake("A") == "a"

    def test_acronym_adjacent(self):
        # Regex only splits on lowercase→uppercase boundary
        assert _pascal_to_snake("APIUsers") == "apiusers"


class TestGetRole:
    @pytest.mark.asyncio
    async def test_extracts_role_from_metadata(self):
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        role = _get_role(context)
        assert role == "admin"

    @pytest.mark.asyncio
    async def test_missing_role_raises(self):
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = []
        with pytest.raises(grpc.aio.AbortError):
            _get_role(context)


def _make_pb2_module(type_name: str = "Orders", fields: list[str] | None = None):
    """Build a fake pb2 module with a message class and DESCRIPTOR."""
    if fields is None:
        fields = ["id", "amount"]

    field_descriptors = []
    for f in fields:
        fd = SimpleNamespace(name=f, message_type=None)
        field_descriptors.append(fd)

    descriptor = SimpleNamespace(fields=field_descriptors)
    msg_cls = MagicMock()
    msg_cls.DESCRIPTOR = descriptor

    pb2 = SimpleNamespace(**{type_name: msg_cls, "DESCRIPTOR": SimpleNamespace(services_by_name={})})
    return pb2, msg_cls


def _make_state(role_id: str = "admin", schema=None, ctx=None):
    """Build a minimal mock state for servicer tests."""
    from provisa.compiler.rls import RLSContext

    state = SimpleNamespace(
        schemas={role_id: schema or MagicMock()},
        contexts={role_id: ctx or MagicMock()},
        rls_contexts={role_id: RLSContext.empty()},
        roles={role_id: {"id": role_id, "capabilities": []}},
        source_pools=MagicMock(),
        source_types={},
        source_dialects={},
        masking_rules={},
        mv_registry=SimpleNamespace(get_fresh=lambda: []),
        trino_conn=MagicMock(),
    )
    return state


class TestServicerDynamicDispatch:
    def test_query_handler_returned(self):
        pb2, _ = _make_pb2_module()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        handler = servicer.QueryOrders
        assert callable(handler)

    def test_insert_handler_returned(self):
        pb2, _ = _make_pb2_module()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        handler = servicer.InsertOrders
        assert callable(handler)

    def test_unknown_attribute_raises(self):
        pb2, _ = _make_pb2_module()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())
        with pytest.raises(AttributeError, match="has no attribute"):
            _ = servicer.DoSomethingElse


class TestHandleQuery:
    @pytest.mark.asyncio
    async def test_request_to_sql_to_result(self):
        """Test the full request -> SQL -> result flow with mocks."""
        pb2, msg_cls = _make_pb2_module("Orders", ["id", "amount"])
        state = _make_state()

        servicer = ProvisaServicer(state, pb2, MagicMock())
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()

        fake_compiled = SimpleNamespace(
            sql="SELECT id, amount FROM orders",
            params=[],
            sources=["pg1"],
            columns=[
                SimpleNamespace(field_name="id", nested_in=None),
                SimpleNamespace(field_name="amount", nested_in=None),
            ],
        )
        fake_result = SimpleNamespace(rows=[[1, 100.0], [2, 200.0]])

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
            patch("provisa.transpiler.transpile.transpile", return_value="SELECT id, amount FROM orders"),
            patch("provisa.executor.direct.execute_direct", new_callable=AsyncMock, return_value=fake_result),
        ):
            from provisa.transpiler.router import Route
            mock_route.return_value = SimpleNamespace(route=Route.DIRECT, source_id="pg1", dialect="postgres")

            rows_yielded = []
            async for msg in servicer._handle_query(request, context, "Orders", "orders"):
                rows_yielded.append(msg)

            assert len(rows_yielded) == 2
            msg_cls.assert_any_call(id=1, amount=100.0)
            msg_cls.assert_any_call(id=2, amount=200.0)

    @pytest.mark.asyncio
    async def test_unknown_role_aborts(self):
        """Request with unknown role should abort with NOT_FOUND."""
        pb2, _ = _make_pb2_module()
        state = _make_state(role_id="admin")

        servicer = ProvisaServicer(state, pb2, MagicMock())
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "unknown")]
        context.abort.side_effect = grpc.aio.AbortError(
            grpc.StatusCode.NOT_FOUND, "No schema for role 'unknown'"
        )

        request = MagicMock()
        results = []
        with pytest.raises(grpc.aio.AbortError):
            async for msg in servicer._handle_query(request, context, "Orders", "orders"):
                results.append(msg)

        context.abort.assert_awaited_once_with(
            grpc.StatusCode.NOT_FOUND, "No schema for role 'unknown'"
        )


class TestRoleEnforcement:
    @pytest.mark.asyncio
    async def test_different_roles_see_different_fields(self):
        """Verify that different role schemas produce different field sets."""
        admin_pb2, admin_msg = _make_pb2_module("Orders", ["id", "amount", "secret"])
        viewer_pb2, viewer_msg = _make_pb2_module("Orders", ["id", "amount"])

        admin_state = _make_state(role_id="admin")
        viewer_state = _make_state(role_id="viewer")

        admin_servicer = ProvisaServicer(admin_state, admin_pb2, MagicMock())
        viewer_servicer = ProvisaServicer(viewer_state, viewer_pb2, MagicMock())

        # Admin's descriptor exposes 3 fields
        admin_descriptor = admin_msg.DESCRIPTOR
        admin_fields = [f.name for f in admin_descriptor.fields if not f.message_type]
        assert len(admin_fields) == 3
        assert "secret" in admin_fields

        # Viewer's descriptor exposes only 2 fields
        viewer_descriptor = viewer_msg.DESCRIPTOR
        viewer_fields = [f.name for f in viewer_descriptor.fields if not f.message_type]
        assert len(viewer_fields) == 2
        assert "secret" not in viewer_fields


class TestHandleInsert:
    @pytest.mark.asyncio
    async def test_insert_returns_unimplemented(self):
        """Insert RPCs abort with UNIMPLEMENTED."""
        pb2, _ = _make_pb2_module()
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        context = AsyncMock(spec=grpc.aio.ServicerContext)
        request = MagicMock()

        await servicer._handle_insert(request, context, "Orders")
        context.abort.assert_awaited_once_with(
            grpc.StatusCode.UNIMPLEMENTED, "InsertOrders not yet implemented"
        )


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_missing_role_metadata(self):
        """Requests without x-provisa-role header should fail."""
        with pytest.raises(grpc.aio.AbortError):
            context = AsyncMock(spec=grpc.aio.ServicerContext)
            context.invocation_metadata.return_value = []
            _get_role(context)

    @pytest.mark.asyncio
    async def test_unknown_message_type_aborts(self):
        """If pb2 module lacks the message type, abort with INTERNAL."""
        pb2 = SimpleNamespace(DESCRIPTOR=SimpleNamespace(services_by_name={}))
        state = _make_state()

        servicer = ProvisaServicer(state, pb2, MagicMock())
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        context.abort.side_effect = grpc.aio.AbortError(
            grpc.StatusCode.INTERNAL, "Unknown message type Nonexistent"
        )

        request = MagicMock()
        results = []
        with pytest.raises(grpc.aio.AbortError):
            async for msg in servicer._handle_query(request, context, "Nonexistent", "nonexistent"):
                results.append(msg)

        context.abort.assert_awaited_once_with(
            grpc.StatusCode.INTERNAL, "Unknown message type Nonexistent"
        )
