# Copyright (c) 2026 Kenneth Stott
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

from provisa.executor.result import QueryResult
from provisa.grpc.server import ProvisaServicer, _nodes_field, _pascal_to_snake, _rpc_role
from tests.role_state import RoleState


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


class TestRpcRole:
    """On an unsecured deployment the metadata role is all there is (REQ-617).

    The secured path — where the interceptor's validated identity overrides the metadata — is
    covered in tests/unit/test_grpc_auth.py."""

    def test_extracts_role_from_metadata(self):
        assert _rpc_role({"x-provisa-role": "admin"}) == "admin"

    def test_decodes_binary_metadata(self):
        assert _rpc_role({"x-provisa-role": b"admin"}) == "admin"

    def test_missing_role_is_absent(self):
        assert _rpc_role({}) is None


def _make_pb2_module(type_name: str = "Orders", fields: list[str] | None = None):
    """A compiled service, as an organisation's is: ``type_name`` with an ``optional int64``
    field per name in ``fields``, and its ``{type_name}Batch``. The encoder writes a result's
    rows into its real messages (REQ-2194), so the handler is driven over real descriptors."""
    from provisa.grpc.descriptor import OrgDescriptor

    if fields is None:
        fields = ["id", "amount"]
    body = " ".join(f"optional int64 {f} = {n};" for n, f in enumerate(fields, 1))
    pb2 = OrgDescriptor(
        f"""syntax = "proto3";
package provisa.v1;
message {type_name} {{ {body} }}
message {type_name}Batch {{ repeated {type_name} rows = 1; }}
"""
    )
    return pb2, getattr(pb2, type_name)


def _rows(msg_cls, sent: list[bytes]) -> list[dict]:
    """The messages a query handler yielded (the encoder's bytes), each as {field: value}."""
    return [{f.name: v for f, v in msg_cls.FromString(m).ListFields()} for m in sent]


def _result(column_names: list[str], rows: list) -> QueryResult:
    """A buffered result as a terminal returns one: its rows reach the encoder as Arrow."""
    return QueryResult(rows=[tuple(r) for r in rows], column_names=column_names)


def _make_state(role_id: str = "admin", schema=None, ctx=None):
    """Build a minimal mock state for servicer tests."""
    from provisa.cache.store import NoopCacheStore
    from provisa.compiler.rls import RLSContext

    state = RoleState(
        org_id="default",
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
        flight_client=None,
        response_cache_store=NoopCacheStore(),  # AppState always holds a store (app.py)
    )
    # Mandatory terminal-execution binding (REQ-825): bind the reference engine to the stub state.
    from provisa.federation.engine import build_trino_engine
    from provisa.federation.runtime import EngineRuntime

    state.federation_engine = EngineRuntime(build_trino_engine(), state)
    return state


class TestServicerDynamicDispatch:
    def test_query_handler_returned(self):
        pb2, _ = _make_pb2_module()
        servicer = ProvisaServicer(MagicMock(), pb2)
        handler = servicer.QueryOrders
        assert callable(handler)

    def test_insert_handler_returned(self):
        pb2, _ = _make_pb2_module()
        servicer = ProvisaServicer(MagicMock(), pb2)
        handler = servicer.InsertOrders
        assert callable(handler)

    def test_unknown_attribute_raises(self):
        pb2, _ = _make_pb2_module()
        servicer = ProvisaServicer(MagicMock(), pb2)
        with pytest.raises(AttributeError, match="has no attribute"):
            _ = servicer.DoSomethingElse


class TestHandleQuery:
    @pytest.mark.asyncio
    async def test_request_to_sql_to_result_cache_route(self):
        """CACHE/API-routed requests still go through the general _execute_plan terminal."""
        pb2, msg_cls = _make_pb2_module("Orders", ["id", "amount"])
        state = _make_state()

        servicer = ProvisaServicer(state, pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()
        request.limit = 0

        from provisa.transpiler.router import Route

        # Pipeline seams: _handle_query lowers the request to GraphQL text and compiles it, then
        # governs/routes/executes via provisa.pgwire._pipeline. Mock at those boundaries.
        # cache_hit=None: routed to the API cache, not answered from the response cache.
        fake_plan = SimpleNamespace(
            route=Route.CACHE, source_id=None, cache_hit=None, warnings=[], materialize=None
        )
        fake_result = _result(["id", "amount"], [[1, 100.0], [2, 200.0]])

        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_graphql",
                return_value=("{ orders { id amount } }", {}),
            ),
            patch(
                "provisa.api.generated_plan.compile_generated_graphql",
                return_value=[SimpleNamespace(sql="SELECT id, amount FROM orders", params=[])],
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
            ) as mock_execute_plan,
        ):
            rows_yielded = []
            async for msg in servicer._handle_query(request, context, "Orders", "orders"):
                rows_yielded.append(msg)

            assert _rows(msg_cls, rows_yielded) == [
                {"id": 1, "amount": 100},
                {"id": 2, "amount": 200},
            ]
            mock_execute_plan.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_requests_offset_reaches_the_lowering(self):
        pb2, _ = _make_pb2_module("Orders", ["id", "amount"])
        servicer = ProvisaServicer(_make_state(), pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()
        request.limit = 10
        request.offset = 30

        from provisa.transpiler.router import Route

        fake_plan = SimpleNamespace(
            route=Route.CACHE, source_id=None, cache_hit=None, warnings=[], materialize=None
        )
        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_graphql",
                return_value=("{ orders { id amount } }", {}),
            ) as lowering,
            patch(
                "provisa.api.generated_plan.compile_generated_graphql",
                return_value=[SimpleNamespace(sql="SELECT id, amount FROM orders", params=[])],
            ),
            patch(
                "provisa.pgwire._pipeline._govern_and_route_compiled",
                new_callable=AsyncMock,
                return_value=fake_plan,
            ),
            patch(
                "provisa.pgwire._pipeline._execute_plan",
                new_callable=AsyncMock,
                return_value=_result(["id", "amount"], []),
            ),
        ):
            _ = [m async for m in servicer._handle_query(request, context, "Orders", "orders")]

        assert lowering.call_args.kwargs["offset"] == 30

    @pytest.mark.asyncio
    async def test_query_batch_groups_rows_into_batch_messages(self):
        """REQ-1899: Query{Type}Batch streams {Type}Batch messages (repeated rows), grouping up
        to _GRPC_BATCH_ROWS rows per message, instead of one message per row."""
        from provisa.grpc.server import _GRPC_BATCH_ROWS

        pb2, _ = _make_pb2_module("Orders", ["id", "amount"])
        batch_cls = pb2.OrdersBatch
        state = _make_state()

        servicer = ProvisaServicer(state, pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()
        request.limit = 0
        # REQ-1899 (amended): unset (proto3 default 0) — falls back to the server's own
        # _GRPC_BATCH_ROWS default, which is what this test exercises. The client-opt-in path
        # (a non-zero batch_rows) is covered by test_query_batch_honors_client_batch_rows below.
        request.batch_rows = 0

        from provisa.transpiler.router import Route

        # cache_hit=None: routed to the API cache, not answered from the response cache.
        fake_plan = SimpleNamespace(
            route=Route.CACHE, source_id=None, cache_hit=None, warnings=[], materialize=None
        )
        # One more row than _GRPC_BATCH_ROWS so two batch messages are emitted: a full one, then
        # a one-row remainder — proves both the full-batch flush and the trailing partial flush.
        n_rows = _GRPC_BATCH_ROWS + 1
        fake_rows = [[i, float(i)] for i in range(n_rows)]
        fake_result = _result(["id", "amount"], fake_rows)

        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_graphql",
                return_value=("{ orders { id amount } }", {}),
            ),
            patch(
                "provisa.api.generated_plan.compile_generated_graphql",
                return_value=[SimpleNamespace(sql="SELECT id, amount FROM orders", params=[])],
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
            batches_yielded = []
            async for msg in servicer._handle_query_batch(request, context, "Orders"):
                batches_yielded.append(msg)

            sizes = [len(batch_cls.FromString(m).rows) for m in batches_yielded]
            assert sizes == [_GRPC_BATCH_ROWS, 1]

    @pytest.mark.asyncio
    async def test_query_batch_honors_client_batch_rows(self):
        """REQ-1899 (amended): a client-set request.batch_rows overrides the server's own
        _GRPC_BATCH_ROWS default — the batch size is a per-query client opt-in, not a fixed
        server-wide constant, since only the client knows which table it's asking about and
        whether a bigger batch is safe for that table's row width."""
        pb2, _ = _make_pb2_module("Orders", ["id", "amount"])
        batch_cls = pb2.OrdersBatch
        state = _make_state()

        servicer = ProvisaServicer(state, pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()
        request.limit = 0
        request.batch_rows = 3  # smaller than the server default, proves it's actually honored

        from provisa.transpiler.router import Route

        # cache_hit=None: routed to the API cache, not answered from the response cache.
        fake_plan = SimpleNamespace(
            route=Route.CACHE, source_id=None, cache_hit=None, warnings=[], materialize=None
        )
        fake_rows = [[i, float(i)] for i in range(7)]
        fake_result = _result(["id", "amount"], fake_rows)

        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_graphql",
                return_value=("{ orders { id amount } }", {}),
            ),
            patch(
                "provisa.api.generated_plan.compile_generated_graphql",
                return_value=[SimpleNamespace(sql="SELECT id, amount FROM orders", params=[])],
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
            batches_yielded = []
            async for msg in servicer._handle_query_batch(request, context, "Orders"):
                batches_yielded.append(msg)

            # 7 rows at batch_rows=3 -> batches of 3, 3, 1
            sizes = [len(batch_cls.FromString(m).rows) for m in batches_yielded]
            assert sizes == [3, 3, 1]

    @pytest.mark.asyncio
    async def test_direct_route_takes_fast_path(self):
        """REQ-1891: DIRECT route bypasses _execute_plan (and its residency/freshness machinery)
        entirely and calls execute_native directly, mirroring Flight SQL's DIRECT fast path."""
        pb2, msg_cls = _make_pb2_module("Orders", ["id", "amount"])
        state = _make_state()
        state.source_pools = MagicMock()
        state.source_pools.has.return_value = True
        # REQ-1898: this source's driver has no streaming DIRECT read, so the fast path must fall
        # through to the buffered execute_native terminal under test here — the streaming variant
        # (open_stream/fetch) is covered separately by test_direct_route_streams_when_source_supports_it.
        state.source_pools.supports_stream.return_value = False

        servicer = ProvisaServicer(state, pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()
        request.limit = 0

        from provisa.transpiler.router import Route

        fake_plan = SimpleNamespace(
            warnings=[],
            row_limit=None,  # as _Plan: no row limit bounds this read
            materialize=None,  # no delivery forced
            route=Route.DIRECT,
            source_id="pg1",
            sql="SELECT id, amount FROM orders",
            exec_params=None,
            audit_written=False,
            audit=MagicMock(),
            # The DIRECT terminal consults the response cache (REQ-544): an un-hinted plan.
            cache_opt_in=False,
            cache_hit=None,  # as _Plan: not answered before routing
            cache_missed=(),
            cache_ttl=None,
            response_cacheable=True,
            role_id="admin",
            table_ids=(),
            live_caps=(),  # REQ-1909: the capped live sources the pipeline binds at mint (none here)
            live_caps_org=None,
            tier_caps=None,
            tier_plan=None,
        )
        fake_result = _result(["id", "amount"], [[1, 100.0], [2, 200.0]])

        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_graphql",
                return_value=("{ orders { id amount } }", {}),
            ),
            patch(
                "provisa.api.generated_plan.compile_generated_graphql",
                return_value=[SimpleNamespace(sql="SELECT id, amount FROM orders", params=[])],
            ),
            patch(
                "provisa.pgwire._pipeline._govern_and_route_compiled",
                new_callable=AsyncMock,
                return_value=fake_plan,
            ),
            patch(
                "provisa.pgwire._pipeline._execute_plan",
                new_callable=AsyncMock,
            ) as mock_execute_plan,
            patch(
                "provisa.federation.query_residency.ensure_resident",
                new_callable=AsyncMock,
            ) as mock_ensure_resident,
            patch(
                "provisa.pgwire._pipeline.finalize_audit",
                new_callable=AsyncMock,
            ) as mock_finalize_audit,
            patch.object(
                state.federation_engine,
                "execute_native",
                new_callable=AsyncMock,
                return_value=fake_result,
            ) as mock_execute_native,
        ):
            rows_yielded = []
            async for msg in servicer._handle_query(request, context, "Orders", "orders"):
                rows_yielded.append(msg)

            assert _rows(msg_cls, rows_yielded) == [
                {"id": 1, "amount": 100},
                {"id": 2, "amount": 200},
            ]

            # The fast path was taken: _execute_plan (and hence ensure_resident's freshness-check
            # machinery, which only _execute_plan_in_org calls) is never reached on DIRECT route.
            mock_execute_plan.assert_not_awaited()
            mock_ensure_resident.assert_not_awaited()

            # The correct native-execute terminal was called instead, against the DIRECT plan's
            # own source/sql/params.
            mock_execute_native.assert_awaited_once_with(
                state.source_pools, "pg1", "SELECT id, amount FROM orders", []
            )
            mock_finalize_audit.assert_awaited_once_with(fake_plan, 200, state)

    @pytest.mark.asyncio
    async def test_direct_route_streams_when_source_supports_it(self):
        """REQ-1898: a DIRECT-route source whose driver supports streaming (supports_stream=True)
        must be read via open_stream/fetch in bounded batches, not fully buffered via
        execute_native — live-measured regression: large_scan (2M rows) via grpc took 190.7s vs
        flight's ~20.2s for the identical query, because execute_native materialized every row
        before sending the first message."""
        pb2, msg_cls = _make_pb2_module("Orders", ["id", "amount"])
        state = _make_state()
        state.source_pools = MagicMock()
        state.source_pools.has.return_value = True
        state.source_pools.supports_stream.return_value = True

        fake_stream = AsyncMock()
        fake_stream.column_names = ["id", "amount"]
        fake_stream.column_types = None
        fake_stream.fetch = AsyncMock(side_effect=[[[1, 100.0], [2, 200.0]], []])
        state.source_pools.open_stream = AsyncMock(return_value=fake_stream)

        servicer = ProvisaServicer(state, pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        request = MagicMock()
        request.limit = 0

        from provisa.transpiler.router import Route

        fake_plan = SimpleNamespace(
            warnings=[],
            row_limit=None,  # as _Plan: no row limit bounds this read
            materialize=None,  # no delivery forced
            route=Route.DIRECT,
            source_id="pg1",
            sql="SELECT id, amount FROM orders",
            exec_params=None,
            audit_written=False,
            audit=MagicMock(),
            # The DIRECT terminal consults the response cache (REQ-544): an un-hinted plan.
            cache_opt_in=False,
            cache_hit=None,  # as _Plan: not answered before routing
            cache_missed=(),
            cache_ttl=None,
            response_cacheable=True,
            role_id="admin",
            table_ids=(),
            live_caps=(),  # REQ-1909: the capped live sources the pipeline binds at mint (none here)
            live_caps_org=None,
            tier_caps=None,
            tier_plan=None,
        )

        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_graphql",
                return_value=("{ orders { id amount } }", {}),
            ),
            patch(
                "provisa.api.generated_plan.compile_generated_graphql",
                return_value=[SimpleNamespace(sql="SELECT id, amount FROM orders", params=[])],
            ),
            patch(
                "provisa.pgwire._pipeline._govern_and_route_compiled",
                new_callable=AsyncMock,
                return_value=fake_plan,
            ),
            patch(
                "provisa.pgwire._pipeline._execute_plan",
                new_callable=AsyncMock,
            ) as mock_execute_plan,
            patch(
                "provisa.pgwire._pipeline.finalize_audit",
                new_callable=AsyncMock,
            ) as mock_finalize_audit,
            patch.object(
                state.federation_engine,
                "execute_native",
                new_callable=AsyncMock,
            ) as mock_execute_native,
        ):
            rows_yielded = []
            async for msg in servicer._handle_query(request, context, "Orders", "orders"):
                rows_yielded.append(msg)

            assert _rows(msg_cls, rows_yielded) == [
                {"id": 1, "amount": 100},
                {"id": 2, "amount": 200},
            ]

            # The streaming terminal was used, not the fully-buffering one.
            state.source_pools.open_stream.assert_awaited_once_with(
                "pg1", "SELECT id, amount FROM orders", []
            )
            mock_execute_native.assert_not_awaited()
            mock_execute_plan.assert_not_awaited()
            fake_stream.close.assert_awaited_once()
            mock_finalize_audit.assert_awaited_once_with(fake_plan, 200, state)

    @pytest.mark.asyncio
    async def test_unknown_role_aborts(self):
        """Request with unknown role should abort with NOT_FOUND."""
        pb2, _ = _make_pb2_module()
        state = _make_state(role_id="admin")

        servicer = ProvisaServicer(state, pb2)
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

        ProvisaServicer(admin_state, admin_pb2)
        ProvisaServicer(viewer_state, viewer_pb2)

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
        servicer = ProvisaServicer(MagicMock(), pb2)

        context = AsyncMock(spec=grpc.aio.ServicerContext)
        request = MagicMock()

        await servicer._handle_insert(request, context, "Orders")
        assert context.abort.await_count == 1
        assert context.abort.call_args[0][0] == grpc.StatusCode.UNIMPLEMENTED
        assert context.abort.call_args[0][1] == "InsertOrders not yet implemented"


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_missing_role_metadata(self):
        """An RPC that names no role has none to run as, so the handler aborts UNAUTHENTICATED."""
        pb2, _ = _make_pb2_module("Orders", ["id"])
        servicer = ProvisaServicer(_make_state(), pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = []

        rows = [m async for m in servicer._handle_query(MagicMock(), context, "Orders", "orders")]

        assert rows == []
        context.abort.assert_awaited_once_with(
            grpc.StatusCode.UNAUTHENTICATED, "Missing x-provisa-role metadata"
        )

    @pytest.mark.asyncio
    async def test_unknown_message_type_aborts(self):
        """If pb2 module lacks the message type, abort with INTERNAL."""
        pb2 = SimpleNamespace(DESCRIPTOR=SimpleNamespace(services_by_name={}))
        state = _make_state()

        servicer = ProvisaServicer(state, pb2)
        context = AsyncMock(spec=grpc.aio.ServicerContext)
        context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
        context.abort.side_effect = grpc.aio.AbortError(
            grpc.StatusCode.INTERNAL, "Unknown message type Nonexistent"
        )

        request = MagicMock()
        request.limit = 0
        results = []
        with pytest.raises(grpc.aio.AbortError):
            async for msg in servicer._handle_query(request, context, "Nonexistent", "nonexistent"):
                results.append(msg)

        context.abort.assert_awaited_once_with(
            grpc.StatusCode.INTERNAL, "Unknown message type Nonexistent"
        )


_NODES = None


def _nodes_service():
    """Inquiry -> User (many-to-one), User -> Review (one-to-many), and their group rows."""
    global _NODES
    if _NODES is None:
        from provisa.grpc.descriptor import OrgDescriptor

        _NODES = OrgDescriptor(
            """syntax = "proto3";
package provisa.v1;
message Review { optional string comment = 1; }
message User { optional string name = 1; optional string email = 2; repeated Review reviews = 3; }
message Inquiry { optional string status = 1; User user = 2; }
message UserGroupByRow { string group_key = 1; repeated User nodes = 3; }
message InquiryGroupByRow { string group_key = 1; repeated Inquiry nodes = 3; }
"""
        )
    return _NODES


def _node(type_name: str, node: dict):
    """A group's one node as the encoder writes it into its group row (REQ-1405)."""
    pb2 = _nodes_service()
    group = getattr(pb2, f"{type_name}GroupByRow")
    (built,) = group.FromString(_nodes_field(group, getattr(pb2, type_name), [node])).nodes
    return built


class TestGroupNodes:
    """REQ-1405: a group's node -- the nested object its nodes column holds -- as a {Type}
    message, written by the encoder (formerly server._dict_row_to_message)."""

    def test_scalar_fields_assigned(self):
        msg = _node("User", {"name": "Ada", "email": "ada@example.com"})
        assert (msg.name, msg.email) == ("Ada", "ada@example.com")

    def test_none_values_skipped(self):
        msg = _node("User", {"name": "Ada", "email": None})
        assert msg.name == "Ada" and not msg.HasField("email")

    def test_unknown_dict_key_ignored(self):
        msg = _node("User", {"name": "Ada", "not_a_field": "x"})
        assert [f.name for f, _ in msg.ListFields()] == ["name"]

    def test_many_to_one_relation_nests(self):
        msg = _node("Inquiry", {"status": "open", "user": {"name": "Ada"}})
        assert msg.status == "open" and msg.user.name == "Ada"

    def test_one_to_many_relation_nests(self):
        msg = _node("User", {"name": "Ada", "reviews": [{"comment": "great"}, {"comment": "ok"}]})
        assert msg.name == "Ada"
        assert [r.comment for r in msg.reviews] == ["great", "ok"]

    def test_a_relation_that_is_not_an_object_is_skipped(self):
        # a many-to-one relation is an object; a list or a scalar under its name is not one and
        # is left out, never an error
        msg = _node("Inquiry", {"status": "open", "user": [{"name": "Ada"}]})
        assert msg.status == "open" and not msg.HasField("user")
        msg = _node("Inquiry", {"status": "open", "user": "Ada"})
        assert not msg.HasField("user")
