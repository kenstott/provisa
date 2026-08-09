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

from provisa.grpc.server import ProvisaServicer, _pascal_to_snake, _rpc_role


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
    """Build a fake pb2 module with a message class and DESCRIPTOR."""
    if fields is None:
        fields = ["id", "amount"]

    from google.protobuf.descriptor import FieldDescriptor

    field_descriptors = []
    for f in fields:
        # ``type`` is what the value coercion switches on, so the fake carries protobuf's own
        # constant rather than omitting it — an int64 column coerces as an int64 here too.
        fd = SimpleNamespace(name=f, message_type=None, type=FieldDescriptor.TYPE_INT64)
        field_descriptors.append(fd)

    # A real protobuf descriptor exposes both, and the servicer looks a column up by name when
    # it builds a message — a fake with only ``fields`` tests a descriptor protobuf never hands it.
    descriptor = SimpleNamespace(
        fields=field_descriptors,
        fields_by_name={fd.name: fd for fd in field_descriptors},
    )
    msg_cls = MagicMock()
    msg_cls.DESCRIPTOR = descriptor

    pb2 = SimpleNamespace(
        **{type_name: msg_cls, "DESCRIPTOR": SimpleNamespace(services_by_name={})}
    )
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
        flight_client=None,
    )
    # Mandatory terminal-execution binding (REQ-825): bind the reference engine to the stub state.
    from provisa.federation.engine import build_trino_engine
    from provisa.federation.runtime import EngineRuntime

    state.federation_engine = EngineRuntime(build_trino_engine(), state)
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
        request.limit = 0

        from provisa.transpiler.router import Route

        # New pipeline seam: _handle_query lowers the request to a semantic SELECT, then
        # governs/routes/executes via provisa.pgwire._pipeline. Mock at that boundary.
        fake_plan = SimpleNamespace(route=Route.DIRECT, source_id="pg1")
        fake_result = SimpleNamespace(column_names=["id", "amount"], rows=[[1, 100.0], [2, 200.0]])

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

        ProvisaServicer(admin_state, admin_pb2, MagicMock())
        ProvisaServicer(viewer_state, viewer_pb2, MagicMock())

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
        assert context.abort.await_count == 1
        assert context.abort.call_args[0][0] == grpc.StatusCode.UNIMPLEMENTED
        assert context.abort.call_args[0][1] == "InsertOrders not yet implemented"


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_missing_role_metadata(self):
        """An RPC that names no role has none to run as, so the handler aborts UNAUTHENTICATED."""
        pb2, _ = _make_pb2_module("Orders", ["id"])
        servicer = ProvisaServicer(_make_state(), pb2, MagicMock())
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

        servicer = ProvisaServicer(state, pb2, MagicMock())
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


class _FakeSingularProxy:
    """Stands in for the sub-message accessor protobuf returns for a singular message field,
    supporting only the ``.CopyFrom`` call ``_dict_row_to_message`` makes on it."""

    def __init__(self, msg, key):
        self._msg = msg
        self._key = key

    def CopyFrom(self, other):
        self._msg._singular[self._key] = other


def _fake_field(name, message_type=None, repeated=False):
    from google.protobuf.descriptor import FieldDescriptor

    return SimpleNamespace(
        name=name,
        type=FieldDescriptor.TYPE_MESSAGE if message_type else FieldDescriptor.TYPE_STRING,
        message_type=message_type,
        label=FieldDescriptor.LABEL_REPEATED if repeated else FieldDescriptor.LABEL_OPTIONAL,
        LABEL_REPEATED=FieldDescriptor.LABEL_REPEATED,
    )


def _make_fake_msg_cls(name, scalar_fields=(), message_fields=None):
    """A minimal stand-in for a generated proto message class: real construction (unlike
    MagicMock, whose calls don't reflect kwargs), scalar fields stored verbatim, and
    CopyFrom/extend support for nested singular/repeated message fields — exactly what
    ``_dict_row_to_message`` (REQ-1405) exercises."""
    message_fields = message_fields or {}
    field_descs = {f: _fake_field(f) for f in scalar_fields}
    for field_name, (sub_cls, repeated) in message_fields.items():
        mt = SimpleNamespace(name=sub_cls.__name__, full_name=f"test.{sub_cls.__name__}")
        field_descs[field_name] = _fake_field(field_name, message_type=mt, repeated=repeated)

    class FakeMsg:
        DESCRIPTOR = SimpleNamespace(fields_by_name=field_descs)
        _message_fields = message_fields

        def __init__(self, **kwargs):
            self._scalars = dict(kwargs)
            self._repeated = {f: [] for f, (_sub, rep) in message_fields.items() if rep}
            self._singular = {f: None for f, (_sub, rep) in message_fields.items() if not rep}

        def __getattr__(self, item):
            if item in self.__dict__.get("_repeated", {}):
                return self._repeated[item]
            if item in self.__dict__.get("_singular", {}):
                return _FakeSingularProxy(self, item)
            raise AttributeError(item)

    FakeMsg.__name__ = name
    return FakeMsg


class TestDictRowToMessage:
    """REQ-1405: server.py's recursive dict -> proto message builder for nodes_sql rows."""

    def test_scalar_fields_assigned(self):
        User = _make_fake_msg_cls("User", scalar_fields=["name", "email"])
        pb2 = SimpleNamespace(User=User)
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        msg = servicer._dict_row_to_message(User, {"name": "Ada", "email": "ada@example.com"})

        assert msg._scalars == {"name": "Ada", "email": "ada@example.com"}

    def test_none_values_skipped(self):
        User = _make_fake_msg_cls("User", scalar_fields=["name", "email"])
        pb2 = SimpleNamespace(User=User)
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        msg = servicer._dict_row_to_message(User, {"name": "Ada", "email": None})

        assert msg._scalars == {"name": "Ada"}

    def test_unknown_dict_key_ignored(self):
        User = _make_fake_msg_cls("User", scalar_fields=["name"])
        pb2 = SimpleNamespace(User=User)
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        msg = servicer._dict_row_to_message(User, {"name": "Ada", "not_a_field": "x"})

        assert msg._scalars == {"name": "Ada"}

    def test_many_to_one_relation_nests_via_copy_from(self):
        User = _make_fake_msg_cls("User", scalar_fields=["name"])
        Inquiry = _make_fake_msg_cls(
            "Inquiry", scalar_fields=["status"], message_fields={"user": (User, False)}
        )
        pb2 = SimpleNamespace(Inquiry=Inquiry, User=User)
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        msg = servicer._dict_row_to_message(
            Inquiry, {"status": "open", "user": {"name": "Ada"}}
        )

        assert msg._scalars == {"status": "open"}
        assert isinstance(msg._singular["user"], User)
        assert msg._singular["user"]._scalars == {"name": "Ada"}

    def test_one_to_many_relation_nests_via_extend(self):
        Review = _make_fake_msg_cls("Review", scalar_fields=["comment"])
        User = _make_fake_msg_cls(
            "User", scalar_fields=["name"], message_fields={"reviews": (Review, True)}
        )
        pb2 = SimpleNamespace(User=User, Review=Review)
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        msg = servicer._dict_row_to_message(
            User,
            {"name": "Ada", "reviews": [{"comment": "great"}, {"comment": "ok"}]},
        )

        assert msg._scalars == {"name": "Ada"}
        assert [r._scalars["comment"] for r in msg._repeated["reviews"]] == ["great", "ok"]

    def test_relation_field_missing_from_pb2_module_skipped(self):
        # sub_cls resolution (getattr(self._pb2, field.message_type.name, None)) can miss if the
        # pb2 module doesn't expose the sub-message type under that exact name — must not raise.
        User = _make_fake_msg_cls("User", scalar_fields=["name"])
        Inquiry = _make_fake_msg_cls(
            "Inquiry", scalar_fields=["status"], message_fields={"user": (User, False)}
        )
        pb2 = SimpleNamespace(Inquiry=Inquiry)  # User deliberately omitted
        servicer = ProvisaServicer(MagicMock(), pb2, MagicMock())

        msg = servicer._dict_row_to_message(
            Inquiry, {"status": "open", "user": {"name": "Ada"}}
        )

        assert msg._scalars == {"status": "open"}
        assert msg._singular["user"] is None
