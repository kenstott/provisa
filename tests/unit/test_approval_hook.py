# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0f38e3-6703-484d-891a-a6251a0bce1c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for ABAC approval hook."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from types import SimpleNamespace

from provisa.auth.approval_hook import (
    ApprovalHookConfig,
    ApprovalRequest,
    CircuitBreaker,
    FallbackPolicy,
    GrpcApprovalHook,
    HookType,
    WebhookApprovalHook,
    create_hook,
    load_approval_hook_config,
    should_check,
)


class TestConfigLoading:
    """REQ-247: build the hook config + scope dicts from provisa.yaml."""

    def test_no_block_returns_none(self):
        assert load_approval_hook_config(None) is None
        assert load_approval_hook_config({}) is None

    def test_block_maps_all_fields(self):
        cfg = load_approval_hook_config(
            {
                "type": "unix_socket",
                "socket_path": "/var/run/authz.sock",
                "timeout_ms": 250,
                "fallback": "allow",
                "scope": "all",
            }
        )
        assert cfg is not None
        assert cfg.type == HookType.UNIX_SOCKET
        assert cfg.socket_path == "/var/run/authz.sock"
        assert cfg.timeout_ms == 250
        assert cfg.fallback == FallbackPolicy.ALLOW
        assert cfg.scope == "all"

    def test_setup_populates_state(self):
        from provisa.api.app import AppState
        from provisa.api.app_loaders import _setup_approval_hook

        meta = SimpleNamespace(
            domain_id="sales", schema_name="public", table_name="orders", table_id=7
        )
        config = SimpleNamespace(
            auth=SimpleNamespace(
                approval_hook={"type": "webhook", "url": "http://h/e", "scope": ""}
            ),
            sources=[
                SimpleNamespace(id="pg1", approval_hook=True),
                SimpleNamespace(id="pg2", approval_hook=False),
            ],
            tables=[
                SimpleNamespace(
                    domain_id="sales", schema_name="public", table_name="orders", approval_hook=True
                )
            ],
        )
        st = AppState()
        st.config = config
        st.contexts = {"analyst": SimpleNamespace(tables={"orders": meta})}
        st.approval_hook = None
        st.table_approval_hooks = {}
        st.source_approval_hooks = {}

        _setup_approval_hook(st)

        assert st.approval_hook is not None
        assert st.source_approval_hooks == {"pg1": True}
        assert st.table_approval_hooks == {7: True}

    def test_setup_noop_without_block(self):
        from provisa.api.app import AppState
        from provisa.api.app_loaders import _setup_approval_hook

        st = AppState()
        st.config = SimpleNamespace(auth=SimpleNamespace(approval_hook=None), sources=[], tables=[])
        st.contexts = {}
        st.approval_hook = None
        _setup_approval_hook(st)
        assert st.approval_hook is None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_REQ = ApprovalRequest(
    user="alice",
    roles=["analyst"],
    tables=["orders", "customers"],
    columns=["id", "amount"],
    operation="query",
)


def _cfg(**overrides) -> ApprovalHookConfig:
    defaults = {
        "type": HookType.WEBHOOK,
        "url": "http://hook.test/evaluate",
        "timeout_ms": 500,
        "fallback": FallbackPolicy.DENY,
    }
    defaults.update(overrides)
    return ApprovalHookConfig(**defaults)


# ---------------------------------------------------------------------------
# Scoping tests
# ---------------------------------------------------------------------------


class TestShouldCheck:
    def test_scope_all_always_triggers(self):
        cfg = _cfg(scope="all")
        assert should_check([], [], cfg) is True

    def test_no_hooks_no_trigger(self):
        cfg = _cfg()
        assert should_check(["t1"], ["s1"], cfg) is False

    def test_table_hook_triggers(self):
        cfg = _cfg()
        assert should_check(["t1", "t2"], ["s1"], cfg, table_hooks={"t1": True}) is True

    def test_source_hook_triggers(self):
        cfg = _cfg()
        assert should_check(["t1"], ["s1", "s2"], cfg, source_hooks={"s2": True}) is True

    def test_table_hook_false_no_trigger(self):
        cfg = _cfg()
        assert should_check(["t1"], ["s1"], cfg, table_hooks={"t1": False}) is False

    def test_mixed_scoping(self):
        cfg = _cfg()
        assert (
            should_check(
                ["t1", "t2"],
                ["s1"],
                cfg,
                table_hooks={"t3": True},
                source_hooks={"s1": True},
            )
            is True
        )


# ---------------------------------------------------------------------------
# Webhook mock tests
# ---------------------------------------------------------------------------


class TestHookPosition:
    def test_hook_runs_after_governance(self):
        """REQ-203: the approval hook must be evaluated AFTER RLS/governance, not before."""
        import inspect

        from provisa.api.data import endpoint

        src = inspect.getsource(endpoint._prepare_compiled)
        gov_idx = src.index("apply_governance(semantic_sql_for_validation")
        hook_idx = src.index("approval_hook.evaluate")
        assert gov_idx < hook_idx


class TestWebhookApprovalHook:
    @pytest.mark.asyncio
    async def test_approved(self):
        hook = WebhookApprovalHook(_cfg())
        mock_resp = httpx.Response(
            200,
            json={"approved": True, "reason": "policy pass"},
            request=httpx.Request("POST", "http://hook.test/evaluate"),
        )
        with patch("provisa.auth.approval_hook.httpx.AsyncClient") as mock_cls:
            client = AsyncMock()
            client.post.return_value = mock_resp
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = client

            result = await hook.evaluate(_REQ)

        assert result.approved is True
        assert result.reason == "policy pass"
        client.post.assert_called_once()
        payload = client.post.call_args.kwargs["json"]
        assert payload["user"] == "alice"
        assert payload["tables"] == ["orders", "customers"]

    @pytest.mark.asyncio
    async def test_session_vars_serialized_and_additional_filter_parsed(self):
        """REQ-203: session_vars sent in payload; additional_filter read from response."""
        hook = WebhookApprovalHook(_cfg())
        req = ApprovalRequest(
            user="alice",
            roles=["analyst"],
            tables=["1"],
            columns=["id"],
            operation="query",
            session_vars={"tenant": "acme", "region": "us"},
        )
        mock_resp = httpx.Response(
            200,
            json={"approved": True, "reason": "", "additional_filter": "tenant = 'acme'"},
            request=httpx.Request("POST", "http://hook.test/evaluate"),
        )
        with patch("provisa.auth.approval_hook.httpx.AsyncClient") as mock_cls:
            client = AsyncMock()
            client.post.return_value = mock_resp
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = client

            result = await hook.evaluate(req)

        payload = client.post.call_args.kwargs["json"]
        assert payload["session_vars"] == {"tenant": "acme", "region": "us"}
        assert result.approved is True
        assert result.additional_filter == "tenant = 'acme'"

    @pytest.mark.asyncio
    async def test_denied(self):
        hook = WebhookApprovalHook(_cfg())
        mock_resp = httpx.Response(
            200,
            json={"approved": False, "reason": "PII access blocked"},
            request=httpx.Request("POST", "http://hook.test/evaluate"),
        )
        with patch("provisa.auth.approval_hook.httpx.AsyncClient") as mock_cls:
            client = AsyncMock()
            client.post.return_value = mock_resp
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = client

            result = await hook.evaluate(_REQ)

        assert result.approved is False
        assert result.reason == "PII access blocked"

    @pytest.mark.asyncio
    async def test_timeout_fallback_deny(self):
        hook = WebhookApprovalHook(_cfg(fallback=FallbackPolicy.DENY))
        with patch("provisa.auth.approval_hook.httpx.AsyncClient") as mock_cls:
            client = AsyncMock()
            client.post.side_effect = httpx.TimeoutException("timed out")
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = client

            result = await hook.evaluate(_REQ)

        assert result.approved is False
        assert "fallback deny" in result.reason

    @pytest.mark.asyncio
    async def test_timeout_fallback_allow(self):
        hook = WebhookApprovalHook(_cfg(fallback=FallbackPolicy.ALLOW))
        with patch("provisa.auth.approval_hook.httpx.AsyncClient") as mock_cls:
            client = AsyncMock()
            client.post.side_effect = httpx.TimeoutException("timed out")
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = client

            result = await hook.evaluate(_REQ)

        assert result.approved is True
        assert "fallback allow" in result.reason


# ---------------------------------------------------------------------------
# Circuit breaker tests
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker(threshold=3, cooldown_s=10.0)
        assert cb.is_open is False

    def test_opens_after_threshold(self):
        cb = CircuitBreaker(threshold=3, cooldown_s=10.0)
        for _ in range(3):
            cb.record_failure()
        assert cb.is_open is True

    def test_success_resets(self):
        cb = CircuitBreaker(threshold=3, cooldown_s=10.0)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        assert cb.is_open is False

    def test_half_open_after_cooldown(self):
        cb = CircuitBreaker(threshold=2, cooldown_s=0.0)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is False  # cooldown=0 -> immediately half-open
        assert cb.is_half_open is True

    @pytest.mark.asyncio
    async def test_circuit_breaker_blocks_calls(self):
        cfg = _cfg(
            circuit_breaker_threshold=2,
            circuit_breaker_cooldown_s=60.0,
            fallback=FallbackPolicy.DENY,
        )
        hook = WebhookApprovalHook(cfg)

        with patch("provisa.auth.approval_hook.httpx.AsyncClient") as mock_cls:
            client = AsyncMock()
            client.post.side_effect = httpx.ConnectError("refused")
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = client

            # Trip the breaker
            await hook.evaluate(_REQ)
            await hook.evaluate(_REQ)

        # Now circuit is open — next call should NOT make HTTP request
        with patch("provisa.auth.approval_hook.httpx.AsyncClient") as mock_cls2:
            client2 = AsyncMock()
            client2.__aenter__ = AsyncMock(return_value=client2)
            client2.__aexit__ = AsyncMock(return_value=False)
            mock_cls2.return_value = client2

            result = await hook.evaluate(_REQ)

        assert result.approved is False
        assert "circuit breaker open" in result.reason
        client2.post.assert_not_called()


# ---------------------------------------------------------------------------
# gRPC hook tests
# ---------------------------------------------------------------------------


class TestGrpcApprovalHook:
    def _grpc_cfg(self, **overrides) -> ApprovalHookConfig:
        defaults = {
            "type": HookType.GRPC,
            "url": "localhost:50099",
            "timeout_ms": 500,
            "fallback": FallbackPolicy.DENY,
        }
        defaults.update(overrides)
        return ApprovalHookConfig(**defaults)

    @pytest.mark.asyncio
    async def test_approved(self):
        hook = GrpcApprovalHook(self._grpc_cfg())

        mock_proto_resp = AsyncMock()
        mock_proto_resp.approved = True
        mock_proto_resp.reason = "policy pass"

        mock_stub = AsyncMock()
        mock_stub.Evaluate = AsyncMock(return_value=mock_proto_resp)

        mock_channel = AsyncMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=mock_stub),
        ):
            result = await hook.evaluate(_REQ)

        assert result.approved is True
        assert result.reason == "policy pass"
        mock_stub.Evaluate.assert_awaited_once()
        call_args = mock_stub.Evaluate.call_args
        proto_req = call_args.args[0]
        assert proto_req.user == "alice"
        assert list(proto_req.tables) == ["orders", "customers"]

    @pytest.mark.asyncio
    async def test_denied(self):
        hook = GrpcApprovalHook(self._grpc_cfg())

        mock_proto_resp = AsyncMock()
        mock_proto_resp.approved = False
        mock_proto_resp.reason = "PII access blocked"

        mock_stub = AsyncMock()
        mock_stub.Evaluate = AsyncMock(return_value=mock_proto_resp)

        with (
            patch("grpc.aio.insecure_channel", return_value=AsyncMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=mock_stub),
        ):
            result = await hook.evaluate(_REQ)

        assert result.approved is False
        assert result.reason == "PII access blocked"

    @pytest.mark.asyncio
    async def test_grpc_error_fallback_deny(self):
        import grpc

        hook = GrpcApprovalHook(self._grpc_cfg(fallback=FallbackPolicy.DENY))

        mock_stub = AsyncMock()
        mock_stub.Evaluate = AsyncMock(
            side_effect=grpc.aio.AioRpcError(
                grpc.StatusCode.UNAVAILABLE,
                None,
                None,
            )
        )

        with (
            patch("grpc.aio.insecure_channel", return_value=AsyncMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=mock_stub),
        ):
            result = await hook.evaluate(_REQ)

        assert result.approved is False
        assert "fallback deny" in result.reason

    @pytest.mark.asyncio
    async def test_grpc_error_fallback_allow(self):
        import grpc

        hook = GrpcApprovalHook(self._grpc_cfg(fallback=FallbackPolicy.ALLOW))

        mock_stub = AsyncMock()
        mock_stub.Evaluate = AsyncMock(
            side_effect=grpc.aio.AioRpcError(
                grpc.StatusCode.UNAVAILABLE,
                None,
                None,
            )
        )

        with (
            patch("grpc.aio.insecure_channel", return_value=AsyncMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=mock_stub),
        ):
            result = await hook.evaluate(_REQ)

        assert result.approved is True
        assert "fallback allow" in result.reason

    @pytest.mark.asyncio
    async def test_channel_reused_across_calls(self):
        hook = GrpcApprovalHook(self._grpc_cfg())

        mock_proto_resp = AsyncMock()
        mock_proto_resp.approved = True
        mock_proto_resp.reason = ""

        mock_stub = AsyncMock()
        mock_stub.Evaluate = AsyncMock(return_value=mock_proto_resp)

        with (
            patch("grpc.aio.insecure_channel", return_value=AsyncMock()) as mock_chan_factory,
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=mock_stub),
        ):
            await hook.evaluate(_REQ)
            await hook.evaluate(_REQ)

        # Channel created only once; stub reused
        mock_chan_factory.assert_called_once()
        assert mock_chan_factory.call_count == 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_blocks_after_failures(self):
        import grpc

        cfg = self._grpc_cfg(
            circuit_breaker_threshold=2,
            circuit_breaker_cooldown_s=60.0,
            fallback=FallbackPolicy.DENY,
        )
        hook = GrpcApprovalHook(cfg)

        mock_stub = AsyncMock()
        mock_stub.Evaluate = AsyncMock(
            side_effect=grpc.aio.AioRpcError(
                grpc.StatusCode.UNAVAILABLE,
                None,
                None,
            )
        )

        with (
            patch("grpc.aio.insecure_channel", return_value=AsyncMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=mock_stub),
        ):
            await hook.evaluate(_REQ)
            await hook.evaluate(_REQ)

        # Circuit now open — next call must not reach stub
        mock_stub.Evaluate.reset_mock()
        with (
            patch("grpc.aio.insecure_channel", return_value=AsyncMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=mock_stub),
        ):
            result = await hook.evaluate(_REQ)

        assert result.approved is False
        assert "circuit breaker open" in result.reason
        mock_stub.Evaluate.assert_not_called()


# ---------------------------------------------------------------------------
# Factory tests
# ---------------------------------------------------------------------------


class TestCreateHook:
    def test_webhook(self):
        hook = create_hook(_cfg(type=HookType.WEBHOOK))
        assert isinstance(hook, WebhookApprovalHook)

    def test_grpc(self):
        hook = create_hook(_cfg(type=HookType.GRPC, url="localhost:50051"))
        assert isinstance(hook, GrpcApprovalHook)

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="Unknown hook type"):
            create_hook(ApprovalHookConfig(type="bogus"))  # type: ignore[arg-type]
