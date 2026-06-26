# Copyright (c) 2026 Kenneth Stott
# Canary: b3f7a291-d4c8-4e1a-9f06-52e8cb3d1074
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for audit and auth subsystems.

Tests the real component boundaries — no mocks at the boundary under test.

Covered REQ-IDs:
  Anonymous auth (dev mode):         REQ-535
  gRPC approval hook channel reuse:  REQ-555
  Query audit logging:               REQ-596
"""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(provider=None, mapping_rules=None, default_role="admin", superuser=None):
    """Build a minimal FastAPI app wired with AuthMiddleware."""
    from provisa.auth.middleware import AuthMiddleware

    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules,
        default_role=default_role,
        superuser=superuser,
    )

    @app.get("/probe")
    async def probe(request: Request):
        identity = request.state.identity
        assignments = request.state.assignments
        return {
            "user_id": identity.user_id,
            "roles": identity.roles,
            "role": request.state.role,
            "domain_ids": [a.domain_id for a in assignments],
        }

    return app


# ---------------------------------------------------------------------------
# REQ-535 — Anonymous auth → role resolution boundary
# ---------------------------------------------------------------------------


class TestREQ535AnonymousAuth:
    """REQ-535: In dev mode (no auth provider), any request resolves to anonymous
    identity with all configured roles and wildcard domain access.

    Integration boundary: AuthMiddleware._provider=None → request.state populated
    with real AuthIdentity + RoleAssignment objects (both real components, no mock
    at the boundary).
    """

    def test_anonymous_identity_user_id(self):
        # REQ-535: no provider → user_id is "anonymous"
        app = _make_app(provider=None)
        client = TestClient(app)
        resp = client.get("/probe")
        assert resp.status_code == 200
        assert resp.json()["user_id"] == "anonymous"

    def test_anonymous_identity_has_role(self):
        # REQ-535: no provider → resolved role defaults to admin (or x-provisa-role header)
        app = _make_app(provider=None, default_role="admin")
        client = TestClient(app)
        resp = client.get("/probe")
        assert resp.status_code == 200
        assert resp.json()["role"] == "admin"

    def test_anonymous_assignment_has_wildcard_domain(self):
        # REQ-535: no provider → RoleAssignment has domain_id="*" (wildcard domain access)
        app = _make_app(provider=None)
        client = TestClient(app)
        resp = client.get("/probe")
        assert resp.status_code == 200
        assert "*" in resp.json()["domain_ids"]

    def test_anonymous_role_overridable_via_header(self):
        # REQ-535: in dev mode, x-provisa-role header is honored without validation
        app = _make_app(provider=None)
        client = TestClient(app)
        resp = client.get("/probe", headers={"x-provisa-role": "viewer"})
        assert resp.status_code == 200
        assert resp.json()["role"] == "viewer"

    def test_with_provider_anonymous_fallback_not_used(self):
        # REQ-535: when a real auth provider IS configured, anonymous fallback
        # is NOT applied — missing token returns 401, not anonymous identity.
        # integration: mock-justified — we only mock the provider's verify step;
        # the middleware→identity resolution path is real
        from provisa.auth.models import AuthIdentity, AuthProvider

        class _RealProvider(AuthProvider):
            async def validate_token(self, token: str) -> AuthIdentity:
                if token == "ok":
                    return AuthIdentity(
                        user_id="alice",
                        email=None,
                        display_name=None,
                        roles=["analyst"],
                        raw_claims={},
                    )
                raise ValueError("bad token")

        app = _make_app(provider=_RealProvider())
        client = TestClient(app)
        # No token → must get 401, not anonymous
        resp = client.get("/probe")
        assert resp.status_code == 401

    def test_with_provider_valid_token_is_not_anonymous(self):
        # REQ-535: with provider configured and valid token, identity is NOT anonymous
        # integration: mock-justified — provider.validate_token is the outermost I/O
        # boundary; all middleware logic and identity objects are real
        from provisa.auth.models import AuthIdentity, AuthProvider

        class _RealProvider(AuthProvider):
            async def validate_token(self, token: str) -> AuthIdentity:
                if token == "ok":
                    return AuthIdentity(
                        user_id="bob",
                        email=None,
                        display_name=None,
                        roles=["editor"],
                        raw_claims={},
                    )
                raise ValueError("bad token")

        app = _make_app(provider=_RealProvider())
        client = TestClient(app)
        resp = client.get("/probe", headers={"Authorization": "Bearer ok"})
        assert resp.status_code == 200
        assert resp.json()["user_id"] == "bob"
        assert resp.json()["user_id"] != "anonymous"


# ---------------------------------------------------------------------------
# REQ-555 — gRPC approval hook persistent channel boundary
# ---------------------------------------------------------------------------


class TestREQ555GrpcPersistentChannel:
    """REQ-555: GrpcApprovalHook maintains a single persistent channel per
    instance, reused across calls.  Connection overhead is eliminated.

    Integration boundary: GrpcApprovalHook._ensure_channel() → self._channel field.
    Both GrpcApprovalHook and the channel object are real; gRPC network I/O is
    mocked (integration: mock-justified — no gRPC server in test environment).
    """

    def _grpc_cfg(self, **overrides):
        from provisa.auth.approval_hook import ApprovalHookConfig, FallbackPolicy, HookType

        defaults = {
            "type": HookType.GRPC,
            "url": "localhost:50099",
            "timeout_ms": 500,
            "fallback": FallbackPolicy.DENY,
        }
        defaults.update(overrides)
        return ApprovalHookConfig(**defaults)

    def _make_stub(self):
        mock_resp = AsyncMock()
        mock_resp.approved = True
        mock_resp.reason = ""
        mock_resp.additional_filter = ""
        stub = AsyncMock()
        stub.Evaluate = AsyncMock(return_value=mock_resp)
        return stub

    @pytest.mark.asyncio
    async def test_channel_created_once_on_first_call(self):
        # REQ-555: channel is created exactly once on the first evaluate() call
        from provisa.auth.approval_hook import ApprovalRequest, GrpcApprovalHook

        hook = GrpcApprovalHook(self._grpc_cfg())
        stub = self._make_stub()
        mock_channel = MagicMock()

        req = ApprovalRequest(
            user="alice",
            roles=["analyst"],
            tables=["t1"],
            columns=["id"],
            operation="query",
        )

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel) as chan_factory,
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub),
        ):
            await hook.evaluate(req)

        # Channel factory called exactly once
        chan_factory.assert_called_once()
        # Channel object is stored on the hook
        assert hook._channel is mock_channel

    @pytest.mark.asyncio
    async def test_channel_reused_across_multiple_calls(self):
        # REQ-555: N evaluate() calls produce only ONE channel creation
        from provisa.auth.approval_hook import ApprovalRequest, GrpcApprovalHook

        hook = GrpcApprovalHook(self._grpc_cfg())
        stub = self._make_stub()
        mock_channel = MagicMock()

        req = ApprovalRequest(
            user="alice",
            roles=["analyst"],
            tables=["t1"],
            columns=["id"],
            operation="query",
        )

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel) as chan_factory,
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub),
        ):
            for _ in range(5):
                await hook.evaluate(req)

        # Despite 5 calls, channel created only once
        chan_factory.assert_called_once()
        # Stub evaluated 5 times (channel was reused each time)
        assert stub.Evaluate.await_count == 5

    @pytest.mark.asyncio
    async def test_channel_object_identity_preserved(self):
        # REQ-555: the channel instance stored after first call is the same object
        # referenced in subsequent calls (identity check, not equality)
        from provisa.auth.approval_hook import ApprovalRequest, GrpcApprovalHook

        hook = GrpcApprovalHook(self._grpc_cfg())
        stub = self._make_stub()
        sentinel_channel = MagicMock(name="sentinel-channel")

        req = ApprovalRequest(
            user="alice",
            roles=["analyst"],
            tables=["t1"],
            columns=["id"],
            operation="query",
        )

        with (
            patch("grpc.aio.insecure_channel", return_value=sentinel_channel),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub),
        ):
            await hook.evaluate(req)
            channel_after_first = hook._channel
            await hook.evaluate(req)
            channel_after_second = hook._channel

        assert channel_after_first is sentinel_channel
        assert channel_after_second is sentinel_channel
        # Same object across calls
        assert channel_after_first is channel_after_second

    @pytest.mark.asyncio
    async def test_two_hook_instances_have_independent_channels(self):
        # REQ-555: each GrpcApprovalHook instance manages its own channel;
        # two instances do not share a channel object
        from provisa.auth.approval_hook import ApprovalRequest, GrpcApprovalHook

        cfg = self._grpc_cfg()
        hook_a = GrpcApprovalHook(cfg)
        hook_b = GrpcApprovalHook(cfg)

        channel_a = MagicMock(name="channel-a")
        channel_b = MagicMock(name="channel-b")
        stub = self._make_stub()

        req = ApprovalRequest(
            user="alice",
            roles=["analyst"],
            tables=["t1"],
            columns=["id"],
            operation="query",
        )

        with (
            patch("grpc.aio.insecure_channel", side_effect=[channel_a, channel_b]),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub),
        ):
            await hook_a.evaluate(req)
            await hook_b.evaluate(req)

        assert hook_a._channel is channel_a
        assert hook_b._channel is channel_b
        assert hook_a._channel is not hook_b._channel


# ---------------------------------------------------------------------------
# REQ-596 — Audit log writer → storage boundary
# ---------------------------------------------------------------------------


class TestREQ596AuditLogWriterBoundary:
    """REQ-596: log_query() → asyncpg pool execute() boundary.

    Integration boundary: log_query (real function) → pool.execute (real call
    signature checked via captured arguments).  Actual DB I/O is avoided:
    integration: mock-justified — no live PostgreSQL in unit/integration tier;
    asyncpg pool is patched at the I/O boundary only; all hashing, field
    assembly, and SQL construction logic runs unmodified.
    """

    def _make_pool(self):
        """Return an AsyncMock pool whose execute captures call args."""
        pool = AsyncMock()
        pool.execute = AsyncMock(return_value=None)
        return pool

    @pytest.mark.asyncio
    async def test_query_hash_stored_not_plaintext(self):
        # REQ-596: query text is never stored verbatim; only SHA-256 hash is persisted
        from provisa.audit.query_log import log_query

        query_text = "SELECT id, email FROM users WHERE tenant_id = 'acme'"
        expected_hash = hashlib.sha256(query_text.encode()).hexdigest()

        pool = self._make_pool()
        await log_query(
            pool,
            tenant_id="tenant-1",
            user_id="user-1",
            role_id="analyst",
            query_text=query_text,
            table_ids=["users"],
            source="graphql",
            status_code=200,
            duration_ms=42,
        )

        call_args = pool.execute.call_args[0]
        # Positional params: (sql, tenant_id, user_id, role_id, query_hash, ...)
        params = call_args[1:]
        query_hash_param = params[3]  # 4th positional param is query_hash
        assert query_hash_param == expected_hash
        # Raw query text must never appear in any argument
        assert query_text not in str(call_args)

    @pytest.mark.asyncio
    async def test_same_query_produces_same_hash(self):
        # REQ-596: deterministic hashing — identical query text → identical stored hash
        from provisa.audit.query_log import log_query

        query_text = "SELECT * FROM orders"
        pool_a = self._make_pool()
        pool_b = self._make_pool()

        await log_query(
            pool_a,
            tenant_id="t1",
            user_id="u1",
            role_id="r1",
            query_text=query_text,
            table_ids=["orders"],
            source="graphql",
            status_code=200,
            duration_ms=10,
        )
        await log_query(
            pool_b,
            tenant_id="t1",
            user_id="u1",
            role_id="r1",
            query_text=query_text,
            table_ids=["orders"],
            source="graphql",
            status_code=200,
            duration_ms=10,
        )

        hash_a = pool_a.execute.call_args[0][1:][3]
        hash_b = pool_b.execute.call_args[0][1:][3]
        assert hash_a == hash_b

    @pytest.mark.asyncio
    async def test_different_queries_produce_different_hashes(self):
        # REQ-596: distinct query text → distinct hash (collision-free for distinct inputs)
        from provisa.audit.query_log import log_query

        pool_a = self._make_pool()
        pool_b = self._make_pool()

        await log_query(
            pool_a,
            tenant_id="t1",
            user_id="u1",
            role_id="r1",
            query_text="SELECT 1",
            table_ids=[],
            source="graphql",
            status_code=200,
            duration_ms=1,
        )
        await log_query(
            pool_b,
            tenant_id="t1",
            user_id="u1",
            role_id="r1",
            query_text="SELECT 2",
            table_ids=[],
            source="graphql",
            status_code=200,
            duration_ms=1,
        )

        hash_a = pool_a.execute.call_args[0][1:][3]
        hash_b = pool_b.execute.call_args[0][1:][3]
        assert hash_a != hash_b

    @pytest.mark.asyncio
    async def test_all_required_fields_passed_to_execute(self):
        # REQ-596: INSERT must bind all required audit columns in order
        from provisa.audit.query_log import log_query

        pool = self._make_pool()
        await log_query(
            pool,
            tenant_id="tenant-abc",
            user_id="user-xyz",
            role_id="editor",
            query_text="query { users { id } }",
            table_ids=["users", "profiles"],
            source="graphql",
            status_code=200,
            duration_ms=15,
        )

        pool.execute.assert_awaited_once()
        call_args = pool.execute.call_args[0]
        sql = call_args[0]
        params = call_args[1:]

        # SQL targets the correct table
        assert "query_audit_log" in sql

        # Bound params match expected positions
        assert params[0] == "tenant-abc"  # tenant_id
        assert params[1] == "user-xyz"  # user_id
        assert params[2] == "editor"  # role_id
        # params[3] is query_hash — tested elsewhere
        assert params[4] == ["users", "profiles"]  # table_ids
        assert params[5] == "graphql"  # source
        assert params[6] == 200  # status_code
        assert params[7] == 15  # duration_ms

    @pytest.mark.asyncio
    async def test_tenant_id_can_be_none(self):
        # REQ-596: tenant_id is nullable (single-tenant deployments have no tenant)
        from provisa.audit.query_log import log_query

        pool = self._make_pool()
        await log_query(
            pool,
            tenant_id=None,
            user_id="user-1",
            role_id="analyst",
            query_text="SELECT 1",
            table_ids=[],
            source="graphql",
            status_code=200,
            duration_ms=5,
        )

        params = pool.execute.call_args[0][1:]
        assert params[0] is None  # tenant_id

    @pytest.mark.asyncio
    async def test_duration_ms_is_positive_integer(self):
        # REQ-596: duration_ms must be a positive integer when correctly supplied
        from provisa.audit.query_log import log_query

        pool = self._make_pool()
        await log_query(
            pool,
            tenant_id="t1",
            user_id="u1",
            role_id="r1",
            query_text="SELECT 1",
            table_ids=[],
            source="graphql",
            status_code=200,
            duration_ms=99,
        )

        params = pool.execute.call_args[0][1:]
        duration = params[7]
        assert isinstance(duration, int)
        assert duration > 0

    def test_log_query_has_no_update_method(self):
        # REQ-596: append-only — log_query module exposes no update/delete entry point
        from provisa.audit import query_log

        assert not hasattr(query_log, "update_query")
        assert not hasattr(query_log, "delete_query")
        assert not hasattr(query_log, "update_log_entry")
        assert not hasattr(query_log, "delete_log_entry")

    def test_schema_sql_blocks_delete_at_db_level(self):
        # REQ-596: append-only enforced in PostgreSQL via a rewrite rule on DELETE
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "no_delete_audit" in AUDIT_SCHEMA_SQL
        assert "ON DELETE TO query_audit_log DO INSTEAD NOTHING" in AUDIT_SCHEMA_SQL

    def test_schema_sql_blocks_update_at_db_level(self):
        # REQ-596: append-only enforced in PostgreSQL via a rewrite rule on UPDATE
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "no_update_audit" in AUDIT_SCHEMA_SQL
        assert "ON UPDATE TO query_audit_log DO INSTEAD NOTHING" in AUDIT_SCHEMA_SQL

    def test_schema_sql_has_tenant_time_index(self):
        # REQ-596: tenant-scoped time-range query index present
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "idx_audit_tenant_time" in AUDIT_SCHEMA_SQL
        assert "tenant_id" in AUDIT_SCHEMA_SQL
        assert "logged_at" in AUDIT_SCHEMA_SQL

    def test_schema_sql_has_user_time_index(self):
        # REQ-596: per-user time-range query index present
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "idx_audit_user_time" in AUDIT_SCHEMA_SQL

    @pytest.mark.asyncio
    async def test_logged_at_column_has_db_default(self):
        # REQ-596: logged_at carries a DEFAULT now() so it is always populated
        # without the caller supplying a value — verified via schema DDL
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "logged_at TIMESTAMPTZ NOT NULL DEFAULT now()" in AUDIT_SCHEMA_SQL

    @pytest.mark.asyncio
    async def test_query_hash_is_sha256_hex_digest(self):
        # REQ-596: hash algorithm must be SHA-256 (64 hex chars)
        from provisa.audit.query_log import log_query

        pool = self._make_pool()
        await log_query(
            pool,
            tenant_id=None,
            user_id="u",
            role_id="r",
            query_text="any query text",
            table_ids=[],
            source="graphql",
            status_code=200,
            duration_ms=1,
        )

        params = pool.execute.call_args[0][1:]
        query_hash = params[3]
        # SHA-256 hex digest is always 64 lowercase hex characters
        assert len(query_hash) == 64
        assert all(c in "0123456789abcdef" for c in query_hash)
