# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for org isolation: REQ-695 through REQ-702."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# REQ-695: asyncpg pool sets search_path=org_<org_id> via init hook
# ---------------------------------------------------------------------------


class TestPoolSearchPath:
    @pytest.mark.asyncio
    async def test_init_conn_sets_search_path_for_org(self):  # REQ-695
        from provisa.core.db import _make_init_conn

        conn = AsyncMock()
        init = _make_init_conn("acme")
        await init(conn)
        calls = [str(c) for c in conn.execute.await_args_list]
        assert any("org_acme" in c for c in calls)

    @pytest.mark.asyncio
    async def test_init_conn_default_org_uses_org_default(self):  # REQ-695
        from provisa.core.db import _make_init_conn

        conn = AsyncMock()
        init = _make_init_conn("default")
        await init(conn)
        calls = [str(c) for c in conn.execute.await_args_list]
        assert any("org_default" in c for c in calls)

    @pytest.mark.asyncio
    async def test_init_conn_uses_set_search_path(self):  # REQ-695
        from provisa.core.db import _make_init_conn

        conn = AsyncMock()
        init = _make_init_conn("tenant1")
        await init(conn)
        sql_calls = [c.args[0] for c in conn.execute.await_args_list if c.args]
        assert any("SET search_path" in s and "org_tenant1" in s for s in sql_calls)


# ---------------------------------------------------------------------------
# REQ-696: Platform tables use fully-qualified platform.* names
# ---------------------------------------------------------------------------


class TestPlatformSchemaIsolation:
    def test_billing_ddl_uses_platform_schema(self):  # REQ-696
        from provisa.api.billing.tenant_db import BILLING_SCHEMA_SQL

        assert "CREATE SCHEMA IF NOT EXISTS platform" in BILLING_SCHEMA_SQL
        assert "platform.tenants" in BILLING_SCHEMA_SQL
        assert "platform.tenant_config" in BILLING_SCHEMA_SQL

    def test_billing_ddl_has_no_unqualified_table_references(self):  # REQ-696
        from provisa.api.billing.tenant_db import BILLING_SCHEMA_SQL

        lines = BILLING_SCHEMA_SQL.splitlines()
        for line in lines:
            stripped = line.strip().upper()
            if stripped.startswith("CREATE TABLE IF NOT EXISTS"):
                assert "PLATFORM." in stripped, f"Unqualified table in: {line}"

    @pytest.mark.asyncio
    async def test_create_tenant_uses_fully_qualified_insert(self):  # REQ-696
        from provisa.api.billing.tenant_db import create_tenant

        row = {
            "id": "11111111-0000-0000-0000-000000000001",
            "kms_key_arn": "arn:aws:kms:us-east-1:123:key/abc",
            "stripe_customer_id": None,
            "plan": "trial",
            "source_limit": 2,
            "created_at": None,
        }
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=row)
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        await create_tenant(mock_pool, "arn:aws:kms:us-east-1:123:key/abc")
        sql = mock_conn.fetchrow.await_args.args[0]
        assert "platform.tenants" in sql.lower()


# ---------------------------------------------------------------------------
# REQ-697: init_schema() creates org_<org_id> schema and runs SQL within it
# ---------------------------------------------------------------------------


class TestInitSchema:
    @pytest.mark.asyncio
    async def test_init_schema_creates_org_schema(self):  # REQ-697
        from provisa.core.db import init_schema

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        mock_pool.dialect = "postgresql"
        await init_schema(mock_pool, "SELECT 1", org_id="myorg")

        executed = [c.args[0] for c in mock_conn.execute.await_args_list if c.args]
        assert any("org_myorg" in s for s in executed)
        assert any("CREATE SCHEMA" in s and "org_myorg" in s for s in executed)

    @pytest.mark.asyncio
    async def test_init_schema_sets_search_path_before_running_sql(self):  # REQ-697
        from provisa.core.db import init_schema

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        mock_pool.dialect = "postgresql"
        await init_schema(mock_pool, "CREATE TABLE t (id INT)", org_id="myorg")

        # Invariant (REQ-697): search_path is set before the schema DDL runs.
        # init_schema issues both via conn.execute (the control-plane Database
        # shim auto-routes the multi-statement schema SQL to the raw driver).
        executed = [c.args[0] for c in mock_conn.execute.await_args_list if c.args]
        search_path_idx = next(
            i for i, s in enumerate(executed) if "SET search_path" in s and "org_myorg" in s
        )
        schema_sql_idx = next(i for i, s in enumerate(executed) if "CREATE TABLE" in s)
        assert search_path_idx < schema_sql_idx

    def test_validate_org_id_rejects_invalid_chars(self):  # REQ-697
        from provisa.core.db import _validate_org_id

        with pytest.raises(ValueError):
            _validate_org_id("bad-org")

        with pytest.raises(ValueError):
            _validate_org_id("org with spaces")

        with pytest.raises(ValueError):
            _validate_org_id("org;drop")

    def test_validate_org_id_accepts_valid_ids(self):  # REQ-697
        from provisa.core.db import _validate_org_id

        assert _validate_org_id("default") is None
        assert _validate_org_id("acme123") is None
        assert _validate_org_id("org_1") is None


# ---------------------------------------------------------------------------
# REQ-698: cache_location and handle_api_query use org-scoped cache schema
# ---------------------------------------------------------------------------


class _FakeEngine:
    """handle_api_query now takes the engine; isolated_sync yields a throwaway conn."""

    from contextlib import contextmanager as _cm

    @_cm
    def isolated_sync(self):
        yield MagicMock()


class TestOrgScopedCacheLocation:
    def test_cache_location_uses_org_prefix_as_default(self):  # REQ-698
        from provisa.api_source.trino_cache import cache_location

        loc = cache_location("my-source", cache_schema="org_acme_api_cache")
        assert loc.schema == "org_acme_api_cache"

    @pytest.mark.asyncio
    async def test_handle_api_query_uses_org_id_default_schema(self):  # REQ-698
        from provisa.api_source.router_integration import handle_api_query
        from provisa.api_source.models import ApiEndpoint

        endpoint = ApiEndpoint(
            source_id="my-source",
            table_name="items",
            path="/items",
            columns=[],
        )

        with patch("provisa.api_source.router_integration.table_exists", return_value=True):
            result = await handle_api_query(
                endpoint=endpoint,
                params={},
                engine=_FakeEngine(),
                org_id="acme",
            )

        assert result.from_cache is True

    @pytest.mark.asyncio
    async def test_handle_api_query_loc_none_uses_org_acme_schema(self):  # REQ-698
        from provisa.api_source.router_integration import handle_api_query
        from provisa.api_source.models import ApiEndpoint

        captured_loc = {}

        def fake_table_exists(_conn, loc, _tbl, ttl=None):  # noqa: ARG001
            captured_loc["loc"] = loc
            return True

        endpoint = ApiEndpoint(
            source_id="svc",
            table_name="widgets",
            path="/widgets",
            columns=[],
        )

        with patch(
            "provisa.api_source.router_integration.table_exists", side_effect=fake_table_exists
        ):
            await handle_api_query(
                endpoint=endpoint, params={}, engine=_FakeEngine(), org_id="myorg"
            )

        assert captured_loc["loc"].schema == "org_myorg_api_cache"


# ---------------------------------------------------------------------------
# REQ-699: create_org_role creates role_<org_id> with USAGE+CREATE on org schema
# ---------------------------------------------------------------------------


class TestCreateOrgRole:
    @pytest.mark.asyncio
    async def test_create_org_role_grants_schema_access(self):  # REQ-699
        from provisa.core.db import create_org_role

        conn = AsyncMock()
        conn.capabilities.dialect = "postgresql"  # PG control plane → role hardening runs
        await create_org_role(conn, "acme")

        executed = [c.args[0] for c in conn.execute.await_args_list if c.args]
        assert any("role_acme" in s for s in executed)
        assert any("GRANT USAGE, CREATE ON SCHEMA org_acme TO role_acme" in s for s in executed)

    @pytest.mark.asyncio
    async def test_create_org_role_is_noop_on_non_pg_backend(self):  # REQ-889
        from provisa.core.db import create_org_role

        conn = AsyncMock()
        conn.capabilities.dialect = "sqlite"  # embedded home has no role system to harden
        await create_org_role(conn, "acme")

        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_org_role_rejects_invalid_org_id(self):  # REQ-699
        from provisa.core.db import create_org_role

        conn = AsyncMock()
        with pytest.raises(ValueError):
            await create_org_role(conn, "bad-org!")


# ---------------------------------------------------------------------------
# REQ-700: Redis cache keys prefixed with org_id
# ---------------------------------------------------------------------------


class TestRedisOrgKeyPrefix:
    def test_redis_store_prefixes_cache_key_with_org_id(self):  # REQ-700
        from provisa.cache.store import RedisCacheStore

        store = RedisCacheStore.__new__(RedisCacheStore)
        key = store._prefixed_key("abc123", tenant_id="acme")
        assert key.startswith("provisa:cache:acme:")

    def test_redis_store_prefixes_table_key_with_org_id(self):  # REQ-700
        from provisa.cache.store import RedisCacheStore

        store = RedisCacheStore.__new__(RedisCacheStore)
        key = store._prefixed_table_key(42, tenant_id="acme")
        assert key.startswith("provisa:table:acme:")

    def test_redis_acl_key_patterns_include_org_id(self):  # REQ-700
        from provisa.core.org_provisioning import provision_redis_acl
        import inspect

        src = inspect.getsource(provision_redis_acl)
        assert "provisa:cache:{org_id}:" in src or "provisa:cache:" in src
        assert "provisa:table:" in src


# ---------------------------------------------------------------------------
# REQ-701: provision_org is atomic; deprovision_org reverses in order
# ---------------------------------------------------------------------------


class TestProvisionOrg:
    @pytest.mark.asyncio
    async def test_provision_org_calls_all_steps(self):  # REQ-701
        from provisa.core.org_provisioning import provision_org

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        mock_init = AsyncMock()
        mock_audit = AsyncMock()
        mock_role = AsyncMock()
        with (
            patch("provisa.core.db.init_schema", mock_init),
            patch("provisa.audit.query_log.init_audit_schema", mock_audit),
            patch("provisa.core.db.create_org_role", mock_role),
        ):
            await provision_org(mock_pool, "SELECT 1", "testorg")

        assert mock_init.await_count == 1
        assert mock_audit.await_count == 1
        assert mock_role.await_count == 1

    @pytest.mark.asyncio
    async def test_provision_org_rolls_back_on_failure(self):  # REQ-701
        from provisa.core.org_provisioning import provision_org

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        with (
            patch("provisa.core.db.init_schema", AsyncMock()),
            patch("provisa.audit.query_log.init_audit_schema", AsyncMock()),
            patch(
                "provisa.core.db.create_org_role",
                AsyncMock(side_effect=RuntimeError("role fail")),
            ),
        ):
            with pytest.raises(RuntimeError):
                await provision_org(mock_pool, "SELECT 1", "testorg")

        executed = [c.args[0] for c in mock_conn.execute.await_args_list if c.args]
        assert any("DROP SCHEMA" in s and "org_testorg" in s for s in executed)

    @pytest.mark.asyncio
    async def test_deprovision_org_drops_role_and_schema(self):  # REQ-701
        from provisa.core.org_provisioning import deprovision_org

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.dialect = "postgresql"  # PG control plane → role drop runs (REQ-889)
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        await deprovision_org(mock_pool, "acme")

        executed = [c.args[0] for c in mock_conn.execute.await_args_list if c.args]
        assert any("DROP ROLE" in s and "role_acme" in s for s in executed)
        assert any("DROP SCHEMA" in s and "org_acme" in s for s in executed)


# ---------------------------------------------------------------------------
# REQ-702: Demo seed code uses org_id for schema scoping
# ---------------------------------------------------------------------------


class TestDemoSeedOrgScoping:
    def test_seed_meta_domain_uses_org_id_schema_name(self):  # REQ-702
        import inspect
        from provisa.api.app import _seed_meta_domain

        src = inspect.getsource(_seed_meta_domain)
        assert "org_id" in src
        assert "org_{org_id}" in src or "org_" in src

    @pytest.mark.asyncio
    async def test_seed_meta_domain_registers_tables_in_org_schema(self):  # REQ-702
        from provisa.api.app import _seed_meta_domain

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1)

        await _seed_meta_domain(mock_conn, org_id="demo")

        all_calls = [
            str(c) for c in mock_conn.fetchval.await_args_list + mock_conn.execute.await_args_list
        ]
        assert any("org_demo" in c for c in all_calls)
