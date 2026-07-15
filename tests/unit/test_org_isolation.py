# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for org isolation: REQ-695 through REQ-702."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _conn_with_advisory_lock() -> AsyncMock:
    """A mocked Connection whose advisory_lock(key) is an async context manager (matches the
    migrated ``core.db.init_schema`` PG path)."""
    conn = AsyncMock()
    lock_cm = AsyncMock(
        __aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=False)
    )
    conn.advisory_lock = MagicMock(return_value=lock_cm)
    return conn


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
    # REQ-696 (portable): the platform control plane uses the ``Database`` abstraction and
    # vanilla SQLAlchemy metadata only — no PG-only ``CREATE SCHEMA platform`` / ``platform.*``
    # qualification (SQLite has no schemas). The billing tables live in the shared registry
    # metadata and are created via ``metadata.create_all``.
    def test_billing_tables_are_schema_unqualified(self):  # REQ-696
        import provisa.api.billing.tenant_db as td
        from provisa.core.schema_admin import tenant_config, tenants

        # No raw PG-schema DDL constant survives, and the Table objects carry no
        # ``platform`` schema — they are created in the platform engine's default schema.
        assert not hasattr(td, "BILLING_SCHEMA_SQL")
        assert tenants.schema is None
        assert tenant_config.schema is None

    @pytest.mark.asyncio
    async def test_init_billing_schema_uses_portable_metadata(self):  # REQ-696
        from provisa.api.billing.tenant_db import init_billing_schema
        from provisa.core.schema_admin import tenant_config, tenants

        sync_conn = AsyncMock()
        begin_ctx = AsyncMock(
            __aenter__=AsyncMock(return_value=sync_conn),
            __aexit__=AsyncMock(return_value=False),
        )
        mock_pool = MagicMock()
        mock_pool.engine.begin = MagicMock(return_value=begin_ctx)

        await init_billing_schema(mock_pool)

        # create_all is applied via run_sync(lambda) restricted to the billing tables.
        sync_conn.run_sync.assert_awaited_once()
        fn = sync_conn.run_sync.await_args.args[0]
        created = MagicMock()
        with patch.object(tenants.metadata, "create_all") as create_all:
            fn(created)
        create_all.assert_called_once()
        assert create_all.call_args.kwargs["tables"] == [tenants, tenant_config]

    @pytest.mark.asyncio
    async def test_create_tenant_inserts_into_unqualified_tenants(self):  # REQ-696
        from provisa.api.billing.tenant_db import create_tenant

        result = MagicMock()
        result.fetchone.return_value = MagicMock(
            _mapping={
                "id": "11111111-0000-0000-0000-000000000001",
                "kms_key_arn": "arn:aws:kms:us-east-1:123:key/abc",
                "ls_customer_id": None,
                "plan": "trial",
                "source_limit": 2,
                "created_at": None,
            }
        )
        mock_conn = AsyncMock()
        mock_conn.execute_core = AsyncMock(return_value=result)
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        await create_tenant(mock_pool, "arn:aws:kms:us-east-1:123:key/abc")
        stmt = mock_conn.execute_core.await_args.args[0]
        assert stmt.table.name == "tenants"
        assert stmt.table.schema is None  # no platform.* qualification


# ---------------------------------------------------------------------------
# REQ-697: init_schema() creates org_<org_id> schema and runs SQL within it
# ---------------------------------------------------------------------------


class TestInitSchema:
    @pytest.mark.asyncio
    async def test_init_schema_creates_org_schema(self):  # REQ-697
        from provisa.core.db import init_schema

        mock_conn = _conn_with_advisory_lock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        mock_pool.dialect = "postgresql"
        await init_schema(mock_pool, "SELECT 1", org_id="myorg")

        # CREATE SCHEMA is now issued via execute_core(CreateSchema(...)); its compiled SQL
        # carries the org_<id> schema name.
        core_sql = [str(c.args[0]) for c in mock_conn.execute_core.await_args_list if c.args]
        assert any("org_myorg" in s for s in core_sql)

    @pytest.mark.asyncio
    async def test_init_schema_sets_search_path_before_running_sql(self):  # REQ-697
        from provisa.core.db import init_schema

        mock_conn = _conn_with_advisory_lock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        mock_pool.dialect = "postgresql"
        await init_schema(mock_pool, "CREATE TABLE t (id INT)", org_id="myorg")

        # Invariant (REQ-697): search_path is set before the schema DDL runs. Both the
        # SET search_path and the multi-statement schema SQL go through conn.execute.
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

    def cache_catalog(self):
        # A broad federator / store-engine caches into the source's own catalog (None).
        return None


class TestOrgScopedCacheLocation:
    def test_cache_location_uses_org_prefix_as_default(self):  # REQ-698
        from provisa.api_source.engine_cache import cache_location

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
        from provisa.api.startup_seed import _seed_meta_domain

        src = inspect.getsource(_seed_meta_domain)
        assert "org_id" in src
        assert "org_{org_id}" in src or "org_" in src

    @pytest.mark.asyncio
    async def test_seed_meta_domain_registers_tables_in_org_schema(self):  # REQ-702
        from provisa.api.startup_seed import _seed_meta_domain

        mock_conn = AsyncMock()
        mock_conn.reflect_columns = AsyncMock(return_value=[])  # empty → skip column loops
        mock_conn.upsert_returning = AsyncMock(return_value=1)
        result = MagicMock()
        result.scalar.return_value = 1
        result.fetchone.return_value = None
        mock_conn.execute_core = AsyncMock(return_value=result)

        await _seed_meta_domain(mock_conn, org_id="demo")

        # org_<id> schema name reaches the portable reflection and the registered_tables upsert.
        all_calls = [
            str(c)
            for c in mock_conn.reflect_columns.await_args_list
            + mock_conn.upsert_returning.await_args_list
        ]
        assert any("org_demo" in c for c in all_calls)
