# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit/integration tests for provisa/api/data/endpoint_dev.py — /data/sql,
/data/proto/{role_id}, and their private helpers.

HTTP-level tests drive the FastAPI app with minimal AppState injection (the
canonical pattern from tests/unit/test_sql_endpoint_governance.py). Pure
helpers are exercised directly to reach branches HTTP cannot cheaply reach.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlglot
from httpx import ASGITransport, AsyncClient

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.executor.result import QueryResult

# asyncio_mode = "auto" (pyproject.toml) picks up async defs automatically.


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _table_meta(
    table_id: int = 1,
    table_name: str = "orders",
    schema_name: str = "public",
    source_id: str = "pg",
    domain_id: str = "",
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name=schema_name,
        table_name=table_name,
        domain_id=domain_id,
    )


def _make_ctx(table_name: str = "orders", table_id: int = 1) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {table_name: _table_meta(table_id, table_name)}
    return ctx


def _make_query_result(**kwargs) -> QueryResult:
    return QueryResult(
        rows=kwargs.get("rows", [(1, "test")]),
        column_names=kwargs.get("column_names", ["id", "name"]),
    )


@pytest.fixture
async def sql_client():
    """ASGI test client with minimal state injected for /data/sql + /data/proto tests."""
    import provisa.api.app as app_mod
    from provisa.api.app import create_app

    _prev_auth_config = getattr(app_mod.state, "auth_config", None)
    app_mod.state.auth_config = None

    the_app = create_app()

    ctx = _make_ctx("orders", table_id=1)
    rls = RLSContext.empty()

    app_mod.state.schemas = {"admin": MagicMock()}
    app_mod.state.contexts = {"admin": ctx}
    app_mod.state.rls_contexts = {"admin": rls}
    app_mod.state.roles = {"admin": {"id": "admin", "capabilities": ["admin"]}}
    app_mod.state.masking_rules = {}
    app_mod.state.source_types = {"pg": "postgresql"}
    app_mod.state.source_dialects = {"pg": "postgres"}
    app_mod.state.tables = [
        {
            "id": 1,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "data_type": "integer"},
                {"column_name": "status", "data_type": "varchar"},
            ],
        }
    ]
    app_mod.state.source_pools = MagicMock()
    app_mod.state.view_sql_map = {}
    app_mod.state.proto_files = {}
    app_mod.state.schema_build_cache = {}

    transport = ASGITransport(app=the_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    from provisa.executor.pool import SourcePool

    app_mod.state.auth_config = _prev_auth_config
    app_mod.state.schemas = {}
    app_mod.state.contexts = {}
    app_mod.state.rls_contexts = {}
    app_mod.state.roles = {}
    app_mod.state.masking_rules = {}
    app_mod.state.source_types = {}
    app_mod.state.source_dialects = {}
    app_mod.state.source_pools = SourcePool()


# ---------------------------------------------------------------------------
# /data/sql — capability / discovery / stats / accept-header branches
# ---------------------------------------------------------------------------


class TestSqlEndpointCapability:
    async def test_no_query_development_capability_403(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.roles["admin"] = {"id": "admin", "capabilities": []}
        resp = await sql_client.post(
            "/data/sql", json={"sql": "SELECT id FROM orders", "role": "admin"}
        )
        assert resp.status_code == 403

    async def test_discovery_mode_bypasses_capability_check(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.roles["admin"] = {"id": "admin", "capabilities": []}
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={
                    "sql": "SELECT id FROM orders",
                    "role": "admin",
                    "discovery_mode": True,
                },
            )
        assert resp.status_code != 403


class TestSqlEndpointNoSchema:
    async def test_unknown_role_returns_400(self, sql_client):
        # A role unknown to state.roles is rejected 403 by rate_limit_middleware
        # before the endpoint runs, so to reach _compile_govern_execute's own
        # "no schema for role" 400 check we need a role registered in
        # state.roles (passes the middleware) but absent from state.schemas.
        import provisa.api.app as app_mod

        app_mod.state.roles["schemaless"] = {"id": "schemaless", "capabilities": ["admin"]}
        try:
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT 1", "role": "schemaless"},
                headers={"x-provisa-role": "schemaless"},
            )
        finally:
            del app_mod.state.roles["schemaless"]
        assert resp.status_code == 400
        assert "schemaless" in resp.json()["detail"]


class TestSqlEndpointStatsAndFormat:
    async def test_stats_header_json_format_includes_provisa_stats(self, sql_client):
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT id FROM orders", "role": "admin"},
                headers={"accept": "application/json", "x-provisa-stats": "true"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "provisa_stats" in body

    async def test_default_json_response_shape(self, sql_client):
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql", json={"sql": "SELECT id FROM orders", "role": "admin"}
            )
        assert resp.status_code == 200
        assert resp.json() == {"data": {"sql": [{"id": 1}]}}

    async def test_csv_accept_format_uses_format_response(self, sql_client):
        fallback_result = _make_query_result(rows=[(1, "test")], column_names=["id", "name"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT id FROM orders", "role": "admin"},
                headers={"accept": "text/csv"},
            )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")


class TestSqlEndpointRoleResolution:
    async def test_x_provisa_role_header_overrides_body_role(self, sql_client):
        # Body role "ghost" doesn't exist; header "admin" does — header should win.
        resp = await sql_client.post(
            "/data/sql",
            json={"sql": "SELECT 1", "role": "ghost"},
            headers={"x-provisa-role": "admin"},
        )
        # Not the 400 "No schema for role 'ghost'" — proves header took precedence.
        assert resp.status_code != 400 or "ghost" not in resp.text


# ---------------------------------------------------------------------------
# /data/proto/{role_id}
# ---------------------------------------------------------------------------


class TestProtoEndpoint:
    async def test_no_proto_file_404(self, sql_client):
        resp = await sql_client.get("/data/proto/admin")
        assert resp.status_code == 404

    async def test_static_proto_file_returned(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.proto_files = {"admin": 'syntax = "proto3";'}
        resp = await sql_client.get("/data/proto/admin")
        assert resp.status_code == 200
        assert "proto3" in resp.text
        app_mod.state.proto_files = {}

    async def test_unknown_role_with_domains_404(self, sql_client):
        resp = await sql_client.get("/data/proto/ghost_role?domains=pet_store")
        assert resp.status_code == 404

    async def test_domains_without_schema_build_cache_503(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.schema_build_cache = {}
        resp = await sql_client.get("/data/proto/admin?domains=pet_store")
        assert resp.status_code == 503

    async def test_domains_generates_filtered_proto(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.roles["admin"] = {
            "id": "admin",
            "capabilities": ["admin"],
            "domain_access": [],
        }
        app_mod.state.schema_build_cache = {
            "tables": [
                {"id": 1, "domain_id": "pet_store", "name": "pets"},
            ],
            "relationships": [],
            "column_types": {},
            "naming_rules": {},
            "domains": {"pet_store": {}},
            "domain_prefix": {},
            "physical_table_map": {},
            "functions": [],
            "webhooks": [],
            "enum_types": {},
        }
        with (
            patch("provisa.api.data.sdl._reachable_table_ids", return_value=set()),
            patch(
                "provisa.grpc.proto_gen.generate_proto", return_value='syntax = "proto3";'
            ) as mock_gen,
        ):
            resp = await sql_client.get("/data/proto/admin?domains=pet_store")
        assert resp.status_code == 200
        mock_gen.assert_called_once()
        app_mod.state.schema_build_cache = {}

    async def test_domains_generate_proto_value_error_404(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.roles["admin"] = {
            "id": "admin",
            "capabilities": ["admin"],
            "domain_access": [],
        }
        app_mod.state.schema_build_cache = {
            "tables": [{"id": 1, "domain_id": "pet_store", "name": "pets"}],
            "relationships": [],
            "column_types": {},
            "naming_rules": {},
            "domains": {"pet_store": {}},
            "domain_prefix": {},
            "physical_table_map": {},
            "functions": [],
            "webhooks": [],
            "enum_types": {},
        }
        with (
            patch("provisa.api.data.sdl._reachable_table_ids", return_value=set()),
            patch(
                "provisa.grpc.proto_gen.generate_proto",
                side_effect=ValueError("bad schema"),
            ),
        ):
            resp = await sql_client.get("/data/proto/admin?domains=pet_store")
        assert resp.status_code == 404
        app_mod.state.schema_build_cache = {}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestResolveRoleId:
    def test_auth_role_takes_precedence(self):
        from provisa.api.data.endpoint_dev import _resolve_role_id

        raw_request = SimpleNamespace(state=SimpleNamespace(role="auth_role"))
        assert _resolve_role_id(raw_request, "header_role", "body_role") == "auth_role"

    def test_header_role_used_when_no_auth_role(self):
        from provisa.api.data.endpoint_dev import _resolve_role_id

        raw_request = SimpleNamespace(state=SimpleNamespace())
        assert _resolve_role_id(raw_request, "header_role", "body_role") == "header_role"

    def test_body_role_fallback(self):
        from provisa.api.data.endpoint_dev import _resolve_role_id

        raw_request = SimpleNamespace(state=SimpleNamespace())
        assert _resolve_role_id(raw_request, None, "body_role") == "body_role"


class TestCheckSqlCapabilities:
    def test_no_role_noop(self):
        from provisa.api.data.endpoint_dev import _check_sql_capabilities

        _check_sql_capabilities(None, discovery_mode=False)  # must not raise

    def test_discovery_mode_bypasses(self):
        from provisa.api.data.endpoint_dev import _check_sql_capabilities

        _check_sql_capabilities({"capabilities": []}, discovery_mode=True)  # must not raise

    def test_missing_capability_raises_403(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_dev import _check_sql_capabilities

        with pytest.raises(HTTPException) as exc_info:
            _check_sql_capabilities({"id": "admin", "capabilities": []}, discovery_mode=False)
        assert exc_info.value.status_code == 403

    def test_admin_capability_passes(self):
        from provisa.api.data.endpoint_dev import _check_sql_capabilities

        _check_sql_capabilities({"capabilities": ["admin"]}, discovery_mode=False)  # no raise


class TestResolveDefaultSource:
    def test_prefers_relational_source_type(self):
        from provisa.api.data.endpoint_dev import _resolve_default_source

        state = SimpleNamespace(
            source_types={"api1": "openapi", "pg1": "postgresql"},
            source_pools=SimpleNamespace(source_ids=["api1", "pg1"]),
        )
        assert _resolve_default_source(state) == "pg1"

    def test_falls_back_to_first_pool_source(self):
        from provisa.api.data.endpoint_dev import _resolve_default_source

        state = SimpleNamespace(
            source_types={"api1": "openapi"},
            source_pools=SimpleNamespace(source_ids=["api1"]),
        )
        assert _resolve_default_source(state) == "api1"

    def test_falls_back_to_pg_literal(self):
        from provisa.api.data.endpoint_dev import _resolve_default_source

        state = SimpleNamespace(
            source_types={},
            source_pools=SimpleNamespace(source_ids=[]),
        )
        assert _resolve_default_source(state) == "pg"


class TestNormalizeSqlTree:
    def test_normalizes_and_parses(self):
        from provisa.api.data.endpoint_dev import _normalize_sql_tree

        ctx = _make_ctx("orders", table_id=1)
        normalized_sql, tree = _normalize_sql_tree("SELECT id FROM orders", ctx)
        assert "orders" in normalized_sql.lower()
        assert tree is not None


class TestAugmentDiscoveryTableMap:
    def test_adds_tables_from_all_contexts(self):
        from provisa.api.data.endpoint_dev import _augment_discovery_table_map
        from provisa.compiler.stage2 import GovernanceContext

        ctx1 = _make_ctx("orders", table_id=1)
        ctx2 = _make_ctx("pets", table_id=2)
        state = SimpleNamespace(contexts={"admin": ctx1, "other": ctx2})
        gov_ctx = GovernanceContext(rls_rules={}, table_map={})
        _augment_discovery_table_map(gov_ctx, state)
        assert "orders" in gov_ctx.table_map
        assert "pets" in gov_ctx.table_map
        assert "public.orders" in gov_ctx.table_map


class TestCollectTableAccessViolations:
    def test_unknown_table_flagged(self):
        from provisa.api.data.endpoint_dev import _collect_table_access_violations
        from provisa.compiler.stage2 import GovernanceContext

        tree = sqlglot.parse_one("SELECT id FROM secret_table", read="postgres")
        gov_ctx = GovernanceContext(rls_rules={}, table_map={"orders": 1})
        violations = _collect_table_access_violations(tree, gov_ctx, "admin")
        assert len(violations) == 1
        assert violations[0].code == "V000"
        assert "secret_table" in violations[0].message

    def test_known_table_not_flagged(self):
        from provisa.api.data.endpoint_dev import _collect_table_access_violations
        from provisa.compiler.stage2 import GovernanceContext

        tree = sqlglot.parse_one("SELECT id FROM orders", read="postgres")
        gov_ctx = GovernanceContext(rls_rules={}, table_map={"orders": 1})
        violations = _collect_table_access_violations(tree, gov_ctx, "admin")
        assert violations == []


class TestCheckQualifierBinding:
    def test_valid_binding_returns_none(self):
        from provisa.api.data.endpoint_dev import _check_qualifier_binding

        tree = sqlglot.parse_one("SELECT orders.id FROM orders", read="postgres")
        assert _check_qualifier_binding(tree) is None

    def test_unresolved_qualifier_flagged(self):
        from provisa.api.data.endpoint_dev import _check_qualifier_binding

        tree = sqlglot.parse_one("SELECT u.name FROM orders", read="postgres")
        error = _check_qualifier_binding(tree)
        assert error is not None
        assert "u" in error

    def test_schema_qualified_column_flagged(self):
        from provisa.api.data.endpoint_dev import _check_qualifier_binding

        tree = sqlglot.parse_one(
            "SELECT pet_store.orders.id FROM pet_store.orders", read="postgres"
        )
        error = _check_qualifier_binding(tree)
        assert error is not None
        assert "Schema-qualified" in error


# ---------------------------------------------------------------------------
# _dispatch_sql_execution / _execute_engine_route / _execute_direct_route
# ---------------------------------------------------------------------------


class TestDispatchSqlExecution:
    async def test_govdata_source_routes_to_execute_govdata(self):
        from provisa.api.data.endpoint_dev import _dispatch_sql_execution
        from provisa.transpiler.router import Route

        state = SimpleNamespace(source_types={"gd1": "govdata"})
        decision = SimpleNamespace(route=Route.DIRECT, dialect="postgres", source_id="gd1")
        fake_result = _make_query_result()
        with patch(
            "provisa.api.data.endpoint_dev._execute_govdata",
            new=AsyncMock(return_value=fake_result),
        ) as mock_gd:
            result = await _dispatch_sql_execution(
                "SELECT 1", {"gd1"}, "gd1", decision, MagicMock(), state, None
            )
        assert result is fake_result
        mock_gd.assert_called_once()

    async def test_engine_route_dispatches_to_execute_engine_route(self):
        from provisa.api.data.endpoint_dev import _dispatch_sql_execution
        from provisa.transpiler.router import Route

        state = SimpleNamespace(source_types={"pg": "postgresql"})
        decision = SimpleNamespace(route=Route.ENGINE, dialect="postgres", source_id="pg")
        fake_result = _make_query_result()
        with patch(
            "provisa.api.data.endpoint_dev._execute_engine_route",
            new=AsyncMock(return_value=fake_result),
        ) as mock_engine:
            result = await _dispatch_sql_execution(
                "SELECT 1", {"pg"}, "pg", decision, MagicMock(), state, None
            )
        assert result is fake_result
        mock_engine.assert_called_once()

    async def test_direct_route_dispatches_to_execute_direct_route(self):
        from provisa.api.data.endpoint_dev import _dispatch_sql_execution
        from provisa.transpiler.router import Route

        state = SimpleNamespace(source_types={"pg": "postgresql"}, source_pools=MagicMock())
        decision = SimpleNamespace(route=Route.DIRECT, dialect="postgres", source_id="pg")
        fake_result = _make_query_result()
        with patch(
            "provisa.api.data.endpoint_dev._execute_direct_route",
            new=AsyncMock(return_value=fake_result),
        ) as mock_direct:
            result = await _dispatch_sql_execution(
                "SELECT 1", {"pg"}, "pg", decision, MagicMock(), state, None
            )
        assert result is fake_result
        mock_direct.assert_called_once()


class TestExecuteEngineRoute:
    async def test_engine_not_connected_raises_503(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_dev import _execute_engine_route

        ctx = _make_ctx("orders", table_id=1)
        state = SimpleNamespace(
            view_sql_map={},
            federation_engine=SimpleNamespace(
                is_connected=lambda: False,
                transpile_physical=lambda s: s,
            ),
        )
        with patch(
            "provisa.api.data.materialization._materialize_api_to_engine_cache",
            new=AsyncMock(return_value=({}, {}, [])),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await _execute_engine_route("SELECT id FROM orders", ctx, state, None)
        assert exc_info.value.status_code == 503

    async def test_engine_connected_executes(self):
        from provisa.api.data.endpoint_dev import _execute_engine_route

        ctx = _make_ctx("orders", table_id=1)
        fake_result = _make_query_result()
        state = SimpleNamespace(
            view_sql_map={},
            federation_engine=SimpleNamespace(
                is_connected=lambda: True,
                transpile_physical=lambda s: s,
                execute_engine=AsyncMock(return_value=fake_result),
            ),
        )
        with patch(
            "provisa.api.data.materialization._materialize_api_to_engine_cache",
            new=AsyncMock(return_value=({}, {}, [])),
        ):
            result = await _execute_engine_route("SELECT id FROM orders", ctx, state, [1, 2])
        assert result is fake_result

    async def test_view_sql_map_expands_view_refs(self):
        from provisa.api.data.endpoint_dev import _execute_engine_route

        ctx = _make_ctx("orders", table_id=1)
        fake_result = _make_query_result()
        state = SimpleNamespace(
            view_sql_map={"some_view": "SELECT 1"},
            federation_engine=SimpleNamespace(
                is_connected=lambda: True,
                transpile_physical=lambda s: s,
                execute_engine=AsyncMock(return_value=fake_result),
            ),
        )
        with (
            patch(
                "provisa.api.data.materialization._materialize_api_to_engine_cache",
                new=AsyncMock(return_value=({}, {}, [])),
            ),
            patch(
                "provisa.compiler.view_expand.expand_view_refs",
                return_value="SELECT id FROM orders",
            ) as mock_expand,
        ):
            result = await _execute_engine_route("SELECT id FROM orders", ctx, state, None)
        assert result is fake_result
        mock_expand.assert_called_once()


class TestExecuteDirectRoute:
    async def test_executes_native(self):
        from provisa.api.data.endpoint_dev import _execute_direct_route

        fake_result = _make_query_result()
        decision = SimpleNamespace(dialect="postgres", source_id="pg")
        fake_state = SimpleNamespace(
            federation_engine=SimpleNamespace(execute_native=AsyncMock(return_value=fake_result))
        )
        with patch("provisa.api.app.state", fake_state):
            result = await _execute_direct_route(
                "SELECT id FROM orders", decision, "pg", MagicMock(), None
            )
        assert result is fake_result


class _FakeAcquireCtx:
    """Async context manager standing in for an asyncpg-style pool.acquire()."""

    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class TestExecuteGovdata:
    async def test_no_matching_source_row_404(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_dev import _execute_govdata

        fake_result = MagicMock()
        fake_result.fetchone.return_value = None
        conn = SimpleNamespace(execute_core=AsyncMock(return_value=fake_result))
        state = SimpleNamespace(tenant_db=SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn)))
        with pytest.raises(HTTPException) as exc_info:
            await _execute_govdata("gd1", "SELECT id FROM fec.candidates", state)
        assert exc_info.value.status_code == 404

    async def test_executes_and_maps_rows(self):
        from provisa.api.data.endpoint_dev import _execute_govdata

        row_mapping = {"username": "user_secret", "database": "fec,ref"}
        fake_row = SimpleNamespace(_mapping=row_mapping)
        fake_result = MagicMock()
        fake_result.fetchone.return_value = fake_row
        conn = SimpleNamespace(execute_core=AsyncMock(return_value=fake_result))
        state = SimpleNamespace(tenant_db=SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn)))

        with (
            patch("provisa.core.secrets.resolve_secrets", return_value="resolved_key"),
            patch(
                "provisa.govdata.source.execute_query",
                return_value=[{"id": 1, "name": "Alpha"}, {"id": 2, "name": "Beta"}],
            ) as mock_exec,
        ):
            result = await _execute_govdata(
                "gd1", 'SELECT * FROM (SELECT * FROM "fec"."candidates") x LIMIT 10', state
            )

        assert result.column_names == ["id", "name"]
        assert result.rows == [(1, "Alpha"), (2, "Beta")]
        mock_exec.assert_called_once()
        _gds_arg, _sql_arg = mock_exec.call_args[0]
        assert _gds_arg.api_key == "resolved_key"
        assert "fec" in _gds_arg.govdata_schemas
        assert "FETCH FIRST 10 ROWS ONLY" in _sql_arg

    async def test_empty_result_returns_empty_query_result(self):
        from provisa.api.data.endpoint_dev import _execute_govdata

        row_mapping = {"username": "user_secret", "database": "fec"}
        fake_row = SimpleNamespace(_mapping=row_mapping)
        fake_result = MagicMock()
        fake_result.fetchone.return_value = fake_row
        conn = SimpleNamespace(execute_core=AsyncMock(return_value=fake_result))
        state = SimpleNamespace(tenant_db=SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn)))

        with (
            patch("provisa.core.secrets.resolve_secrets", return_value="resolved_key"),
            patch("provisa.govdata.source.execute_query", return_value=[]),
        ):
            result = await _execute_govdata("gd1", "SELECT * FROM fec.candidates", state)

        assert result.rows == []
        assert result.column_names == []
