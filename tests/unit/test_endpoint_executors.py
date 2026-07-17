# Copyright (c) 2026 Kenneth Stott
# Canary: ea247cb6-e79b-4ddb-9f37-cdabc8779e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/api/data/endpoint_executors.py.

All dependencies are deferred (function-local) imports in the source module,
so patches target the *source* module path, never
provisa.api.data.endpoint_executors.<name>.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import contextmanager

import pytest

from provisa.api_source.engine_cache import CacheLocation
from provisa.api_source.models import ApiColumn, ApiColumnType, ParamType
from provisa.compiler.sql_types import CompiledQuery, ColumnRef
from provisa.executor.result import QueryResult


# asyncio_mode = "auto" (pyproject.toml) picks up async defs automatically.


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _col(
    name: str, param_type: ParamType | None = None, param_name: str | None = None
) -> ApiColumn:
    return ApiColumn(
        name=name,
        type=ApiColumnType.string,
        filterable=True,
        param_type=param_type,
        param_name=param_name,
    )


def _compiled(**kwargs) -> CompiledQuery:
    defaults: dict[str, Any] = dict(
        sql="SELECT id FROM pets",
        params=[],
        root_field="pets",
        columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"pg"},
        canonical_field="pets",
    )
    defaults.update(kwargs)
    return CompiledQuery(**defaults)


@contextmanager
def _fake_isolated_sync():
    yield MagicMock()


def _make_ctx(table_name: str = "pets", table_id: int = 1, source_id: str = "pg"):
    from provisa.compiler.sql_gen import CompilationContext, TableMeta

    ctx = CompilationContext()
    ctx.tables = {
        table_name: TableMeta(
            table_id=table_id,
            field_name=table_name,
            type_name=table_name.capitalize(),
            source_id=source_id,
            catalog_name=source_id,
            schema_name="public",
            table_name=table_name,
            domain_id="",
        )
    }
    ctx.joins = {}
    return ctx


def _query_result(rows=None, column_names=None) -> QueryResult:
    return QueryResult(
        rows=rows if rows is not None else [(1, "Fido")],
        column_names=column_names if column_names is not None else ["id", "name"],
    )


# ---------------------------------------------------------------------------
# _execute_api_source
# ---------------------------------------------------------------------------


class TestExecuteApiSource:
    async def test_no_endpoint_registered_raises_400(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _execute_api_source

        ctx = _make_ctx("pets")
        compiled = _compiled()
        state = SimpleNamespace(contexts={"admin": ctx}, api_endpoints={})
        with pytest.raises(HTTPException) as exc_info:
            await _execute_api_source(compiled, ctx, state, "api1", "pets", "json")
        assert exc_info.value.status_code == 400

    async def test_hot_table_bypass(self):
        from provisa.api.data.endpoint_executors import _execute_api_source
        from provisa.cache.hot_tables import HotTableEntry

        ctx = _make_ctx("pets")
        compiled = _compiled()
        ep = SimpleNamespace(columns=[_col("id")], source_id="api1", table_name="pets")
        hot_entry = HotTableEntry(
            table_name="pets",
            catalog="",
            schema="",
            pk_column="",
            rows=[{"id": 1, "name": "Fido"}],
            column_names=["id", "name"],
            is_api=True,
        )
        hot_mgr = SimpleNamespace(
            is_hot=lambda tn: True,
            get_entry=lambda tn: hot_entry,
        )
        engine = SimpleNamespace(
            transpile_physical=lambda s: s,
            execute_engine_sync=lambda sql, params: _query_result(),
        )
        state = SimpleNamespace(
            contexts={"admin": ctx},
            api_endpoints={"pets": ep},
            hot_manager=hot_mgr,
            federation_engine=engine,
            api_sources={},
            org_id="default",
        )
        with patch(
            "provisa.api_source.engine_cache.cache_location",
            return_value=CacheLocation("cat", "sch", "relational"),
        ):
            (
                field_rows,
                response_data,
                phase1_ms,
                phase2_ms,
                physical_sql,
                from_cache,
            ) = await _execute_api_source(compiled, ctx, state, "api1", "pets", "json")
        assert phase1_ms == 0.0
        assert from_cache is True

    async def test_in_process_cache_hit_phase2(self):
        from provisa.api.data.endpoint_executors import _execute_api_source

        ctx = _make_ctx("pets")
        compiled = _compiled()
        ep = SimpleNamespace(columns=[_col("id")], source_id="api1", table_name="pets")
        engine = SimpleNamespace(
            transpile_physical=lambda s: s,
            execute_engine_sync=lambda sql, params: _query_result(),
            isolated_sync=_fake_isolated_sync,
        )
        state = SimpleNamespace(
            contexts={"admin": ctx},
            api_endpoints={"pets": ep},
            hot_manager=None,
            federation_engine=engine,
            api_sources={},
            org_id="default",
            source_cache={},
            response_cache_default_ttl=300,
        )
        with (
            patch(
                "provisa.api_source.engine_cache.cache_location",
                return_value=CacheLocation("cat", "sch", "relational"),
            ),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_pets"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
            patch(
                "provisa.api_source.engine_cache.rewrite_from_cache",
                return_value="SELECT id FROM r_pets",
            ),
            patch(
                "provisa.api.data.endpoint_executors._materialize_api_to_engine_cache",
                new=AsyncMock(return_value=({}, {}, [])),
            ),
        ):
            (
                field_rows,
                response_data,
                phase1_ms,
                phase2_ms,
                physical_sql,
                from_cache,
            ) = await _execute_api_source(compiled, ctx, state, "api1", "pets", "json")
        assert phase1_ms == 0.0
        assert from_cache is True

    async def test_cache_miss_hydrates_via_handle_api_query(self):
        from provisa.api.data.endpoint_executors import _execute_api_source

        ctx = _make_ctx("pets")
        compiled = _compiled()
        ep = SimpleNamespace(columns=[_col("id")], source_id="api1", table_name="pets")
        engine = SimpleNamespace(
            transpile_physical=lambda s: s,
            execute_engine_sync=lambda sql, params: _query_result(),
            isolated_sync=_fake_isolated_sync,
        )
        state = SimpleNamespace(
            contexts={"admin": ctx},
            api_endpoints={"pets": ep},
            hot_manager=None,
            federation_engine=engine,
            api_sources={},
            org_id="default",
            source_cache={},
            response_cache_default_ttl=300,
        )
        handle_result = SimpleNamespace(
            cache_table="r_pets", from_cache=False, rows=[{"id": 1, "name": "Fido"}]
        )
        with (
            patch(
                "provisa.api_source.engine_cache.cache_location",
                return_value=CacheLocation("cat", "sch", "relational"),
            ),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_pets"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch(
                "provisa.api_source.router_integration.handle_api_query",
                new=AsyncMock(return_value=handle_result),
            ),
            patch(
                "provisa.api_source.engine_cache.rewrite_from_cache",
                return_value="SELECT id FROM r_pets",
            ),
            patch(
                "provisa.api.data.endpoint_executors._materialize_api_to_engine_cache",
                new=AsyncMock(return_value=({}, {}, [])),
            ),
        ):
            (
                field_rows,
                response_data,
                phase1_ms,
                phase2_ms,
                physical_sql,
                from_cache,
            ) = await _execute_api_source(compiled, ctx, state, "api1", "pets", "json")
        assert from_cache is False


# ---------------------------------------------------------------------------
# _execute_grpc_remote_source
# ---------------------------------------------------------------------------


class TestExecuteGrpcRemoteSource:
    async def test_no_registered_source_raises_400(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _execute_grpc_remote_source

        ctx = _make_ctx("pets")
        compiled = _compiled()
        state = SimpleNamespace(grpc_remote_sources={}, contexts={"admin": ctx})
        with pytest.raises(HTTPException) as exc_info:
            await _execute_grpc_remote_source(compiled, ctx, state, "grpc1", "pets", "json")
        assert exc_info.value.status_code == 400

    async def test_no_matching_query_raises_400(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _execute_grpc_remote_source

        ctx = _make_ctx("pets")
        compiled = _compiled()
        reg = {"namespace": "", "queries": []}
        state = SimpleNamespace(grpc_remote_sources={"grpc1": reg}, contexts={"admin": ctx})
        with pytest.raises(HTTPException) as exc_info:
            await _execute_grpc_remote_source(compiled, ctx, state, "grpc1", "pets", "json")
        assert exc_info.value.status_code == 400

    async def test_cache_hit_rewrites_and_skips_fetch(self):
        from provisa.api.data.endpoint_executors import _execute_grpc_remote_source

        ctx = _make_ctx("Svc__GetPets")
        compiled = _compiled()
        grpc_query = SimpleNamespace(service="Svc", method="GetPets")
        reg = {"namespace": "", "queries": [grpc_query], "pb2": MagicMock()}
        engine = SimpleNamespace(
            isolated_sync=_fake_isolated_sync,
            transpile_physical=lambda s: s,
            execute_engine_sync=lambda sql, params: _query_result(),
        )
        state = SimpleNamespace(
            grpc_remote_sources={"grpc1": reg},
            contexts={"admin": ctx},
            federation_engine=engine,
            org_id="default",
            hot_manager=None,
        )
        with (
            patch("provisa.api_source.engine_cache.resolved_cache_catalog", return_value="cat"),
            patch(
                "provisa.api_source.engine_cache.cache_location",
                return_value=CacheLocation("cat", "sch", "relational"),
            ),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_grpc"),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
            patch(
                "provisa.api_source.engine_cache.rewrite_from_cache",
                return_value="SELECT id FROM r_grpc",
            ),
        ):
            (
                field_rows,
                response_data,
                phase1_ms,
                phase2_ms,
                physical_sql,
                from_cache,
            ) = await _execute_grpc_remote_source(
                compiled, ctx, state, "grpc1", "Svc__GetPets", "json"
            )
        assert phase1_ms == 0.0

    async def test_cache_miss_fetches_and_lands_cache(self):
        from provisa.api.data.endpoint_executors import _execute_grpc_remote_source

        ctx = _make_ctx("Svc__GetPets")
        compiled = _compiled()
        grpc_query = SimpleNamespace(
            service="Svc",
            method="GetPets",
            full_method_path="/svc/GetPets",
            input_message="In",
            output_message="Out",
            server_streaming=False,
            columns=[],
        )
        reg = {
            "namespace": "",
            "queries": [grpc_query],
            "pb2": MagicMock(),
            "cache_ttl": 300,
        }
        engine = SimpleNamespace(
            isolated_sync=_fake_isolated_sync,
            transpile_physical=lambda s: s,
            execute_engine_sync=lambda sql, params: _query_result(),
        )
        state = SimpleNamespace(
            grpc_remote_sources={"grpc1": reg},
            contexts={"admin": ctx},
            federation_engine=engine,
            org_id="default",
            hot_manager=None,
        )
        with (
            patch("provisa.api_source.engine_cache.resolved_cache_catalog", return_value="cat"),
            patch(
                "provisa.api_source.engine_cache.cache_location",
                return_value=CacheLocation("cat", "sch", "relational"),
            ),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_grpc"),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch(
                "provisa.source_adapters.grpc_remote_adapter.fetch",
                new=AsyncMock(return_value=[{"id": 1, "name": "Fido"}]),
            ),
            patch("provisa.api_source.engine_cache.land_api_cache", new=AsyncMock()),
            patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
        ):
            (
                field_rows,
                response_data,
                phase1_ms,
                phase2_ms,
                physical_sql,
                from_cache,
            ) = await _execute_grpc_remote_source(
                compiled, ctx, state, "grpc1", "Svc__GetPets", "json"
            )
        assert from_cache is False

    async def test_cache_write_failure_falls_back_to_values_cte(self):
        from provisa.api.data.endpoint_executors import _execute_grpc_remote_source

        ctx = _make_ctx("Svc__GetPets")
        compiled = _compiled()
        grpc_query = SimpleNamespace(
            service="Svc",
            method="GetPets",
            full_method_path="/svc/GetPets",
            input_message="In",
            output_message="Out",
            server_streaming=False,
            columns=[],
        )
        reg = {
            "namespace": "",
            "queries": [grpc_query],
            "pb2": MagicMock(),
            "cache_ttl": 300,
        }
        engine = SimpleNamespace(
            isolated_sync=_fake_isolated_sync,
            transpile_physical=lambda s: s,
            execute_engine_sync=lambda sql, params: _query_result(),
        )
        state = SimpleNamespace(
            grpc_remote_sources={"grpc1": reg},
            contexts={"admin": ctx},
            federation_engine=engine,
            org_id="default",
            hot_manager=None,
        )
        with (
            patch("provisa.api_source.engine_cache.resolved_cache_catalog", return_value="cat"),
            patch(
                "provisa.api_source.engine_cache.cache_location",
                return_value=CacheLocation("cat", "sch", "relational"),
            ),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_grpc"),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch(
                "provisa.source_adapters.grpc_remote_adapter.fetch",
                new=AsyncMock(return_value=[{"id": 1, "name": "Fido"}]),
            ),
            patch(
                "provisa.api_source.engine_cache.land_api_cache",
                new=AsyncMock(side_effect=RuntimeError("write failed")),
            ),
        ):
            (
                field_rows,
                response_data,
                phase1_ms,
                phase2_ms,
                physical_sql,
                from_cache,
            ) = await _execute_grpc_remote_source(
                compiled, ctx, state, "grpc1", "Svc__GetPets", "json"
            )
        assert from_cache is False


# ---------------------------------------------------------------------------
# _execute_engine_standard
# ---------------------------------------------------------------------------


class TestExecuteEngineStandard:
    async def test_engine_not_connected_raises_503(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _execute_engine_standard

        ctx = _make_ctx("pets")
        compiled = _compiled()
        state = SimpleNamespace(federation_engine=SimpleNamespace(is_connected=lambda: False))
        with pytest.raises(HTTPException) as exc_info:
            await _execute_engine_standard(
                compiled, ctx, state, "admin", "pets", None, {}, "SELECT..."
            )
        assert exc_info.value.status_code == 503

    async def test_executes_and_returns_nine_tuple(self):
        from provisa.api.data.endpoint_executors import _execute_engine_standard

        ctx = _make_ctx("pets")
        compiled = _compiled()
        engine_result = _query_result()
        engine = SimpleNamespace(
            is_connected=lambda: True,
            transpile_physical=lambda s: s,
            execute_engine=AsyncMock(return_value=engine_result),
        )
        state = SimpleNamespace(
            federation_engine=engine,
            source_types={"pg": "postgresql"},
            source_federation_hints={},
            hot_manager=None,
            engine_conn_kwargs=None,
        )
        with (
            patch(
                "provisa.api.data.endpoint_executors._hydrate_api_tables_before_engine",
                new=AsyncMock(return_value=({}, {}, 0, 0)),
            ),
            patch(
                "provisa.api.data.endpoint_executors._materialize_api_to_engine_cache",
                new=AsyncMock(return_value=({}, {}, [])),
            ),
        ):
            result = await _execute_engine_standard(
                compiled, ctx, state, "admin", "pets", None, {}, "SELECT..."
            )
        assert result[0] is engine_result
        assert len(result) == 9

    async def test_probe_limit_and_hot_promotion_task(self):
        from provisa.api.data.endpoint_executors import _execute_engine_standard

        ctx = _make_ctx("pets")
        compiled = _compiled()
        engine_result = _query_result()
        engine = SimpleNamespace(
            is_connected=lambda: True,
            transpile_physical=lambda s: s,
            execute_engine=AsyncMock(return_value=engine_result),
        )
        hot_mgr = SimpleNamespace(maybe_promote=AsyncMock())
        state = SimpleNamespace(
            federation_engine=engine,
            source_types={"pg": "postgresql"},
            source_federation_hints={},
            hot_manager=hot_mgr,
            engine_conn_kwargs=None,
        )
        with (
            patch(
                "provisa.api.data.endpoint_executors._hydrate_api_tables_before_engine",
                new=AsyncMock(return_value=({}, {}, 0, 0)),
            ),
            patch(
                "provisa.api.data.endpoint_executors._materialize_api_to_engine_cache",
                new=AsyncMock(return_value=({}, {}, [])),
            ),
        ):
            await _execute_engine_standard(
                compiled, ctx, state, "admin", "pets", 100, {"k": "v"}, None
            )
            import asyncio

            await asyncio.sleep(0)
        hot_mgr.maybe_promote.assert_called_once()


# ---------------------------------------------------------------------------
# _exec_nodes_query
# ---------------------------------------------------------------------------


class TestExecNodesQuery:
    async def test_direct_route(self):
        from provisa.api.data.endpoint_executors import _exec_nodes_query
        from provisa.transpiler.router import Route

        ctx = _make_ctx("pets")
        compiled = _compiled(nodes_sql="SELECT id FROM pets", nodes_params=[])
        nodes_result = _query_result()
        engine = SimpleNamespace(execute_native=AsyncMock(return_value=nodes_result))
        decision = SimpleNamespace(route=Route.DIRECT, source_id="pg", dialect="postgres")
        state = SimpleNamespace(federation_engine=engine, source_pools=MagicMock())
        result = await _exec_nodes_query(compiled, ctx, state, decision)
        assert result is nodes_result

    async def test_engine_route(self):
        from provisa.api.data.endpoint_executors import _exec_nodes_query
        from provisa.transpiler.router import Route

        ctx = _make_ctx("pets")
        compiled = _compiled(nodes_sql="SELECT id FROM pets", nodes_params=[])
        nodes_result = _query_result()
        engine = SimpleNamespace(
            transpile_physical=lambda s: s,
            execute_engine=AsyncMock(return_value=nodes_result),
        )
        decision = SimpleNamespace(route=Route.ENGINE, source_id=None, dialect="postgres")
        state = SimpleNamespace(federation_engine=engine, engine_conn_kwargs=None)
        result = await _exec_nodes_query(compiled, ctx, state, decision)
        assert result is nodes_result


# ---------------------------------------------------------------------------
# _store_response_cache / _store_api_source_cache
# ---------------------------------------------------------------------------


class TestStoreResponseCache:
    async def test_stores_when_ttl_positive(self):
        from provisa.api.data.endpoint_executors import _store_response_cache

        ctx = _make_ctx("pets", table_id=42)
        compiled = _compiled()
        state = SimpleNamespace(
            source_cache={"pg": {}},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
        )
        with (
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 300)),
            patch(
                "provisa.api.data.endpoint_executors.store_result", new=AsyncMock()
            ) as mock_store,
        ):
            await _store_response_cache(
                state, "ck1", {"data": {}}, "pets", ctx, compiled, None, False
            )
        mock_store.assert_called_once()

    async def test_no_cache_flag_skips_store(self):
        from provisa.api.data.endpoint_executors import _store_response_cache

        ctx = _make_ctx("pets", table_id=42)
        compiled = _compiled()
        state = SimpleNamespace(
            source_cache={"pg": {}},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
        )
        with (
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 300)),
            patch(
                "provisa.api.data.endpoint_executors.store_result", new=AsyncMock()
            ) as mock_store,
        ):
            await _store_response_cache(
                state, "ck1", {"data": {}}, "pets", ctx, compiled, None, True
            )
        mock_store.assert_not_called()

    async def test_zero_ttl_skips_store(self):
        from provisa.api.data.endpoint_executors import _store_response_cache

        ctx = _make_ctx("pets", table_id=42)
        compiled = _compiled()
        state = SimpleNamespace(
            source_cache={"pg": {}},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
        )
        with (
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 0)),
            patch(
                "provisa.api.data.endpoint_executors.store_result", new=AsyncMock()
            ) as mock_store,
        ):
            await _store_response_cache(
                state, "ck1", {"data": {}}, "pets", ctx, compiled, None, False
            )
        mock_store.assert_not_called()


class TestStoreApiSourceCache:
    async def test_stores_when_ttl_positive(self):
        from provisa.api.data.endpoint_executors import _store_api_source_cache

        ctx = _make_ctx("pets", table_id=7)
        state = SimpleNamespace(
            source_cache={"api1": {}},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
        )
        with (
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 300)),
            patch(
                "provisa.api.data.endpoint_executors.store_result", new=AsyncMock()
            ) as mock_store,
        ):
            await _store_api_source_cache(
                state, "ck1", {"data": {}}, "pets", "pets", ctx, "api1", None, False
            )
        mock_store.assert_called_once()


# ---------------------------------------------------------------------------
# _exec_api_route
# ---------------------------------------------------------------------------


class TestExecApiRoute:
    async def test_grpc_remote_dispatch(self):
        from provisa.api.data.endpoint_executors import _exec_api_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        decision = SimpleNamespace(source_id="grpc1")
        state = SimpleNamespace(
            source_types={"grpc1": "grpc_remote"},
            source_cache={},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
        )
        exec_result = ([{"id": 1}], {"data": {"pets": [{"id": 1}]}}, 1.0, 2.0, "SELECT 1", True)
        with (
            patch(
                "provisa.api.data.endpoint_executors._execute_grpc_remote_source",
                new=AsyncMock(return_value=exec_result),
            ),
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 300)),
            patch("provisa.api.data.endpoint_executors.store_result", new=AsyncMock()),
        ):
            root_field, field_rows, _, ck, _ = await _exec_api_route(
                compiled, ctx, state, decision, "pets", "json", "ck1", None, False
            )
        assert root_field == "pets"
        assert field_rows == [{"id": 1}]

    async def test_api_source_dispatch(self):
        from provisa.api.data.endpoint_executors import _exec_api_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        decision = SimpleNamespace(source_id="api1")
        state = SimpleNamespace(
            source_types={"api1": "openapi"},
            source_cache={},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
            api_sources={},
        )
        exec_result = ([{"id": 1}], {"data": {"pets": [{"id": 1}]}}, 1.0, 2.0, "SELECT 1", False)
        with (
            patch(
                "provisa.api.data.endpoint_executors._execute_api_source",
                new=AsyncMock(return_value=exec_result),
            ),
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 300)),
            patch("provisa.api.data.endpoint_executors.store_result", new=AsyncMock()),
        ):
            root_field, field_rows, _, ck, _ = await _exec_api_route(
                compiled, ctx, state, decision, "pets", "json", "ck1", None, False
            )
        assert field_rows == [{"id": 1}]

    async def test_http_exception_reraised(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _exec_api_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        decision = SimpleNamespace(source_id="api1")
        state = SimpleNamespace(source_types={"api1": "openapi"})
        with patch(
            "provisa.api.data.endpoint_executors._execute_api_source",
            new=AsyncMock(side_effect=HTTPException(status_code=400, detail="bad")),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await _exec_api_route(
                    compiled, ctx, state, decision, "pets", "json", "ck1", None, False
                )
        assert exc_info.value.status_code == 400

    async def test_connection_error_becomes_503(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _exec_api_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        decision = SimpleNamespace(source_id="api1")
        state = SimpleNamespace(source_types={"api1": "openapi"})
        with patch(
            "provisa.api.data.endpoint_executors._execute_api_source",
            new=AsyncMock(side_effect=ConnectionError("down")),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await _exec_api_route(
                    compiled, ctx, state, decision, "pets", "json", "ck1", None, False
                )
        assert exc_info.value.status_code == 503

    async def test_generic_exception_becomes_500(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _exec_api_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        decision = SimpleNamespace(source_id="api1")
        state = SimpleNamespace(source_types={"api1": "openapi"})
        with patch(
            "provisa.api.data.endpoint_executors._execute_api_source",
            new=AsyncMock(side_effect=ValueError("boom")),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await _exec_api_route(
                    compiled, ctx, state, decision, "pets", "json", "ck1", None, False
                )
        assert exc_info.value.status_code == 500

    async def test_non_dict_response_skips_cache_store(self):
        from provisa.api.data.endpoint_executors import _exec_api_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        decision = SimpleNamespace(source_id="api1")
        state = SimpleNamespace(
            source_types={"api1": "openapi"},
            source_cache={},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
            api_sources={},
        )
        exec_result = ([1, 2], b"raw-bytes", 1.0, 2.0, "SELECT 1", False)
        with (
            patch(
                "provisa.api.data.endpoint_executors._execute_api_source",
                new=AsyncMock(return_value=exec_result),
            ),
            patch(
                "provisa.api.data.endpoint_executors.store_result", new=AsyncMock()
            ) as mock_store,
        ):
            await _exec_api_route(
                compiled, ctx, state, decision, "pets", "json", "ck1", None, False
            )
        mock_store.assert_not_called()


# ---------------------------------------------------------------------------
# _exec_ctas_route
# ---------------------------------------------------------------------------


class TestExecCtasRoute:
    async def test_full_ctas_flow(self):
        from provisa.api.data.endpoint_executors import _exec_ctas_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        engine = SimpleNamespace(
            transpile_physical=lambda s: s,
            ctas_redirect=lambda sql, fmt: {"s3_prefix": "s3://bucket/x", "row_count": 5},
        )
        state = SimpleNamespace(federation_engine=engine)
        redirect_config = SimpleNamespace(ttl=3600)
        with (
            patch(
                "provisa.api.data.endpoint_executors._hydrate_api_tables_before_engine",
                new=AsyncMock(return_value=({}, {}, 0, 0)),
            ),
            patch(
                "provisa.api.data.endpoint_executors._materialize_api_to_engine_cache",
                new=AsyncMock(return_value=({}, {}, [])),
            ),
            patch(
                "provisa.executor.redirect.presign_ctas_result",
                new=AsyncMock(return_value="https://presigned"),
            ),
            patch("provisa.executor.redirect.schedule_s3_cleanup", new=AsyncMock()),
        ):
            result = await _exec_ctas_route(compiled, ctx, state, "parquet", redirect_config)
            import asyncio

            await asyncio.sleep(0)
        assert result["redirect_url"] == "https://presigned"
        assert result["row_count"] == 5
        assert result["content_type"] == "application/vnd.apache.parquet"

    async def test_dropped_tables_rewrites_union_branches(self):
        from provisa.api.data.endpoint_executors import _exec_ctas_route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        engine = SimpleNamespace(
            transpile_physical=lambda s: s,
            ctas_redirect=lambda sql, fmt: {"s3_prefix": "s3://bucket/x", "row_count": 0},
        )
        state = SimpleNamespace(federation_engine=engine)
        redirect_config = SimpleNamespace(ttl=3600)
        with (
            patch(
                "provisa.api.data.endpoint_executors._hydrate_api_tables_before_engine",
                new=AsyncMock(return_value=({}, {}, 0, 0)),
            ),
            patch(
                "provisa.api.data.endpoint_executors._materialize_api_to_engine_cache",
                new=AsyncMock(return_value=({}, {}, ["dead_table"])),
            ),
            patch(
                "provisa.compiler.nf_extractor.drop_union_branches_for_table",
                return_value="SELECT id FROM pets",
            ) as mock_drop,
            patch(
                "provisa.executor.redirect.presign_ctas_result",
                new=AsyncMock(return_value="https://presigned"),
            ),
            patch("provisa.executor.redirect.schedule_s3_cleanup", new=AsyncMock()),
        ):
            await _exec_ctas_route(compiled, ctx, state, "orc", redirect_config)
            import asyncio

            await asyncio.sleep(0)
        mock_drop.assert_called_once()


# ---------------------------------------------------------------------------
# _exec_probe_redirect
# ---------------------------------------------------------------------------


class TestExecProbeRedirect:
    async def test_direct_route(self):
        from provisa.api.data.endpoint_executors import _exec_probe_redirect
        from provisa.transpiler.router import Route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        full_result = _query_result()
        engine = SimpleNamespace(execute_native=AsyncMock(return_value=full_result))
        decision = SimpleNamespace(route=Route.DIRECT, source_id="pg", dialect="postgres")
        state = SimpleNamespace(federation_engine=engine, source_pools=MagicMock())
        redirect_config = SimpleNamespace(ttl=3600)
        with patch(
            "provisa.executor.redirect.upload_and_presign",
            new=AsyncMock(return_value={"redirect_url": "https://x"}),
        ) as mock_upload:
            result = await _exec_probe_redirect(
                compiled, ctx, state, decision, {}, "parquet", redirect_config
            )
        assert result == {"redirect_url": "https://x"}
        mock_upload.assert_called_once()

    async def test_engine_route(self):
        from provisa.api.data.endpoint_executors import _exec_probe_redirect
        from provisa.transpiler.router import Route

        ctx = _make_ctx("pets")
        compiled = _compiled()
        full_result = _query_result()
        engine = SimpleNamespace(
            transpile_physical=lambda s: s,
            execute_engine=AsyncMock(return_value=full_result),
        )
        decision = SimpleNamespace(route=Route.ENGINE, source_id=None, dialect="postgres")
        state = SimpleNamespace(federation_engine=engine, engine_conn_kwargs=None)
        redirect_config = SimpleNamespace(ttl=3600)
        with patch(
            "provisa.executor.redirect.upload_and_presign",
            new=AsyncMock(return_value={"redirect_url": "https://y"}),
        ) as mock_upload:
            result = await _exec_probe_redirect(
                compiled,
                ctx,
                state,
                decision,
                {"hint": "v"},
                "csv",
                redirect_config,
                role_id="admin",
            )
        assert result == {"redirect_url": "https://y"}
        mock_upload.assert_called_once()


# ---------------------------------------------------------------------------
# _exec_inline_result
# ---------------------------------------------------------------------------


class TestExecInlineResult:
    async def test_plain_select_format_response(self):
        from provisa.api.data.endpoint_executors import _exec_inline_result
        from provisa.transpiler.router import Route

        ctx = _make_ctx("pets", table_id=9)
        compiled = _compiled()
        result = _query_result()
        decision = SimpleNamespace(route=Route.ENGINE, source_id=None)
        state = SimpleNamespace(
            source_cache={},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
        )
        with (
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 300)),
            patch(
                "provisa.api.data.endpoint_executors.store_result", new=AsyncMock()
            ) as mock_store,
            patch("provisa.api.data.endpoint_helpers._record_per_source_stats"),
        ):
            root_field, field_rows, _, ck, _ = await _exec_inline_result(
                compiled,
                ctx,
                state,
                decision,
                "pets",
                result,
                "json",
                "ck1",
                None,
                False,
                0.0,
                {},
                {},
                1.0,
                0,
                0,
                "SELECT id FROM pets",
            )
        assert root_field == "pets"
        mock_store.assert_called_once()

    async def test_group_by_serialization(self):
        from provisa.api.data.endpoint_executors import _exec_inline_result
        from provisa.transpiler.router import Route

        ctx = _make_ctx("pets", table_id=9)
        compiled = _compiled(
            nodes_sql="SELECT id FROM pets",
            nodes_params=[],
            nodes_columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
            is_group_by=True,
        )
        result = _query_result()
        nodes_result = _query_result()
        decision = SimpleNamespace(route=Route.ENGINE, source_id=None)
        state = SimpleNamespace(
            source_cache={},
            table_cache={},
            response_cache_default_ttl=300,
            response_cache_store=MagicMock(),
            federation_engine=SimpleNamespace(
                transpile_physical=lambda s: s,
                execute_engine=AsyncMock(return_value=nodes_result),
            ),
            engine_conn_kwargs=None,
        )
        with (
            patch(
                "provisa.api.data.endpoint_executors.serialize_group_by",
                return_value={"data": {"pets": [{"id": 1}]}},
            ) as mock_gb,
            patch("provisa.cache.policy.resolve_policy", return_value=(None, 300)),
            patch("provisa.api.data.endpoint_executors.store_result", new=AsyncMock()),
            patch("provisa.api.data.endpoint_helpers._record_per_source_stats"),
        ):
            root_field, field_rows, _, ck, _ = await _exec_inline_result(
                compiled,
                ctx,
                state,
                decision,
                "pets",
                result,
                "json",
                "ck1",
                None,
                False,
                0.0,
                {},
                {},
                1.0,
                0,
                0,
                "SELECT id FROM pets",
            )
        mock_gb.assert_called_once()
        assert field_rows == [{"id": 1}]

    async def test_nodes_query_connection_error_becomes_503(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_executors import _exec_inline_result
        from provisa.transpiler.router import Route

        ctx = _make_ctx("pets", table_id=9)
        compiled = _compiled(
            nodes_sql="SELECT id FROM pets",
            nodes_params=[],
            nodes_columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        )
        result = _query_result()
        decision = SimpleNamespace(route=Route.ENGINE, source_id=None)
        state = SimpleNamespace(
            federation_engine=SimpleNamespace(
                transpile_physical=lambda s: s,
                execute_engine=AsyncMock(side_effect=ConnectionError("down")),
            ),
            engine_conn_kwargs=None,
        )
        with pytest.raises(HTTPException) as exc_info:
            await _exec_inline_result(
                compiled,
                ctx,
                state,
                decision,
                "pets",
                result,
                "json",
                "ck1",
                None,
                False,
                0.0,
                {},
                {},
                1.0,
                0,
                0,
                "SELECT id FROM pets",
            )
        assert exc_info.value.status_code == 503
