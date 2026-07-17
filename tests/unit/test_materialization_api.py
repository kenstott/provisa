# Copyright (c) 2026 Kenneth Stott
# Canary: 4a7e698e-32b4-4945-bf4a-62b069334f9d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/api/data/materialization.py — API-source materialization
helpers used by the engine-routed /data/sql and /data/query paths.

These are pure-function / mocked-dependency unit tests (not full-stack e2e): the
materialization module's own dependencies (engine_cache, land_api_cache,
handle_api_query, execute_remote) are patched so branches are reachable without a
running federation engine or PG.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api.data.materialization import (
    _lookup_ep,
    _lookup_gql_remote_table,
    _mat_api_ep_table,
    _mat_fetch_rows_from_pg,
    _mat_fetch_rows_from_rest,
    _mat_gql_remote_table,
    _mat_store_rows,
    _materialize_api_to_engine_cache,
    _normalize_mat_value,
    _promote_joined_from_pg,
)
from provisa.api_source.engine_cache import CacheLocation
from provisa.api_source.models import ApiColumn, ApiColumnType, ParamType

# asyncio_mode = "auto" (pyproject.toml) picks up async defs automatically;
# no module-level pytest.mark.asyncio needed (and it would warn on sync tests).


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestLookupEp:
    def test_found(self):
        ep = object()
        state = SimpleNamespace(api_endpoints={"pets": ep})
        assert _lookup_ep(state, "pets") is ep

    def test_missing(self):
        state = SimpleNamespace(api_endpoints={})
        assert _lookup_ep(state, "pets") is None

    def test_no_attr_defaults_empty(self):
        state = SimpleNamespace()
        assert _lookup_ep(state, "pets") is None


class TestLookupGqlRemoteTable:
    def test_match_sql_name(self):
        reg = {"tables": [{"sql_name": "pets", "name": "Pet"}]}
        state = SimpleNamespace(graphql_remote_sources={"gh": reg})
        found_reg, found_tbl = _lookup_gql_remote_table(state, "pets")
        assert found_reg is reg
        assert found_tbl["sql_name"] == "pets"

    def test_no_match(self):
        state = SimpleNamespace(graphql_remote_sources={"gh": {"tables": []}})
        found_reg, found_tbl = _lookup_gql_remote_table(state, "pets")
        assert found_reg is None
        assert found_tbl is None


class TestNormalizeMatValue:
    def test_dict_becomes_json(self):
        assert _normalize_mat_value({"a": 1}) == '{"a": 1}'

    def test_list_becomes_json(self):
        assert _normalize_mat_value([1, 2]) == "[1, 2]"

    def test_none_passthrough(self):
        assert _normalize_mat_value(None) is None

    def test_scalar_passthrough(self):
        assert _normalize_mat_value(42) == 42
        assert _normalize_mat_value(3.14) == 3.14
        assert _normalize_mat_value(True) is True

    def test_other_stringified(self):
        class Weird:
            def __str__(self):
                return "weird"

        assert _normalize_mat_value(Weird()) == "weird"


# ---------------------------------------------------------------------------
# _mat_fetch_rows_from_pg
# ---------------------------------------------------------------------------


class _FakeAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *a):
        return False


class TestMatFetchRowsFromPg:
    async def test_no_tenant_db_returns_empty(self):
        state = SimpleNamespace(tenant_db=None)
        ep = SimpleNamespace(table_name="pets")
        rows, ok = await _mat_fetch_rows_from_pg(ep, ["id"], set(), state)
        assert rows == []
        assert ok is False

    async def test_success(self):
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"id": 1, "name": "Fido", "_cached_at": "x"}])
        tenant_db = SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn))
        state = SimpleNamespace(tenant_db=tenant_db)
        ep = SimpleNamespace(table_name="pets")
        rows, ok = await _mat_fetch_rows_from_pg(ep, ["id", "name"], {"_cached_at"}, state)
        assert ok is True
        assert rows == [{"id": 1, "name": "Fido"}]

    async def test_pg_failure_returns_empty(self):
        tenant_db = SimpleNamespace(acquire=MagicMock(side_effect=RuntimeError("down")))
        state = SimpleNamespace(tenant_db=tenant_db)
        ep = SimpleNamespace(table_name="pets")
        rows, ok = await _mat_fetch_rows_from_pg(ep, ["id"], set(), state)
        assert rows == []
        assert ok is False


# ---------------------------------------------------------------------------
# _mat_fetch_rows_from_rest
# ---------------------------------------------------------------------------


class TestMatFetchRowsFromRest:
    async def test_cache_hit_registers_rewrite_returns_none(self):
        rest_result = SimpleNamespace(from_cache=True, rows=[])
        cache_rewrites: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with patch(
            "provisa.api_source.router_integration.handle_api_query",
            new=AsyncMock(return_value=rest_result),
        ):
            out = await _mat_fetch_rows_from_rest(
                SimpleNamespace(table_name="pets"),
                ["id"],
                MagicMock(),
                None,
                "src",
                SimpleNamespace(source_cache={}, response_cache_default_ttl=300),
                loc,
                "r_abc",
                cache_rewrites,
            )
        assert out is None
        assert cache_rewrites["pets"] == (loc, "r_abc")

    async def test_cache_miss_returns_normalized_rows(self):
        rest_result = SimpleNamespace(
            from_cache=False, rows=[{"id": 1, "name": "Fido", "extra": "x"}]
        )
        cache_rewrites: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with patch(
            "provisa.api_source.router_integration.handle_api_query",
            new=AsyncMock(return_value=rest_result),
        ):
            out = await _mat_fetch_rows_from_rest(
                SimpleNamespace(table_name="pets"),
                ["id", "name"],
                MagicMock(),
                None,
                "src",
                SimpleNamespace(source_cache={}, response_cache_default_ttl=300),
                loc,
                "r_abc",
                cache_rewrites,
            )
        assert out == [{"id": 1, "name": "Fido"}]
        assert cache_rewrites == {}


# ---------------------------------------------------------------------------
# _mat_store_rows
# ---------------------------------------------------------------------------


@contextmanager
def _fake_isolated_sync():
    yield MagicMock()


class TestMatStoreRows:
    async def test_small_result_inlines_hot_entry(self):
        engine = MagicMock()
        engine.isolated_sync = _fake_isolated_sync
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        response_cols = [_col("id"), _col("name")]

        with (
            patch("provisa.api_source.engine_cache.create_and_insert") as mock_insert,
            patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
        ):
            _mat_store_rows(
                "pets",
                [{"id": 1, "name": "Fido"}],
                ["id", "name"],
                loc,
                "r_abc",
                500,
                None,
                response_cols,
                engine,
                300,
                MagicMock(),
                cache_rewrites,
                values_cte_entries,
            )
            mock_insert.assert_called_once()

        assert "pets" in values_cte_entries
        assert values_cte_entries["pets"].rows == [{"id": 1, "name": "Fido"}]
        assert cache_rewrites == {}

    async def test_large_result_uses_cache_rewrite_not_inline(self):
        engine = MagicMock()
        engine.isolated_sync = _fake_isolated_sync
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        response_cols = [_col("id")]
        big_rows = [{"id": i} for i in range(5)]

        with (
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
        ):
            _mat_store_rows(
                "pets",
                big_rows,
                ["id"],
                loc,
                "r_abc",
                2,  # hot threshold smaller than row count
                None,
                response_cols,
                engine,
                300,
                MagicMock(),
                cache_rewrites,
                values_cte_entries,
            )

        assert values_cte_entries == {}
        assert cache_rewrites["pets"] == (loc, "r_abc")

    async def test_hot_mgr_updated_when_inlined(self):
        engine = MagicMock()
        engine.isolated_sync = _fake_isolated_sync
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        response_cols = [_col("id")]
        hot_mgr = SimpleNamespace(_hot_tables={})

        with (
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
        ):
            _mat_store_rows(
                "pets",
                [{"id": 1}],
                ["id"],
                loc,
                "r_abc",
                500,
                hot_mgr,
                response_cols,
                engine,
                300,
                MagicMock(),
                cache_rewrites,
                values_cte_entries,
            )

        assert "pets" in hot_mgr._hot_tables


# ---------------------------------------------------------------------------
# _promote_joined_from_pg
# ---------------------------------------------------------------------------


class TestPromoteJoinedFromPg:
    async def test_promotes_within_threshold(self):
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"id": 1, "name": "Fido", "_cached_at": "x"}])
        tenant_db = SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn))
        state = SimpleNamespace(tenant_db=tenant_db)
        hot_mgr = SimpleNamespace(_hot_tables={})
        loc = CacheLocation("cat", "sch", "relational")
        ep = SimpleNamespace(table_name="pets")

        await _promote_joined_from_pg(
            state, ep, "pets", hot_mgr, ["id", "name"], {"_cached_at"}, loc, 500
        )
        assert "pets" in hot_mgr._hot_tables
        assert hot_mgr._hot_tables["pets"].rows == [{"id": 1, "name": "Fido"}]

    async def test_over_threshold_not_promoted(self):
        rows = [{"id": i} for i in range(5)]
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        tenant_db = SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn))
        state = SimpleNamespace(tenant_db=tenant_db)
        hot_mgr = SimpleNamespace(_hot_tables={})
        loc = CacheLocation("cat", "sch", "relational")
        ep = SimpleNamespace(table_name="pets")

        await _promote_joined_from_pg(state, ep, "pets", hot_mgr, ["id"], set(), loc, 2)
        assert hot_mgr._hot_tables == {}

    async def test_fetch_failure_swallowed(self):
        tenant_db = SimpleNamespace(acquire=MagicMock(side_effect=RuntimeError("down")))
        state = SimpleNamespace(tenant_db=tenant_db)
        hot_mgr = SimpleNamespace(_hot_tables={})
        loc = CacheLocation("cat", "sch", "relational")
        ep = SimpleNamespace(table_name="pets")

        # Must not raise — best-effort promotion.
        await _promote_joined_from_pg(state, ep, "pets", hot_mgr, ["id"], set(), loc, 500)
        assert hot_mgr._hot_tables == {}


# ---------------------------------------------------------------------------
# _mat_api_ep_table
# ---------------------------------------------------------------------------


def _ep(columns):
    return SimpleNamespace(source_id="src", table_name="pets", ttl=60, columns=columns)


def _col(name, param_type=None):
    return ApiColumn(name=name, type=ApiColumnType.string, param_type=param_type)


class TestMatApiEpTable:
    async def test_no_response_columns_skips(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id", param_type="path")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        with (
            patch("provisa.api_source.engine_cache.cache_location") as m_loc,
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
        ):
            m_loc.return_value = CacheLocation("cat", "sch", "relational")
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert cache_rewrites == {}
        assert values_cte_entries == {}

    async def test_in_process_cache_hit(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert cache_rewrites["pets"] == (loc, "r_x")

    async def test_cache_miss_pg_hydrate_then_store(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=SimpleNamespace(
                acquire=lambda: _FakeAcquireCtx(
                    AsyncMock(fetch=AsyncMock(return_value=[{"id": 1}]))
                )
            ),
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert "pets" in values_cte_entries
        assert values_cte_entries["pets"].rows == [{"id": 1}]

    async def test_cache_hit_promotes_when_hot_mgr_and_tenant_db(self):
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"id": 1}])
        tenant_db = SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn))
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=tenant_db,
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        hot_mgr = SimpleNamespace(_hot_tables={})
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, hot_mgr, 500, set(), cache_rewrites, values_cte_entries
            )
        assert cache_rewrites["pets"] == (loc, "r_x")
        # Promotion is fired via asyncio.create_task — give the loop a tick to run it.
        await asyncio.sleep(0)
        assert "pets" in hot_mgr._hot_tables

    async def test_secondary_table_exists_cache_hit(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=True),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert cache_rewrites["pets"] == (loc, "r_x")

    async def test_path_param_skip_on_pg_miss(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id"), _col("owner_id", param_type=ParamType.path)])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert cache_rewrites == {}
        assert values_cte_entries == {}

    async def test_rest_already_cached_returns_without_storing(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        rest_result = SimpleNamespace(from_cache=True, rows=[])
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch(
                "provisa.api_source.router_integration.handle_api_query",
                new=AsyncMock(return_value=rest_result),
            ),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert cache_rewrites["pets"] == (loc, "r_x")
        assert values_cte_entries == {}

    async def test_pg_miss_then_rest_fallback(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        rest_result = SimpleNamespace(from_cache=False, rows=[{"id": 7}])
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch(
                "provisa.api_source.router_integration.handle_api_query",
                new=AsyncMock(return_value=rest_result),
            ),
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert values_cte_entries["pets"].rows == [{"id": 7}]

    async def test_rest_fallback_failure_skips(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch(
                "provisa.api_source.router_integration.handle_api_query",
                new=AsyncMock(side_effect=RuntimeError("rest down")),
            ),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert cache_rewrites == {}
        assert values_cte_entries == {}


# ---------------------------------------------------------------------------
# _mat_gql_remote_table
# ---------------------------------------------------------------------------


def _gql_reg():
    return {"source_id": "ghsrc", "url": "https://example.test/graphql"}


def _gql_tbl():
    return {
        "name": "Pet",
        "field_name": "pets",
        "columns": [{"name": "id", "type": "integer"}, {"name": "name", "type": "text"}],
    }


class TestMatGqlRemoteTable:
    async def test_sqlite_store_inlines_without_caching(self):
        state = SimpleNamespace(
            graphql_remote_sources={},
            org_id="default",
            federation_engine=SimpleNamespace(
                materialize_store_dsn=lambda: "sqlite:///x.db",
                cache_catalog=lambda: "cat",
            ),
            config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        )
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        with patch(
            "provisa.graphql_remote.executor.execute_remote",
            new=AsyncMock(return_value=[{"id": 1, "name": "Fido"}]),
        ):
            await _mat_gql_remote_table(
                "pets",
                _gql_reg(),
                _gql_tbl(),
                state,
                None,
                500,
                cache_rewrites,
                values_cte_entries,
            )
        assert cache_rewrites == {}
        assert values_cte_entries["pets"].rows == [{"id": 1, "name": "Fido"}]

    async def test_engine_cache_hit_registers_rewrite(self):
        state = SimpleNamespace(
            graphql_remote_sources={},
            org_id="default",
            federation_engine=SimpleNamespace(
                materialize_store_dsn=lambda: "postgresql://x/y",
                cache_catalog=lambda: "cat",
                isolated_sync=_fake_isolated_sync,
            ),
            config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        )
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            await _mat_gql_remote_table(
                "pets",
                _gql_reg(),
                _gql_tbl(),
                state,
                None,
                500,
                cache_rewrites,
                values_cte_entries,
            )
        assert "pets" in cache_rewrites
        assert values_cte_entries == {}

    async def test_engine_cache_miss_fetches_and_lands(self):
        state = SimpleNamespace(
            graphql_remote_sources={},
            org_id="default",
            federation_engine=SimpleNamespace(
                materialize_store_dsn=lambda: "postgresql://x/y",
                cache_catalog=lambda: "cat",
                isolated_sync=_fake_isolated_sync,
            ),
            config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        )
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch(
                "provisa.graphql_remote.executor.execute_remote",
                new=AsyncMock(return_value=[{"id": 1, "name": "Fido"}]),
            ),
            patch("provisa.api_source.engine_cache.land_api_cache", new=AsyncMock()),
            patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
        ):
            await _mat_gql_remote_table(
                "pets",
                _gql_reg(),
                _gql_tbl(),
                state,
                None,
                500,
                cache_rewrites,
                values_cte_entries,
            )
        # Small result (1 row <= hot threshold) → inlined as VALUES CTE, not cache rewrite.
        assert values_cte_entries["pets"].rows == [{"id": 1, "name": "Fido"}]
        assert cache_rewrites == {}

    async def test_fetch_failure_raises_runtime_error(self):
        state = SimpleNamespace(
            graphql_remote_sources={},
            org_id="default",
            federation_engine=SimpleNamespace(
                materialize_store_dsn=lambda: "postgresql://x/y",
                cache_catalog=lambda: "cat",
                isolated_sync=_fake_isolated_sync,
            ),
            config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        )
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch(
                "provisa.graphql_remote.executor.execute_remote",
                new=AsyncMock(side_effect=RuntimeError("remote down")),
            ),
        ):
            with pytest.raises(RuntimeError, match="GQL remote fetch failed"):
                await _mat_gql_remote_table(
                    "pets", _gql_reg(), _gql_tbl(), state, None, 500, {}, {}
                )


# ---------------------------------------------------------------------------
# _materialize_api_to_engine_cache
# ---------------------------------------------------------------------------


class TestMaterializeApiToEngineCache:
    async def test_no_api_tables_returns_empty(self):
        state = SimpleNamespace(hot_manager=None)
        rewrites, ctes, dropped = await _materialize_api_to_engine_cache("SELECT 1", state)
        assert rewrites == {}
        assert ctes == {}
        assert dropped == []

    async def test_hot_table_short_circuits(self):
        from provisa.cache.hot_tables import HotTableEntry

        entry = HotTableEntry(
            table_name="pets",
            catalog="cat",
            schema="sch",
            pk_column="id",
            rows=[{"id": 1}],
            column_names=["id"],
        )
        hot_mgr = SimpleNamespace(
            is_hot=lambda tn: tn == "pets", get_entry=lambda tn: entry, auto_threshold=500
        )
        state = SimpleNamespace(hot_manager=hot_mgr, api_endpoints={}, graphql_remote_sources={})
        rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
            "SELECT * FROM pets", state
        )
        assert rewrites == {}
        assert ctes["pets"] is entry
        assert dropped == []

    async def test_no_pg_pool_skips_api_endpoint_table(self):
        ep = _ep([_col("id")])
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={"pets": ep},
            graphql_remote_sources={},
            tenant_db=None,
        )
        rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
            "SELECT * FROM pets", state
        )
        assert rewrites == {}
        assert ctes == {}
        assert dropped == []

    async def test_gql_remote_missing_required_arg_drops_branch(self):
        reg = {
            "source_id": "ghsrc",
            "url": "https://example.test/graphql",
            "tables": [
                {
                    "sql_name": "pets",
                    "name": "Pet",
                    "field_name": "pets",
                    "required_args": [{"name": "name"}],
                    "columns": [{"name": "id", "type": "integer"}],
                }
            ],
        }
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={},
            graphql_remote_sources={"gh": reg},
        )
        rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
            "SELECT * FROM pets", state, nf_args={}
        )
        assert dropped == ["pets"]
        assert rewrites == {}
        assert ctes == {}

    async def test_unmaterializable_api_table_dropped(self):
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={},
            graphql_remote_sources={},
        )
        # 'pets' is not a known API endpoint nor a graphql_remote table → falls through
        # the `if ep is None:` branch's `continue`, never reaching dropped_tables — verify
        # a genuinely unknown table produces no rewrite/cte and no crash.
        rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
            "SELECT * FROM pets", state
        )
        assert rewrites == {}
        assert ctes == {}
        assert dropped == []

    async def test_gql_remote_required_arg_resolved_materializes(self):
        reg = {
            "source_id": "ghsrc",
            "url": "https://example.test/graphql",
            "tables": [
                {
                    "sql_name": "pets",
                    "name": "Pet",
                    "field_name": "pets",
                    "required_args": [{"name": "name"}],
                    "columns": [{"name": "id", "type": "integer"}],
                }
            ],
        }
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={},
            graphql_remote_sources={"gh": reg},
            org_id="default",
            federation_engine=SimpleNamespace(
                materialize_store_dsn=lambda: "postgresql://x/y",
                cache_catalog=lambda: "cat",
                isolated_sync=_fake_isolated_sync,
            ),
            config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        )
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
                "SELECT * FROM pets", state, nf_args={"name": "Fido"}
            )
        assert dropped == []
        assert "pets" in rewrites

    async def test_gql_remote_no_required_args_materializes(self):
        reg = {
            "source_id": "ghsrc",
            "url": "https://example.test/graphql",
            "tables": [
                {
                    "sql_name": "pets",
                    "name": "Pet",
                    "field_name": "pets",
                    "columns": [{"name": "id", "type": "integer"}],
                }
            ],
        }
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={},
            graphql_remote_sources={"gh": reg},
            org_id="default",
            federation_engine=SimpleNamespace(
                materialize_store_dsn=lambda: "postgresql://x/y",
                cache_catalog=lambda: "cat",
                isolated_sync=_fake_isolated_sync,
            ),
            config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        )
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
                "SELECT * FROM pets", state
            )
        assert dropped == []
        assert "pets" in rewrites

    async def test_gql_remote_runtime_error_drops_branch(self):
        reg = {
            "source_id": "ghsrc",
            "url": "https://example.test/graphql",
            "tables": [
                {
                    "sql_name": "pets",
                    "name": "Pet",
                    "field_name": "pets",
                    "columns": [{"name": "id", "type": "integer"}],
                }
            ],
        }
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={},
            graphql_remote_sources={"gh": reg},
            org_id="default",
            federation_engine=SimpleNamespace(
                materialize_store_dsn=lambda: "postgresql://x/y",
                cache_catalog=lambda: "cat",
                isolated_sync=_fake_isolated_sync,
            ),
            config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        )
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch(
                "provisa.graphql_remote.executor.execute_remote",
                new=AsyncMock(side_effect=RuntimeError("remote down")),
            ),
        ):
            rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
                "SELECT * FROM pets", state
            )
        assert dropped == ["pets"]

    async def test_ep_found_but_unmaterializable_dropped(self):
        ep = _ep([_col("id"), _col("owner_id", param_type=ParamType.path)])
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={"pets": ep},
            graphql_remote_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            # Non-None so `_has_pg_pool` is True and execution reaches the
            # post-call drop-check branch instead of the earlier `continue`.
            tenant_db=SimpleNamespace(acquire=MagicMock(side_effect=RuntimeError("down"))),
        )
        with (
            patch("provisa.api_source.engine_cache.cache_location") as m_loc,
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
        ):
            m_loc.return_value = CacheLocation("cat", "sch", "relational")
            rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
                "SELECT * FROM pets", state
            )
        assert dropped == ["pets"]
        assert rewrites == {}
        assert ctes == {}
