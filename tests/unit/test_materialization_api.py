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
    _mat_fetch_rows_from_fills,
    _mat_fetch_rows_from_rest,
    _mat_gql_remote_table,
    _mat_store_rows,
    _materialize_api_to_engine_cache,
    _normalize_mat_value,
    _promote_joined_from_fills,
)
from provisa.api_source.engine_cache import CacheLocation
from provisa.api_source.models import ApiColumn, ApiColumnType, ParamType

# asyncio_mode = "auto" (pyproject.toml) picks up async defs automatically;
# no module-level pytest.mark.asyncio needed (and it would warn on sync tests).


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _hot_manager() -> SimpleNamespace:
    """A stand-in hot-table manager: ``hold`` keeps the entry, as the real one does for a name
    one relation claims."""
    held: dict = {}

    def hold(entry) -> bool:
        held[entry.table_name] = entry
        return True

    return SimpleNamespace(_hot_tables=held, hold=hold)


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
        assert found_tbl is not None
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


def _store_state():
    """A state whose engine connection is a real DuckDB store, as every engine hands one out."""
    import duckdb

    from provisa.api_source import engine_cache, fill_cache
    from provisa.executor.session import EngineSession

    fill_cache._mem_fresh.clear()
    fill_cache._shapes.clear()
    engine_cache._SCHEMA_EXISTS_CACHE.clear()
    con = duckdb.connect()

    @contextmanager
    def isolated_sync():
        yield EngineSession(con, dialect="duckdb", placeholder="?")

    engine = SimpleNamespace(cache_catalog=lambda: "memory", isolated_sync=isolated_sync)
    return SimpleNamespace(
        org_id="o1", federation_engine=engine, source_catalogs={}, api_sources={}
    )


class TestMatFetchRowsFromFills:
    """The API step reads a table's fills from the store (``api_source.fill_cache``), never
    from the control plane."""

    def _ep(self):
        from provisa.api_source.models import ApiEndpoint

        return ApiEndpoint(
            source_id="src",
            path="/pets",
            table_name="pets",
            columns=[
                ApiColumn(name="id", type=ApiColumnType.integer),
                ApiColumn(name="name", type=ApiColumnType.string),
            ],
        )

    async def test_no_fill_yet_is_a_miss(self):
        assert await _mat_fetch_rows_from_fills(self._ep(), ["id"], set(), _store_state()) == []

    async def test_rows_are_read_and_the_fills_own_columns_are_not_projected(self):
        from provisa.api_source import fill_cache

        state, ep = _store_state(), self._ep()
        table = fill_cache.fill_table(state, ep, None)
        with state.federation_engine.isolated_sync() as conn:
            fill_cache.store(conn, table, {"h": [{"id": 1, "name": "Fido"}]}, ttl=60)
        meta = {"_params_hash", "_cached_at"}
        assert await _mat_fetch_rows_from_fills(ep, ["id", "name"], meta, state) == [
            {"id": 1, "name": "Fido"}
        ]
        assert await _mat_fetch_rows_from_fills(ep, ["name"], meta, state) == [{"name": "Fido"}]

    async def test_a_failed_read_raises(self):
        """REQ-1661 (amended 2026-09-30): a failed cache read raises -- never an empty result
        that sends the caller to a different source instead."""

        @contextmanager
        def down():
            raise RuntimeError("store down")
            yield

        state = _store_state()
        state.federation_engine.isolated_sync = down
        with pytest.raises(RuntimeError, match="store down"):
            await _mat_fetch_rows_from_fills(self._ep(), ["id"], set(), state)


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
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
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

    async def test_inlined_row_keys_are_snake_cased_to_match_column_names(self):
        """The CTE reads row[c] for c in column_names, and those names are snake_case — raw
        camelCase keys would inline NULL for every camelCase field."""
        engine = MagicMock()
        engine.isolated_sync = _fake_isolated_sync
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")

        with (
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
        ):
            _mat_store_rows(
                "pets",
                [{"id": 1, "photoUrls": '["a"]'}],
                ["id", "photoUrls"],
                loc,
                "r_abc",
                500,
                None,
                [_col("id"), _col("photoUrls")],
                engine,
                300,
                MagicMock(),
                {},
                values_cte_entries,
                all_ep_col_names=["id", "photo_urls"],
            )

        entry = values_cte_entries["pets"]
        assert entry.column_names == ["id", "photo_urls"]
        assert entry.rows == [{"id": 1, "photo_urls": '["a"]'}]

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
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
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
        hot_mgr = _hot_manager()

        with (
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
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


_FILLS = "provisa.api.data.materialization._mat_fetch_rows_from_fills"


class TestPromoteJoinedFromFills:
    async def test_promotes_within_threshold(self):
        hot_mgr = _hot_manager()
        loc = CacheLocation("cat", "sch", "relational")
        ep = SimpleNamespace(table_name="pets")
        with patch(_FILLS, new=AsyncMock(return_value=[{"id": 1, "name": "Fido"}])):
            await _promote_joined_from_fills(
                SimpleNamespace(), ep, "pets", hot_mgr, ["id", "name"], set(), loc, 500
            )
        assert hot_mgr._hot_tables["pets"].rows == [{"id": 1, "name": "Fido"}]

    async def test_over_threshold_not_promoted(self):
        hot_mgr = _hot_manager()
        loc = CacheLocation("cat", "sch", "relational")
        with patch(_FILLS, new=AsyncMock(return_value=[{"id": i} for i in range(5)])):
            await _promote_joined_from_fills(
                SimpleNamespace(),
                SimpleNamespace(table_name="pets"),
                "pets",
                hot_mgr,
                ["id"],
                set(),
                loc,
                2,
            )
        assert hot_mgr._hot_tables == {}

    async def test_fetch_failure_swallowed(self):
        hot_mgr = _hot_manager()
        loc = CacheLocation("cat", "sch", "relational")
        with patch(_FILLS, new=AsyncMock(side_effect=RuntimeError("down"))):
            # Must not raise — best-effort promotion for a later request.
            await _promote_joined_from_fills(
                SimpleNamespace(),
                SimpleNamespace(table_name="pets"),
                "pets",
                hot_mgr,
                ["id"],
                set(),
                loc,
                500,
            )
        assert hot_mgr._hot_tables == {}


def _ep(columns):
    return SimpleNamespace(source_id="src", table_name="pets", ttl=60, columns=columns)


def _col(name, param_type=None, param_only=False):
    return ApiColumn(
        name=name, type=ApiColumnType.string, param_type=param_type, param_only=param_only
    )


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
        # param_only: a path param that does NOT also appear in the response, so the endpoint
        # projects nothing and materialization has to skip it.
        ep = _ep([_col("id", param_type="path", param_only=True)])
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
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch(_FILLS, new=AsyncMock(return_value=[{"id": 1}])),
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert "pets" in values_cte_entries
        assert values_cte_entries["pets"].rows == [{"id": 1}]

    async def test_merged_param_and_response_column_is_still_projected(self):
        """A query param whose name collides with a response field is merged into one column
        carrying both. It holds a response value, so it must be read from the fills."""
        fills = AsyncMock(return_value=[{"id": 1, "status": "sold"}])
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
        )
        ep = _ep([_col("id"), _col("status", param_type="query")])
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch(_FILLS, new=fills),
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
        ):
            await _mat_api_ep_table("pets", ep, state, None, 500, set(), {}, values_cte_entries)

        # The projection is the col_set the step asks the fills for, which dropped `status` while
        # the response set was keyed on param_type.
        assert fills.await_args.args[1] == ["id", "status"]
        assert values_cte_entries["pets"].rows == [{"id": 1, "status": "sold"}]

    async def test_cache_hit_promotes_when_hot_mgr(self):
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
        )
        ep = _ep([_col("id")])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        hot_mgr = _hot_manager()
        with (
            patch(_FILLS, new=AsyncMock(return_value=[{"id": 1}])),
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, hot_mgr, 500, set(), cache_rewrites, values_cte_entries
            )
            # REQ-1882: promotion is detached onto a background worker thread — wait (bounded)
            # for it, with the fills still standing in for the store.
            deadline = asyncio.get_running_loop().time() + 5
            while (
                "pets" not in hot_mgr._hot_tables and asyncio.get_running_loop().time() < deadline
            ):
                await asyncio.sleep(0.01)
        assert cache_rewrites["pets"] == (loc, "r_x")
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

    async def test_path_param_resolved_from_nf_args_materializes(self):
        """A required-path-param endpoint (e.g. get_pet_by_id keyed by petId) must resolve
        the param from nf_args and call the REST fallback with it, instead of unconditionally
        skipping — mirrors the graphql_remote required_args resolution branch above."""
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
            tenant_db=None,
        )
        ep = _ep([_col("id"), _col("petId", param_type=ParamType.path)])
        cache_rewrites: dict = {}
        values_cte_entries: dict = {}
        loc = CacheLocation("cat", "sch", "relational")
        rest_result = SimpleNamespace(from_cache=False, rows=[{"id": 1}])
        m_handle = AsyncMock(return_value=rest_result)
        with (
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch("provisa.api_source.router_integration.handle_api_query", new=m_handle),
            patch("provisa.api_source.engine_cache.create_and_insert"),
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
        ):
            await _mat_api_ep_table(
                "pets",
                ep,
                state,
                None,
                500,
                set(),
                cache_rewrites,
                values_cte_entries,
                nf_args={"petId": "1"},
            )
        assert values_cte_entries["pets"].rows == [{"id": 1}]
        assert m_handle.await_args is not None
        assert m_handle.await_args.args[1] == {"petId": "1"}

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
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
        ):
            await _mat_api_ep_table(
                "pets", ep, state, None, 500, set(), cache_rewrites, values_cte_entries
            )
        assert values_cte_entries["pets"].rows == [{"id": 7}]

    async def test_a_failed_fills_read_fails_the_query_without_a_rest_fetch(self):
        """REQ-1661 (amended 2026-09-30): the fills read failing is an error, not a cue to
        fetch the rows from REST instead."""
        state = SimpleNamespace(
            api_sources={},
            org_id="default",
            federation_engine=MagicMock(),
            source_cache={},
            response_cache_default_ttl=300,
        )
        rest = AsyncMock()
        loc = CacheLocation("cat", "sch", "relational")
        with (
            patch(_FILLS, new=AsyncMock(side_effect=RuntimeError("store down"))),
            patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
            patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_exists", return_value=False),
            patch("provisa.api.data.materialization._mat_fetch_rows_from_rest", new=rest),
            pytest.raises(RuntimeError, match="store down"),
        ):
            await _mat_api_ep_table("pets", _ep([_col("id")]), state, None, 500, set(), {}, {})
        rest.assert_not_awaited()

    async def test_a_failed_rest_fetch_fails_the_query(self):
        """REQ-1661 (amended 2026-09-30): an expired API cache whose live re-fetch fails raises
        the fetch's cause -- never logged and skipped, leaving the engine to answer from stale
        cached rows."""
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
            pytest.raises(RuntimeError, match="rest down"),
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
            config=SimpleNamespace(
                graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)
            ),
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
            config=SimpleNamespace(
                graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)
            ),
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
            config=SimpleNamespace(
                graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)
            ),
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
            patch("provisa.api_source.engine_cache.schedule_drop", new=MagicMock()),
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
            config=SimpleNamespace(
                graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)
            ),
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
        assert dropped == {}

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
        assert dropped == {}

    async def test_an_api_endpoint_table_needs_no_control_plane(self):
        """The fills are in the store: an API endpoint table is materialized whether or not a
        tenant plane is open."""
        ep = _ep([_col("id")])
        state = SimpleNamespace(
            hot_manager=None,
            api_endpoints={"pets": ep},
            graphql_remote_sources={},
            tenant_db=None,
        )
        loc = CacheLocation("cat", "sch", "relational")

        async def materialized(tn, ep, state, hot_mgr, threshold, meta, rewrites, ctes, **kw):
            rewrites[tn] = (loc, "r_x")

        step = AsyncMock(side_effect=materialized)
        with patch("provisa.api.data.materialization._mat_api_ep_table", new=step):
            rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
                "SELECT * FROM pets", state
            )
        assert step.await_args.args[:2] == ("pets", ep)
        assert (rewrites, ctes, dropped) == ({"pets": (loc, "r_x")}, {}, {})

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
        assert dropped == {
            "pets": "requires filter(s) ['name'] — add a WHERE clause with the "
            "required parameter(s)"
        }
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
        assert dropped == {}

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
            config=SimpleNamespace(
                graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)
            ),
        )
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
                "SELECT * FROM pets", state, nf_args={"name": "Fido"}
            )
        assert dropped == {}
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
            config=SimpleNamespace(
                graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)
            ),
        )
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=True),
        ):
            rewrites, ctes, dropped = await _materialize_api_to_engine_cache(
                "SELECT * FROM pets", state
            )
        assert dropped == {}
        assert "pets" in rewrites

    async def test_a_failed_gql_remote_branch_fails_the_query(self):
        """REQ-1661 (amended 2026-09-30): an unreachable remote fails the whole query -- its
        UNION branch is never dropped to return the other branches' rows as if complete."""
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
            config=SimpleNamespace(
                graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)
            ),
        )
        with (
            patch("provisa.api_source.engine_cache.ensure_cache_schema"),
            patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
            patch(
                "provisa.graphql_remote.executor.execute_remote",
                new=AsyncMock(side_effect=RuntimeError("remote down")),
            ),
            pytest.raises(RuntimeError, match="remote down"),
        ):
            await _materialize_api_to_engine_cache("SELECT * FROM pets", state)

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
            # post-call drop-check branch instead of the earlier `continue`. The cache read
            # succeeds with no rows; the missing path param is what leaves it unmaterialized.
            tenant_db=SimpleNamespace(
                acquire=lambda: _FakeAcquireCtx(AsyncMock(fetch=AsyncMock(return_value=[])))
            ),
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
        assert dropped == {"pets": "could not be materialized"}
        assert rewrites == {}
        assert ctes == {}

    @pytest.mark.parametrize(
        ("lookup", "materialize", "reg"),
        [
            (
                "_lookup_grpc_remote_table",
                "_mat_grpc_remote_table",
                ("grpcsrc", object(), object()),
            ),
            ("_lookup_openapi_table", "_mat_openapi_table", ("oasrc", object(), object())),
        ],
    )
    async def test_a_failed_grpc_or_openapi_branch_fails_the_query(self, lookup, materialize, reg):
        """REQ-1661 (amended 2026-09-30): a failed gRPC / OpenAPI remote fetch fails the query
        instead of dropping its UNION branch."""
        state = SimpleNamespace(hot_manager=None, api_endpoints={}, graphql_remote_sources={})
        m = "provisa.api.data.materialization"
        with (
            patch(f"{m}._lookup_grpc_remote_table", return_value=(None, None, None)),
            patch(f"{m}._lookup_openapi_table", return_value=(None, None, None)),
            patch(f"{m}.{lookup}", return_value=reg),
            patch(f"{m}.{materialize}", new=AsyncMock(side_effect=ConnectionError("remote 500"))),
            pytest.raises(ConnectionError, match="remote 500"),
        ):
            await _materialize_api_to_engine_cache(
                "SELECT id FROM pets UNION ALL SELECT id FROM pets", state
            )
