# Copyright (c) 2026 Kenneth Stott
# Canary: 0d9da60a-a199-4890-a8cc-1cb2f7295a2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for materialized view refresh engine.

Uses a mock Trino connection to test CTAS, atomic refresh, row count
tracking, and error handling without requiring a live Trino instance.
"""

from __future__ import annotations

import pytest

from provisa.executor.result import QueryResult
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.refresh import _build_refresh_sql, _target_ref, refresh_mv
from provisa.mv.registry import MVRegistry


class _FakeEngine:
    """Records SQL; SHOW COLUMNS/COUNT(*) return configured rows. ``side_effect`` and
    ``raise_all`` reproduce the old cursor.execute side effects (e.g. table-not-found)."""

    def __init__(self, count=0, show_columns=None, side_effect=None, raise_all=None):
        self.count = count
        self.show_columns = show_columns or []
        self.sqls: list[str] = []
        self._side_effect = side_effect
        self._raise_all = raise_all

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        if self._side_effect is not None:
            self._side_effect(sql)
        if self._raise_all is not None:
            raise self._raise_all
        if "SHOW COLUMNS" in sql:
            return QueryResult(rows=self.show_columns, column_names=[])
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(self.count,)], column_names=[])
        return QueryResult(rows=[], column_names=[])


def _jp_mv(mv_id="mv-orders-customers"):
    return MVDefinition(
        id=mv_id,
        source_tables=["orders", "customers"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        refresh_interval=300,
    )


def _sql_mv(mv_id="mv-customer-stats"):
    return MVDefinition(
        id=mv_id,
        source_tables=["orders", "customers"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        sql=(
            "SELECT c.id AS customer_id, c.name, "
            "COUNT(o.id) AS order_count "
            "FROM orders o JOIN customers c ON o.customer_id = c.id "
            "GROUP BY c.id, c.name"
        ),
        refresh_interval=600,
    )


@pytest.mark.asyncio(loop_scope="session")
class TestBuildRefreshSQL:
    async def test_custom_sql_used_directly(self):
        mv = _sql_mv()
        result = await _build_refresh_sql(mv)
        assert result == mv.sql

    async def test_join_pattern_without_introspection_raises(self):
        mv = _jp_mv()
        with pytest.raises(ValueError, match="engine required to introspect"):
            await _build_refresh_sql(mv)

    async def test_join_pattern_with_column_introspection(self):
        mv = _jp_mv()
        engine = _FakeEngine(show_columns=[("id",), ("name",), ("email",)])
        result = await _build_refresh_sql(mv, engine)
        assert '"customers"."id" AS "customers__id"' in result
        assert '"customers"."name" AS "customers__name"' in result
        assert '"customers"."email" AS "customers__email"' in result
        assert '"orders".*' in result

    async def test_join_pattern_introspection_failure_raises(self):
        mv = _jp_mv()
        engine = _FakeEngine(raise_all=Exception("introspection failed"))
        # Introspection failure must fail loud, not silently drop right-table columns.
        with pytest.raises(RuntimeError, match="could not introspect columns"):
            await _build_refresh_sql(mv, engine)

    async def test_no_sql_no_join_raises(self):
        mv = MVDefinition(
            id="bad-mv",
            source_tables=[],
            target_catalog="pg",
            target_schema="mv",
        )
        with pytest.raises(ValueError, match="neither sql nor join_pattern"):
            await _build_refresh_sql(mv)


class TestMaterializeStoreTarget:
    """Regression: an MV must target the store the ACTIVE engine materializes into. A DuckDB engine
    attaches its store as ``mat_store``; hardcoding ``postgresql`` failed the refresh with
    "Catalog with name postgresql does not exist" on a DuckDB deployment.
    """

    def test_native_backend_targets_attached_store(self):
        # A native engine (DuckDB here) targets the catalog it attaches its store under and the
        # runtime's declared MV schema — never the Postgres store default.
        from provisa.federation.duckdb_backend import DuckDBBackend

        class _RT:
            def ensure_materialize_attached(self) -> str:
                return "mat_store"

            def mv_store_schema(self, org_id: str) -> str:
                return "mat"

        be = DuckDBBackend.__new__(DuckDBBackend)  # skip full engine init
        be._runtime_for = lambda _state: _RT()  # type: ignore[method-assign]
        assert be.materialize_store_target(object(), "acme") == ("mat_store", "mat")

    def test_duckdb_runtime_mv_schema_is_store_schema(self):
        # DuckDB's MV schema is its store schema (mat/main), org-independent.
        from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

        rt = DuckDBFederationRuntime.__new__(DuckDBFederationRuntime)
        rt._store_schema = lambda: "mat"  # type: ignore[method-assign]
        assert rt.mv_store_schema("acme") == "mat"

    def test_warehouse_runtime_mv_schema_is_org_scoped(self):
        # Databricks/BigQuery isolate MVs in an org-scoped cache namespace in the warehouse/project.
        from provisa.federation.databricks_runtime import DatabricksFederationRuntime
        from provisa.federation.bigquery_runtime import BigQueryFederationRuntime

        db = DatabricksFederationRuntime.__new__(DatabricksFederationRuntime)
        bq = BigQueryFederationRuntime.__new__(BigQueryFederationRuntime)
        assert db.mv_store_schema("acme") == "org_acme_mv_cache"
        assert bq.mv_store_schema("acme") == "org_acme_mv_cache"

    def test_base_backend_defaults_to_pg_store(self):
        # An own-store (Postgres) engine keeps the Postgres store target + org-scoped schema.
        from provisa.federation.backend import EngineBackend

        cat, schema = EngineBackend.materialize_store_target(
            None,  # type: ignore[arg-type]  # self unused
            object(),
            "acme",
        )
        assert cat == "postgresql"
        assert schema == "org_acme_mv_cache"


class TestTargetRef:
    def test_fully_qualified(self):
        mv = _jp_mv()
        ref = _target_ref(mv)
        assert ref == '"postgresql"."mv_cache"."mv_mv_orders_customers"'


@pytest.mark.asyncio(loop_scope="session")
class TestRefreshMV:
    async def test_first_refresh_creates_table(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        # Table does not exist — raise on the target existence probe
        # (SELECT * FROM {target} LIMIT 0); the "_shape" probe only runs when it exists.
        def side_effect(sql):
            if "LIMIT 0" in sql and "_shape" not in sql:
                raise Exception("TABLE_NOT_FOUND")

        engine = _FakeEngine(count=42, side_effect=side_effect)
        await refresh_mv(engine, mv, registry)

        assert any("CREATE TABLE" in c for c in engine.sqls)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 42

    async def test_subsequent_refresh_deletes_and_inserts(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        engine = _FakeEngine(count=100)  # SELECT 1 succeeds -> table exists
        await refresh_mv(engine, mv, registry)

        assert any("DELETE FROM" in c for c in engine.sqls)
        assert any("INSERT INTO" in c for c in engine.sqls)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 100

    async def test_refresh_failure_marks_stale(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        engine = _FakeEngine(raise_all=Exception("connection lost"))
        await refresh_mv(engine, mv, registry)

        assert mv.status == MVStatus.STALE
        assert mv.last_error is not None
        assert "connection lost" in mv.last_error

    async def test_refresh_marks_refreshing_during_execution(self):
        mv = _jp_mv()
        registry = MVRegistry()
        registry.register(mv)

        statuses_seen = []

        def capture_status(sql):
            statuses_seen.append(mv.status)

        engine = _FakeEngine(count=10, side_effect=capture_status)
        await refresh_mv(engine, mv, registry)

        assert MVStatus.REFRESHING in statuses_seen
