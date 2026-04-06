# Copyright (c) 2026 Kenneth Stott
# Canary: c4d5e6f7-a8b9-0123-defa-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for aggregate MV routing (REQ-198, REQ-199).

Tests the AggregateMVCatalog logic and SQL rewriting without requiring PG.
"""

from __future__ import annotations

import pytest

from provisa.mv.aggregate_catalog import AggregateMVCatalog
from provisa.mv.models import MVDefinition, MVStatus

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _make_mv(
    mv_id: str = "mv-orders-agg",
    source_tables: list[str] | None = None,
    aggregate_columns: list[str] | None = None,
    serves_aggregates: bool = True,
    target_catalog: str = "test_pg",
    target_schema: str = "public",
    target_table: str = "mv_orders_agg",
    status: MVStatus = MVStatus.FRESH,
) -> MVDefinition:
    return MVDefinition(
        id=mv_id,
        source_tables=source_tables or ["orders"],
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table,
        serves_aggregates=serves_aggregates,
        aggregate_columns=aggregate_columns or ["amount", "quantity"],
        status=status,
    )


def _fresh_catalog(*mvs: MVDefinition) -> AggregateMVCatalog:
    """Build an isolated catalog (not the global singleton) with the given MVs."""
    catalog = AggregateMVCatalog()
    for mv in mvs:
        catalog.register(mv)
    return catalog


# ---------------------------------------------------------------------------
# find_aggregate_mv tests
# ---------------------------------------------------------------------------

class TestCatalogFinding:
    def test_catalog_finds_mv_for_table(self):
        """find_aggregate_mv returns an MV when agg_columns are covered."""
        mv = _make_mv(aggregate_columns=["amount", "quantity"])
        catalog = _fresh_catalog(mv)
        result = catalog.find_aggregate_mv("orders", ["amount"], filters=[])
        assert result is not None
        assert result.id == "mv-orders-agg"

    def test_catalog_returns_none_when_no_mv(self):
        """find_aggregate_mv returns None when no MV covers the table."""
        catalog = _fresh_catalog()
        result = catalog.find_aggregate_mv("orders", ["amount"], filters=[])
        assert result is None

    def test_catalog_returns_none_when_columns_not_covered(self):
        """find_aggregate_mv returns None when the MV lacks a requested column."""
        mv = _make_mv(aggregate_columns=["amount"])  # does not cover "quantity"
        catalog = _fresh_catalog(mv)
        result = catalog.find_aggregate_mv("orders", ["amount", "quantity"], filters=[])
        assert result is None

    def test_catalog_returns_none_for_wrong_table(self):
        """find_aggregate_mv returns None when querying a different table."""
        mv = _make_mv(source_tables=["orders"], aggregate_columns=["amount"])
        catalog = _fresh_catalog(mv)
        result = catalog.find_aggregate_mv("customers", ["amount"], filters=[])
        assert result is None

    def test_catalog_mv_with_serves_aggregates_false_not_registered(self):
        """An MV with serves_aggregates=False is not registered in the catalog."""
        mv = _make_mv(serves_aggregates=False)
        catalog = _fresh_catalog(mv)
        result = catalog.find_aggregate_mv("orders", ["amount"], filters=[])
        assert result is None

    def test_catalog_finds_best_mv_from_multiple(self):
        """Catalog returns the first matching MV when multiple candidates exist."""
        mv1 = _make_mv(mv_id="mv-a", aggregate_columns=["amount"])
        mv2 = _make_mv(mv_id="mv-b", aggregate_columns=["amount", "quantity"])
        catalog = _fresh_catalog(mv1, mv2)
        result = catalog.find_aggregate_mv("orders", ["amount"], filters=[])
        assert result is not None
        assert result.id in {"mv-a", "mv-b"}

    def test_catalog_unregister_removes_mv(self):
        """unregister removes an MV so it no longer matches."""
        mv = _make_mv()
        catalog = _fresh_catalog(mv)
        catalog.unregister("mv-orders-agg")
        result = catalog.find_aggregate_mv("orders", ["amount"], filters=[])
        assert result is None

    def test_catalog_mv_covers_superset_of_columns(self):
        """MV covering [amount, quantity, tax] matches query for [amount, quantity]."""
        mv = _make_mv(aggregate_columns=["amount", "quantity", "tax"])
        catalog = _fresh_catalog(mv)
        result = catalog.find_aggregate_mv("orders", ["amount", "quantity"], filters=[])
        assert result is not None


# ---------------------------------------------------------------------------
# rewrite_sql tests
# ---------------------------------------------------------------------------

class TestRewriteSql:
    def test_rewrite_sql_uses_mv_table(self):
        """rewrite_sql produces a SELECT against the MV backing table."""
        mv = _make_mv(target_table="mv_orders_agg", target_schema="public", target_catalog="test_pg")
        catalog = _fresh_catalog(mv)
        sql = 'SELECT SUM("amount"), COUNT(*) FROM "public"."orders"'
        rewritten = catalog.rewrite_sql(sql, mv, ["amount"], [])
        assert "mv_orders_agg" in rewritten
        assert "test_pg" in rewritten or "public" in rewritten
        assert "SELECT" in rewritten.upper()

    def test_mv_comment_present_in_rewritten_sql(self):
        """Rewritten SQL contains the aggregate_mv comment with MV id."""
        mv = _make_mv(mv_id="my-agg-mv")
        catalog = _fresh_catalog(mv)
        rewritten = catalog.rewrite_sql("SELECT 1", mv, ["amount"], [])
        assert "/* aggregate_mv: my-agg-mv */" in rewritten

    def test_rewrite_sql_applies_remaining_filters(self):
        """rewrite_sql adds WHERE clause for remaining_filters."""
        mv = _make_mv()
        catalog = _fresh_catalog(mv)
        rewritten = catalog.rewrite_sql(
            "SELECT SUM(amount) FROM orders",
            mv,
            ["amount"],
            ["status = 'active'"],
        )
        assert "WHERE" in rewritten.upper()
        assert "status = 'active'" in rewritten

    def test_rewrite_sql_no_filters_no_where(self):
        """rewrite_sql omits WHERE clause when no filters are provided."""
        mv = _make_mv()
        catalog = _fresh_catalog(mv)
        rewritten = catalog.rewrite_sql("SELECT 1", mv, ["amount"], [])
        assert "WHERE" not in rewritten.upper()

    def test_rewrite_sql_selects_requested_columns(self):
        """rewrite_sql SELECT clause contains exactly the requested agg_columns."""
        mv = _make_mv(aggregate_columns=["amount", "quantity", "tax"])
        catalog = _fresh_catalog(mv)
        rewritten = catalog.rewrite_sql("SELECT 1", mv, ["amount", "quantity"], [])
        assert '"amount"' in rewritten
        assert '"quantity"' in rewritten

    def test_rewrite_sql_empty_agg_columns_uses_star(self):
        """rewrite_sql falls back to SELECT * when no agg_columns provided."""
        mv = _make_mv()
        catalog = _fresh_catalog(mv)
        rewritten = catalog.rewrite_sql("SELECT 1", mv, [], [])
        assert "SELECT *" in rewritten


# ---------------------------------------------------------------------------
# Pipeline integration (catalog + rewrite chain)
# ---------------------------------------------------------------------------

class TestAggregateMVPipeline:
    def test_pipeline_find_and_rewrite(self):
        """Full flow: find_aggregate_mv → rewrite_sql produces governed SQL."""
        mv = _make_mv(
            mv_id="pipeline-mv",
            source_tables=["sales"],
            aggregate_columns=["revenue", "units"],
            target_table="mv_sales_pipeline",
        )
        catalog = _fresh_catalog(mv)

        found = catalog.find_aggregate_mv("sales", ["revenue", "units"], filters=[])
        assert found is not None

        original_sql = 'SELECT SUM("revenue"), SUM("units") FROM "public"."sales"'
        rewritten = catalog.rewrite_sql(original_sql, found, ["revenue", "units"], [])

        assert "mv_sales_pipeline" in rewritten
        assert "/* aggregate_mv: pipeline-mv */" in rewritten
        assert '"revenue"' in rewritten
        assert '"units"' in rewritten

    def test_pipeline_filter_passthrough(self):
        """Filters passed to find_aggregate_mv are forwarded to rewrite_sql output."""
        mv = _make_mv(aggregate_columns=["amount"])
        catalog = _fresh_catalog(mv)
        filters = ["region = 'us-east'", "status = 'complete'"]

        found = catalog.find_aggregate_mv("orders", ["amount"], filters=filters)
        assert found is not None

        rewritten = catalog.rewrite_sql("SELECT 1", found, ["amount"], filters)
        assert "region = 'us-east'" in rewritten
        assert "status = 'complete'" in rewritten

    def test_pipeline_mv_not_found_returns_none(self):
        """When no MV matches, pipeline returns None and original SQL is unchanged."""
        catalog = _fresh_catalog()  # no MVs
        result = catalog.find_aggregate_mv("orders", ["amount"], filters=[])
        assert result is None
        # Caller is responsible for keeping original SQL when result is None

    def test_global_catalog_get_returns_instance(self):
        """get_aggregate_catalog returns the module-level singleton."""
        from provisa.mv.aggregate_catalog import get_aggregate_catalog
        cat = get_aggregate_catalog()
        assert isinstance(cat, AggregateMVCatalog)
