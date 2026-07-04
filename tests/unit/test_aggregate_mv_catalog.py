# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for AggregateMVCatalog (REQ-198, REQ-199)."""

from __future__ import annotations

import pytest

from provisa.mv.aggregate_catalog import AggregateMVCatalog
from provisa.mv.models import MVDefinition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mv(
    mv_id: str,
    source_tables: list[str],
    serves_aggregates: bool = True,
    aggregate_columns: list[str] | None = None,
    target_catalog: str = "pg",
    target_schema: str = "public",
    target_table: str | None = None,
    filters: list[str] | None = None,
) -> MVDefinition:
    return MVDefinition(
        id=mv_id,
        source_tables=source_tables,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table or f"mv_{mv_id}",
        serves_aggregates=serves_aggregates,
        aggregate_columns=aggregate_columns or [],
        filters=filters or [],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAggregateMVCatalog:
    @pytest.fixture
    def catalog(self) -> AggregateMVCatalog:
        return AggregateMVCatalog()

    def test_register_non_aggregate_mv_ignored(self, catalog):
        """MVs with serves_aggregates=False must not be indexed."""
        mv = _mv("mv1", ["orders"], serves_aggregates=False, aggregate_columns=["amount"])
        catalog.register(mv)
        result = catalog.find_aggregate_mv("orders", ["amount"], [])
        assert result is None

    def test_find_aggregate_mv_exact_match(self, catalog):
        """MV is returned when requested columns are exactly the MV's aggregate_columns."""
        mv = _mv("mv1", ["orders"], aggregate_columns=["amount", "qty"])
        catalog.register(mv)
        result = catalog.find_aggregate_mv("orders", ["amount", "qty"], [])
        assert result is not None
        assert result.id == "mv1"

    def test_find_aggregate_mv_superset(self, catalog):
        """MV with more columns than requested still matches (superset covers subset)."""
        mv = _mv("mv1", ["orders"], aggregate_columns=["amount", "qty", "tax"])
        catalog.register(mv)
        # Request only a subset of the MV's columns
        result = catalog.find_aggregate_mv("orders", ["amount"], [])
        assert result is not None
        assert result.id == "mv1"

    def test_find_aggregate_mv_no_match_missing_col(self, catalog):
        """MV that doesn't cover all requested columns returns None."""
        mv = _mv("mv1", ["orders"], aggregate_columns=["amount"])
        catalog.register(mv)
        # Request a column the MV does not cover
        result = catalog.find_aggregate_mv("orders", ["amount", "revenue"], [])
        assert result is None

    def test_find_aggregate_mv_wrong_table(self, catalog):
        """MV registered for one table does not match a different table."""
        mv = _mv("mv1", ["orders"], aggregate_columns=["amount"])
        catalog.register(mv)
        result = catalog.find_aggregate_mv("customers", ["amount"], [])
        assert result is None

    def test_unregister_mv(self, catalog):
        """After unregistering an MV it is no longer findable."""
        mv = _mv("mv1", ["orders"], aggregate_columns=["amount"])
        catalog.register(mv)
        # Confirm it was registered
        assert catalog.find_aggregate_mv("orders", ["amount"], []) is not None
        catalog.unregister("mv1")
        assert catalog.find_aggregate_mv("orders", ["amount"], []) is None

    def test_rewrite_sql(self, catalog):
        """rewrite_sql returns SQL with aggregate_mv comment and correct table reference."""
        mv = _mv(
            "mv-orders-daily",
            ["orders"],
            aggregate_columns=["amount", "qty"],
            target_catalog="pg",
            target_schema="mv_schema",
            target_table="mv_orders_daily",
        )
        original_sql = "SELECT SUM(amount) FROM orders"
        rewritten = catalog.rewrite_sql(original_sql, mv, ["amount", "qty"], [])
        assert "/* aggregate_mv:" in rewritten
        assert "mv-orders-daily" in rewritten
        assert '"pg"."mv_schema"."mv_orders_daily"' in rewritten
        assert "amount" in rewritten

    def test_rewrite_sql_with_filters(self, catalog):
        """rewrite_sql appends remaining_filters as a WHERE clause."""
        mv = _mv("mv1", ["orders"], aggregate_columns=["amount"])
        rewritten = catalog.rewrite_sql(
            "SELECT SUM(amount) FROM orders WHERE status = 'active'",
            mv,
            ["amount"],
            ["status = 'active'"],
        )
        assert "WHERE" in rewritten
        assert "status = 'active'" in rewritten

    def test_rewrite_sql_no_filters(self, catalog):
        """rewrite_sql with no remaining_filters does not add a WHERE clause."""
        mv = _mv("mv1", ["orders"], aggregate_columns=["amount"])
        rewritten = catalog.rewrite_sql("SELECT SUM(amount) FROM orders", mv, ["amount"], [])
        assert "WHERE" not in rewritten


class TestMVDefinitionDefaultFields:
    def test_mv_model_default_fields(self):
        """MVDefinition defaults: serves_aggregates=False, aggregate_columns=[]."""
        mv = MVDefinition(
            id="test-mv",
            source_tables=["orders"],
            target_catalog="pg",
            target_schema="public",
        )
        assert mv.serves_aggregates is False
        assert mv.aggregate_columns == []

    def test_target_table_auto_generated(self):
        """target_table is auto-generated from id when not specified."""
        mv = MVDefinition(
            id="my-mv-id",
            source_tables=["orders"],
            target_catalog="pg",
            target_schema="public",
        )
        assert mv.target_table == "mv_my_mv_id"

    def test_serves_aggregates_true(self):
        """MVDefinition with serves_aggregates=True is reflected correctly."""
        mv = MVDefinition(
            id="agg-mv",
            source_tables=["sales"],
            target_catalog="pg",
            target_schema="public",
            serves_aggregates=True,
            aggregate_columns=["revenue", "units"],
        )
        assert mv.serves_aggregates is True
        assert "revenue" in mv.aggregate_columns
        assert "units" in mv.aggregate_columns


# ---------------------------------------------------------------------------
# REQ-882: filter subset-safety, query rewrite, and registry population
# ---------------------------------------------------------------------------


class TestFilterSubsetSafety:
    def test_unfiltered_mv_always_safe(self):
        cat = AggregateMVCatalog()
        cat.register(_mv("mv1", ["orders"], aggregate_columns=["amount"], filters=[]))
        # Query with a filter can use an unfiltered MV (remaining filter re-applied at rewrite).
        assert cat.find_aggregate_mv("orders", ["amount"], ["region = 'us'"]) is not None

    def test_mv_filter_subset_of_query_is_safe(self):
        cat = AggregateMVCatalog()
        cat.register(
            _mv("mv1", ["orders"], aggregate_columns=["amount"], filters=["status = 'active'"])
        )
        # MV filter is one of the query's filters → safe.
        got = cat.find_aggregate_mv("orders", ["amount"], ["status = 'active'", "region = 'us'"])
        assert got is not None and got.id == "mv1"

    def test_mv_filter_not_in_query_is_rejected(self):
        cat = AggregateMVCatalog()
        cat.register(
            _mv("mv1", ["orders"], aggregate_columns=["amount"], filters=["status = 'active'"])
        )
        # Query has no such filter → MV is more restrictive → must NOT be used.
        assert cat.find_aggregate_mv("orders", ["amount"], ["region = 'us'"]) is None
        assert cat.find_aggregate_mv("orders", ["amount"], []) is None


class TestRewriteAggregateQuery:
    def _cat(self, **kw):
        from provisa.mv.aggregate_catalog import AggregateMVCatalog

        cat = AggregateMVCatalog()
        cat.register(_mv("agg1", ["orders"], aggregate_columns=["amount"], **kw))
        return cat

    def test_rewrites_single_table_aggregate(self):
        from provisa.mv.aggregate_catalog import rewrite_aggregate_query

        cat = self._cat()
        out = rewrite_aggregate_query("SELECT SUM(amount) FROM orders", cat)
        assert out is not None
        sql, mv = out
        assert mv.id == "agg1"
        assert "mv_agg1" in sql

    def test_non_aggregate_query_not_rewritten(self):
        from provisa.mv.aggregate_catalog import rewrite_aggregate_query

        assert rewrite_aggregate_query("SELECT amount FROM orders", self._cat()) is None

    def test_joined_query_not_rewritten(self):
        from provisa.mv.aggregate_catalog import rewrite_aggregate_query

        sql = "SELECT SUM(o.amount) FROM orders o JOIN customers c ON o.cid = c.id"
        assert rewrite_aggregate_query(sql, self._cat()) is None

    def test_filtered_query_reapplies_remaining_filter(self):
        from provisa.mv.aggregate_catalog import rewrite_aggregate_query

        cat = self._cat(filters=["status = 'active'"])
        out = rewrite_aggregate_query(
            "SELECT SUM(amount) FROM orders WHERE status = 'active' AND region = 'us'", cat
        )
        assert out is not None
        sql, _mv = out
        assert "region = 'us'" in sql  # the query-only filter is re-applied on the MV

    def test_query_missing_mv_filter_not_rewritten(self):
        from provisa.mv.aggregate_catalog import rewrite_aggregate_query

        cat = self._cat(filters=["status = 'active'"])
        # Query lacks the MV's pre-computed filter → unsafe → no rewrite.
        assert rewrite_aggregate_query("SELECT SUM(amount) FROM orders", cat) is None


class TestRegistryPopulatesCatalog:
    def test_register_populates_aggregate_catalog(self):
        from provisa.mv.aggregate_catalog import get_aggregate_catalog
        from provisa.mv.registry import MVRegistry

        reg = MVRegistry()
        mv = _mv("reg-agg", ["widgets"], aggregate_columns=["price"])
        reg.register(mv)
        assert get_aggregate_catalog().find_aggregate_mv("widgets", ["price"], []) is not None
        reg.unregister("reg-agg")
        assert get_aggregate_catalog().find_aggregate_mv("widgets", ["price"], []) is None
