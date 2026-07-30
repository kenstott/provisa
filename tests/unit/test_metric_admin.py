# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e7d20-51fb-4a83-b6d9-0e2a68c1f574
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Metric repository CRUD + expression validation (REQ-1317, REQ-1320).

The repo is the single write seam for governed metric definitions: every upsert validates the
expression (must parse under sqlglot and contain at least one aggregate) — an invalid metric is
a hard error and never reaches storage.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from provisa.core.database import Database
from provisa.core.models import Metric
from provisa.core.repositories import metric as metric_repo
from provisa.core.schema_org import metrics

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _db(tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'metrics.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: metrics.metadata.create_all(s, tables=[metrics]))
    try:
        yield Database(engine, name="cp")
    finally:
        await engine.dispose()


# ── expression validation (REQ-1317) ─────────────────────────────────────────


async def test_non_aggregate_expression_rejected():
    with pytest.raises(ValueError, match="aggregate"):
        metric_repo.validate_expression("orders.amount + orders.tax")


async def test_unparsable_expression_rejected():
    with pytest.raises(ValueError, match="does not parse"):
        metric_repo.validate_expression("SUM((")


async def test_aggregate_expressions_accepted():
    metric_repo.validate_expression("SUM(orders.amount)")
    metric_repo.validate_expression("SUM(orders.amount) - SUM(orders.refunds)")
    metric_repo.validate_expression("COUNT(orders.id)")


async def test_upsert_of_non_aggregate_metric_is_a_hard_error(tmp_path):
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            with pytest.raises(ValueError, match="aggregate"):
                await metric_repo.upsert(
                    conn, Metric(name="bad_metric", expression="orders.amount")
                )
            assert await metric_repo.list_all(conn) == []  # nothing stored


# ── CRUD ─────────────────────────────────────────────────────────────────────


async def test_upsert_get_list_delete_roundtrip(tmp_path):
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await metric_repo.upsert(
                conn,
                Metric(
                    name="net_revenue",
                    expression="SUM(orders.amount) - SUM(orders.refunds)",
                    datatype="decimal",
                    description="Net revenue",
                    ai_context="Gross minus refunds.",
                    visible_to=["finance"],
                ),
            )
            await metric_repo.upsert(
                conn,
                Metric(
                    name="sales_amount_sum",
                    expression="SUM(Sales.amount)",
                    from_fact="Sales",  # REQ-1320: fact-derived
                ),
            )
            rows = await metric_repo.list_all(conn)
            assert [r["name"] for r in rows] == ["net_revenue", "sales_amount_sum"]

            got = await metric_repo.get(conn, "net_revenue")
            assert got is not None
            assert got["expression"] == "SUM(orders.amount) - SUM(orders.refunds)"
            assert got["datatype"] == "decimal"
            assert got["ai_context"] == "Gross minus refunds."
            assert got["visible_to"] == ["finance"]
            assert got["from_fact"] is None

            derived = await metric_repo.get(conn, "sales_amount_sum")
            assert derived is not None
            assert derived["from_fact"] == "Sales"
            assert derived["visible_to"] == ["*"]  # model default

            assert await metric_repo.delete(conn, "net_revenue") is True
            assert await metric_repo.get(conn, "net_revenue") is None
            assert await metric_repo.delete(conn, "net_revenue") is False  # already gone


async def test_upsert_replaces_by_name(tmp_path):
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await metric_repo.upsert(conn, Metric(name="gmv", expression="SUM(orders.amount)"))
            await metric_repo.upsert(
                conn,
                Metric(name="gmv", expression="SUM(orders.amount) + SUM(orders.tax)", datatype="decimal"),
            )
            rows = await metric_repo.list_all(conn)
    assert len(rows) == 1
    assert rows[0]["expression"] == "SUM(orders.amount) + SUM(orders.tax)"
    assert rows[0]["datatype"] == "decimal"


# ── metric-composed views: admin registration + regeneration (REQ-1318) ──────


@asynccontextmanager
async def _admin_db(tmp_path):
    """Sqlite control plane carrying the registries the metric-view compiler reads."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from provisa.core.schema_org import registered_tables, relationships, roles, table_columns

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'admin.db'}")
    async with engine.begin() as c:
        # roles: table_repo resolves control-plane (cross_org) roles from the DB (REQ-1337), so
        # every fixture that registers tables must carry the roles table the bootstrap guarantees.
        await c.run_sync(
            lambda s: metrics.metadata.create_all(
                s, tables=[metrics, registered_tables, table_columns, relationships, roles]
            )
        )
    try:
        yield Database(engine, name="cp")
    finally:
        await engine.dispose()


async def _seed_semantic_layer(conn):
    """orders(amount, refunds, customer_id) → customers(id, region) + net_revenue metric."""
    from provisa.core.models import Cardinality, Column, Relationship, Table
    from provisa.core.repositories import relationship as relationship_repo
    from provisa.core.repositories import table as table_repo

    def _tbl(name, cols):
        return Table(
            source_id="pg",
            domain_id="sales",
            schema_name="public",
            table_name=name,
            columns=[Column(name=c, visible_to=["*"]) for c in cols],
        )

    await table_repo.upsert(conn, _tbl("orders", ["amount", "refunds", "customer_id"]))
    await table_repo.upsert(conn, _tbl("customers", ["id", "region"]))
    await relationship_repo.upsert(
        conn,
        Relationship(
            id="orders_customer",
            source_table_id="orders",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality=Cardinality.many_to_one,
        ),
    )
    await metric_repo.upsert(
        conn,
        Metric(name="net_revenue", expression="SUM(orders.amount) - SUM(orders.refunds)"),
    )


async def test_register_view_metrics_generates_and_persists_view_sql(tmp_path):
    # REQ-1318: a metric-composed view registers with the spec persisted AND the
    # generated SELECT stored in view_sql (the SQL flows everywhere view_sql does).
    from provisa.api.admin._metric_views import compile_view_metrics_sql
    from provisa.core.models import ViewMetricsSpec
    from provisa.core.repositories import table as table_repo
    from provisa.core.models import Table

    async with _admin_db(tmp_path) as db:
        async with db.acquire() as conn:
            await _seed_semantic_layer(conn)
            spec = ViewMetricsSpec(metrics=["net_revenue"], dimensions=["region"])
            sql = await compile_view_metrics_sql(conn, spec)
            assert "SUM(orders.amount) - SUM(orders.refunds) AS net_revenue" in sql
            assert "customers.region AS region" in sql
            assert "GROUP BY" in sql

            model = Table(
                source_id="__derived__",
                domain_id="sales",
                schema_name="views",
                table_name="revenue_by_region",
                columns=[],
                view_metrics=spec,
            )
            model.view_sql = sql  # register_table generates then persists (REQ-1318)
            await table_repo.upsert(conn, model)

            row = await table_repo.get_by_name(conn, "__derived__", "views", "revenue_by_region")
            assert row is not None
            assert row["view_sql"] == sql
            assert row["view_metrics"] == spec.model_dump()


async def test_view_sql_and_view_metrics_together_is_hard_error():
    # REQ-1318: the spec is the source of truth — free-hand SQL alongside it is rejected.
    from provisa.api.admin._live_mappers import table_model_from_input
    from provisa.api.admin.types import TableInput, ViewMetricsInput

    inp = TableInput(
        source_id="__derived__",
        domain_id="sales",
        schema_name="views",
        table_name="conflicted",
        columns=[],
        view_sql="SELECT 1",
        view_metrics=ViewMetricsInput(metrics=["net_revenue"], dimensions=[]),
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        table_model_from_input(inp, [], [], "conflicted")


async def test_metric_upsert_regenerates_dependent_view_sql(tmp_path):
    # REQ-1318 core property: changing a metric regenerates every view whose spec
    # references it — the stored SQL tracks the updated business definition.
    from provisa.api.admin._metric_views import compile_view_metrics_sql, regenerate_metric_views
    from provisa.core.models import Table, ViewMetricsSpec
    from provisa.core.repositories import table as table_repo

    async with _admin_db(tmp_path) as db:
        async with db.acquire() as conn:
            await _seed_semantic_layer(conn)
            spec = ViewMetricsSpec(metrics=["net_revenue"], dimensions=["region"])
            model = Table(
                source_id="__derived__",
                domain_id="sales",
                schema_name="views",
                table_name="revenue_by_region",
                columns=[],
                view_metrics=spec,
            )
            model.view_sql = await compile_view_metrics_sql(conn, spec)
            await table_repo.upsert(conn, model)

            # unrelated metric → nothing regenerates
            await metric_repo.upsert(conn, Metric(name="order_count", expression="COUNT(orders.amount)"))
            assert await regenerate_metric_views(conn, "order_count") == []

            # the referenced metric changes → the stored view SQL is regenerated
            await metric_repo.upsert(
                conn, Metric(name="net_revenue", expression="SUM(orders.amount)")
            )
            assert await regenerate_metric_views(conn, "net_revenue") == ["revenue_by_region"]

            row = await table_repo.get_by_name(conn, "__derived__", "views", "revenue_by_region")
            assert row is not None
            assert "SUM(orders.amount) AS net_revenue" in row["view_sql"]
            assert "refunds" not in row["view_sql"]
