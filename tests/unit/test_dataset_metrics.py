# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e5f91-c3a8-4d06-9e14-7a0c6d2f8b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2243: a metric names a glossary term, and a dataset lists the governed metrics it uses --
a metric view its declared metrics, a table or view the metrics whose expression references it --
each with the term's definition and calculation method, as its reader's role may see them."""

# Requirements: REQ-2243

from __future__ import annotations

import types
from typing import Any

import pytest

from provisa.api.admin._metric_views import dataset_metrics
from provisa.core.models import Metric, Table, ViewMetricsSpec
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import metric as metric_repo
from provisa.core.repositories import table as table_repo
from tests.unit.test_metric_admin import _admin_db, _seed_semantic_layer

pytestmark = pytest.mark.asyncio

SCHEMA = "org_default"
DEFINITION = "Revenue kept after refunds."
METHOD = "Gross order value less refunds, on the order date."


async def _seed(conn, *, with_view: bool = True) -> dict[str, int]:
    """orders, customers and a view declared from net_revenue; three metrics, one naming a term."""
    await _seed_semantic_layer(conn)
    term_id = await glossary_repo.create_abstract_term(
        conn, "net revenue", definition=DEFINITION, domains={"sales"}
    )
    await glossary_repo.set_calculation_method(conn, term_id, METHOD)
    for metric in (
        Metric(
            name="net_revenue",
            expression="SUM(orders.amount) - SUM(orders.refunds)",
            description="Net revenue",
            ai_context="Gross minus refunds.",
            visible_to=["finance"],
            term="net revenue",
        ),
        Metric(name="order_count", expression="COUNT(orders.amount)", visible_to=["sales"]),
        Metric(name="customer_count", expression="COUNT(customers.id)", visible_to=["finance"]),
    ):
        await metric_repo.upsert(conn, metric)
    ids = {}
    if with_view:
        view = Table(
            source_id="__derived__",
            domain_id="sales",
            schema_name="views",
            table_name="revenue_by_region",
            columns=[],
            view_metrics=ViewMetricsSpec(metrics=["net_revenue"], dimensions=["region"]),
        )
        view.view_sql = "SELECT 1"  # registration generates it and persists both (REQ-1318)
        await table_repo.upsert(conn, view)
    for name in ("orders", "customers") + (("revenue_by_region",) if with_view else ()):
        row = await table_repo.find_by_table_name(conn, name)
        assert row is not None
        ids[name] = row["id"]
    ids["term"] = term_id
    return ids


async def test_a_metric_names_a_term_and_reads_it_back(control_plane):
    async with _admin_db(control_plane) as db:
        async with db.acquire() as conn:
            await _seed(conn)
            stored = await metric_repo.get(conn, "net_revenue")
            assert stored is not None and stored["term"] == "net revenue"
            with pytest.raises(ValueError, match="names the glossary term 'nothing'"):
                await metric_repo.upsert(
                    conn, Metric(name="bad", expression="COUNT(orders.id)", term="nothing")
                )


async def test_a_dataset_lists_the_metrics_it_uses(control_plane):
    async with _admin_db(control_plane) as db:
        async with db.acquire() as conn:
            ids = await _seed(conn)
            by_table = {
                name: [m["name"] for m in await dataset_metrics(conn, ids[name])]
                for name in ("orders", "customers", "revenue_by_region")
            }
            orders = {m["name"]: m for m in await dataset_metrics(conn, ids["orders"])}
    assert by_table == {
        "orders": ["net_revenue", "order_count"],
        "customers": ["customer_count"],
        # A view declared from metrics lists the metrics it closes.
        "revenue_by_region": ["net_revenue"],
    }
    assert orders["net_revenue"] == {
        "name": "net_revenue",
        "description": "Net revenue",
        "ai_context": "Gross minus refunds.",
        "visible_to": ["finance"],
        "term": "net revenue",
        "term_definition": DEFINITION,
        "term_calculation_method": METHOD,
    }
    assert orders["order_count"]["term"] is None
    assert orders["order_count"]["term_definition"] is None


def _info(role_id: str) -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(role=role_id))
    return types.SimpleNamespace(context={"request": request})


async def test_the_read_hides_what_visible_to_hides_and_shows_no_formula(
    control_plane, monkeypatch
):
    from provisa.api.admin import schema_query
    from provisa.api.admin.catalog_scope import CatalogScope
    from provisa.api.admin.schema_query import Query

    async with _admin_db(control_plane) as db:
        async with db.acquire() as conn:
            ids = await _seed(conn)

    async def _pool():
        return db

    def _scope(info):
        role = info.context["request"].state.role
        return CatalogScope(whole=False, reach=None, served={}, governance=False, role=role)

    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "catalog_scope", _scope)
    monkeypatch.setattr("provisa.security.meta_role.acting_roles", lambda _state, role: [role])
    finance = await Query().table_metrics(_info("finance"), ids["orders"])
    sales = await Query().table_metrics(_info("sales"), ids["orders"])
    assert [m.name for m in finance] == ["net_revenue"]
    assert [m.name for m in sales] == ["order_count"]
    [net] = finance
    assert (net.term, net.term_definition, net.term_calculation_method) == (
        "net revenue",
        DEFINITION,
        METHOD,
    )
    assert not hasattr(net, "expression")


async def test_a_terms_deletion_leaves_the_metric_unnamed(control_plane):
    async with _admin_db(control_plane) as db:
        async with db.acquire() as conn:
            ids = await _seed(conn)
            assert await glossary_repo.delete_term(conn, ids["term"])
            stored = await metric_repo.get(conn, "net_revenue")
    assert stored is not None and stored["term"] is None


async def test_the_term_a_metric_names_is_versioned_with_it(control_plane):
    """The model store writes it as the term's path, and a deploy reads that path back."""
    from provisa.core.env_deploy import _decompose, _existing_ids, _roles_after
    from provisa.core.env_merge import requirements
    from provisa.core.env_project import project
    from provisa.core.store_config import store_model

    async with _admin_db(control_plane) as db:
        async with db.acquire() as conn:
            ids = await _seed(conn, with_view=False)
            tree = await project(conn, SCHEMA)
            existing = await _existing_ids(conn, SCHEMA)
            roles = await _roles_after(conn, SCHEMA, tree)
            stated = {m["name"]: m for m in (await store_model(conn))["metrics"]}
    [path] = [p for p, b in tree.items() if b.get("name") == "net_revenue" and "expression" in b]
    term_path = next(p for p, b in tree.items() if b.get("name") == "net revenue")
    assert tree[path]["term"] == term_path
    assert "term_id" not in tree[path]
    assert term_path in requirements(path, tree[path], tree)
    rows = _decompose(tree, existing, roles)
    [row] = [r for r in rows["metrics"] if r["name"] == "net_revenue"]
    assert row["term_id"] == ids["term"]
    assert stated["net_revenue"]["term"] == "net revenue"
