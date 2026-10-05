# Copyright (c) 2026 Kenneth Stott
# Canary: 965408dc-0247-44b2-b387-8d5ea408a04c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1921/1922: a view's build reads its inputs through the one pipeline, governed as the
region's administrator with the building region as its region attribute."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.mv.models import MVDefinition
from provisa.transpiler.router import Route

_REGIONS = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def node_in():
    from provisa.core import process_region

    was = process_region._region

    def _bind(region):
        process_region.bind_launch(_REGIONS if region else None, requested=region)

    yield _bind
    process_region._region = was


def _view(semantic: str | None = "SELECT id, home FROM sales.orders") -> MVDefinition:
    return MVDefinition(
        id="view-eu_orders",
        source_tables=[],
        target_catalog="mat_store",
        target_schema="mv",
        target_table="mv_eu_orders",
        sql='SELECT "id", "home" FROM "pg"."public"."orders"',  # lowered at rebuild
        semantic_sql=semantic,
    )


class _Engine:
    dialect = "duckdb"

    def address_replicas(self, sql: str) -> str:
        return f"/*addressed*/ {sql}"


@pytest.fixture
def pipeline(monkeypatch):
    """The app state and the one pipeline's entry points, recorded."""
    from provisa.api import app
    from provisa.federation import query_residency
    from provisa.pgwire import _pipeline

    calls = SimpleNamespace(routed=[], resident=[], executed=[], refuse=None, route=Route.ENGINE)
    monkeypatch.setattr(
        app,
        "state",
        SimpleNamespace(roles={"org_admin": {"session_vars": {"tier": "gold"}}}, view_sql_map={}),
    )

    async def _govern_and_route(sql, role_id, **kw):
        calls.routed.append((sql, role_id, kw))
        return SimpleNamespace(
            route=calls.route, physical_sql="SELECT id, home FROM governed", exec_params=None
        )

    async def _residency(_state, plan):
        calls.resident.append(plan.physical_sql)
        if calls.refuse is not None:
            raise calls.refuse

    async def _execute_plan(plan):
        calls.executed.append(plan.physical_sql)
        return SimpleNamespace(column_names=["id", "home"], rows=[(1, "us")])

    monkeypatch.setattr(_pipeline, "_govern_and_route", _govern_and_route)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute_plan)
    monkeypatch.setattr(query_residency, "prepare_engine_residency", _residency)
    return calls


@pytest.mark.asyncio
async def test_a_regions_build_is_governed_by_the_pipeline_as_its_administrator(pipeline, node_in):
    from provisa.mv.governed_build import view_build_sql

    node_in("us")
    sql = await view_build_sql(_view(), _Engine())
    # The view's SQL as defined, through the one pipeline as org_admin with the role's own
    # constants and this node's region — no caller's values — routed to the engine it lands
    # through; its inputs made readable as the administrator's read's are, before it runs.
    assert pipeline.routed == [
        (
            "SELECT id, home FROM sales.orders",
            "org_admin",
            {"session_vars": {"tier": "gold", "region": "us"}, "route_hint": "engine"},
        )
    ]
    assert pipeline.resident == ["SELECT id, home FROM governed"]
    assert sql == "SELECT id, home FROM governed"


@pytest.mark.asyncio
async def test_the_event_loops_build_reads_through_the_pipelines_terminal(pipeline, node_in):
    from provisa.mv.governed_build import view_build_rows

    node_in("us")
    assert await view_build_rows(_view(), _Engine()) == [{"id": 1, "home": "us"}]
    assert pipeline.executed == ["SELECT id, home FROM governed"]


@pytest.mark.asyncio
async def test_an_input_another_region_cannot_serve_fails_the_build_naming_it(pipeline, node_in):
    from provisa.core.region_stores import HomeRegionUnavailable
    from provisa.mv.governed_build import view_build_sql

    node_in("us")
    pipeline.refuse = HomeRegionUnavailable("orders", "eu", "is not built")
    with pytest.raises(HomeRegionUnavailable) as refused:
        await view_build_sql(_view(), _Engine())
    assert refused.value.params == {"table": "orders", "region": "eu"}


@pytest.mark.asyncio
async def test_a_view_the_pipeline_cannot_govern_is_refused_never_built_physically(
    pipeline, node_in
):
    from provisa.mv.governed_build import ViewNotBuildable, view_build_sql

    node_in("us")
    with pytest.raises(ViewNotBuildable, match="no definition"):
        await view_build_sql(_view(semantic=None), _Engine())
    pipeline.route = Route.DIRECT
    with pytest.raises(ViewNotBuildable, match="routes"):
        await view_build_sql(_view(), _Engine())


@pytest.mark.asyncio
async def test_with_no_platform_regions_a_build_reads_as_before(pipeline, node_in):
    from provisa.mv.governed_build import view_build_sql

    node_in(None)
    sql = await view_build_sql(_view(), _Engine())
    assert pipeline.routed == [] and pipeline.resident == []
    assert sql == '/*addressed*/ SELECT "id", "home" FROM "pg"."public"."orders"'
