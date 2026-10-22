# Copyright (c) 2026 Kenneth Stott
# Canary: 965408dc-0247-44b2-b387-8d5ea408a04c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1921/1922: a view's build reads its inputs through the one pipeline, governed as the
region's administrator with the building region as its region attribute -- the one default
region where the platform declares none."""

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

    def engine_physical(self, sql: str) -> str:
        return f"/*as the engine executes it*/ {sql}"


@pytest.fixture
def pipeline(monkeypatch):
    """The app state and the one pipeline's entry points, recorded."""
    from provisa.api import app
    from provisa.federation import query_residency
    from provisa.pgwire import _pipeline

    calls = SimpleNamespace(
        routed=[], resident=[], executed=[], refuse=None, route=Route.ENGINE, column_types=None
    )
    monkeypatch.setattr(
        app,
        "state",
        SimpleNamespace(roles={"org_admin": {"session_vars": {"tier": "gold"}}}, view_sql_map={}),
    )

    async def _govern_and_route(sql, role_id, **kw):
        calls.routed.append((sql, role_id, kw))
        route = Route.ENGINE if kw.get("route_hint") == "engine" else calls.route
        return SimpleNamespace(
            route=route,
            physical_sql="SELECT id, home FROM governed",
            exec_params=None,
            dialect="postgres",
            columns=None,
        )

    async def _residency(_state, plan):
        calls.resident.append(plan.physical_sql)
        if calls.refuse is not None:
            raise calls.refuse

    async def _execute_plan(plan):
        calls.executed.append(plan.physical_sql)
        return SimpleNamespace(
            column_names=["id", "home"], rows=[(1, "us")], column_types=calls.column_types
        )

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
    # constants and this node's region — no caller's values — routed as a query with the same
    # definition is (REQ-2108); its inputs made readable as the administrator's read's are.
    assert pipeline.routed == [
        (
            "SELECT id, home FROM sales.orders",
            "org_admin",
            {"session_vars": {"tier": "gold", "region": "us"}, "route_hint": None},
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
    pipeline.route = Route.API
    with pytest.raises(ViewNotBuildable, match="routes"):
        await view_build_sql(_view(), _Engine())


@pytest.mark.asyncio
async def test_with_no_platform_regions_a_build_is_governed_the_same_way(pipeline, node_in):
    """With no platform regions a build still goes through the one pipeline as the
    administrator, in the one default region: its definition is read as a direct statement
    reads it, never its precompiled physical SQL straight to the engine."""
    from provisa.core.regions import DEFAULT_REGION
    from provisa.mv.governed_build import view_build_rows, view_build_sql

    node_in(None)
    sql = await view_build_sql(_view(), _Engine())
    assert pipeline.routed == [
        (
            "SELECT id, home FROM sales.orders",
            "org_admin",
            {"session_vars": {"tier": "gold", "region": DEFAULT_REGION}, "route_hint": None},
        )
    ]
    assert pipeline.resident == ["SELECT id, home FROM governed"]
    assert sql == "SELECT id, home FROM governed"
    assert await view_build_rows(_view(), _Engine()) == [{"id": 1, "home": "us"}]
    assert pipeline.executed == ["SELECT id, home FROM governed"]


def _join_view() -> MVDefinition:
    from provisa.mv.models import JoinPattern, TableIdentity

    return MVDefinition(
        id="mv-orders-customers",
        source_tables=["orders", "customers"],
        inputs=[TableIdentity("pg", "public", t) for t in ("orders", "customers")],
        target_catalog="mat_store",
        target_schema="mv",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
    )


def _meta(table: str) -> SimpleNamespace:
    return SimpleNamespace(
        source_id="pg",
        schema_name="public",
        table_name=table,
        original_table_name="",
        domain_id="sales",
        display_name=table,
        field_name=table,
    )


@pytest.fixture
def model(monkeypatch, pipeline):
    """The model's names and registered columns of the join view's tables."""
    from provisa.api import app
    from provisa.federation import registry_view

    app.state.view_context = SimpleNamespace(
        tables={"orders": _meta("orders"), "customers": _meta("customers")}
    )

    async def _tables(_state):
        return [
            SimpleNamespace(
                source_id="pg",
                schema_name="public",
                table_name="customers",
                columns=[SimpleNamespace(name="id"), SimpleNamespace(name="home")],
            )
        ]

    monkeypatch.setattr(registry_view, "registered_tables", _tables)
    return pipeline


@pytest.mark.asyncio
async def test_a_join_pattern_view_is_governed_as_its_semantic_join(model, node_in):
    """A join-pattern view is a view: in a region deployment its build is the same join in the
    model's terms, through the one pipeline as the region's administrator."""
    from provisa.mv.governed_build import view_build_sql

    node_in("us")
    await view_build_sql(_join_view(), _Engine())
    [(sql, role, _kw)] = model.routed
    assert role == "org_admin"
    assert sql == (
        'SELECT "orders".*, "customers"."id" AS "customers__id", '
        '"customers"."home" AS "customers__home" '
        'FROM "sales"."orders" AS "orders" LEFT JOIN "sales"."customers" AS "customers" '
        'ON "orders"."customer_id" = "customers"."id"'
    )


@pytest.mark.asyncio
async def test_a_join_view_over_a_table_the_model_cannot_name_is_refused(model, node_in):
    from provisa.api import app
    from provisa.mv.governed_build import ViewNotBuildable, view_build_sql

    node_in("us")
    del app.state.view_context.tables["orders"]
    with pytest.raises(ViewNotBuildable, match="'orders' has no name in the model"):
        await view_build_sql(_join_view(), _Engine())


@pytest.mark.asyncio
async def test_a_view_naming_another_region_is_built_only_there(pipeline, node_in):
    """REQ-1921, A VIEW MAY NAME A REGION: built and refreshed only by its region."""
    from provisa.mv.governed_build import ViewNotBuildable, view_build_rows, view_build_sql

    node_in("us")
    view = _view()
    view.region = "eu"
    with pytest.raises(ViewNotBuildable, match="names region 'eu'"):
        await view_build_sql(view, _Engine())
    with pytest.raises(ViewNotBuildable, match="names region 'eu'"):
        await view_build_rows(view, _Engine())
    assert pipeline.routed == []
    view.region = "us"
    await view_build_sql(view, _Engine())
    assert len(pipeline.routed) == 1


def test_the_event_loop_wires_only_the_views_this_region_builds(node_in):
    from provisa.events.app_wiring import _views_built_here
    from provisa.mv.registry import MVRegistry

    node_in("us")
    reg = MVRegistry()
    for mv_id, region in (("everywhere", None), ("ours", "us"), ("theirs", "eu")):
        view = _view()
        view.id, view.target_table, view.region = mv_id, f"mv_{mv_id}", region
        reg.register(view)
    assert sorted(v.id for v in _views_built_here(reg)) == ["everywhere", "ours"]


class _StagingEngine(_Engine):
    """An engine that lands a DIRECT build's rows (the store's write face) and answers a shape."""

    def __init__(self) -> None:
        self.landed: list[dict] = []
        self.ran: list[str] = []

    async def land_source_table(self, **kw):
        self.landed.append(kw)
        return f"{kw['schema']}.{kw['table']}"

    async def execute_engine(self, sql, **_kw):
        self.ran.append(sql)
        return SimpleNamespace(column_names=["id", "home"], column_types=["INTEGER", "VARCHAR"])


@pytest.mark.asyncio
async def test_a_single_source_build_runs_whole_at_its_source_and_stages_only_its_rows(
    pipeline, node_in
):
    """REQ-2108: routed DIRECT, the build runs at its source on any engine kind; only its result
    rows come back, landed beside the view's table, and every build step reads them there."""
    from provisa.mv.governed_build import lineage_sql, staged_table, view_build_sql

    node_in("us")
    pipeline.route = Route.DIRECT
    pipeline.column_types = ["int4", "text"]
    engine = _StagingEngine()
    view = _view()
    sql = await view_build_sql(view, engine)
    assert pipeline.executed == ["SELECT id, home FROM governed"]  # once, at its source
    assert pipeline.resident == []  # nothing read through the engine
    assert engine.landed == [
        {
            "schema": "mv",
            "table": "mv_eu_orders__direct",
            "columns": [("id", "integer"), ("home", "text")],
            "rows": [{"id": 1, "home": "us"}],
        }
    ]
    assert staged_table(view) == "mv_eu_orders__direct"
    assert sql == 'SELECT * FROM "mat_store"."mv"."mv_eu_orders__direct"'
    # its lineage is read from its definition, which names what it derives from
    assert lineage_sql(view, sql) == view.sql
    assert lineage_sql(view, "SELECT id, home FROM governed") == "SELECT id, home FROM governed"


@pytest.mark.asyncio
async def test_a_source_that_reports_no_types_takes_its_shape_from_the_engine(pipeline, node_in):
    """REQ-2108: a driver that reports no result types (the SQLAlchemy drivers) leaves the staged
    table's shape to the engine, which reads the definition with LIMIT 0 and no rows."""
    from provisa.mv.governed_build import view_build_sql

    node_in("us")
    pipeline.route = Route.DIRECT
    engine = _StagingEngine()
    await view_build_sql(_view(), engine)
    assert engine.ran == ["SELECT * FROM (SELECT id, home FROM governed) AS _shape LIMIT 0"]
    assert pipeline.routed[-1][2]["route_hint"] == "engine"
    assert engine.landed[0]["columns"] == [("id", "integer"), ("home", "text")]
