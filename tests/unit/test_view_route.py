# Copyright (c) 2026 Kenneth Stott
# Canary: b2d68407-c5dd-4df8-b4f0-1b3ea32ecfe3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a statement that reads a view is routed (REQ-135, REQ-1163), in the one semantic statement.

A view is replaced by its SQL before governance, so the governed statement names only the tables
the view reads. When those are one source's, the statement may go to that source's own driver:
the direct lowering is built from the governed statement, which holds the view's SQL, so no bare
view name reaches the driver. A view kept through governance -- materialized and seen whole by
the reader, another region's copy, a bitemporal view -- still reaches routing as a reference,
which no source's driver knows: it goes to the engine, where its stored rows are put in."""

# Requirements: REQ-135, REQ-1163
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import sqlglot
from sqlglot import exp

from provisa.compiler.directives import NO_CACHE_HINT
from provisa.compiler.sql_gen import TableMeta
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.transpiler.router import Route
from tests.unit.test_buffered_auto_threshold import _decision, _Source

pytestmark = pytest.mark.asyncio

_VIEW_SQL = "SELECT id FROM sales.orders"
_TOTALS = TableMeta(
    table_id=2,
    field_name="order_totals",
    type_name="OrderTotals",
    source_id="__derived__",
    catalog_name="__derived__",
    schema_name="views",
    table_name="order_totals",
    domain_id="sales",
)


def _state(*, kept: bool) -> SimpleNamespace:
    from tests.unit.test_governed_sql_engine_internals import _state as gov_state

    state = gov_state(["*"])
    state.contexts["analyst"].tables["order_totals"] = _TOTALS
    state.tables = [
        *state.tables,
        {
            "id": 2,
            "source_id": "__derived__",
            "schema_name": "views",
            "table_name": "order_totals",
            "domain_id": "sales",
            "view_sql": _VIEW_SQL,
            "columns": [{"column_name": "id", "data_type": "integer", "visible_to": ["analyst"]}],
        },
    ]
    state.view_sql_map = {"order_totals": _VIEW_SQL}
    registry = MVRegistry()
    if kept:
        # Materialized, fresh, and seen whole by the reader: answered from its stored rows.
        registry.register(
            MVDefinition(
                id="view-order_totals",
                source_tables=[],
                target_catalog="mat_store",
                target_schema="org_a_mv_cache",
                target_table="mv_order_totals",
                sql=_VIEW_SQL,
                semantic_sql=_VIEW_SQL,
                status=MVStatus.FRESH,
            )
        )
    state.mv_registry = registry
    state.role_build_inputs = {"draft_tables": [], "withdrawn_tables": []}
    state.source_dialects = {"pg": "postgres"}
    state.source_catalogs = {"pg": "pg"}
    state.source_pools = SimpleNamespace(source_ids=["pg"], has=lambda sid: sid == "pg")
    state.federation_engine = _Source(lambda sql, params: [])
    return state


@pytest.fixture(autouse=True)
def _org(bind_org):
    bind_org("org-a")  # the org the stand-in serves, bound as every surface binds it


def _tables(sql: str) -> set[str]:
    """Every table the statement reads, by its qualified name: what a driver or an engine is asked
    to find (an alias names no table)."""
    return {
        ".".join(p for p in (t.catalog, t.db, t.name) if p)
        for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)
    }


async def _plan(monkeypatch, *, kept: bool):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline, governed_plan

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    state = _state(kept=kept)
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    # The router's own decision, as the cost model gives it for a statement on one source.
    with patch.object(
        _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision("DIRECT"))
    ):
        return await _pipeline._govern_and_route_compiled(
            'SELECT "id" FROM "sales"."order_totals"',
            "analyst",
            state=state,
            cache_hint=NO_CACHE_HINT,
            sdl_joins=True,
        )


async def test_a_view_on_one_source_goes_to_its_driver_as_the_views_sql(monkeypatch):
    plan = await _plan(monkeypatch, kept=False)
    assert plan.route == Route.DIRECT and plan.exec_sql is not None
    # The direct lowering is the governed statement's, which holds the view's SQL: the driver is
    # asked for the view's table, and never for the view.
    assert _tables(plan.exec_sql) == {"sales.orders"}, plan.exec_sql


async def test_a_kept_view_reaches_routing_as_a_reference_and_goes_to_the_engine(monkeypatch):
    plan = await _plan(monkeypatch, kept=True)
    assert plan.route == Route.ENGINE and plan.exec_sql is not None
    # Its stored rows are put in at optimization; the engine is asked for those, never the view.
    assert _tables(plan.exec_sql) == {"mat_store.org_a_mv_cache.mv_order_totals"}, plan.exec_sql
