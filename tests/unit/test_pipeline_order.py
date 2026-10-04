# Copyright (c) 2026 Kenneth Stott
# Canary: 19e346e2-744f-46a6-8465-20b922380b7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Planning-phase ordering: governance → post-governance optimization → routing (REQ-863).

Routing (extract_sources / decide_route) MUST consume the OUTPUT of the post-governance
optimization stage. A source whose tables the optimization inlined/pruned must NOT be routed
(so a collapsed federated query routes DIRECT); a source ADDED by an RLS subquery predicate
MUST be routed.
"""

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import (
    GovernanceContext,
    extract_sources,
    reduce_sources_for_routing,
)
from provisa.transpiler.router import Route, decide_route


def _meta(table_id, name, source_id):
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        table_name=name,
    )


def _ctx():
    ctx = CompilationContext()
    ctx.tables = {
        "orders": _meta(1, "orders", "sales-pg"),
        "countries": _meta(2, "countries", "lookup-pg"),
    }
    ctx.joins = {}
    return ctx


def _gov():
    return GovernanceContext(
        table_map={
            "orders": 1,
            "public.orders": 1,
            "countries": 2,
            "public.countries": 2,
        }
    )


_JOIN_SQL = (
    'SELECT "o"."id", "c"."code" FROM "public"."orders" "o" '
    'LEFT JOIN "public"."countries" "c" ON "o"."country_id" = "c"."id"'
)


def test_pre_optimization_two_sources_route_engine():
    sources = extract_sources(_JOIN_SQL, _gov(), _ctx())
    assert sources == {"sales-pg", "lookup-pg"}
    decision = decide_route(
        sources=sources,
        source_types={"sales-pg": "postgresql", "lookup-pg": "postgresql"},
        source_dialects={"sales-pg": "postgres", "lookup-pg": "postgres"},
        source_dsns={"sales-pg": "dsnA", "lookup-pg": "dsnB"},  # distinct DBs → federated
        operator_floor={},
    )
    assert decision.route == Route.ENGINE


def test_source_removed_by_optimization_is_not_routed():
    """When the optimization stage inlines the lookup table, its source drops from routing → DIRECT."""
    sources = reduce_sources_for_routing(
        _JOIN_SQL, _gov(), _ctx(), inlined_table_names={"countries"}
    )
    assert sources == {"sales-pg"}  # lookup-pg gone: countries was inlined as a VALUES CTE
    decision = decide_route(
        sources=sources,
        source_types={"sales-pg": "postgresql"},
        source_dialects={"sales-pg": "postgres"},
        source_dsns={"sales-pg": "dsnA"},
        operator_floor={},
    )
    assert decision.route == Route.DIRECT
    assert decision.source_id == "sales-pg"


def test_source_with_a_remaining_live_table_stays():
    """Inlining a table that is NOT referenced must not drop a live source."""
    sources = reduce_sources_for_routing(
        _JOIN_SQL, _gov(), _ctx(), inlined_table_names={"unrelated"}
    )
    assert sources == {"sales-pg", "lookup-pg"}


def test_source_added_by_rls_subquery_is_routed():
    """A source referenced only in an RLS-added WHERE subquery predicate IS observed by routing.

    Governance may ADD sources (RLS subquery predicates); extract_sources over the governed SQL
    must include them so they participate in the route decision.
    """
    governed = (
        'SELECT "o"."id" FROM "public"."orders" "o" '
        'WHERE "o"."country_id" IN (SELECT "c"."id" FROM "public"."countries" "c")'
    )
    sources = extract_sources(governed, _gov(), _ctx())
    assert sources == {"sales-pg", "lookup-pg"}  # RLS-added countries source is present


def test_an_inlined_table_known_by_its_physical_name_under_a_display_alias_drops_its_source():
    """The optimizer names an inlined API table by its physical name (``get_inventory``); the
    query and the governance map carry its display name (``pet_store.inventory``). The source
    still drops from routing: a fully inlined statement has no API source left to route to (it was
    routed Route.API and refused with data.no_direct_route)."""
    ctx = CompilationContext()
    meta = TableMeta(
        table_id=7,
        field_name="pet_store__inventory",
        type_name="Inventory",
        source_id="petstore-api",
        catalog_name="petstore_api",
        schema_name="default",
        table_name="get_inventory",
        domain_id="pet_store",
        display_name="inventory",
    )
    ctx.tables = {"pet_store__inventory": meta}
    ctx.joins = {}
    gov = GovernanceContext(table_map={"pet_store.inventory": 7, "inventory": 7})
    sql = 'SELECT "inventory"."count" FROM "pet_store"."inventory" AS "inventory"'
    assert reduce_sources_for_routing(sql, gov, ctx, inlined_table_names={"get_inventory"}) == set()
