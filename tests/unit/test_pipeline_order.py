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
