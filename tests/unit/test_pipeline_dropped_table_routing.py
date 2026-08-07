# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e4f91-3a5b-4d6e-9f1a-8b2c5d7e9f0a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression test: _optimize_and_route must not raise for an API table that fails
materialization but is NOT inside a UNION (e.g. a single-object lookup keyed by a
required path param, such as an openapi get_pet_by_id endpoint).

ROOT CAUSE: _materialize_api_to_engine_cache appends ANY table that fails
materialization to `dropped_tables`, regardless of whether the SQL actually contains a
UNION referencing it. drop_union_branches_for_table is a no-op for a plain single-source
SELECT, so the table name remains in exec_sql after the "drop" attempt. A guard added to
_optimize_and_route treated that no-op as fatal and raised RuntimeError, breaking every
plain (non-union) query against a non-materializable API table.

FIX: when drop_union_branches_for_table is a no-op, the table was never removed from
exec_sql — it stays untouched and participates in normal source extraction / routing as
a live source, instead of being (a) treated as an error or (b) incorrectly folded into
the "_inlined" set used to reduce the routing source set.
"""

from __future__ import annotations

from types import SimpleNamespace

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.compiler.rls import RLSContext
from provisa.pgwire._pipeline import _optimize_and_route
from provisa.transpiler.router import Route

SOURCE_ID = "ghsrc"
TABLE_ID = 7


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "pets": TableMeta(
            table_id=TABLE_ID,
            field_name="pets",
            type_name="Pets",
            source_id=SOURCE_ID,
            catalog_name=SOURCE_ID,
            schema_name="public",
            table_name="pets",
            domain_id="petstore",
        )
    }
    return ctx


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={
            "gh": {
                "source_id": SOURCE_ID,
                "url": "https://example.test/graphql",
                "tables": [
                    {
                        "sql_name": "pets",
                        "name": "Pet",
                        "field_name": "pets",
                        "required_args": [{"name": "name"}],
                        "columns": [{"name": "id", "type": "integer"}],
                    }
                ],
            }
        },
        source_types={SOURCE_ID: "graphql_api"},
        source_dialects={SOURCE_ID: None},
        source_dsns={},
        source_pools=SimpleNamespace(source_ids={SOURCE_ID}, has=lambda _: False),
    )


async def test_non_union_unmaterializable_api_table_routes_instead_of_raising():
    """A single-source (non-UNION) query against a table that fails materialization
    must fall through to normal routing, not raise RuntimeError."""
    ctx = _ctx()
    rls = RLSContext.empty()
    gov_ctx = build_governance_context("analyst", rls, {}, ctx, tables=[])
    state = _state()

    exec_sql, decision, default_source, optimized, sources = await _optimize_and_route(
        "SELECT * FROM pets",
        "SELECT * FROM pets",
        gov_ctx,
        ctx,
        state,
        nf_args={},
    )

    assert "pets" in exec_sql
    assert sources == {SOURCE_ID}
    assert decision.route == Route.API
