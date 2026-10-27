# Copyright (c) 2026 Kenneth Stott
# Canary: 5ba1d1eb-231f-46d8-baac-1912fdaf75a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2129: column_lineage_edges, the model's column lineage in the meta domain.

The edges are the ones the model build writes (the build's own step, here), folded from each
view's sub-DAG so a view's internal steps never cut the chain. Each end relates to
table_columns_meta by column_key; a caller sees an edge only where both ends are visible to it, and
the view is governance metadata whole.
"""

# Requirements: REQ-2129

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.security.rights import META_DOMAIN_ID

# B (finance) is read by the compiled view A (sales); V2 reads A through a CTE.
B, A, V2 = 1, 2, 3


async def _seed(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.schema_org import domains, registered_tables, sources, table_columns

    for domain in ("sales", "finance"):
        await conn.execute_core(insert(domains).values(id=domain, description=domain))
    await conn.execute_core(insert(sources).values(id="erp", type="postgres"))
    await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="postgres"))
    views = {
        A: ("sales", "a", "SELECT b.amount AS total FROM finance.b b"),
        V2: (
            "sales",
            "v2",
            "WITH x AS (SELECT total * 2 AS doubled FROM sales.a) SELECT doubled FROM x",
        ),
    }
    await conn.execute_core(
        insert(registered_tables).values(
            id=B, source_id="erp", domain_id="finance", schema_name="public", table_name="b"
        )
    )
    for table_id, (domain, name, sql) in views.items():
        await conn.execute_core(
            insert(registered_tables).values(
                id=table_id,
                source_id=DERIVED_SOURCE_ID,
                domain_id=domain,
                schema_name="public",
                table_name=name,
                view_sql=sql,
            )
        )
    for table_id, domain, column in (
        (B, "finance", "amount"),
        (A, "sales", "total"),
        (V2, "sales", "doubled"),
    ):
        await conn.execute_core(
            insert(table_columns).values(
                table_id=table_id, domain_id=domain, column_name=column, data_type="numeric"
            )
        )


@pytest.mark.asyncio
async def test_a_columns_upstream_column_is_one_edge_naming_the_view(control_plane):
    from sqlalchemy import text

    from provisa.api.admin.lineage_router import write_lineage_edges
    from provisa.api.startup_seed import _adapt_view_ddl

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            # The model build's own step.
            await write_lineage_edges(conn, SimpleNamespace(tracked_functions={}))
            for name in ("table_columns", "column_lineage_edges"):
                await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS[name]))

            # a.total's upstream columns, reached from its table_columns_meta row by the seeded
            # UPSTREAM edge's join columns.
            _, _, src, src_col, tgt_col, *_ = next(
                r for r in META_RELATIONSHIPS if r[0] == "meta:table_columns:upstream_lineage"
            )
            assert (src, src_col, tgt_col) == ("table_columns", "column_key", "target_column_key")
            rows = await conn.execute_core(
                text(
                    "SELECT e.source_relation, e.source_column, e.target_relation, "
                    "e.target_column, e.via, e.via_kind, e.source_column_key "
                    "FROM table_columns_meta c "
                    "JOIN column_lineage_edges e ON e.target_column_key = c.column_key "
                    "WHERE c.table_id = :a AND c.column_name = 'total'"
                ).bindparams(a=A)
            )
            upstream = [tuple(r) for r in rows.fetchall()]
            assert upstream == [
                ("finance.b", "amount", "sales.a", "total", "sales.a", "view", f"{B}:amount")
            ]

            # The CTE inside v2 is walked through: a.total -> v2.doubled, named by v2.
            rows = await conn.execute_core(
                text(
                    "SELECT source_relation, source_column, target_relation, target_column, via "
                    "FROM column_lineage_edges WHERE target_table_id = :v2"
                ).bindparams(v2=V2)
            )
            assert [tuple(r) for r in rows.fetchall()] == [
                ("sales.a", "total", "sales.v2", "doubled", "sales.v2")
            ]
    finally:
        await db.close()


_EDGES_TID = 300


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["column_lineage_edges"] = TableMeta(
        table_id=_EDGES_TID,
        field_name="column_lineage_edges",
        type_name="ColumnLineageEdges",
        source_id="provisa-admin",
        catalog_name="main",
        schema_name="public",
        table_name="column_lineage_edges",
        domain_id=META_DOMAIN_ID,
    )
    return ctx


def _gov(capabilities: list[str], domain_access: list[str]):
    role = {"id": "analyst", "capabilities": capabilities, "domain_access": domain_access}
    open_column = [{"column_name": "id", "visible_to": ["analyst"]}]
    tables = [
        {"id": _EDGES_TID, "columns": [{"column_name": "via", "data_type": "varchar"}]},
        {"id": A, "domain_id": "sales", "columns": open_column},
        {"id": B, "domain_id": "finance", "columns": open_column},
    ]
    return build_governance_context(
        "analyst", SimpleNamespace(rules={}), {}, _ctx(), tables, role, relationships=[]
    )


def test_without_view_governance_no_edge_is_seen():
    assert _gov([], [META_DOMAIN_ID]).rls_rules[_EDGES_TID] == "1 = 0"
    assert _EDGES_TID not in _gov(["view_governance"], [META_DOMAIN_ID]).rls_rules


def test_an_edge_is_seen_only_where_both_of_its_columns_are():
    """A role reaching sales and not finance is described sales' tables only, so an edge from
    finance.b into sales.a is not among the edges it sees."""
    rule = _gov(["view_governance"], ["sales"]).rls_rules[_EDGES_TID]
    assert rule == f"source_table_id IN ({A}) AND target_table_id IN ({A})"
