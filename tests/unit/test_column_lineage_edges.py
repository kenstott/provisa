# Copyright (c) 2026 Kenneth Stott
# Canary: 5ba1d1eb-231f-46d8-baac-1912fdaf75a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2129, REQ-2156: the model's column lineage, as the model build writes it.

The edges are the ones the model build writes (the build's own step, here) to the lineage edge
table, folded from each view's sub-DAG so a view's internal steps never cut the chain. They are no
view of the meta domain: column lineage is answered only by the lineage reads that cut it to their
caller's rights.
"""

# Requirements: REQ-2129, REQ-2156

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core.models import DERIVED_SOURCE_ID

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
    from sqlalchemy import select

    from provisa.api.admin.lineage_router import write_lineage_edges
    from provisa.core.schema_org import lineage_edges as e

    def edges(where):
        return select(
            e.c.source_relation,
            e.c.source_column,
            e.c.target_relation,
            e.c.target_column,
            e.c.via,
            e.c.via_kind,
            e.c.source_table_id,
        ).where(where)

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            # The model build's own step.
            await write_lineage_edges(conn, SimpleNamespace(tracked_functions={}))
            rows = await conn.execute_core(
                edges((e.c.target_table_id == A) & (e.c.target_column_name == "total"))
            )
            assert [tuple(r) for r in rows.fetchall()] == [
                ("finance.b", "amount", "sales.a", "total", "sales.a", "view", B)
            ]
            # The CTE inside v2 is walked through: a.total -> v2.doubled, named by v2.
            rows = await conn.execute_core(edges(e.c.target_table_id == V2))
            assert [tuple(r)[:5] for r in rows.fetchall()] == [
                ("sales.a", "total", "sales.v2", "doubled", "sales.v2")
            ]
    finally:
        await db.close()


def test_column_lineage_is_no_view_of_the_meta_domain():
    """A second, uncut path to column lineage: none is served."""
    assert "column_lineage_edges" not in _META_TABLE_VIEWS
    assert not [r for r in META_RELATIONSHIPS if "column_lineage_edges" in r]
