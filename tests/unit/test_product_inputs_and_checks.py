# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2126 INPUTS AND RELATED, REQ-1443 PRODUCT ASSIGNMENT: a product's nearest physical
inputs, the product it is built from, and how it shows each check that touches it.

A source table S, a plain view PV over it, a materialized view M over PV in product Q, and a view
RV over M in product R. Q's input is S (PV is expanded at query time, so it is no input); R's is
M (an MV's refresh is where its own inputs are read). Four checker tables scan M and S, assigned
to Q, to R and to none. The lineage is the model build's own (its write step, here).
"""

# Requirements: REQ-2126, REQ-1443

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.dq.registration import write_checker_targets
from provisa.lineage.inputs import MATERIALIZED_VIEW, SOURCE_TABLE, NearestInput

S, PV, M, RV = 1, 2, 3, 4
K_PROMISE, K_EXPECTATION, K_ORPHAN, K_UNASSIGNED = 5, 6, 7, 8


async def _seed(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.schema_org import (
        data_products,
        domains,
        registered_tables,
        sources,
        table_columns,
    )

    await conn.execute_core(insert(domains).values(id="s", description="s"))
    for sid, kind in (("erp", "postgres"), (DERIVED_SOURCE_ID, "postgres"), ("dq", "soda")):
        await conn.execute_core(insert(sources).values(id=sid, type=kind))
    for pid in ("q", "r"):
        await conn.execute_core(insert(data_products).values(id=pid, domain_id="s", name=pid))
    derived = dict(source_id=DERIVED_SOURCE_ID)
    tables = [
        dict(id=S, source_id="erp", table_name="s"),
        dict(id=PV, table_name="pv", view_sql="SELECT s.x AS x FROM s.s s", **derived),
        dict(
            id=M,
            table_name="m",
            view_sql="SELECT pv.x AS x FROM s.pv pv",
            materialize=True,
            product_id="q",
            **derived,
        ),
        dict(
            id=RV, table_name="rv", view_sql="SELECT m.x AS x FROM s.m m", product_id="r", **derived
        ),
    ]
    checkers = [(K_PROMISE, "q"), (K_EXPECTATION, "q"), (K_ORPHAN, "r"), (K_UNASSIGNED, None)]
    tables += [
        dict(id=k, source_id="dq", table_name=f"k{k}", dq_contract="checks", product_id=pid)
        for k, pid in checkers
    ]
    for t in tables:
        await conn.execute_core(
            insert(registered_tables).values(domain_id="s", schema_name="public", **t)
        )
    for table_id in (S, PV, M, RV):
        await conn.execute_core(
            insert(table_columns).values(
                table_id=table_id, domain_id="s", column_name="x", data_type="integer"
            )
        )


async def _build(conn) -> None:
    """The model build's two writes, then the views over them."""
    from provisa.api.admin.lineage_router import write_lineage_edges
    from provisa.api.startup_seed import _adapt_view_ddl

    await write_lineage_edges(conn, SimpleNamespace(tracked_functions={}))
    await write_checker_targets(
        conn, {K_PROMISE: M, K_EXPECTATION: S, K_ORPHAN: S, K_UNASSIGNED: S}
    )
    for name in ("table_inputs", "data_product_edges", "product_checks"):
        await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS[name]))


async def _rows(conn, sql: str) -> list[dict]:
    from sqlalchemy import text

    return [dict(r._mapping) for r in (await conn.execute_core(text(sql))).fetchall()]


@pytest.mark.asyncio
async def test_inputs_stop_at_the_first_source_table_or_materialized_view(control_plane):
    from provisa.lineage.inputs import nearest_physical_inputs

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            await _build(conn)
            m_inputs = await nearest_physical_inputs(conn, M)
            rv_inputs = await nearest_physical_inputs(conn, RV)
            lineage = await _rows(
                conn,
                "SELECT product_id, target_relation, hops, nearest_input FROM data_product_edges "
                "WHERE kind = 'lineage' ORDER BY product_id, target_relation",
            )
            derivation = await _rows(
                conn,
                "SELECT product_id, target_product_id, path_count, supporting_tables "
                "FROM data_product_edges WHERE kind = 'derivation'",
            )
    finally:
        await db.close()

    # M's refresh reads S through the plain view PV; RV reads M, an MV, and stops there.
    assert m_inputs == [NearestInput(S, "s.s", SOURCE_TABLE, "erp/public/s")]
    assert rv_inputs == [NearestInput(M, "s.m", MATERIALIZED_VIEW, f"{DERIVED_SOURCE_ID}/public/m")]

    # The Lineage section keeps the whole ancestry; the inputs are the rows marked nearest.
    assert [
        (r["product_id"], r["target_relation"], r["hops"], r["nearest_input"]) for r in lineage
    ] == [
        ("q", "s.pv", 1, False),
        ("q", "s.rv", 1, None),  # one hop down: RV derives from Q's M
        ("q", "s.s", 2, True),
        ("r", "s.m", 1, True),
        ("r", "s.pv", 2, False),
        ("r", "s.s", 3, False),
    ]

    # R is built from Q's member M: a derivation between products, not a relationship.
    assert derivation == [
        {"product_id": "r", "target_product_id": "q", "path_count": 1, "supporting_tables": "m"}
    ]


@pytest.mark.asyncio
async def test_each_product_shows_each_check_in_the_state_its_assignment_gives_it(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            await _build(conn)
            rows = await _rows(
                conn,
                "SELECT product_id, checker_table_id, checked_table_id, assigned_product_id, "
                "state FROM product_checks",
            )
    finally:
        await db.close()

    assert sorted(
        (r["product_id"], r["checker_table_id"], r["assigned_product_id"], r["state"]) for r in rows
    ) == [
        # Assigned to Q and scanning Q's output port M: Q's promise. M is R's input: partner DQ.
        ("q", K_PROMISE, "q", "promise"),
        # Assigned to Q and scanning Q's input S: Q's expectation.
        ("q", K_EXPECTATION, "q", "expectation"),
        # Assigned to R, scanning S, which is neither R's input nor its output: R's orphan; and
        # S is Q's input, so Q sees it as partner DQ.
        ("q", K_ORPHAN, "r", "partner_dq"),
        # Assigned to none and scanning Q's input S: partner DQ for Q.
        ("q", K_UNASSIGNED, None, "partner_dq"),
        ("r", K_PROMISE, "q", "partner_dq"),
        ("r", K_ORPHAN, "r", "orphan"),
    ]
