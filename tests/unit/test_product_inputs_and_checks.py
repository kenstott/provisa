# Copyright (c) 2026 Kenneth Stott
# Canary: 7ba718e6-1124-49ff-a665-e92baeccca91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2126 INPUTS AND RELATED, REQ-2157: a product's nearest physical inputs, and how it shows
each check that touches it -- expectation, promise, related or unattached.

A source table S, a plain view PV over it, a materialized view M over PV in product Q, and a view
RV over M in product R. Q's input is S (PV is expanded at query time, so it is no input); R's is
M (an MV's refresh is where its own inputs are read). Four checker tables scan M and S, assigned
to Q, to R and to none. The lineage is the model build's own (its write step, here).
"""

# Requirements: REQ-2126, REQ-1443, REQ-2157

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
    for name in ("table_inputs", "data_product_edges"):
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
            kinds = await _rows(conn, "SELECT DISTINCT kind FROM data_product_edges")
    finally:
        await db.close()

    # M's refresh reads S through the plain view PV; RV reads M, an MV, and stops there.
    assert m_inputs == [NearestInput(S, "s.s", SOURCE_TABLE, "erp/public/s")]
    assert rv_inputs == [NearestInput(M, "s.m", MATERIALIZED_VIEW, f"{DERIVED_SOURCE_ID}/public/m")]

    # A product's lineage, and what it is built from, are the product lineage endpoint's
    # (REQ-2156): data_product_edges holds its relationships only.
    assert {r["kind"] for r in kinds} <= {"relationship", "related"}


@pytest.mark.asyncio
async def test_each_product_shows_each_check_by_what_it_checks_and_who_owns_it(control_plane):
    from provisa.api.admin.lineage_router import _fetch_checkers
    from provisa.lineage.inputs import nearest_physical_inputs
    from provisa.lineage.product import product_dq

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            await _build(conn)
            inputs = {
                product: {i.table_id for i in await nearest_physical_inputs(conn, member)}
                for product, member in (("q", M), ("r", RV))
            }
        checkers = await _fetch_checkers(SimpleNamespace(model_db=db))
    finally:
        await db.close()

    # A reader served every checker and every checked column.
    served = {k: {"ok"} for k in (K_PROMISE, K_EXPECTATION, K_ORPHAN, K_UNASSIGNED)} | {
        t: {"x"} for t in (S, PV, M, RV)
    }

    def lists(product: str, member: int) -> dict[str, list[int]]:
        out = product_dq(product, checkers, members={member}, inputs=inputs[product], served=served)
        return {name: [e["checker_table_id"] for e in entries] for name, entries in out.items()}

    assert lists("q", M) == {
        # Q's check on its member M, and its check on its input S.
        "promises": [K_PROMISE],
        "expectations": [K_EXPECTATION],
        # R's check, and nobody's, on Q's input S.
        "related": [K_ORPHAN, K_UNASSIGNED],
        "unattached": [],
    }
    assert lists("r", RV) == {
        "promises": [],
        "expectations": [],
        # Q's promise on M, which is R's input.
        "related": [K_PROMISE],
        # R's check on S, which is neither R's member nor its input: a curation gap for R.
        "unattached": [K_ORPHAN],
    }
