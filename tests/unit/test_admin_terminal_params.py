# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed statement on the meta (provisa-admin) terminal runs with its bound parameters.

A JSON:API filter or a GraphQL argument reaches the plan as a $N placeholder and its value in
exec_params, as on every other route. The admin terminal ran the text alone, and PostgreSQL
refused it: "bind message supplies 0 parameters, but prepared statement requires 2".
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_the_meta_terminal_binds_the_plans_parameters(control_plane):
    from sqlalchemy import insert

    from provisa.core.schema_org import data_products, domains
    from provisa.pgwire._pipeline import _Plan, _run_plan_terminal
    from provisa.transpiler.router import Route

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales", description="sales"))
            for pid in ("customer_360", "orders"):
                await conn.execute_core(
                    insert(data_products).values(id=pid, domain_id="sales", name=pid)
                )
        plan = _Plan(
            route=Route.DIRECT,
            sql='SELECT "id" FROM "data_products" WHERE "id" = $1 LIMIT $2',
            source_id="provisa-admin",
            dialect="postgres",
            exec_params=["customer_360", 10],
        )
        state = SimpleNamespace(model_db=db, federation_engine=None)
        result = await _run_plan_terminal(plan, state)
    finally:
        await db.close()

    assert result.column_names == ["id"]
    assert [tuple(r) for r in result.rows] == [("customer_360",)]
