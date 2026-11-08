# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A projection writes each table's children in that table's file and in no other (REQ-1524,
REQ-1882).

The model commit after every write projects the whole model (env_project.project). It filtered
every child list -- columns, relationships, row filters -- once per table, quadratic in the model
(the core lane's runner, run 42440880718, py-spy: a third of a commit). The children are now
grouped once by the table they belong to; each table's file must still hold exactly its own.

The control plane is real; the model is two tables in ``sales``, each with its own columns, one
relationship from ``orders`` to ``customers``, and a row filter on each.
"""

# Requirements: REQ-1524, REQ-1882

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core.env_project import _by, project
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    rls_rules,
    roles,
    sources,
)

SCHEMA = "org_default"
_COLUMNS = {"orders": ["id", "customer_id", "total"], "customers": ["id", "name"]}


@pytest.fixture
async def model(control_plane) -> Any:
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        for name, cols in _COLUMNS.items():
            await table_repo.upsert(
                conn,
                Table(
                    source_id="__derived__",
                    domain_id="sales",
                    schema_name="s",
                    table_name=name,
                    columns=[Column(name=c, data_type="integer", visible_to=[]) for c in cols],
                    view_sql="SELECT 1",
                ),
            )
            ids[name] = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.table_name == name)
                )
            ).scalar_one()
        await conn.execute_core(
            insert(relationships).values(
                id="orders_customer",
                source_table_id=ids["orders"],
                target_table_id=ids["customers"],
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
            )
        )
        if (
            await conn.execute_core(select(roles.c.id).where(roles.c.id == "analyst"))
        ).first() is None:
            await conn.execute_core(insert(roles).values(id="analyst", description="Reads."))
        for name, expr in (("orders", b"total > 0"), ("customers", b"name IS NOT NULL")):
            await conn.execute_core(
                insert(rls_rules).values(
                    table_id=ids[name], role_id="analyst", filter_expr=expr, description=name
                )
            )
    return db


async def test_each_tables_file_holds_its_own_children_and_no_others(model) -> None:
    async with model.acquire() as conn:
        tree = await project(conn, SCHEMA)
    orders = tree["sales/tables/orders.yaml"]
    customers = tree["sales/tables/customers.yaml"]
    assert [c["column_name"] for c in orders["columns"]] == sorted(_COLUMNS["orders"])
    assert [c["column_name"] for c in customers["columns"]] == sorted(_COLUMNS["customers"])
    assert [r["id"] for r in orders["relationships"]] == ["orders_customer"]
    assert orders["relationships"][0]["target"] == "sales/tables/customers.yaml"
    assert "relationships" not in customers
    assert [p["description"] for p in orders["row_policies"]] == ["orders"]
    assert [p["description"] for p in customers["row_policies"]] == ["customers"]


def test_rows_are_grouped_by_their_key_each_group_in_the_rows_order() -> None:
    rows = [{"t": 2, "n": "a"}, {"t": 1, "n": "b"}, {"t": 2, "n": "c"}, {"t": None, "n": "d"}]
    groups = _by(rows, "t")
    assert groups == {
        2: [{"t": 2, "n": "a"}, {"t": 2, "n": "c"}],
        1: [{"t": 1, "n": "b"}],
        None: [{"t": None, "n": "d"}],
    }
    assert groups[2][0] is rows[0], "the rows themselves, as the per-table filter gave them"
