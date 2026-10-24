# Copyright (c) 2026 Kenneth Stott
# Canary: e4dfda3d-a051-4108-a268-3a3ca154cade
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A checker table's rules reach the meta domain as rows: parsed from its contract when it is
registered, stored with it, and served by the dq_rules view one check per row in contract order --
what a data product's panel lists without parsing anything itself."""

# Requirements: REQ-1443

from __future__ import annotations

from sqlalchemy import insert, text

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources
from provisa.dq.registration import derive_checker_table

CONTRACT = """
dataset: provisa/sales/orders
checks:
  - row_count:
      must_be_greater_than: 0
columns:
  - name: customer
    checks:
      - missing:
"""


async def test_a_saved_checker_tables_rules_are_meta_rows_in_contract_order(control_plane):
    db = control_plane.org()
    table = Table(
        source_id="dq",
        domain_id="quality",
        schema="quality",
        table="orders_scans",
        dq_contract=CONTRACT,
        cache_ttl=3600,
        columns=[Column(name="placeholder", data_type="varchar", visible_to=["analyst"])],
    )
    derive_checker_table(table, "soda")
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="dq", type="soda"))
        await conn.execute_core(insert(domains).values(id="quality"))
        table_id = await table_repo.upsert(conn, table)
        await conn.execute_core(text(_META_TABLE_VIEWS["dq_rules"]))
        rows = (
            await conn.execute_core(
                text(
                    "SELECT id, table_id, checker, position, column_name, check_type, definition "
                    "FROM dq_rules ORDER BY position"
                )
            )
        ).fetchall()
    assert [tuple(r) for r in rows] == [
        (f"{table_id}:1", table_id, "soda", 1, "", "row_count", "must_be_greater_than: 0"),
        (f"{table_id}:2", table_id, "soda", 2, "customer", "missing", ""),
    ]
