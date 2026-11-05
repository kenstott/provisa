# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1e9f42-3b85-4d07-a2e6-9f4d8b1c5a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-013, REQ-1882: every registered table with its columns, read in a fixed number of queries.

Each schema rebuild lists every table with its columns. Read one table at a time that was a query
per table -- 74 of them per rebuild on the e2e model -- and the rebuild held its connection for all
of them. The columns are read in one query and grouped by table: the same tables, the same
columns, in the same order.
"""

# Requirements: REQ-013, REQ-1882

from __future__ import annotations

from typing import Any, cast

import pytest

from provisa.core.repositories import table as table_repo

_TABLES = 6


async def _seed(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.schema_org import domains, registered_tables, sources, table_columns

    await conn.execute_core(insert(domains).values(id="d", description="d"))
    await conn.execute_core(insert(sources).values(id="s", type="postgres"))
    for tid in range(1, _TABLES + 1):
        await conn.execute_core(
            insert(registered_tables).values(
                id=tid, source_id="s", domain_id="d", schema_name="public", table_name=f"t{tid}"
            )
        )
        # Inserted in reverse name order: a column's place is its registration, not its name.
        for column in ("zeta", "alpha", f"c{tid}")[: 1 + tid % 3]:
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=tid, domain_id="d", column_name=column, data_type="text"
                )
            )


class _Counting:
    """``conn`` with its statements counted."""

    def __init__(self, conn) -> None:
        self._conn = conn
        self.statements = 0

    async def execute_core(self, *a, **k):
        self.statements += 1
        return await self._conn.execute_core(*a, **k)

    def __getattr__(self, name):
        return getattr(self._conn, name)


@pytest.mark.asyncio
async def test_every_table_is_listed_with_its_columns_in_a_fixed_number_of_queries(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            counting = _Counting(conn)
            listed = await table_repo.list_all(cast(Any, counting))
            one_by_one = {t["id"]: await table_repo.load_columns(conn, t["id"]) for t in listed}
    finally:
        await db.close()

    assert [t["id"] for t in listed] == list(range(1, _TABLES + 1))
    # The same columns, in the same order, as each table read on its own.
    assert {t["id"]: t["columns"] for t in listed} == one_by_one
    assert [c["column_name"] for c in listed[1]["columns"]] == ["zeta", "alpha", "c2"]
    # Tables, columns and write grants: three statements, whatever the number of tables.
    assert counting.statements <= 3


class _CountingFetch(_Counting):
    async def fetch(self, *a, **k):
        self.statements += 1
        return await self._conn.fetch(*a, **k)


@pytest.mark.asyncio
async def test_the_rebuilds_table_read_is_a_fixed_number_of_queries(control_plane):
    """The schema rebuild's read (admin.db_queries.fetch_tables): every table in service with its
    columns in registration order, whatever the number of tables."""
    from provisa.api.admin.db_queries import fetch_tables

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            counting = _CountingFetch(conn)
            tables = await fetch_tables(cast(Any, counting))
            drafts = await fetch_tables(cast(Any, counting), draft=True)
    finally:
        await db.close()

    assert [t["id"] for t in tables] == list(range(1, _TABLES + 1)) and drafts == []
    assert [c["column_name"] for c in tables[1]["columns"]] == ["zeta", "alpha", "c2"]
    assert [c["column_name"] for c in tables[4]["columns"]] == ["zeta", "alpha", "c5"]
    # Two reads: tables, columns and write grants each, whatever the number of tables.
    assert counting.statements <= 6
