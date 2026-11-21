# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1e4d27-0c5a-4f93-b6d2-7a3e1f9c5b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Removing a table frees it at once (REQ-433, amended 2026-11-20).

Deleting a table retires it (REQ-2248): its row stays. Registering that source table again --
in its domain or another, and when it was registered with no domain -- restores the same table
into the domain the registration names, instead of refusing it as claimed or as a retired name.
"""

# Requirements: REQ-433, REQ-2248

from __future__ import annotations

from sqlalchemy import insert, select

from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables
from tests.unit.test_table_save_atomic import _register, _served, plane  # noqa: F401, F811


async def _rows(db) -> list[tuple[int, str, bool]]:
    async with db.acquire() as conn:
        return [
            (r.id, r.domain_id, r.retired)
            for r in (
                await conn.execute_core(
                    select(
                        registered_tables.c.id,
                        registered_tables.c.domain_id,
                        registered_tables.c.retired,
                    ).where(registered_tables.c.table_name == "orders")
                )
            ).fetchall()
        ]


async def test_a_deleted_table_registers_again_in_its_domain(plane):  # noqa: F811
    first = await _register()
    assert first.success, first.message
    ((table_id, _, _),) = await _rows(plane)
    async with plane.acquire() as conn:
        assert await table_repo.delete(conn, table_id) is True
    again = await _register()
    assert again.success, again.message
    assert await _rows(plane) == [(table_id, "sales", False)]


async def test_a_deleted_table_registered_with_no_domain_registers_in_a_domain(plane):  # noqa: F811
    """The maintainer's case: a table registered with no domain, deleted, then registered in
    pet-store. It was refused as claimed by domain '' (first-come ownership)."""
    async with plane.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="pet-store"))
        table_id = (
            await conn.execute_core(
                insert(registered_tables)
                .values(source_id="pg", domain_id="", schema_name="public", table_name="orders")
                .returning(registered_tables.c.id)
            )
        ).scalar_one()
        assert await table_repo.delete(conn, table_id) is True
    result = await _register(domain_id="pet-store")
    assert result.success, result.message
    assert await _rows(plane) == [(table_id, "pet-store", False)]
