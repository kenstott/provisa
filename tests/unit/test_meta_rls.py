# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-828 coupling #3: app-layer meta-governance RLS.

Adversarial tenant-isolation tests proving the guard holds on the EMBEDDED admin stores
(SQLite + DuckDB) that have no native row-level security — the whole point of moving
enforcement out of Postgres. A leak here is a cross-tenant control-plane breach.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import delete, insert, select, update

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.meta_rls import apply_meta_tenant_guard, current_meta_tenant, meta_tenant_scope
from provisa.core.schema_org import domains

pytestmark = pytest.mark.asyncio

_EMBEDDED = ["sqlite+aiosqlite:///:memory:", "duckdb:///:memory:"]

# tenant_id is a UUID column. Strings are the ambient form (JWT claim → the guard coerces); the
# raw, un-guarded seed inserts below use UUID objects the column accepts directly.
_A = "11111111-1111-1111-1111-111111111111"
_B = "22222222-2222-2222-2222-222222222222"


async def _store(uri: str) -> Database:
    db = Database(create_engine_from_url(uri), name="meta-rls-test")
    await _init_schema_portable(db)
    return db


async def _seed(conn) -> None:
    # Two tenants' rows plus a shared (NULL-tenant) row, written OUTSIDE any tenant scope.
    await conn.execute_core(
        insert(domains).values(id="d_a", description="A", tenant_id=uuid.UUID(_A))
    )
    await conn.execute_core(
        insert(domains).values(id="d_b", description="B", tenant_id=uuid.UUID(_B))
    )
    await conn.execute_core(insert(domains).values(id="d_shared", description="S", tenant_id=None))


async def _ids(conn) -> set[str]:
    res = await conn.execute_core(select(domains.c.id))
    return {r[0] for r in res.fetchall()}


@pytest.mark.parametrize("uri", _EMBEDDED)
async def test_select_confined_to_tenant_plus_shared(uri):
    db = await _store(uri)
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            with meta_tenant_scope(_A):
                ids = await _ids(conn)
                assert "d_a" in ids and "d_shared" in ids  # own + shared visible
                assert "d_b" not in ids  # other tenant's row is NOT visible
            with meta_tenant_scope(_B):
                ids = await _ids(conn)
                assert "d_b" in ids and "d_shared" in ids
                assert "d_a" not in ids
    finally:
        await db.close()


@pytest.mark.parametrize("uri", _EMBEDDED)
async def test_no_scope_sees_all(uri):
    db = await _store(uri)
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            # multitenancy=False / seeding: no tenant in scope → guard is a no-op, full visibility.
            assert {"d_a", "d_b", "d_shared"} <= await _ids(conn)
    finally:
        await db.close()


@pytest.mark.parametrize("uri", _EMBEDDED)
async def test_insert_stamped_with_scoped_tenant(uri):
    db = await _store(uri)
    try:
        async with db.acquire() as conn:
            with meta_tenant_scope(_A):
                # No tenant_id supplied — the guard stamps it (coercing the string to UUID).
                await conn.execute_core(insert(domains).values(id="d_new", description="N"))
            res = await conn.execute_core(
                select(domains.c.tenant_id).where(domains.c.id == "d_new")
            )
            assert str(res.fetchone()[0]) == _A
    finally:
        await db.close()


@pytest.mark.parametrize("uri", _EMBEDDED)
async def test_insert_cannot_land_under_another_tenant(uri):
    db = await _store(uri)
    try:
        async with db.acquire() as conn:
            with meta_tenant_scope(_A):
                # A caller attempting to write a B-owned row is forced back to A.
                await conn.execute_core(
                    insert(domains).values(id="d_x", description="X", tenant_id=uuid.UUID(_B))
                )
            res = await conn.execute_core(select(domains.c.tenant_id).where(domains.c.id == "d_x"))
            assert str(res.fetchone()[0]) == _A
    finally:
        await db.close()


@pytest.mark.parametrize("uri", _EMBEDDED)
async def test_update_cannot_touch_other_tenant_rows(uri):
    db = await _store(uri)
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            with meta_tenant_scope(_A):
                await conn.execute_core(
                    update(domains).where(domains.c.id == "d_b").values(description="HACKED")
                )
            # d_b belongs to B — the tenant-A update must not have reached it.
            res = await conn.execute_core(
                select(domains.c.description).where(domains.c.id == "d_b")
            )
            assert res.fetchone()[0] == "B"
    finally:
        await db.close()


@pytest.mark.parametrize("uri", _EMBEDDED)
async def test_delete_cannot_remove_other_tenant_rows(uri):
    db = await _store(uri)
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            with meta_tenant_scope(_A):
                await conn.execute_core(delete(domains).where(domains.c.id == "d_b"))
            # No tenant scope → observe the true table: d_b survives the tenant-A delete.
            assert "d_b" in await _ids(conn)
    finally:
        await db.close()


def test_guard_is_noop_without_tenant():
    # No tenant in scope → statement returned identically (single-tenant path unaffected).
    stmt = select(domains)
    assert apply_meta_tenant_guard(stmt) is stmt
    assert current_meta_tenant() is None
