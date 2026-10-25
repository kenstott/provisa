# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Profiling a table takes reading all of it (REQ-2152, ruling Q6a).

A profile describes every column of its table, and its result tables carry row rules naming
every one. So a table joins a profiler, and a result table of its profile is registered, only by
a caller served every column of it; any other is refused naming no column. A refused result table
is answered exactly as one whose table is not a member.

The model: ``orders`` (id, secret) is a member of profiler ``prof``.
"""

# Requirements: REQ-2152, REQ-1934

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources
from provisa.profiler.registration import profile_reach_refusal

pytestmark = pytest.mark.asyncio


def _orders(profiler: str | None) -> Table:
    return Table(
        source_id="warehouse",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        profiler_source_id=profiler,
        columns=[
            Column(name="id", data_type="bigint", visible_to=[]),
            Column(name="secret", data_type="varchar", visible_to=[]),
        ],
    )


def _result(table_name: str) -> Table:
    return Table(
        source_id="prof",
        domain_id="sales",
        schema_name="org_default",
        table_name=table_name,
        columns=[Column(name="run_id", data_type="varchar", visible_to=[])],
    )


@pytest.fixture
async def model(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="warehouse", type="postgresql"))
        await conn.execute_core(
            insert(sources).values(id="prof", type="data_profiler", mapping={"cron": "0 3 * * *"})
        )
        await conn.execute_core(insert(domains).values(id="sales"))
        member_id = await table_repo.upsert(conn, _orders("prof"))
    assert member_id is not None
    return db, member_id


async def _reads_all(table_id: int) -> bool:
    return True


async def _reads_part(table_id: int) -> bool:
    return False


async def test_a_result_table_is_registered_only_by_a_caller_reading_all_of_its_table(model):
    db, member_id = model
    result = _result(f"orders_{member_id}_profile_columns")
    async with db.acquire() as conn:
        assert await profile_reach_refusal(conn, result, _reads_all) is None
        refused = await profile_reach_refusal(conn, result, _reads_part)
    # Answered as a table that is not a member: the refusal tells nothing of its columns.
    assert refused == (
        f"Table {result.table_name!r}: 'orders' (id {member_id}) is not a member of profiler "
        "'prof', so it has no such profile to expose"
    )


async def test_a_table_joins_a_profiler_only_by_a_caller_reading_all_of_it(model):
    db, _member_id = model
    async with db.acquire() as conn:
        await table_repo.upsert(conn, _orders(None))  # leaves the profiler
        joining = _orders("prof")
        assert await profile_reach_refusal(conn, joining, _reads_all) is None
        refused = await profile_reach_refusal(conn, joining, _reads_part)
        # Saving a table that stays in its profiler changes no membership: nothing to refuse.
        await table_repo.upsert(conn, _orders("prof"))
        staying = await profile_reach_refusal(conn, _orders("prof"), _reads_part)
    assert refused is not None and "secret" not in refused and "id" not in refused.split()
    assert staying is None


async def test_the_catalog_offers_only_a_member_its_caller_reads_all_of(model, monkeypatch):
    import types

    from provisa.api.admin import profiler_router
    from provisa.api.admin.catalog_scope import CatalogScope

    db, member_id = model
    monkeypatch.setattr(profiler_router, "_db", lambda: db)
    monkeypatch.setattr(profiler_router, "require_capability_request", lambda request, cap: None)
    request = types.SimpleNamespace(state=types.SimpleNamespace())

    def _serving(columns: set[str]) -> None:
        scope = CatalogScope(
            whole=False,
            reach=None,
            served={member_id: columns},
            governance=False,
            role="r",
            compiled=frozenset({member_id}),
        )
        monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)

    _serving({"id"})
    assert await profiler_router.profiler_catalog(request, "prof") == []
