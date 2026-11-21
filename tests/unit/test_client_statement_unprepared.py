# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8f1c62-9a4d-4b7e-8c15-d06a2f7b9e43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement run once is not kept prepared on the pooled connection. The control plane prepares
each statement on its first execution and keeps it (``prepare_threshold=0``, an LRU of
``_PREPARED_MAX``); a client's own statement, its literals written into it, never runs again, and
a catalog statement's plan held in the backend is tens of megabytes."""

# Requirements: REQ-1882, REQ-2190

from __future__ import annotations

import pytest

from provisa.core.database import Database


async def _held(conn) -> list[str]:
    return [r[0] for r in await conn.fetch("SELECT statement FROM pg_prepared_statements")]


@pytest.mark.asyncio
async def test_a_statement_run_once_leaves_nothing_prepared(control_plane):
    db = Database(control_plane.engine(pool_size=1, max_overflow=0), name="once")
    async with db.acquire() as conn:
        columns, rows = await conn.fetch_with_columns("SELECT 40 + 2 AS run_once", prepared=False)
        assert (columns, [tuple(r) for r in rows]) == (["run_once"], [(42,)])
        _, oids, rows = await conn.fetch_described(
            "SELECT 39 + $1::int AS described_once", 3, bind_as_given=True, prepared=False
        )
        assert (oids, [tuple(r) for r in rows]) == ([23], [(42,)])
        assert not [s for s in await _held(conn) if "_once" in s]


@pytest.mark.asyncio
async def test_a_statement_after_one_run_once_is_prepared_as_before(control_plane):
    db = Database(control_plane.engine(pool_size=1, max_overflow=0), name="after")
    async with db.acquire() as conn:
        await conn.fetch_with_columns("SELECT 1 AS run_once", prepared=False)
        await conn.fetch_with_columns("SELECT 2 AS kept")
        assert [s for s in await _held(conn) if "AS kept" in s]
