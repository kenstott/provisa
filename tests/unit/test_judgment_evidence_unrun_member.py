# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2094: a table a configuration's seed puts on a profiler has no result relations until its
first run writes them. A judgment saved on it then (a tag or a fake in the same seed) is made on no
evidence; reading its latest run must not reach for a relation that does not exist. A fresh
deployment of the testdata demo stopped at startup on exactly that (relation
"customers_<id>_profile_runs" does not exist)."""

# Requirements: REQ-2094

from __future__ import annotations

import pytest
from sqlalchemy import insert
from sqlalchemy.schema import CreateTable

pytestmark = pytest.mark.unit

_REG = {"source_id": "shop-db", "schema_name": "public", "table_name": "customers"}


async def _member(db) -> int:
    """customers on shop-db, a member of the profiler shop-profiler, never run."""
    from provisa.core.schema_org import domains, registered_tables, sources

    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="shop"))
        await conn.execute_core(insert(sources).values(id="shop-db", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="shop-profiler", type="data_profiler"))
        result = await conn.execute_core(
            insert(registered_tables)
            .values(**_REG, domain_id="shop", profiler_source_id="shop-profiler")
            .returning(registered_tables.c.id)
        )
        return int(result.scalar())


async def test_a_member_never_run_gives_no_evidence(control_plane):
    from provisa.judgments.evidence import evidence, latest_run
    from provisa.profiler.schema import result_exists

    db = control_plane.org()
    tid = await _member(db)
    async with db.acquire() as conn:
        assert not await result_exists(conn, "customers", tid, "runs", None)
        assert await latest_run(conn, None, _REG) is None
        assert await evidence(conn, None, _REG, "email") == {}


async def test_once_it_has_run_its_latest_run_is_the_evidence(control_plane):
    import datetime as dt

    from provisa.judgments.evidence import latest_run
    from provisa.profiler.schema import result_exists, result_sa_table

    db = control_plane.org()
    tid = await _member(db)
    runs = result_sa_table("customers", tid, "runs")
    async with db.acquire() as conn:
        await conn.execute_core(CreateTable(runs))
        assert await result_exists(conn, "customers", tid, "runs", None)
        assert await latest_run(conn, None, _REG) is None  # written, no run yet
        await conn.execute_core(
            insert(runs).values(
                run_id="r1",
                run_time=dt.datetime(2026, 10, 19, tzinfo=dt.timezone.utc),
                status="succeeded",
                sample_method="full",
            )
        )
        assert await latest_run(conn, None, _REG) == (tid, "r1")
