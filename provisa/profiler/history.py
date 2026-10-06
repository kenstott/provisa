# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2e5c17-94d3-4b60-a1f8-3d7c9e0b2f45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A member table's stored runs, read back from its result relations (REQ-1934).

The relations live in the org's control-plane schema, written by the run (``provisa.profiler.run``);
reading them back is reading the profiler's own record, not the profiled table, so it goes to the
model store directly as the runs view does.
"""

# Requirements: REQ-1934

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.schema import CreateTable

from provisa.profiler.schema import result_sa_table

# The kinds a run's measures are read from (``compare.measures_of``).
MEASURE_KINDS: tuple[str, ...] = (
    "runs",
    "columns",
    "quantiles",
    "top_values",
    "fanout",
    "fanout_runs",
    "duplicates",
    "correlations",
    "dependencies",
)


async def relation(conn: Any, table_name: str, table_id: int, kind: str) -> Any:
    """The relation ``kind`` of the member table, created empty if its first run has not written
    it: a member that has not run yet has an empty history, not a missing one."""
    table = result_sa_table(table_name, table_id, kind)
    await conn.execute_core(CreateTable(table, if_not_exists=True))
    return table


async def previous_runs(
    conn: Any, table_name: str, table_id: int, before: datetime, limit: int | None
) -> list[tuple[str, datetime]]:
    """``[(run_id, run_time)]`` of the table's latest ``limit`` (None: all) successful runs before
    ``before``, latest first."""
    runs = await relation(conn, table_name, table_id, "runs")
    query = (
        select(runs.c.run_id, runs.c.run_time)
        .where(runs.c.status == "succeeded", runs.c.run_time < before)
        .order_by(runs.c.run_time.desc())
    )
    result = await conn.execute_core(query if limit is None else query.limit(limit))
    return [(r[0], as_utc(r[1])) for r in result.fetchall()]


def as_utc(at: datetime) -> datetime:
    """A stored run time as an aware UTC instant. Every run time is written aware in UTC; a store
    whose timestamp type keeps no zone (SQLite) hands it back naive, still in UTC."""
    return at.replace(tzinfo=UTC) if at.tzinfo is None else at.astimezone(UTC)


async def run_results(
    conn: Any, table_name: str, table_id: int, run_ids: list[str], kinds: tuple[str, ...]
) -> dict[str, dict[str, list[dict]]]:
    """``{run_id: {kind: rows}}`` for each of ``run_ids``."""
    out: dict[str, dict[str, list[dict]]] = {rid: {k: [] for k in kinds} for rid in run_ids}
    if not run_ids:
        return out
    for kind in kinds:
        table = await relation(conn, table_name, table_id, kind)
        result = await conn.execute_core(select(table).where(table.c.run_id.in_(run_ids)))
        for r in result.fetchall():
            row = dict(r._mapping)
            out[row["run_id"]][kind].append(row)
    return out
