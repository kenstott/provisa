# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c5e-7b24-4d96-9e0a-c2d5b8f14a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2129: a model build stores every lineage edge, however many there are. A multi-VALUES
insert binds every cell and overflows Postgres's 65,535-parameter limit, so the write is an
executemany."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from provisa.api.admin import lineage_router
from provisa.lineage.edges import FoldedEdge

# 10 columns per row: more rows than the limit allows in one statement.
_EDGES = 65_535 // 10 + 1_000


@pytest.mark.asyncio
async def test_more_edges_than_the_parameter_limit_are_stored(control_plane, monkeypatch):
    from provisa.core.schema_org import lineage_edges

    async def _no_rows(conn):
        return []

    edges = {FoldedEdge(f"s{i}", "c", f"t{i}", "c", "v", "view") for i in range(_EDGES)}
    monkeypatch.setattr(lineage_router, "_registry_columns", _no_rows)
    monkeypatch.setattr(lineage_router, "_view_rows", _no_rows)
    monkeypatch.setattr(lineage_router, "_registry_views", lambda rows, mv: ([], []))
    monkeypatch.setattr("provisa.lineage.merge.federation_subdags", lambda *a, **k: [])
    monkeypatch.setattr("provisa.lineage.edges.folded_edges", lambda *a, **k: edges)
    monkeypatch.setattr("provisa.lineage.commands.command_write_graphs", lambda c: [])

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await lineage_router.write_lineage_edges(conn, SimpleNamespace(tracked_functions=[]))
            stored = (
                await conn.execute_core(select(func.count()).select_from(lineage_edges))
            ).scalar_one()
    finally:
        await db.close()
    assert stored == _EDGES
