# Copyright (c) 2026 Kenneth Stott
# Canary: 9d41c7e2-3b58-4f06-a1d9-6e2b8c04f7a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A schema rebuild reads a SQL catalog's views' columns only where it uses them (REQ-1882,
REQ-2190).

The meta domain registers every SQL catalog view (hundreds of them), each with its columns. The
rebuild runs on a request thread, and reading those columns where nothing uses them made every
rebuild slower as the catalog grew: the replica routing read (twice per rebuild) routes no
table of a built-in source, and the store model leaves every seeded table out but its id.

The control plane is real; the model is ``sales.orders`` (two columns) and three catalog views
on the built-in ``provisa-admin`` source (four columns each).
"""

# Requirements: REQ-1882, REQ-2190

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert

from provisa.api.admin import db_queries
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources
from provisa.security.rights import META_DOMAIN_ID, SQL_CATALOG_TABLES

pytestmark = pytest.mark.asyncio

_VIEWS = sorted(name for _, name in SQL_CATALOG_TABLES)[:3]
_VIEW_COLUMNS = ["a", "b", "c", "d"]


@pytest.fixture
async def plane(control_plane) -> Any:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await table_repo.upsert(
            conn,
            Table(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                columns=[Column(name=c, visible_to=[], data_type="integer") for c in ("id", "n")],
            ),
        )
        for name in _VIEWS:
            await table_repo.upsert(
                conn,
                Table(
                    source_id="provisa-admin",
                    domain_id=META_DOMAIN_ID,
                    schema_name="pg_catalog",
                    table_name=name,
                    columns=[
                        Column(name=c, visible_to=[], data_type="varchar") for c in _VIEW_COLUMNS
                    ],
                ),
            )
    return db


def _column_rows(tables: list[dict]) -> int:
    return sum(len(t.get("columns") or []) for t in tables)


async def test_the_replica_registry_reads_no_catalog_view(plane, monkeypatch) -> None:
    from provisa.federation import replica_routing

    read: list[int] = []
    real = db_queries.fetch_tables

    async def counted(conn, **kw):
        out = await real(conn, **kw)
        read.append(_column_rows(out))
        return out

    monkeypatch.setattr(db_queries, "fetch_tables", counted)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", lambda _s: "store")
    state = SimpleNamespace(config=object(), model_db=plane, tenant_db=plane)

    registry = await replica_routing._registry(state)

    assert {r["table_name"] for r in registry.registered} == {"orders"}
    assert read == [2]  # one read, the two columns of orders and none of a view


async def test_the_store_model_reads_no_seeded_table_s_columns(plane, monkeypatch) -> None:
    from provisa.core.store_config import store_model

    read: list[int] = []
    real = table_repo.list_all

    async def counted(conn, **kw):
        out = await real(conn, **kw)
        read.append(_column_rows(out))
        return out

    monkeypatch.setattr(table_repo, "list_all", counted)
    async with plane.acquire() as conn:
        model = await store_model(conn)

    assert [t["table"] for t in model["tables"]] == ["orders"]
    assert [c["name"] for c in model["tables"][0]["columns"]] == ["id", "n"]
    assert read == [2]


async def test_a_table_left_without_its_parts_carries_no_empty_ones(plane) -> None:
    """A table ``parts_of`` refuses is its row alone: an absent key, never an empty list a reader
    could take for a table with no columns."""
    async with plane.acquire() as conn:
        tables = await table_repo.list_all(conn, parts_of=lambda t: t["table_name"] == "orders")
    by_name = {t["table_name"]: t for t in tables}
    assert set(by_name) == {"orders", *_VIEWS}
    assert [c["column_name"] for c in by_name["orders"]["columns"]] == ["id", "n"]
    assert "write_grants" in by_name["orders"]
    for name in _VIEWS:
        assert "columns" not in by_name[name] and "write_grants" not in by_name[name]
