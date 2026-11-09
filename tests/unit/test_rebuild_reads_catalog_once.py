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
table of a built-in source, and the store model leaves the built-in system tables out but their
ids; demo-seeded tables (graphql-demo) ARE model tables and appear in config.tables (REQ-1634).

The control plane is real; the model is ``sales.orders`` (two columns) and three catalog views
on the built-in ``provisa-admin`` source (four columns each).
"""

# Requirements: REQ-1882, REQ-2190, REQ-1634

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


@pytest.fixture
def counted_reads(plane, monkeypatch) -> list[int]:
    """The column rows of each ``fetch_tables`` read, in order."""
    read: list[int] = []
    real = db_queries.fetch_tables

    async def counted(conn, **kw):
        out = await real(conn, **kw)
        read.append(_column_rows(out))
        return out

    monkeypatch.setattr(db_queries, "fetch_tables", counted)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", lambda _s: "store")
    return read


async def test_one_rebuild_reads_the_model_for_its_routes_once(plane, counted_reads) -> None:
    """A rebuild reconciles the landed tables and publishes the routes, each by ``_registry``:
    within one rebuild the model is read for both once; outside one, every call reads it."""
    from provisa.federation import replica_routing

    state = SimpleNamespace(config=object(), model_db=plane, tenant_db=plane)
    with replica_routing.one_model_read():
        first = await replica_routing._registry(state)
        second = await replica_routing._registry(state)
    assert counted_reads == [2]
    assert first.registered is second.registered

    await replica_routing._registry(state)
    await replica_routing._registry(state)
    assert counted_reads == [2, 2, 2]


async def test_a_task_made_in_a_rebuild_reads_afresh_once_it_ends(plane, counted_reads) -> None:
    """A task started inside the rebuild carries its context past the rebuild's end: from then
    on it reads the model as any caller does, never the rebuild's read."""
    import asyncio

    from provisa.federation import replica_routing

    state = SimpleNamespace(config=object(), model_db=plane, tenant_db=plane)
    ended = asyncio.Event()

    async def later() -> None:
        await ended.wait()
        await replica_routing._registry(state)

    with replica_routing.one_model_read():
        await replica_routing._registry(state)
        task = asyncio.create_task(later())
    ended.set()
    await task
    assert counted_reads == [2, 2]


def test_the_rebuild_runs_in_one_model_read() -> None:
    import ast
    import inspect

    from provisa.api import app

    def named(call: ast.expr) -> str | None:
        return (
            call.func.id if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) else None
        )

    blocks = [
        node
        for node in ast.walk(ast.parse(inspect.getsource(app._rebuild_schemas)))
        if isinstance(node, ast.With)
        and "one_model_read" in {named(item.context_expr) for item in node.items}
    ]
    assert [
        call
        for block in blocks
        for call in ast.walk(block)
        if named(call) == "_rebuild_schemas_impl"
    ]


@pytest.fixture
async def plane_with_graphql_demo(control_plane) -> Any:
    """Control plane with a pg table (orders), provisa-admin catalog views, AND a
    graphql-demo table (assignments) — the Root Cause #1 scenario."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="graphql-demo", type="graphql_remote"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(insert(domains).values(id="shelter"))
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
        # REQ-1634: demo-seeded table, as auto-registered by _auto_register_graphql_demo
        await table_repo.upsert(
            conn,
            Table(
                source_id="graphql-demo",
                domain_id="shelter",
                schema_name="graphql",
                table_name="assignments",
                columns=[Column(name="id", data_type="integer", visible_to=[])],
            ),
        )
    return db


async def test_the_store_model_includes_graphql_demo_table_with_columns(
    plane_with_graphql_demo,
) -> None:
    """Root Cause #1: a graphql-demo (demo-seeded) table must appear in store_model's
    ``tables`` with its columns so product_memberships can resolve it.  Built-in system
    tables (provisa-admin catalog views) must still be excluded.  REQ-1634."""
    from provisa.core.store_config import store_model

    async with plane_with_graphql_demo.acquire() as conn:
        model = await store_model(conn)

    table_names = [t["table"] for t in model["tables"]]
    # graphql-demo table is a model table — must be present
    assert "assignments" in table_names
    # provisa-admin catalog views are built-in system tables — still excluded
    for view in _VIEWS:
        assert view not in table_names
    # the assignments table must have its columns
    [asgn] = [t for t in model["tables"] if t["table"] == "assignments"]
    assert [c["name"] for c in asgn["columns"]] == ["id"]
