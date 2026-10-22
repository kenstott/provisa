# Copyright (c) 2026 Kenneth Stott
# Canary: 439fa7c3-7185-42bb-a382-2dd04c1caadf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-2126): a booted server's model build writes its lineage edges, and the meta domain's
data_product_edges reads them.

A data product's member view reads a governed Postgres table. The server starts from its
configuration (the control-plane DDL, the model build, the meta seed), and the product's lineage
edge to that table is read over the governed SQL path, on the DuckDB engine with a control plane of
its own and on the test stack's Trino.
"""

# Requirements: REQ-2126, REQ-2129

from __future__ import annotations

import os

import httpx
import pytest
import pytest_asyncio

pytestmark = [
    pytest.mark.requires_trino,
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

pytest.importorskip("duckdb")

_CONFIG = "tests/fixtures/data_product_edges_config.yaml"
_ORG = "data_product_edges_e2e"
_SCHEMA = "dpe_e2e"


async def _pg():
    import asyncpg

    return await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )


@pytest_asyncio.fixture(scope="module", params=["duckdb", "trino"])
async def server(request):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.orders (id int PRIMARY KEY, customer text)")
    finally:
        await conn.close()
    org = f"{_ORG}_{request.param}"
    if request.param == "duckdb":
        srv = IsolatedServer(org, engine="duckdb", config=_CONFIG, control_plane="own")
    else:
        srv = IsolatedServer(org, engine="trino", config=_CONFIG)
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        conn = await _pg()
        try:
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        finally:
            await conn.close()
        if request.param == "trino":
            await drop_org_schema(org)


async def test_the_products_view_is_derived_from_the_table_it_reads(server):
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.post(
            "/data/sql",
            json={
                "sql": "SELECT kind, direction, target_relation, hops, vias "
                "FROM meta.data_product_edges WHERE product_id = 'order_book'",
                "role": "org_admin",
            },
        )
    assert resp.status_code == 200, f"{resp.text}\n{server.dump_stderr_debug()[-6000:]}"
    body = resp.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    assert [(r["kind"], r["direction"], r["target_relation"], r["hops"]) for r in rows] == [
        ("lineage", "upstream", "dpe.orders", 1)
    ], rows


async def test_the_views_columns_are_derived_from_the_tables_columns(server):
    """REQ-2129: the same build's column edges, read through column_lineage_edges."""
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.post(
            "/data/sql",
            json={
                "sql": "SELECT source_relation, source_column, target_relation, target_column, "
                "via FROM meta.column_lineage_edges WHERE target_relation = 'dpe.view_open_orders'",
                "role": "org_admin",
            },
        )
    assert resp.status_code == 200, f"{resp.text}\n{server.dump_stderr_debug()[-6000:]}"
    body = resp.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    assert sorted(
        (r["source_relation"], r["source_column"], r["target_column"], r["via"]) for r in rows
    ) == [
        ("dpe.orders", "customer", "customer", "dpe.view_open_orders"),
        ("dpe.orders", "id", "id", "dpe.view_open_orders"),
    ], rows
