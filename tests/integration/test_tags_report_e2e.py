# Copyright (c) 2026 Kenneth Stott
# Canary: a799a4cd-8a06-42c4-981b-fb00be1f3c4b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-2208, THE TAGS REPORT): a booted server publishes the tags on its model as the meta
table tag_instances and the ops report tags_report, read through SQL over HTTP.

The configuration has a table in each of two domains, the tag approved on both tables and on a
column of the sales table, and a role reading the sales domain alone. The server starts from it on
the test stack's PostgreSQL (the control-plane DDL, the model build, the meta and ops seeds); the
administrator reads every tag, and the sales role reads the tags of the sales objects only.
"""

# Requirements: REQ-2208

from __future__ import annotations

import os

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")

_CONFIG = "tests/fixtures/tags_report_config.yaml"
_ORG = "tags_report_e2e"
_SCHEMA = "tr_e2e"


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


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def server():
    from tests.integration.isolated_server import IsolatedServer

    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.orders (id int PRIMARY KEY, email text)")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.staff (id int PRIMARY KEY)")
    finally:
        await conn.close()
    srv = IsolatedServer(_ORG, engine="duckdb", config=_CONFIG, control_plane="own")
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


async def _rows(server, sql: str, role: str) -> list[dict]:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.post("/data/sql", json={"sql": sql}, headers={"X-Provisa-Role": role})
    assert resp.status_code == 200, f"{resp.text}\n{server.dump_stderr_debug()[-6000:]}"
    body = resp.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return rows


_EVERY_TAG = [
    ("approved", "column", "trs", "assigned", "clean"),
    ("approved", "table", "trh", "assigned", "clean"),
    ("approved", "table", "trs", "assigned", "clean"),
]


@pytest.mark.parametrize(
    "relation", ["meta.tag_instances", "ops.tags_report"], ids=["meta_table", "report"]
)
async def test_the_administrator_reads_every_tag_on_the_model(server, relation):
    rows = await _rows(
        server,
        f"SELECT tag_id, object_kind, domain_id, via, status FROM {relation} "
        "WHERE tag_id = 'approved'",
        "org_admin",
    )
    found = sorted(
        (r["tag_id"], r["object_kind"], r["domain_id"], r["via"], r["status"]) for r in rows
    )
    assert found == _EVERY_TAG, rows


@pytest.mark.parametrize(
    "relation", ["meta.tag_instances", "ops.tags_report"], ids=["meta_table", "report"]
)
async def test_a_role_reading_one_domain_reads_the_tags_of_its_objects_only(server, relation):
    rows = await _rows(
        server,
        f"SELECT tag_id, object_kind, domain_id FROM {relation} WHERE tag_id = 'approved'",
        "sales_reader",
    )
    found = sorted((r["tag_id"], r["object_kind"], r["domain_id"]) for r in rows)
    assert found == [("approved", "column", "trs"), ("approved", "table", "trs")], rows
