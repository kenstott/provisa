# Copyright (c) 2026 Kenneth Stott
# Canary: 9d27c4e1-3f5a-4b86-a0d2-6e8b1c7f5a43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-2208, decision 2026-11-13): a booted server answers "which columns are relevant to this
library document" with one governed SQL query over the meta table policy_coverage.

The configuration has a table in each of two domains, the tag personal on a column of each, the tag
checked on a column of the sales table, and a role reading the sales domain alone. Once the server
is up, a privacy policy read in both domains is added to its library: its Personal data section
carries the tag personal, and the checked assignment cites the section. The administrator reads
the three columns relevant to the policy; the sales role reads the two in sales.
"""

# Requirements: REQ-2208

from __future__ import annotations

import os

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")

_CONFIG = "tests/fixtures/tag_policy_views_config.yaml"
_ORG = "tag_policy_views_e2e"
_SCHEMA = "tp_e2e"
_PERSONAL = "Personal data"


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


async def _library(srv) -> tuple[int, dict[str, str]]:
    """The privacy policy added, its section tagged and cited: its id, and each column's key by
    its table and name."""
    from sqlalchemy import select

    from provisa.core.models import TagAssignment
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.schema_org import registered_tables
    from provisa.reference_library import store
    from provisa.reference_library.citations import Place

    db = srv.plane.org(_ORG)
    try:
        async with db.acquire() as conn:
            tables = {
                r.table_name: r.id
                for r in (
                    await conn.execute_core(
                        select(registered_tables.c.id, registered_tables.c.table_name).where(
                            registered_tables.c.schema_name == _SCHEMA
                        )
                    )
                ).fetchall()
            }
            policy = await store.insert_document(
                conn,
                title="privacy",
                source="https://example.com/privacy",
                source_kind=store.URL,
                media_type="text/markdown",
                content_sha256="a" * 64,
                status=store.KEPT,
                embedding_model="m",
                added_by="org_admin",
                domains=["tps", "tph"],
                passages=[
                    store.StoredPassage(
                        ordinal=0,
                        section=_PERSONAL,
                        page=1,
                        text="Personal data is any information about a person.",
                        vector=[0.1, 0.9],
                    )
                ],
            )
            await tag_repo.assign(
                conn,
                TagAssignment(
                    tag_id="personal",
                    object_type="section",
                    document_id=policy,
                    section_heading=_PERSONAL,
                ),
            )
            await tag_repo.cite_assignment(
                conn,
                TagAssignment(
                    tag_id="checked",
                    object_type="column",
                    table_id=tables["orders"],
                    column_name="id",
                ),
                Place(policy, _PERSONAL),
                "steward",
            )
    finally:
        await db.close()
    return policy, {
        f"{table}.{column}": f"{tables[table]}:{column}"
        for table, column in (("orders", "id"), ("orders", "email"), ("staff", "name"))
    }


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def server():
    from tests.integration.isolated_server import IsolatedServer

    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.orders (id int PRIMARY KEY, email text)")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.staff (id int PRIMARY KEY, name text)")
    finally:
        await conn.close()
    srv = IsolatedServer(_ORG, engine="duckdb", config=_CONFIG, control_plane="own")
    srv.start()
    try:
        policy, columns = await _library(srv)
        yield srv, policy, columns
    finally:
        srv.stop_process()
        conn = await _pg()
        try:
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        finally:
            await conn.close()


async def _rows(srv, sql: str, role: str) -> list[dict]:
    async with httpx.AsyncClient(base_url=srv.base_url, timeout=120.0) as client:
        resp = await client.post("/data/sql", json={"sql": sql}, headers={"X-Provisa-Role": role})
    assert resp.status_code == 200, f"{resp.text}\n{srv.dump_stderr_debug()[-6000:]}"
    body = resp.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return rows


def _relevant(policy: int) -> str:
    return (
        "SELECT DISTINCT column_key, link FROM meta.policy_coverage "
        f"WHERE document_id = {policy} AND object_kind = 'column'"
    )


async def test_the_administrator_reads_every_column_relevant_to_the_document(server):
    srv, policy, columns = server
    rows = await _rows(srv, _relevant(policy), "org_admin")
    assert sorted((r["column_key"], r["link"]) for r in rows) == sorted(
        [
            (columns["orders.id"], "assignment cites it"),
            (columns["orders.email"], "shared tag"),
            (columns["staff.name"], "shared tag"),
        ]
    ), rows


async def test_a_role_reading_one_domain_reads_the_relevant_columns_there_only(server):
    srv, policy, columns = server
    rows = await _rows(srv, _relevant(policy), "sales_reader")
    assert sorted((r["column_key"], r["link"]) for r in rows) == sorted(
        [
            (columns["orders.id"], "assignment cites it"),
            (columns["orders.email"], "shared tag"),
        ]
    ), rows


def _walk(policy: int) -> str:
    """From the document to its columns over the seeded edges document -> coverage -> column,
    joined on the columns each relationship row names."""
    from provisa.api._meta_seed import META_RELATIONSHIPS

    seeded = {r[0]: (r[3], r[4]) for r in META_RELATIONSHIPS}
    doc_col, cov_doc = seeded["meta:library_documents:policy_coverage"]
    cov_col, col_col = seeded["meta:policy_coverage:column"]
    return (
        "SELECT DISTINCT c.column_key FROM meta.library_documents d "
        f"JOIN meta.policy_coverage p ON p.{cov_doc} = d.{doc_col} "
        f"JOIN meta.table_columns c ON c.{col_col} = p.{cov_col} "
        f"WHERE d.id = {policy}"
    )


@pytest.mark.parametrize(
    ("role", "reached"),
    [
        ("org_admin", ["orders.id", "orders.email", "staff.name"]),
        ("sales_reader", ["orders.id", "orders.email"]),
    ],
)
async def test_a_document_reaches_its_relevant_columns_over_the_relationships(
    server, role, reached
):
    srv, policy, columns = server
    rows = await _rows(srv, _walk(policy), role)
    assert sorted(r["column_key"] for r in rows) == sorted(columns[c] for c in reached), rows
