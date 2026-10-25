# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8e1f72-5a9b-4d06-b2e4-7f1a0c9d6e83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin's Tables listing reads what its tables need once for all of them (REQ-016). Read
table by table -- columns, pii tags, source, write grants and the OpenAPI source's spec each a
query or more per table, the spec parsed again for every table of it -- the listing of the e2e
model's ~70 tables took more than 26s, and the Tables page sat on "Loading tables...".

Against a control plane of its own (embedded PostgreSQL), as the listing reads it."""

# Requirements: REQ-016

from __future__ import annotations

import json
import types

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_helpers
from provisa.core.schema_org import (
    domains,
    registered_tables,
    sources,
    table_columns,
    table_write_grants,
)

_TABLES = 70


@pytest.fixture
def plane(control_plane, tmp_path, monkeypatch):
    """70 tables: 50 of a PostgreSQL source and 20 of an OpenAPI one, each with columns, a pii
    tag on one and a write grant on the PostgreSQL ones."""
    from provisa.core.schema_org import roles, tag_assignments

    spec = {
        "openapi": "3.0.0",
        "info": {"title": "t", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            f"/items{i}": {
                "get": {
                    "operationId": f"items{i}",
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {"id": {"type": "integer"}},
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            }
            for i in range(20)
        },
    }
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(spec))
    database = control_plane.org()
    with database.engine.begin() as c:
        c.execute(insert(domains).values(id="sales"))
        c.execute(insert(sources).values(id="pg", type="postgresql"))
        c.execute(insert(sources).values(id="api", type="openapi", path=str(spec_path)))
        if c.execute(select(roles.c.id).where(roles.c.id == "analyst")).first() is None:
            c.execute(insert(roles).values(id="analyst"))
        for table_id in range(1, _TABLES + 1):
            api = table_id > 50
            name = f"items{table_id - 51}" if api else f"t{table_id}"
            c.execute(
                insert(registered_tables).values(
                    id=table_id,
                    source_id="api" if api else "pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                )
            )
            for col in ("id", "name", "email"):
                c.execute(
                    insert(table_columns).values(
                        table_id=table_id, column_name=col, visible_to=["analyst"]
                    )
                )
            c.execute(
                insert(tag_assignments).values(
                    tag_id="pii",
                    base_tag_id="pii",
                    object_type="column",
                    table_id=table_id,
                    column_name="email",
                    object_key=f"column:{table_id}:email",
                )
            )
            if not api:
                c.execute(
                    insert(table_write_grants).values(
                        table_id=table_id,
                        role_id="analyst",
                        operation="insert",
                        columns=["name"],
                        presets=[],
                    )
                )

    async def _pool():
        return database

    monkeypatch.setattr(schema_helpers, "_get_pool", _pool)
    from provisa.federation.engine import build_engine

    for name, value in (
        ("openapi_specs", {}),
        ("api_endpoints", {}),
        ("graphql_remote_sources", {}),
        ("federation_engine", types.SimpleNamespace(engine=build_engine("duckdb"))),
        ("config", types.SimpleNamespace(graphql_remote=types.SimpleNamespace(max_rows=1000))),
    ):
        monkeypatch.setattr(appmod.state, name, value, raising=False)
    schema_helpers._parsed_specs.clear()
    return database


class _Counting:
    """A connection that counts the statements it runs."""

    def __init__(self, conn):
        self._conn = conn
        self.statements = 0

    async def execute_core(self, stmt):
        self.statements += 1
        return await self._conn.execute_core(stmt)


async def _listed(database):
    async with database.acquire() as conn:
        rows = [
            dict(r._mapping)
            for r in (
                await conn.execute_core(select(registered_tables).order_by(registered_tables.c.id))
            ).fetchall()
        ]
        counting = _Counting(conn)
        listing = await schema_helpers.table_listing(counting, rows)
        tables = [
            await schema_helpers._fetch_table_with_columns(counting, r, [], True, listing)
            for r in rows
        ]
    return tables, counting.statements


@pytest.mark.asyncio
async def test_the_listing_reads_one_query_per_kind_whatever_the_number_of_tables(
    plane, monkeypatch
):
    import provisa.openapi.mapper as mapper

    parsed = 0
    real = mapper.parse_spec

    def _counted(spec):
        nonlocal parsed
        parsed += 1
        return real(spec)

    monkeypatch.setattr(mapper, "parse_spec", _counted)
    tables, statements = await _listed(plane)
    assert len(tables) == _TABLES
    # Columns, pii tags, sources, write grants: one each, not one each per table.
    assert statements == 4, statements
    assert parsed == 1  # the OpenAPI source's spec, once for its 20 tables
    by_name = {t.table_name: t for t in tables}
    assert [c.column_name for c in by_name["t1"].columns] == ["id", "name", "email"]
    assert [c.is_pii for c in by_name["t1"].columns] == [False, False, True]
    assert [(g.role, g.operation) for g in by_name["t1"].write_grants] == [("analyst", "insert")]
    assert by_name["items3"].api_endpoint == "[GET] https://api.example.test/items3"
    assert by_name["items3"].write_grants == []


@pytest.mark.asyncio
async def test_one_table_alone_reads_its_own(plane):
    async with plane.acquire() as conn:
        (row,) = [
            dict(r._mapping)
            for r in (
                await conn.execute_core(
                    select(registered_tables).where(registered_tables.c.id == 55)
                )
            ).fetchall()
        ]
        table = await schema_helpers._fetch_table_with_columns(conn, row, [], True)
    assert table.api_endpoint == "[GET] https://api.example.test/items4"
    assert [c.is_pii for c in table.columns] == [False, False, True]
