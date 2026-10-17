# Copyright (c) 2026 Kenneth Stott
# Canary: 255e9410-e685-45df-8e85-2b930bd820a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One pooled PostgreSQL connection serves handles scoped to different org schemas
(``Database.search_path``) and keeps one prepared statement per SQL text. A statement prepared
under one schema then runs under the other."""

# Requirements: REQ-1882

from __future__ import annotations

import pytest

from provisa.core.database import Database

_SQL = "SELECT w.* FROM items w ORDER BY w.id"


def _handles(control_plane, one: str, two: str) -> tuple[Database, Database]:
    with control_plane.engine().begin() as conn:
        for schema, ddl in (("scope_one", one), ("scope_two", two)):
            conn.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            conn.exec_driver_sql(f'CREATE TABLE "{schema}".items ({ddl})')
            conn.exec_driver_sql(
                f"INSERT INTO \"{schema}\".items (id, name) VALUES (1, '{schema}')"
            )
    engine = control_plane.engine(pool_size=1, max_overflow=0)
    return (
        Database(engine, name="one", search_path="scope_one"),
        Database(engine, name="two", search_path="scope_two"),
    )


async def _read(db: Database) -> list[dict]:
    async with db.acquire() as conn:
        return [dict(r) for r in await conn.fetch(_SQL)]


@pytest.mark.asyncio
async def test_a_statement_reads_a_schema_whose_table_has_another_column_order(control_plane):
    one, two = _handles(
        control_plane,
        "id integer PRIMARY KEY, name text NOT NULL, kind text",
        "id integer PRIMARY KEY, kind text, name text NOT NULL",
    )
    assert await _read(one) == [{"id": 1, "name": "scope_one", "kind": None}]
    assert await _read(two) == [{"id": 1, "kind": None, "name": "scope_two"}]
    assert await _read(one) == [{"id": 1, "name": "scope_one", "kind": None}]


@pytest.mark.asyncio
async def test_a_statement_reads_the_schema_its_handle_is_scoped_to(control_plane):
    one, two = _handles(
        control_plane,
        "id integer PRIMARY KEY, name text NOT NULL",
        "id integer PRIMARY KEY, name text NOT NULL",
    )
    assert await _read(one) == [{"id": 1, "name": "scope_one"}]
    assert await _read(two) == [{"id": 1, "name": "scope_two"}]
    assert await _read(one) == [{"id": 1, "name": "scope_one"}]
