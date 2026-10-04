# Copyright (c) 2026 Kenneth Stott
# Canary: c04900a5-b8b5-4635-9cfe-60e5767610e3

"""The env/git projection works on a schemaless control plane (SQLite/DuckDB).

env_project schema-qualifies every read (``<schema>.<table>``) for the Postgres multi-tenant
layout. A SQLite control plane has no named schemas, so a qualified reference does not resolve
and the whole projection — and the model-stamp read it brackets — failed with "no such table:
<schema>.config_stamp", which env_repo's REQ-1524 blanket except then turned into a silently
drifted environment on every commit. On a schemaless backend the reads are unqualified."""

from __future__ import annotations

import asyncio

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.env_project import model_stamp, project

pytestmark = pytest.mark.unit


@pytest.fixture
def db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    d = Database(engine, name="org")
    asyncio.run(init_schema(d, "", org_id="default"))
    yield d
    engine.dispose()


def test_model_stamp_reads_from_the_default_schema_on_sqlite(db):
    async def _go():
        async with db.acquire() as conn:
            assert conn.capabilities.schemas is False
            # A schema name is still passed (the org's), but SQLite has no such schema — the read
            # must fall back to the unqualified table rather than raising "no such table".
            return await model_stamp(conn, "org_acme")

    assert asyncio.run(_go()) >= 0


def test_project_reads_the_default_schema_on_sqlite(db):
    async def _go():
        async with db.acquire() as conn:
            return await project(conn, "org_acme")

    tree = asyncio.run(_go())
    assert isinstance(tree, dict)  # it projects without raising on a schemaless backend
