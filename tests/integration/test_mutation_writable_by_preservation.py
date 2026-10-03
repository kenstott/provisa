# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2c8e04-7b31-4d59-9f6a-0c3e5b8d1a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-870: re-introspection preserves admin-granted writable_by, proven against live PG.

A mutation is registered and an admin grants it (writable_by=['ops']). Re-running
introspection upserts the same function by name with an empty writable_by (discovered
mutations default-deny) — the admin grant MUST survive. A later explicit, non-empty
grant still applies. Uses a throwaway schema so it never touches real config.
"""

from __future__ import annotations

import json
import os

import pytest
from provisa.core.database import create_engine_from_url

from provisa.core.database import Database
from provisa.core.models import Function
from provisa.core.repositories import function as function_repo

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_PG_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "test_req870_wb"


def _fn(writable_by):
    return Function(
        name="createOrder",
        source_id="remote1",
        schema_name="public",
        function_name="createOrder",
        returns="",
        writable_by=writable_by,
        kind="mutation",
        domain_id="orders",  # a command sits in a domain (REQ-1531)
    )


@pytest.fixture
async def db():
    engine = create_engine_from_url(_PG_URL)
    database = Database(engine, name="req870", search_path=_SCHEMA)
    # The canonical org metadata, not hand-written DDL: a hand copy of tracked_functions drifted
    # (it lacked REQ-1634's product_id) and every upsert then failed on the missing column.
    from sqlalchemy import text

    from provisa.core.schema_org import metadata as org_metadata

    with engine.begin() as sc:
        sc.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        sc.execute(text(f"CREATE SCHEMA {_SCHEMA}"))
        sc.execute(text(f"SET search_path TO {_SCHEMA}"))
        org_metadata.create_all(sc)
    yield database
    async with database.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    engine.dispose()


async def _writable_by(conn):
    row = await conn.fetchrow(
        f"SELECT writable_by FROM {_SCHEMA}.tracked_functions WHERE name='createOrder'"
    )
    return list(row["writable_by"] or [])


async def test_reintrospection_preserves_admin_grant(db):
    async with db.acquire() as conn:
        # 1. Discovered mutation registers default-deny (empty writable_by).
        await function_repo.upsert_function(conn, _fn([]), origin="admin")
        assert await _writable_by(conn) == []

        # 2. Admin grants the mutation to a role (by name). writable_by is JSONB, so bind
        # the value via an explicit CAST (a bare list positional bind would hit asyncpg's
        # text codec through the Database shim) — this is the shim-supported jsonb write.
        await conn.execute(
            f"UPDATE {_SCHEMA}.tracked_functions "
            "SET writable_by = CAST($1 AS jsonb) WHERE name='createOrder'",
            json.dumps(["ops"]),
        )
        assert await _writable_by(conn) == ["ops"]

        # 3. Re-introspection upserts by name with empty writable_by — grant must survive.
        await function_repo.upsert_function(conn, _fn([]), origin="admin")
        assert await _writable_by(conn) == ["ops"]


async def test_explicit_nonempty_grant_still_applies(db):
    async with db.acquire() as conn:
        await function_repo.upsert_function(conn, _fn(["ops"]), origin="admin")
        assert await _writable_by(conn) == ["ops"]
        # An explicit, non-empty writable_by (e.g. config-declared) overrides.
        await function_repo.upsert_function(conn, _fn(["ops", "analysts"]), origin="admin")
        assert await _writable_by(conn) == ["ops", "analysts"]
