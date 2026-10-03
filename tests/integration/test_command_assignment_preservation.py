# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2c8e04-7b31-4d59-9f6a-0c3e5b8d1a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Discovery assigns a command safely and keeps an admin's assignment, against live PG.

A command carries one role list, ``visible_to`` (empty = every role). A newly discovered
mutation is assigned to the org admin alone; a discovered read to every role. Re-running
introspection on a command an admin has assigned keeps that assignment.
"""

from __future__ import annotations

import json
import os

import pytest

from provisa.api.admin.introspect import DiscoveredRoutine, register_discovered_routines
from provisa.core.database import Database, create_engine_from_url

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_PG_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "test_req870_wb"


def _routine(kind: str = "mutation") -> DiscoveredRoutine:
    return DiscoveredRoutine(
        schema_name="public", routine_name="createOrder", kind=kind, returns_setof=False
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


async def _assigned(conn):
    row = await conn.fetchrow(
        f"SELECT visible_to FROM {_SCHEMA}.tracked_functions WHERE name='createOrder'"
    )
    return list(row["visible_to"] or [])


async def test_a_discovered_mutation_is_assigned_to_the_org_admin_alone(db):
    async with db.acquire() as conn:
        await register_discovered_routines(conn, "remote1", [_routine()], domain_id="orders")
        assert await _assigned(conn) == ["org_admin"]


async def test_a_discovered_read_is_assigned_to_every_role(db):
    async with db.acquire() as conn:
        await register_discovered_routines(conn, "remote1", [_routine("query")], domain_id="orders")
        assert await _assigned(conn) == []


async def test_reintrospection_keeps_the_roles_a_command_was_assigned(db):
    async with db.acquire() as conn:
        await register_discovered_routines(conn, "remote1", [_routine()], domain_id="orders")
        # An admin assigns the command to a role. visible_to is JSONB, so bind the value via an
        # explicit CAST (a bare list positional bind would hit asyncpg's text codec through the
        # Database shim).
        await conn.execute(
            f"UPDATE {_SCHEMA}.tracked_functions "
            "SET visible_to = CAST($1 AS jsonb) WHERE name='createOrder'",
            json.dumps(["ops"]),
        )
        assert await _assigned(conn) == ["ops"]
        # Introspection finds the same routine again: the assignment survives.
        await register_discovered_routines(conn, "remote1", [_routine()], domain_id="orders")
        assert await _assigned(conn) == ["ops"]
