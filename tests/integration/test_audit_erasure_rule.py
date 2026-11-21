# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c2a91-7d4e-4b8a-9c15-e2a0b7d84f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The query audit log's UPDATE rule lets an account's erasure through and nothing else.

query_audit_log is append-only at the database level (REQ-074, REQ-596): a PostgreSQL rule turns
every DELETE and UPDATE into nothing. Erasing an account (REQ-1312) replaces the person's id on
their audit entries with a tombstone. Under the unconditional rule that UPDATE did nothing and
raised nothing, so an erased person stayed named in the log. The rule now passes exactly that
change -- user_id, alone, from an id to that id's tombstone -- and still swallows every other.

Laid out by init_audit_schema, as production lays it out, on an embedded PostgreSQL of the
test's own: a rule only PostgreSQL enforces is shown only on PostgreSQL."""

# Requirements: REQ-074, REQ-596, REQ-1312

from __future__ import annotations

import asyncio

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import update

from provisa.audit.query_log import init_audit_schema
from provisa.core.database import Database, create_engine_from_url
from provisa.core.org_membership import tombstone_id
from provisa.core.schema_org import query_audit_log

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_SCHEMA = "org_erasure"  # org_schema("erasure")
_COLUMNS = "user_id, role_id, query_hash, table_names, source, status_code, duration_ms, region"


@pytest.fixture
def pg():
    from tests.embedded_pg import start_local_postgres

    server = start_local_postgres()
    try:
        yield server
    finally:
        server.stop()


async def _init(pg) -> None:
    db = Database(create_engine_from_url(pg.url()), name="audit", search_path=_SCHEMA)
    try:
        await init_audit_schema(db, org_id="erasure")
    finally:
        await db.close()


async def _erase(pg, user_id: str) -> None:
    """The UPDATE remove_account issues for one org's record."""
    db = Database(create_engine_from_url(pg.url()), name="audit", search_path=_SCHEMA)
    try:
        async with db.acquire() as conn:
            await conn.execute_core(
                update(query_audit_log)
                .where(query_audit_log.c.user_id == user_id)
                .values(user_id=tombstone_id(user_id))
            )
    finally:
        await db.close()


def _sql(pg, statement: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(_SCHEMA)))
        cur = conn.execute(sql.SQL(statement), params)  # pyright: ignore[reportArgumentType]
        return cur.fetchall() if cur.description else []


def _seed(pg) -> None:
    for user_id in ("bob", "carol"):
        _sql(
            pg,
            f"INSERT INTO query_audit_log ({_COLUMNS}) "
            "VALUES (%s, 'analyst', 'h', '[]', 'test', 200, 1, 'default')",
            (user_id,),
        )


def _log(pg) -> list[tuple]:
    return _sql(pg, "SELECT user_id, status_code FROM query_audit_log ORDER BY id")


def test_erasure_replaces_the_id_with_its_tombstone(pg):  # REQ-1312
    asyncio.run(_init(pg))
    _seed(pg)

    asyncio.run(_erase(pg, "bob"))

    assert _log(pg) == [(tombstone_id("bob"), 200), ("carol", 200)]


def test_a_schema_laid_out_under_the_unconditional_rule_gets_the_erasure_rule(pg):  # REQ-1312
    """An existing database: its rule predates erasure, and initializing its schema again (as a
    changed layout digest does on start) replaces it."""
    _sql(pg, f"CREATE SCHEMA {_SCHEMA}")
    _sql(
        pg,
        "CREATE TABLE query_audit_log (id BIGSERIAL PRIMARY KEY, tenant_id TEXT, "
        "user_id TEXT NOT NULL, role_id TEXT NOT NULL, query_hash TEXT NOT NULL, "
        "query_text_enc BYTEA, table_ids JSONB NOT NULL DEFAULT '[]', table_names JSONB NOT NULL, "
        "source TEXT NOT NULL, status_code INT NOT NULL, duration_ms INT NOT NULL, "
        "region TEXT NOT NULL, logged_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    _sql(pg, "CREATE RULE no_update_audit AS ON UPDATE TO query_audit_log DO INSTEAD NOTHING")
    _seed(pg)
    asyncio.run(_erase(pg, "bob"))
    assert _log(pg) == [("bob", 200), ("carol", 200)]  # the old rule swallows erasure

    asyncio.run(_init(pg))
    asyncio.run(_erase(pg, "bob"))

    assert _log(pg) == [(tombstone_id("bob"), 200), ("carol", 200)]


@pytest.mark.parametrize(
    ("statement", "params"),
    [
        ("UPDATE query_audit_log SET status_code = 500", ()),
        ("UPDATE query_audit_log SET user_id = 'eve' WHERE user_id = 'bob'", ()),
        # Someone else's tombstone would misattribute the entry.
        ("UPDATE query_audit_log SET user_id = %s WHERE user_id = 'carol'", ("bob",)),
        # The tombstone with any other change rides nothing through.
        (
            "UPDATE query_audit_log SET user_id = %s, status_code = 500 WHERE user_id = 'bob'",
            ("bob",),
        ),
        (
            "UPDATE query_audit_log SET user_id = %s, query_hash = 'x' WHERE user_id = 'bob'",
            ("bob",),
        ),
        ("DELETE FROM query_audit_log", ()),
    ],
)
def test_every_other_change_does_nothing(pg, statement, params):  # REQ-074, REQ-596
    asyncio.run(_init(pg))
    _seed(pg)

    _sql(pg, statement, tuple(tombstone_id(p) for p in params))

    assert _log(pg) == [("bob", 200), ("carol", 200)]
    assert _sql(pg, "SELECT query_hash FROM query_audit_log") == [("h",), ("h",)]


def test_a_tombstone_is_never_rewritten(pg):  # REQ-1312
    asyncio.run(_init(pg))
    _seed(pg)
    asyncio.run(_erase(pg, "bob"))

    asyncio.run(_erase(pg, tombstone_id("bob")))

    assert _log(pg) == [(tombstone_id("bob"), 200), ("carol", 200)]
