# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2d5b1f-7c4a-4a39-b6e0-3d9f1c2a7e85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL control plane on psycopg 3 against a real Postgres: control-plane statements are
prepared server-side once per connection and reused (never behind PgBouncer), NOTIFY payloads reach
their listener, and bulk_copy lands every value type through COPY."""

# Requirements: REQ-828, REQ-837, REQ-052, REQ-053, REQ-990

from __future__ import annotations

import asyncio
import os
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from provisa.core.database import Database, create_engine_from_url

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_URL = (
    f"postgresql+psycopg://{os.environ.get('PG_USER', 'provisa')}"
    f":{os.environ.get('PG_PASSWORD', 'provisa')}"
    f"@{os.environ.get('PG_HOST', 'localhost')}:{os.environ.get('PG_PORT', '5432')}"
    f"/{os.environ.get('PG_DATABASE', 'provisa')}"
)
_SCHEMA = "cp_psycopg_it"
_MARKED = "SELECT $1::int + 1 AS n /* cp-prepared-probe */"
_PREPARED = (
    "SELECT count(*), coalesce(sum(generic_plans + custom_plans), 0) FROM pg_prepared_statements "
    "WHERE statement LIKE '%cp-prepared-probe%' AND statement NOT LIKE '%pg_prepared_statements%'"
)


def _db(url: str = _URL) -> Database:
    # One pooled connection, so every acquire lands on the same server session.
    return Database(create_engine_from_url(url, pool_size=1, max_overflow=0), name="cp-it")


async def test_a_control_plane_statement_is_prepared_once_and_reused():
    db = _db()
    try:
        for i in range(3):
            async with db.acquire() as conn:
                assert await conn.fetchval(_MARKED, i) == i + 1
        async with db.acquire() as conn:
            prepared, executions = (await conn.fetch(_PREPARED))[0]
        assert prepared == 1  # one server-side prepared statement for the three calls
        assert executions == 3  # all three ran on it
    finally:
        await db.close()
        db.engine.dispose()


async def test_behind_pgbouncer_nothing_is_prepared():
    db = _db(_URL + "?use_pgbouncer=true")
    try:
        for i in range(3):
            async with db.acquire() as conn:
                assert await conn.fetchval(_MARKED, i) == i + 1
        async with db.acquire() as conn:
            prepared, _ = (await conn.fetch(_PREPARED))[0]
        assert prepared == 0
    finally:
        await db.close()
        db.engine.dispose()


async def test_a_notify_reaches_its_listener():
    db = _db()
    got: asyncio.Queue[str] = asyncio.Queue()

    def _on_notify(_db, _pid, _channel, payload):
        got.put_nowait(payload)

    try:
        await db.add_listener("cp_psycopg_it", _on_notify)
        async with db.acquire() as conn:
            await conn.execute("SELECT pg_notify('cp_psycopg_it', 'hello')")
        assert await asyncio.wait_for(got.get(), timeout=10) == "hello"
        await db.remove_listener("cp_psycopg_it", _on_notify)
    finally:
        await db.close()
        db.engine.dispose()


async def test_bulk_copy_lands_every_value_type():
    db = _db()
    md = sa.MetaData(schema=_SCHEMA)
    tbl = sa.Table(
        "copied",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("label", sa.Text),
        sa.Column("doc", JSONB),
        sa.Column("blob", sa.LargeBinary),
        sa.Column("amount", sa.Numeric(18, 2)),
        sa.Column("tags", ARRAY(sa.Text)),
        sa.Column("flag", sa.Boolean),
    )
    rows = [
        {
            "id": 1,
            "label": 'a "quoted", comma',
            "doc": {"k": [1, 2]},
            "blob": b"\x00\x01",
            "amount": Decimal("12.34"),
            "tags": ["x", "y z"],
            "flag": True,
        },
        {
            "id": 2,
            "label": "",
            "doc": None,
            "blob": None,
            "amount": None,
            "tags": [],
            "flag": False,
        },
        {"id": 3},
    ]
    try:
        async with db.acquire() as conn:
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
            await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        with db.engine.begin() as sc:
            md.create_all(sc)
        async with db.acquire() as conn:
            assert await conn.bulk_copy(tbl, rows) == 3
            got = await conn.fetch(
                f"SELECT id, label, doc, blob, amount, tags, flag FROM {_SCHEMA}.copied ORDER BY id"
            )
        assert [tuple(r) for r in got] == [
            (
                1,
                'a "quoted", comma',
                {"k": [1, 2]},
                b"\x00\x01",
                Decimal("12.34"),
                ["x", "y z"],
                True,
            ),
            (2, "", None, None, None, [], False),
            (3, None, None, None, None, None, None),
        ]
    finally:
        async with db.acquire() as conn:
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await db.close()
        db.engine.dispose()
