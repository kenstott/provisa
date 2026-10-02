# Copyright (c) 2026 Kenneth Stott
# Canary: 0bfebb39-0768-4c5e-a0ac-08fec0108d30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in a PostgreSQL store is written by one COPY held open for the build,
into a build table that is swapped in atomically. A reader sees the previous replica until the
swap; a build that dies leaves it intact.
"""

# Requirements: REQ-1915, REQ-1912

from __future__ import annotations

import datetime as dt
import os
import uuid

import psycopg
import pytest
import sqlalchemy as sa

from provisa.federation.data_replicator import EngineCaps, Method, data_replicator
from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_guard import ReplicaTargetError
from provisa.federation.replica_source import CursorSource
from provisa.federation.replica_target import PostgresStoreTarget, build_table_name

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_USER = os.environ.get("PG_USER", "provisa")
_PG_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_BASE = f"postgresql+psycopg://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}"
_ADMIN_URL = f"{_BASE}/{os.environ.get('PG_DATABASE', 'provisa')}"

SCHEMA = replica_schema("o1")
TABLE = "src__public__orders"
COLUMNS = [
    ("id", "bigint"),
    ("name", "text"),
    ("amount", "numeric"),
    ("placed_at", "timestamp"),
    ("open", "boolean"),
    ("doc", "json"),
    ("wait", "interval"),
]
ODD = 'tab\there, "quoted", back\\slash\nnewline; and \\N'


def _rows(n, start=0, tag="a"):
    return [
        {
            "id": i,
            "name": ODD if i == start else f"{tag}{i}",
            "amount": f"{i}.25",
            "placed_at": dt.datetime(2026, 1, 1, 12, 0, 0) + dt.timedelta(seconds=i),
            "open": i % 2 == 0,
            "doc": {"k": [i, "v'"]} if i % 3 == 0 else None,
            "wait": dt.timedelta(days=1, seconds=i),
        }
        for i in range(start, start + n)
    ]


@pytest.fixture
def store():
    """A fresh store database; yields its DSN and a plain connection for looking at it."""
    name = f"replica_copy_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(_ADMIN_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    dsn = f"{_BASE}/{name}"
    look = psycopg.connect(dsn.replace("+psycopg", ""), autocommit=True)
    try:
        yield dsn, look
    finally:
        look.close()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


class _ReadsStore:
    caps = EngineCaps(False, frozenset())

    def __init__(self):
        self.swaps = 0

    async def copy(self):
        raise AssertionError("this engine copies nothing")

    async def after_swap(self):
        self.swaps += 1


def _target(dsn, pk=("id",)):
    return PostgresStoreTarget(
        dsn, schema=SCHEMA, table=TABLE, columns=COLUMNS, pk_columns=list(pk)
    )


def _source(rows, per_fetch=250):
    async def row_batches(_batch_rows):
        for start in range(0, len(rows), per_fetch):
            yield rows[start : start + per_fetch]

    return CursorSource(row_batches, COLUMNS)


async def _noop(_rows_copied):
    return None


def _count(look) -> int:
    return look.execute(f'SELECT count(*) FROM "{SCHEMA}"."{TABLE}"').fetchone()[0]


async def test_rows_stream_into_a_build_table_and_are_swapped_in_with_their_key(store):
    dsn, look = store
    engine = _ReadsStore()
    job = data_replicator(_source(_rows(1_000)), _target(dsn), engine, batch_rows=200)
    outcome = await job.run(_noop)

    assert job.method is Method.STREAM_BATCHES and outcome.rows_copied == 1_000
    assert engine.swaps == 1
    assert _count(look) == 1_000
    first = look.execute(
        f'SELECT name, amount, placed_at, open, doc, wait FROM "{SCHEMA}"."{TABLE}" WHERE id = 0'
    ).fetchone()
    assert first[5] == dt.timedelta(days=1)
    assert first[0] == ODD  # every character arrived as data
    assert str(first[1]) == "0.25" and first[2] == dt.datetime(2026, 1, 1, 12, 0, 0)
    assert first[3] is True and first[4] == {"k": [0, "v'"]}
    assert look.execute(
        f'SELECT doc IS NULL, name FROM "{SCHEMA}"."{TABLE}" WHERE id = 1'
    ).fetchone() == (True, "a1")
    key = look.execute(
        "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
        "AND a.attnum = ANY(i.indkey) WHERE i.indisprimary AND i.indrelid = %s::regclass",
        (f'"{SCHEMA}"."{TABLE}"',),
    ).fetchall()
    assert key == [("id",)]
    # Nothing is left beside the replica.
    tables = look.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s", (SCHEMA,)
    ).fetchall()
    assert tables == [(TABLE,)]


async def test_a_reader_sees_the_previous_replica_until_the_swap(store):
    dsn, look = store
    await data_replicator(_source(_rows(300)), _target(dsn), _ReadsStore(), batch_rows=100).run(
        _noop
    )
    seen_during: list[int] = []

    async def refreshing(_batch_rows):
        for start in range(0, 900, 300):
            seen_during.append(_count(look))  # another session, while the build is open
            yield _rows(300, start, tag="b")

    job = data_replicator(
        CursorSource(refreshing, COLUMNS), _target(dsn), _ReadsStore(), batch_rows=300
    )
    await job.run(_noop)
    assert seen_during == [300, 300, 300]  # never empty, never part-filled
    assert _count(look) == 900
    assert look.execute(f'SELECT name FROM "{SCHEMA}"."{TABLE}" WHERE id = 5').fetchone() == ("b5",)


async def test_a_build_that_dies_leaves_the_previous_replica_and_the_next_one_succeeds(store):
    dsn, look = store
    await data_replicator(_source(_rows(300)), _target(dsn), _ReadsStore(), batch_rows=100).run(
        _noop
    )

    async def dying(_batch_rows):
        yield _rows(100, tag="c")
        raise RuntimeError("source went away")

    with pytest.raises(RuntimeError, match="source went away"):
        await data_replicator(
            CursorSource(dying, COLUMNS), _target(dsn), _ReadsStore(), batch_rows=100
        ).run(_noop)
    assert _count(look) == 300
    assert look.execute(f'SELECT name FROM "{SCHEMA}"."{TABLE}" WHERE id = 5').fetchone() == ("a5",)

    # A build whose process dies mid-copy: its session ends, and the server discards the build.
    target = _target(dsn)
    await target.begin()
    target._copy.write_row([1, "x", "1", None, True, None, None])
    pid = target._conn.info.backend_pid
    look.execute("SELECT pg_terminate_backend(%s)", (pid,))
    await target.abort()  # what is left of the dead build in this process is released
    assert _count(look) == 300
    leftover = look.execute(
        "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s",
        (SCHEMA, build_table_name(TABLE)),
    ).fetchone()[0]
    assert leftover == 0

    await data_replicator(_source(_rows(50)), _target(dsn), _ReadsStore(), batch_rows=100).run(
        _noop
    )
    assert _count(look) == 50


async def test_an_unchanged_refresh_leaves_the_replica_table_as_it_was(store):
    dsn, look = store
    built = await data_replicator(
        _source(_rows(200)), _target(dsn), _ReadsStore(), batch_rows=100
    ).run(_noop)
    oid = look.execute("SELECT %s::regclass::oid", (f'"{SCHEMA}"."{TABLE}"',)).fetchone()[0]
    engine = _ReadsStore()
    again = await data_replicator(
        _source(list(reversed(_rows(200)))),
        _target(dsn),
        engine,
        batch_rows=100,
        prior_hash=built.content_hash,
    ).run(_noop)
    assert again.changed is False and engine.swaps == 0
    assert look.execute("SELECT %s::regclass::oid", (f'"{SCHEMA}"."{TABLE}"',)).fetchone()[0] == oid


async def test_a_view_at_the_replicas_name_is_refused_and_nothing_is_written(store):
    dsn, look = store
    look.execute(f'CREATE SCHEMA "{SCHEMA}"')
    look.execute(f'CREATE VIEW "{SCHEMA}"."{TABLE}" AS SELECT 1 AS id')
    target = _target(dsn)
    with pytest.raises(ReplicaTargetError):
        await data_replicator(_source(_rows(10)), target, _ReadsStore(), batch_rows=10).run(_noop)
    assert look.execute(
        "SELECT count(*) FROM pg_tables WHERE schemaname = %s", (SCHEMA,)
    ).fetchone() == (0,)
