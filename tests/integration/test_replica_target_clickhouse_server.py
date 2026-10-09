# Copyright (c) 2026 Kenneth Stott
# Canary: 8a25d995-1c87-4f14-a24f-2364a036eac1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: the ClickHouse replica target against a ClickHouse server over HTTP — the client's
Arrow insert, and the atomic exchange on the server's default database engine."""

# Requirements: REQ-1915

from __future__ import annotations

import datetime as dt
import os

import pytest

from provisa.federation.clickhouse_runtime import _ServerBackend
from provisa.federation.data_replicator import EngineCaps, data_replicator
from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_source import CursorSource
from provisa.federation.replica_target import ClickHouseStoreTarget

pytestmark = [pytest.mark.integration, pytest.mark.requires_clickhouse]

SCHEMA = replica_schema("chswap")
TABLE = "src__public__orders"
COLUMNS = [
    ("id", "bigint"),
    ("name", "text"),
    ("amount", "numeric"),
    ("placed_at", "timestamp"),
    ("open", "boolean"),
    ("doc", "json"),
]


def _rows(n, start=0, tag="a"):
    return [
        {
            "id": i,
            "name": f'{tag}{i} it\'s; "q"',
            "amount": f"{i}.25",
            "placed_at": dt.datetime(2026, 1, 1) + dt.timedelta(seconds=i),
            "open": i % 2 == 0,
            "doc": {"k": i} if i % 3 == 0 else None,
        }
        for i in range(start, start + n)
    ]


class _ReadsStore:
    caps = EngineCaps(False, frozenset())

    async def copy(self, prior_hash):
        raise AssertionError("this engine copies nothing")

    async def after_swap(self):
        return None


async def _inline(fn):
    return fn()


async def _noop(_rows_copied):
    return None


@pytest.fixture
def server():
    backend = _ServerBackend(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    backend.command(f'DROP DATABASE IF EXISTS "{SCHEMA}"')
    try:
        yield backend
    finally:
        backend.command(f'DROP DATABASE IF EXISTS "{SCHEMA}"')
        backend.close()


def _target(backend):
    return ClickHouseStoreTarget(
        backend, _inline, schema=SCHEMA, table=TABLE, columns=COLUMNS, pk_columns=["id"]
    )


def _count(backend) -> int:
    return int(backend.query(f'SELECT count() FROM "{SCHEMA}"."{TABLE}"')[0][0][0])


async def test_arrow_batches_are_inserted_and_exchanged_in_atomically_on_a_server(server):
    first = _target(server)
    # The server's default engine for a plain CREATE DATABASE is the one EXCHANGE needs.
    assert first.database_engine == "Atomic" and first.caps.atomic_swap

    async def source(rows):
        async def row_batches(_batch_rows):
            for start in range(0, len(rows), 250):
                yield rows[start : start + 250]

        return CursorSource(row_batches, COLUMNS)

    await data_replicator(await source(_rows(600)), first, _ReadsStore(), batch_rows=200).run(_noop)
    assert _count(server) == 600
    row, _ = server.query(
        f'SELECT name, toString(amount), open, doc FROM "{SCHEMA}"."{TABLE}" WHERE id = 3'
    )
    assert row[0][0] == 'a3 it\'s; "q"' and row[0][1].startswith("3.25")
    assert row[0][2] in (False, 0) and '"k"' in row[0][3]

    seen: list[int] = []

    async def refreshing(_batch_rows):
        for start in range(0, 900, 300):
            seen.append(_count(server))
            yield _rows(300, start, tag="b")

    await data_replicator(
        CursorSource(refreshing, COLUMNS), _target(server), _ReadsStore(), batch_rows=300
    ).run(_noop)
    assert seen == [600, 600, 600]  # the previous replica until the exchange
    assert _count(server) == 900
    tables, _ = server.query(f"SELECT name FROM system.tables WHERE database = '{SCHEMA}'")
    assert [str(t[0]) for t in tables] == [TABLE]


def test_two_statements_at_once_on_one_runtime_both_answer(server):
    """Every request runs on its own thread and the engine's runtime holds one client. With a
    server session named, the second of two statements in flight was refused
    ("Session ... is locked by a concurrent client", SESSION_IS_LOCKED)."""
    from concurrent.futures import ThreadPoolExecutor

    def _slow(n: int) -> int:
        rows, _ = server.query(f"SELECT {n} + sleep(1)")
        return int(rows[0][0])

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sorted(pool.map(_slow, range(4))) == [0, 1, 2, 3]


def test_a_statement_sent_the_moment_a_stream_is_read_answers(server):
    """The org-delete case's shape: a lazy stream's rows are read and the next statement follows
    at once, before the server has finished with the first."""
    for _ in range(50):
        _schema, batches = server.query_arrow_stream("SELECT number FROM system.numbers LIMIT 3")
        assert sum(b.num_rows for b in batches) == 3
        rows, _ = server.query("SELECT 1")
        assert rows == [(1,)]
