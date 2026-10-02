# Copyright (c) 2026 Kenneth Stott
# Canary: 22a8357a-4b66-49c5-b4d1-24943a83eaae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in the embedded DuckDB store is filled in a build table, a batch per
store operation, and swapped in atomically."""

import datetime as dt

import duckdb
import pytest

from provisa.federation.data_replicator import EngineCaps, Method, data_replicator
from provisa.federation.materialize_broker import get_broker, store_canary
from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_guard import ReplicaTargetError
from provisa.federation.replica_source import CursorSource
from provisa.federation.replica_target import DuckDBStoreTarget, build_table_name

SCHEMA = replica_schema("o1")
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

    def __init__(self):
        self.swaps = 0

    async def copy(self, prior_hash):
        raise AssertionError("this engine copies nothing")

    async def after_swap(self):
        self.swaps += 1


@pytest.fixture
def store(tmp_path):
    path = str(tmp_path / "materialize.duckdb")
    return path, get_broker(path)


def _target(broker):
    return DuckDBStoreTarget(broker, schema=SCHEMA, table=TABLE, columns=COLUMNS)


def _source(rows, per_fetch=250):
    async def row_batches(_batch_rows):
        for start in range(0, len(rows), per_fetch):
            yield rows[start : start + per_fetch]

    return CursorSource(row_batches, COLUMNS)


async def _noop(_rows_copied):
    return None


def _look(path, sql):
    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _count(path):
    return _look(path, f'SELECT count(*) FROM "{SCHEMA}"."{TABLE}"')[0][0]


async def test_rows_stream_into_a_build_table_and_are_swapped_in(store):
    path, broker = store
    engine = _ReadsStore()
    job = data_replicator(_source(_rows(1_000)), _target(broker), engine, batch_rows=200)
    outcome = await job.run(_noop)

    assert job.method is Method.STREAM_BATCHES and outcome.rows_copied == 1_000
    assert engine.swaps == 1 and _count(path) == 1_000
    name, amount, placed, is_open, doc = _look(
        path, f'SELECT name, amount, placed_at, open, doc FROM "{SCHEMA}"."{TABLE}" WHERE id = 3'
    )[0]
    assert name == 'a3 it\'s; "q"' and str(amount) in ("3.25", "3.250")
    assert placed == dt.datetime(2026, 1, 1, 0, 0, 3) and is_open is False
    assert '"k"' in str(doc)
    tables = _look(path, f"SELECT table_name FROM duckdb_tables() WHERE schema_name = '{SCHEMA}'")
    assert tables == [(TABLE,)]  # no build table left beside the replica


async def test_a_reader_sees_the_previous_replica_until_the_swap_and_keeps_its_copy(store):
    path, broker = store
    await data_replicator(_source(_rows(300)), _target(broker), _ReadsStore(), batch_rows=100).run(
        _noop
    )
    generation = store_canary(path)
    seen: list[tuple[int, int]] = []

    async def refreshing(_batch_rows):
        for start in range(0, 900, 300):
            seen.append((_count(path), store_canary(path)))
            yield _rows(300, start, tag="b")

    await data_replicator(
        CursorSource(refreshing, COLUMNS), _target(broker), _ReadsStore(), batch_rows=300
    ).run(_noop)
    # Never empty or part-filled, and a build's batches do not make readers drop their copies.
    assert seen == [(300, generation)] * 3
    assert _count(path) == 900 and store_canary(path) > generation


async def test_a_failed_build_leaves_the_previous_replica_and_a_dead_ones_table_is_dropped(store):
    path, broker = store
    await data_replicator(_source(_rows(300)), _target(broker), _ReadsStore(), batch_rows=100).run(
        _noop
    )

    async def dying(_batch_rows):
        yield _rows(100, tag="c")
        raise RuntimeError("source went away")

    with pytest.raises(RuntimeError, match="source went away"):
        await data_replicator(
            CursorSource(dying, COLUMNS), _target(broker), _ReadsStore(), batch_rows=100
        ).run(_noop)
    assert _count(path) == 300
    build = build_table_name(TABLE)
    exists = f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{build}'"
    assert _look(path, exists) == [(0,)]

    # A build whose process died after a batch: its build table is still in the file.
    dead = _target(broker)
    await dead.begin()
    batch = [b async for b in _source(_rows(10)).batches(10)][0]
    await dead.write(batch, [])
    assert _look(path, exists) == [(1,)] and _count(path) == 300
    await data_replicator(_source(_rows(50)), _target(broker), _ReadsStore(), batch_rows=100).run(
        _noop
    )
    assert _count(path) == 50 and _look(path, exists) == [(0,)]


async def test_a_view_at_the_replicas_name_is_refused(store):
    path, broker = store
    broker.execute(f'CREATE SCHEMA mat_store."{SCHEMA}"')
    broker.execute(f'CREATE VIEW mat_store."{SCHEMA}"."{TABLE}" AS SELECT 1 AS id')
    with pytest.raises(ReplicaTargetError):
        await data_replicator(
            _source(_rows(10)), _target(broker), _ReadsStore(), batch_rows=10
        ).run(_noop)
    assert _look(path, "SELECT count(*) FROM duckdb_tables()") == [(0,)]
