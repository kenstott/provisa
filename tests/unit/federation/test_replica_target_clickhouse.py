# Copyright (c) 2026 Kenneth Stott
# Canary: 4f602c71-d5d9-4b32-b8fb-77c22bce4da2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in a ClickHouse store is filled from Arrow batches in a build table and
exchanged with the replica in one atomic statement. Run against the embedded ClickHouse engine."""

import datetime as dt
import decimal
import uuid

import pyarrow as pa
import pytest

from provisa.federation.clickhouse_runtime import _EmbeddedBackend, _NativeBackend
from provisa.federation.clickhouse_store import native_column
from provisa.federation.data_replicator import (
    EngineCaps,
    Method,
    NoReplicationMethod,
    data_replicator,
)
from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_guard import ReplicaTargetError
from provisa.federation.replica_source import CursorSource
from provisa.federation.replica_target import ClickHouseStoreTarget, build_table_name

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


async def _inline(fn):
    return fn()


@pytest.fixture(scope="module")
def backend():
    engine = _EmbeddedBackend()
    yield engine
    engine.close()


@pytest.fixture
def clean(backend):
    backend.command(f'DROP DATABASE IF EXISTS "{SCHEMA}"')
    yield backend
    backend.command(f'DROP DATABASE IF EXISTS "{SCHEMA}"')


def _target(backend, pk=("id",)):
    return ClickHouseStoreTarget(
        backend, _inline, schema=SCHEMA, table=TABLE, columns=COLUMNS, pk_columns=list(pk)
    )


def _source(rows, per_fetch=250):
    async def row_batches(_batch_rows):
        for start in range(0, len(rows), per_fetch):
            yield rows[start : start + per_fetch]

    return CursorSource(row_batches, COLUMNS)


async def _noop(_rows_copied):
    return None


def _count(backend) -> int:
    return int(backend.query(f'SELECT count() FROM "{SCHEMA}"."{TABLE}"')[0][0][0])


def _tables(backend) -> list[str]:
    rows, _ = backend.query(f"SELECT name FROM system.tables WHERE database = '{SCHEMA}'")
    return sorted(str(r[0]) for r in rows)


async def test_batches_are_inserted_into_a_build_table_and_exchanged_in(clean):
    target = _target(clean)
    assert target.database_engine == "Atomic" and target.caps.atomic_swap
    engine = _ReadsStore()
    job = data_replicator(_source(_rows(1_000)), target, engine, batch_rows=200)
    outcome = await job.run(_noop)

    assert job.method is Method.STREAM_BATCHES and outcome.rows_copied == 1_000
    assert engine.swaps == 1 and _count(clean) == 1_000
    rows, _ = clean.query(
        f'SELECT name, toString(amount), open, doc FROM "{SCHEMA}"."{TABLE}" WHERE id = 3'
    )
    assert rows[0][0] == 'a3 it\'s; "q"' and rows[0][1].startswith("3.25")
    assert rows[0][2] in (False, 0) and '"k"' in rows[0][3]
    nulls, _ = clean.query(f'SELECT count() FROM "{SCHEMA}"."{TABLE}" WHERE doc IS NULL')
    assert int(nulls[0][0]) == 666  # a NULL stays NULL
    assert _tables(clean) == [TABLE]  # first build: renamed into place, nothing left beside it


async def test_a_reader_sees_the_previous_replica_until_the_exchange(clean):
    await data_replicator(_source(_rows(300)), _target(clean), _ReadsStore(), batch_rows=100).run(
        _noop
    )
    seen: list[int] = []

    async def refreshing(_batch_rows):
        for start in range(0, 900, 300):
            seen.append(_count(clean))
            yield _rows(300, start, tag="b")

    await data_replicator(
        CursorSource(refreshing, COLUMNS), _target(clean), _ReadsStore(), batch_rows=300
    ).run(_noop)
    assert seen == [300, 300, 300]
    assert _count(clean) == 900
    assert _tables(clean) == [TABLE]  # the exchanged-out previous rows were dropped


async def test_a_failed_build_leaves_the_previous_replica_and_a_dead_ones_table_is_dropped(clean):
    await data_replicator(_source(_rows(300)), _target(clean), _ReadsStore(), batch_rows=100).run(
        _noop
    )

    async def dying(_batch_rows):
        yield _rows(100, tag="c")
        raise RuntimeError("source went away")

    with pytest.raises(RuntimeError, match="source went away"):
        await data_replicator(
            CursorSource(dying, COLUMNS), _target(clean), _ReadsStore(), batch_rows=100
        ).run(_noop)
    assert _count(clean) == 300 and _tables(clean) == [TABLE]

    dead = _target(clean)  # a build whose process died after a batch
    await dead.begin()
    await dead.write([b async for b in _source(_rows(10)).batches(10)][0], [])
    assert _tables(clean) == sorted([TABLE, build_table_name(TABLE)])
    await data_replicator(_source(_rows(50)), _target(clean), _ReadsStore(), batch_rows=100).run(
        _noop
    )
    assert _count(clean) == 50 and _tables(clean) == [TABLE]


async def test_a_database_that_cannot_exchange_atomically_is_refused_before_any_read(clean):
    clean.command(f'CREATE DATABASE "{SCHEMA}" ENGINE = Memory')
    target = _target(clean)
    assert target.database_engine == "Memory" and not target.caps.atomic_swap

    async def never(_batch_rows):
        raise AssertionError("no row is read for a store that cannot swap")
        yield []

    with pytest.raises(NoReplicationMethod, match="cannot swap a finished table in atomically"):
        data_replicator(CursorSource(never, COLUMNS), target, _ReadsStore(), batch_rows=10)


async def test_a_view_at_the_replicas_name_is_refused(clean):
    clean.command(f'CREATE DATABASE "{SCHEMA}"')
    clean.command(f'CREATE VIEW "{SCHEMA}"."{TABLE}" AS SELECT 1 AS id')
    with pytest.raises(ReplicaTargetError):
        await data_replicator(_source(_rows(10)), _target(clean), _ReadsStore(), batch_rows=10).run(
            _noop
        )
    assert _tables(clean) == [TABLE]  # only the view; no build table was made


def test_the_native_protocol_takes_a_batch_as_one_columnar_block():
    calls = []

    class _Client:
        def execute(self, sql, data, columnar):
            calls.append((sql, data, columnar))

    native = _NativeBackend.__new__(_NativeBackend)
    native._client = _Client()
    columns = [("id", "bigint"), ("amount", "numeric"), ("ref", "uuid")]
    ref = "12345678-1234-5678-1234-567812345678"
    batch = pa.record_batch(
        {"id": pa.array([1, 2]), "amount": pa.array(["1.50", None]), "ref": pa.array([ref, None])}
    )
    native.insert_arrow("db", "t", columns, batch)
    assert calls == [
        (
            'INSERT INTO "db"."t" ("id", "amount", "ref") VALUES',
            [[1, 2], [decimal.Decimal("1.50"), None], [uuid.UUID(ref), None]],
            True,
        )
    ]
    assert native_column(["x", None], "text") == ["x", None]
