# Copyright (c) 2026 Kenneth Stott
# Canary: 72906e88-b252-479c-a2ee-14acc72b537c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: every source read for replication is a stream of bounded Arrow record batches, and
each kind of read declares what it is."""

from types import SimpleNamespace

import pyarrow as pa
import pytest

from provisa.events.source_loader import SourceRowLoader, UnsupportedSourceFetch
from provisa.federation.data_replicator import (
    EngineCaps,
    EngineRun,
    NoReplicationMethod,
    SourceRead,
    TargetWrite,
)
from provisa.federation.replica_address import ReplicaAddress
from provisa.federation.replica_parties import PgStatementCopy, StoreReadingEngine, store_target
from provisa.federation.replica_source import (
    CursorSource,
    DirectTableSource,
    DocumentSource,
    EngineTableSource,
)
from provisa.federation.replica_target import PostgresStoreTarget, build_table_name

COLUMNS = [("id", "bigint"), ("name", "text")]


async def _drain(source, batch_rows):
    return [b async for b in source.batches(batch_rows)]


class _Engine:
    """An engine runtime whose stream hands back two record batches."""

    def __init__(self):
        self.sql: list[str] = []
        self.closed = False

    def execute_engine_stream(self, sql):
        self.sql.append(sql)
        schema = pa.schema([("id", pa.int64())])
        outer = self

        class _Stream:
            def __iter__(self):
                yield pa.record_batch([pa.array(range(0, 5))], schema=schema)
                yield pa.record_batch([pa.array(range(5, 7))], schema=schema)

            def close(self):
                outer.closed = True

        return schema, _Stream()


async def test_the_engine_stream_is_cut_to_the_batch_bound_and_closed():
    engine = _Engine()
    source = EngineTableSource(engine, '"cat"."s"."t"', in_place=True)
    batches = await _drain(source, 2)
    assert [b.num_rows for b in batches] == [2, 2, 1, 2]
    assert [v for b in batches for v in b.column(0).to_pylist()] == list(range(7))
    assert engine.sql == ['SELECT * FROM "cat"."s"."t"'] and engine.closed
    assert source.caps.reads == {SourceRead.ARROW_STREAM, SourceRead.ENGINE_REACHABLE}
    assert EngineTableSource(engine, "x", in_place=False).caps.reads == {SourceRead.ARROW_STREAM}


async def test_a_driver_cursor_is_fetched_a_bounded_batch_at_a_time_and_closed():
    fetched: list[int] = []

    class _Stream:
        column_names = ["id", "name"]
        closed = False

        def __init__(self):
            self._rows = [(i, f"n{i}") for i in range(5)]

        async def fetch(self, size):
            fetched.append(size)
            chunk, self._rows = self._rows[:size], self._rows[size:]
            return chunk

        async def close(self):
            _Stream.closed = True

    async def open_stream():
        return _Stream()

    batches = await _drain(DirectTableSource(open_stream, COLUMNS), 2)
    assert [b.num_rows for b in batches] == [2, 2, 1]
    assert batches[0].to_pylist() == [{"id": 0, "name": "n0"}, {"id": 1, "name": "n1"}]
    assert set(fetched) == {2} and _Stream.closed
    assert DirectTableSource.caps.reads == {SourceRead.CURSOR}


async def test_a_document_is_held_whole_and_written_in_bounded_batches():
    async def load():
        return [{"id": i, "name": None} for i in range(5)]

    batches = await _drain(DocumentSource(load, COLUMNS), 2)
    assert [b.num_rows for b in batches] == [2, 2, 1]
    assert DocumentSource.caps.reads == {SourceRead.SINGLE_DOCUMENT}

    async def row_batches(batch_rows):
        yield [{"id": i, "name": "x"} for i in range(batch_rows + 1)]  # an adapter's larger page

    assert [b.num_rows for b in await _drain(CursorSource(row_batches, COLUMNS), 3)] == [3, 1]


def _state(*, streams=False):
    return SimpleNamespace(
        source_pools=SimpleNamespace(supports_stream=lambda _id: streams),
        source_dialects={"src": "postgres"},
    )


def _source(stype, **more):
    return SimpleNamespace(id="src", type=stype, **more)


TABLE = SimpleNamespace(schema_name="public", table_name="orders")


def test_each_source_type_is_given_its_own_kind_of_read(monkeypatch):
    from provisa.core import operator_floor
    from provisa.federation import strategy

    monkeypatch.setattr(strategy, "engine_attaches", lambda _engine, stype: stype == "postgresql")

    async def openapi_loader(source, table):
        return []

    loader = SourceRowLoader(_Engine(), adapter_loaders={"openapi": openapi_loader})
    monkeypatch.setattr(operator_floor, "floor_setting", lambda _source: None)

    attached = loader.replica_source(_state(), _source("postgresql"), TABLE, COLUMNS)
    assert isinstance(attached, EngineTableSource)
    assert SourceRead.ENGINE_REACHABLE in attached.caps.reads
    assert attached._ref == '"src"."public"."orders"'

    unattached = loader.replica_source(_state(), _source("mysql"), TABLE, COLUMNS)
    assert unattached.caps.reads == {SourceRead.ARROW_STREAM}

    document = loader.replica_source(_state(), _source("openapi"), TABLE, COLUMNS)
    assert isinstance(document, DocumentSource)

    with pytest.raises(UnsupportedSourceFetch, match="rss"):
        loader.replica_source(_state(), _source("rss"), TABLE, COLUMNS)

    # A source the operator floors is read by its own driver's cursor, never through the engine.
    monkeypatch.setattr(operator_floor, "floor_setting", lambda _source: "always")
    floored = loader.replica_source(_state(streams=True), _source("postgresql"), TABLE, COLUMNS)
    assert isinstance(floored, DirectTableSource)


def test_a_postgresql_store_has_one_write_face_and_other_stores_are_refused_by_name():
    address = ReplicaAddress("org_o1_replicas", "src__public__orders")
    target = store_target(
        "postgresql",
        "postgresql+psycopg://u:p@h:5432/db",
        address=address,
        columns=COLUMNS,
        pk_columns=["id"],
        engine_writes_store=False,
    )
    assert isinstance(target, PostgresStoreTarget)
    assert target.caps.writes == {TargetWrite.COPY_STREAM} and target.caps.atomic_swap
    assert target._dsn == "postgresql://u:p@h:5432/db"

    by_engine = store_target(
        "postgresql",
        "postgresql://h/db",
        address=address,
        columns=COLUMNS,
        pk_columns=[],
        engine_writes_store=True,
    )
    assert by_engine.caps.writes == {TargetWrite.COPY_STREAM, TargetWrite.STATEMENT_COPY}
    assert PostgresStoreTarget.caps.writes == {TargetWrite.COPY_STREAM}  # the class is untouched

    with pytest.raises(NoReplicationMethod, match="no replica write face for a 'snowflake' store"):
        store_target(
            "snowflake",
            "snowflake://x",
            address=address,
            columns=COLUMNS,
            pk_columns=[],
            engine_writes_store=False,
        )


def test_the_build_table_name_is_short_fixed_and_one_per_replica():
    long = "source_with_a_long_id__schema_with_a_long_name__table_with_a_long_name_too"
    assert build_table_name(long) == build_table_name(long)
    assert build_table_name(long) != build_table_name(long + "x")
    assert len(build_table_name(long)) == 31 and build_table_name(long).startswith("build__")


def test_the_engine_parties_declare_what_they_do():
    assert StoreReadingEngine.caps == EngineCaps(False, frozenset())
    assert PgStatementCopy.caps == EngineCaps(True, frozenset({EngineRun.STATEMENT}))


async def test_an_engine_that_only_reads_its_store_runs_its_own_step_after_a_swap():
    calls = []

    class _Backend:
        async def after_replica_swap(self, state):
            calls.append(state)

    engine = StoreReadingEngine(_Backend(), "state")
    await engine.after_swap()
    assert calls == ["state"]
    with pytest.raises(NoReplicationMethod):
        await engine.copy()
