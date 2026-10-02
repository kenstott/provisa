# Copyright (c) 2026 Kenneth Stott
# Canary: 9f16b9a4-d69c-46a7-85e6-ed13fba0d084
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: an adapter whose client has a cursor is read for replication through that cursor,
a bounded batch at a time — not as one list of every row."""

import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import fakeredis
import httpx
import pyarrow as pa
import respx

from provisa.events import source_loader as sl
from provisa.federation.data_replicator import SourceRead
from provisa.federation.replica_source import (
    ArrowStreamSource,
    BlockingCursorSource,
    CursorSource,
    DocumentSource,
)


def _table(schema="s", name="t", cols=("id", "name")):
    return SimpleNamespace(
        schema_name=schema,
        table_name=name,
        columns=[SimpleNamespace(name=c, native_filter_type=None) for c in cols],
    )


async def _batches(source, batch_rows):
    return [b.to_pylist() async for b in source.batches(batch_rows)]


async def test_sqlite_is_read_from_its_cursor_in_bounded_batches(tmp_path):
    path = tmp_path / "s.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    con.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(5)])
    con.commit()
    con.close()
    source = SimpleNamespace(id="lite", path=str(path))
    reader = sl.make_sqlite_loader().replica_source(
        source, _table(), [("id", "bigint"), ("name", "text")]
    )
    assert isinstance(reader, BlockingCursorSource)
    assert reader.caps.reads == {SourceRead.CURSOR}
    got = await _batches(reader, 2)
    assert [len(b) for b in got] == [2, 2, 1]
    assert got[0][0] == {"id": 0, "name": "n0"}


async def test_cassandra_pages_at_the_batch_bound(monkeypatch):
    from provisa.cassandra import fetch as cf

    statements = []

    class _Session:
        def execute(self, stmt):
            statements.append((stmt.query_string, stmt.fetch_size))
            return iter(SimpleNamespace(id=i, name=f"n{i}") for i in range(5))

    @contextmanager
    def _cluster(self):
        yield SimpleNamespace(connect=lambda: _Session())

    monkeypatch.setattr(cf.CassandraConnection, "cluster", _cluster)
    source = SimpleNamespace(id="c", host="localhost", port=9042, username="", password="")
    reader = sl.make_cassandra_loader().replica_source(
        source, _table("ks", "ev"), [("id", "bigint"), ("name", "text")]
    )
    got = await _batches(reader, 2)
    assert [len(b) for b in got] == [2, 2, 1]
    assert statements == [('SELECT "id", "name" FROM "ks"."ev"', 2)]


async def test_mongodb_reads_off_one_cursor_fetching_a_batch_per_round_trip(monkeypatch):
    from provisa.mongodb import fetch as mf

    asked = []

    class _Cursor:
        def __init__(self, docs):
            self._docs = docs

        def batch_size(self, n):
            asked.append(n)
            return self

        def __iter__(self):
            return iter(self._docs)

    class _Collection:
        def find(self, _filter, projection):
            asked.append(projection)
            return _Cursor([{"id": i, "name": f"n{i}"} for i in range(5)])

    @contextmanager
    def _client(self):
        yield {"db": {"coll": _Collection()}}

    monkeypatch.setattr(mf.MongoConnection, "client", _client)
    source = SimpleNamespace(
        id="m", host="localhost", port=27017, username="", password="", database="db"
    )
    reader = sl.make_mongodb_loader().replica_source(
        source, _table("db", "coll"), [("id", "bigint"), ("name", "text")]
    )
    got = await _batches(reader, 2)
    assert [len(b) for b in got] == [2, 2, 1]
    assert asked == [{"id": 1, "name": 1, "_id": 0}, 2]


async def test_redis_scans_its_keys_in_bounded_batches(monkeypatch):
    from provisa.redis import fetch as rf

    srv = fakeredis.FakeServer()
    monkeypatch.setattr(
        rf.RedisConnection,
        "client",
        lambda self: fakeredis.FakeRedis(server=srv, decode_responses=True),
    )
    c = fakeredis.FakeRedis(server=srv, decode_responses=True)
    for i in range(3):
        c.hset(f"agent:{i}", mapping={"name": f"n{i}"})
    c.set("agent:plain", "x")  # a string under a hash table's prefix: not a row of it
    source = SimpleNamespace(id="r", host="h", port=1, password="", mapping={})
    reader = sl.make_redis_loader().replica_source(
        source, _table("default", "agent", ("key", "name")), [("key", "text"), ("name", "text")]
    )
    got = await _batches(reader, 2)
    assert [len(b) for b in got] == [2, 1]
    assert sorted(r["key"] for b in got for r in b) == ["agent:0", "agent:1", "agent:2"]


async def test_kafka_yields_one_poll_at_a_time(monkeypatch):
    import aiokafka

    class _Consumer:
        def __init__(self, topic, **kw):
            self._polls = [[b'{"id": 1, "name": "a"}', b'{"id": 2, "name": "b"}'], [b'{"id": 3}']]
            self.stopped = False
            _Consumer.last = self

        async def start(self):
            return None

        async def stop(self):
            self.stopped = True

        async def getmany(self, timeout_ms=0):
            if not self._polls:
                return {}
            return {("tp", 0): [SimpleNamespace(value=v) for v in self._polls.pop(0)]}

    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", _Consumer)
    source = SimpleNamespace(id="k", host="h", port=9092)
    reader = sl.make_kafka_loader().replica_source(
        source, _table(name="topic"), [("id", "bigint"), ("name", "text")]
    )
    assert isinstance(reader, CursorSource)
    got = await _batches(reader, 100)
    assert got == [
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        [{"id": 3, "name": None}],
    ]
    assert _Consumer.last.stopped


@respx.mock
async def test_elasticsearch_scrolls_a_page_of_the_batch_size_at_a_time():
    base = "http://es.test:9200"
    respx.get(f"{base}/tickets/_mapping").mock(
        return_value=httpx.Response(
            200,
            json={"tickets": {"mappings": {"properties": {"ticket_id": {"type": "keyword"}}}}},
        )
    )
    first = respx.post(f"{base}/tickets/_search").mock(
        return_value=httpx.Response(
            200, json={"_scroll_id": "s", "hits": {"hits": [{"_source": {"ticket_id": "T-1"}}]}}
        )
    )
    respx.post(f"{base}/_search/scroll").mock(
        side_effect=[
            httpx.Response(
                200, json={"_scroll_id": "s", "hits": {"hits": [{"_source": {"ticket_id": "T-2"}}]}}
            ),
            httpx.Response(200, json={"_scroll_id": "s", "hits": {"hits": []}}),
        ]
    )
    released = respx.delete(f"{base}/_search/scroll").mock(return_value=httpx.Response(200))
    source = SimpleNamespace(
        id="es", host="es.test", port=9200, username="", password="", mapping={}
    )
    reader = sl.make_elasticsearch_loader().replica_source(
        source, _table(name="tickets", cols=("ticket_id",)), [("ticket_id", "text")]
    )
    got = await _batches(reader, 1)
    assert got == [[{"ticket_id": "T-1"}], [{"ticket_id": "T-2"}]]
    assert b'"size":1' in first.calls[0].request.content.replace(b" ", b"")
    assert released.called


async def test_a_clickhouse_source_streams_arrow_batches_and_closes_its_driver(monkeypatch):
    events = []

    class _Driver:
        def configure(self, extra):
            events.append("configure")

        async def connect(self, *args):
            events.append("connect")

        def iter_arrow_batches(self, sql):
            events.append(sql)
            yield pa.record_batch({"id": pa.array([1, 2, 3])})
            yield pa.record_batch({"id": pa.array([4])})

        async def close(self):
            events.append("close")

    from provisa.executor.drivers import clickhouse as ch_driver

    monkeypatch.setattr(ch_driver, "ClickHouseDriver", _Driver)
    source = SimpleNamespace(
        id="ch", host="h", port=8123, username="u", password="", database="d", federation_hints={}
    )
    reader = sl.make_clickhouse_loader().replica_source(
        source, _table(name="t"), [("id", "bigint")]
    )
    assert isinstance(reader, ArrowStreamSource)
    assert reader.caps.reads == {SourceRead.ARROW_STREAM}
    got = await _batches(reader, 2)
    assert [[r["id"] for r in b] for b in got] == [[1, 2], [3], [4]]
    assert events == ["configure", "connect", 'SELECT "id" FROM "t"', "close"]


async def test_a_blocking_cursor_is_closed_when_the_reader_stops_early():
    closed = []

    def row_batches(batch_rows):
        try:
            for start in range(0, 100, batch_rows):
                yield [{"id": i, "name": None} for i in range(start, start + batch_rows)]
        finally:
            closed.append(True)

    stream = BlockingCursorSource(row_batches, [("id", "bigint"), ("name", "text")]).batches(10)
    assert (await stream.__anext__()).num_rows == 10
    await stream.aclose()
    assert closed == [True]


def test_an_adapter_without_a_cursor_is_read_as_a_single_document(monkeypatch):
    from provisa.core import operator_floor

    monkeypatch.setattr(operator_floor, "floor_setting", lambda _source: None)

    async def rss_loader(source, table):
        return []

    loader = sl.SourceRowLoader(
        object(), adapter_loaders={"rss": rss_loader, "sqlite": sl.make_sqlite_loader()}
    )
    state = SimpleNamespace(source_pools=None)
    document = loader.replica_source(state, SimpleNamespace(id="f", type="rss"), _table(), [])
    assert isinstance(document, DocumentSource)
    cursor = loader.replica_source(
        state, SimpleNamespace(id="l", type="sqlite", path="/x.db"), _table(), [("id", "bigint")]
    )
    assert isinstance(cursor, BlockingCursorSource)
