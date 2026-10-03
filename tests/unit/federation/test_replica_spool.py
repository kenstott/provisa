# Copyright (c) 2026 Kenneth Stott
# Canary: ee1a410b-62a7-4d3e-a27b-73d76db0e165
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a single-document source is spooled to a file on the node while its replica is
built: the file exists during the build and is gone after completion, after a failure and after
an abandoned build; the node's spool directory is bounded; leftovers of a crashed build go."""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx

from provisa.events import source_loader as sl
from provisa.federation.data_replicator import (
    NOT_OPTIMAL_READS,
    EngineCaps,
    Method,
    SourceRead,
    TargetCaps,
    TargetLoad,
    TargetWrite,
    choose_method,
)
from provisa.federation.replica_spool import (
    SPOOL_SUFFIX,
    AnswerNotJson,
    ReplicaSpoolFull,
    SpooledDocumentSource,
    sweep,
)

_N = 5000  # rows in a "large" answer: many chunks of body, many batches


class _Chunks(httpx.SyncByteStream):
    """A response body the fake server streams in pieces, with no declared length."""

    def __init__(self, body: bytes, size: int = 4096) -> None:
        self._body, self._size = body, size

    def __iter__(self):
        for start in range(0, len(self._body), self._size):
            yield self._body[start : start + self._size]


def _streamed(body: bytes, status: int = 200) -> httpx.Response:
    return httpx.Response(status, stream=_Chunks(body))


def _table(name: str, cols: tuple[str, ...]):
    return SimpleNamespace(
        schema_name="s",
        table_name=name,
        columns=[SimpleNamespace(name=c, native_filter_type=None) for c in cols],
    )


def _spool_files(directory: Path) -> list[Path]:
    return sorted(directory.glob(f"*{SPOOL_SUFFIX}"))


# -- one case per converted kind: (route, body, replica source, columns, first row) ------------


def _druid():
    body = json.dumps([{"id": i, "name": f"n{i}"} for i in range(_N)]).encode()
    route = respx.post("http://druid.test:8082/druid/v2/sql").mock(return_value=_streamed(body))
    source = SimpleNamespace(id="dr", host="druid.test", port=8082)
    reader = sl.make_druid_loader().replica_source(
        source, _table("events", ("id", "name")), [("id", "bigint"), ("name", "text")]
    )
    return route, reader, {"id": 0, "name": "n0"}


def _pinot():
    body = json.dumps(
        {
            "resultTable": {
                "dataSchema": {"columnNames": ["id", "name"], "columnDataTypes": ["INT", "STRING"]},
                "rows": [[i, f"n{i}"] for i in range(_N)],
            },
            "exceptions": [],
        }
    ).encode()
    route = respx.post("http://pinot.test:8099/query/sql").mock(return_value=_streamed(body))
    source = SimpleNamespace(
        id="pi",
        host="pinot.test",
        port=9000,
        federation_hints={"pinot_broker_url": "http://pinot.test:8099"},
    )
    reader = sl.make_pinot_loader().replica_source(
        source, _table("events", ("id", "name")), [("id", "bigint"), ("name", "text")]
    )
    return route, reader, {"id": 0, "name": "n0"}


def _prometheus():
    body = json.dumps(
        {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"job": f"j{s}"},
                        "values": [[1_700_000_000 + i, "1.5"] for i in range(50)],
                    }
                    for s in range(_N // 50)
                ],
            },
        }
    ).encode()
    route = respx.get("http://prom.test:9090/api/v1/query_range").mock(return_value=_streamed(body))
    source = SimpleNamespace(
        id="pr", host="prom.test", port=9090, password="", mapping={"url": "http://prom.test:9090"}
    )
    reader = sl.make_prometheus_loader().replica_source(
        source,
        _table("up", ("timestamp", "value", "job")),
        [("timestamp", "timestamp"), ("value", "double"), ("job", "text")],
    )
    return route, reader, None


def _graphql():
    body = json.dumps(
        {"data": {"orders": [{"id": i, "name": f"n{i}"} for i in range(_N)]}}
    ).encode()
    route = respx.post("http://gql.test/graphql").mock(return_value=_streamed(body))
    registrations = {
        "g": {
            "url": "http://gql.test/graphql",
            "auth": None,
            "tables": [
                {
                    "name": "orders",
                    "sql_name": "orders",
                    "columns": [{"name": "id"}, {"name": "name"}],
                }
            ],
        }
    }
    reader = sl.make_graphql_remote_loader(registrations).replica_source(
        SimpleNamespace(id="g"),
        _table("orders", ("id", "name")),
        [("id", "bigint"), ("name", "text")],
    )
    return route, reader, {"id": 0, "name": "n0"}


def _rss():
    items = "".join(
        f"<item><title>t{i}</title><link>http://x/{i}</link><guid>g{i}</guid>"
        "<pubDate>Fri, 02 Jan 2026 03:04:05 GMT</pubDate></item>"
        for i in range(_N)
    )
    body = (
        f'<?xml version="1.0"?><rss version="2.0"><channel><title>f</title>{items}</channel></rss>'
    )
    route = respx.get("http://feed.test:80/rss").mock(return_value=_streamed(body.encode()))
    source = SimpleNamespace(
        id="rs", host="feed.test", port=80, path="/rss", federation_hints={"use_ssl": "false"}
    )
    reader = sl.make_rss_loader().replica_source(
        source,
        _table("feed", ("title", "link", "id", "published")),
        [("title", "text"), ("link", "text"), ("id", "text"), ("published", "timestamp")],
    )
    return route, reader, None


_KINDS = {
    "druid": _druid,
    "pinot": _pinot,
    "prometheus": _prometheus,
    "graphql_remote": _graphql,
    "rss": _rss,
}


@pytest.fixture
def spool(tmp_path, monkeypatch):
    """The node's spool directory for the test, with the default bound."""
    from provisa.federation import replica_spool

    directory = tmp_path / "replica-spool"
    monkeypatch.setattr(replica_spool, "spool_directory", lambda: directory)
    monkeypatch.setattr(replica_spool, "spool_limit", lambda: 2 * 1024**3)
    return directory


@pytest.mark.parametrize("kind", sorted(_KINDS))
@respx.mock
async def test_the_answer_is_spooled_during_the_build_and_gone_after_it(kind, spool):
    route, reader, first = _KINDS[kind]()
    assert isinstance(reader, SpooledDocumentSource)
    assert reader.caps.reads == {SourceRead.SINGLE_DOCUMENT_SPOOLED}

    rows, during = 0, []
    async for batch in reader.batches(1000):
        assert batch.num_rows <= 1000
        if not rows and first is not None:
            assert batch.slice(0, 1).to_pylist()[0] == first
        rows += batch.num_rows
        files = _spool_files(spool)
        during.append(files)
        assert [f.stat().st_mode & 0o777 for f in files] == [0o600]  # one file, the node's only
    assert rows == _N
    assert len(during) == _N // 1000
    assert spool.stat().st_mode & 0o777 == 0o700
    assert _spool_files(spool) == []
    assert route.call_count == 1


@pytest.mark.parametrize("kind", sorted(_KINDS))
@respx.mock
async def test_an_abandoned_build_removes_its_spool_file(kind, spool):
    _route, reader, _first = _KINDS[kind]()
    stream = reader.batches(1000)
    await stream.__anext__()
    assert len(_spool_files(spool)) == 1
    await stream.aclose()
    assert _spool_files(spool) == []


@pytest.mark.parametrize("kind", sorted(_KINDS))
@respx.mock
async def test_a_failed_read_removes_its_spool_file(kind, spool):
    route, reader, _first = _KINDS[kind]()
    route.mock(return_value=_streamed(b'{"error": "down"}', status=503))
    with pytest.raises(httpx.HTTPStatusError):
        [b async for b in reader.batches(1000)]
    assert _spool_files(spool) == []


@respx.mock
async def test_an_answer_with_an_invalid_escape_fails_by_name_without_quoting_the_data(spool):
    """A remote GraphQL endpoint that writes a backslash-u not followed by four hex digits:
    the build's error names the table, the parser's reason and where in the answer, on one
    line, and quotes none of the document."""
    route, reader, _first = _graphql()
    rows = [{"id": i, "name": f"n{i}"} for i in range(_N)]
    body = json.dumps({"data": {"orders": rows}}).encode()
    route.mock(return_value=_streamed(body.replace(b'"n4000"', b'"C:\\users\\uZZ"')))
    with pytest.raises(AnswerNotJson) as failed:
        [b async for b in reader.batches(1000)]
    message = str(failed.value)
    assert message.startswith(
        "the answer read for the replica of g.orders is not valid JSON: lexical error: "
        "invalid (non-hex) character occurs after '\\u' inside string (in the 64 KiB before byte "
    )
    assert "\n" not in message and "uZZ" not in message and "n3999" not in message
    assert 0 < failed.value.before_byte <= len(body) + 16
    assert _spool_files(spool) == []


@respx.mock
async def test_a_cut_off_answer_fails_the_same_way(spool):
    route, reader, _first = _druid()
    route.mock(return_value=_streamed(b'[{"id": 1, "name": "a"}, {"id": 2, "na'))
    with pytest.raises(AnswerNotJson, match="replica of .* is not valid JSON: parse error: "):
        [b async for b in reader.batches(1000)]
    assert _spool_files(spool) == []


@respx.mock
async def test_an_answer_that_fails_in_its_own_terms_is_refused_and_removed(spool):
    route, reader, _first = _pinot()
    route.mock(return_value=_streamed(json.dumps({"exceptions": [{"message": "bad"}]}).encode()))
    with pytest.raises(ValueError, match="Pinot query .* failed"):
        [b async for b in reader.batches(1000)]
    route, reader, _first = _prometheus()
    route.mock(
        return_value=_streamed(
            json.dumps({"status": "error", "errorType": "bad_data", "error": "x"}).encode()
        )
    )
    with pytest.raises(ValueError, match="Prometheus API error: bad_data: x"):
        [b async for b in reader.batches(1000)]
    route, reader, _first = _druid()
    route.mock(return_value=_streamed(b'[{"id": 1}, {"id": '))  # the body ends mid-document
    with pytest.raises(Exception, match="(?i)incomplete|premature|parse"):
        [b async for b in reader.batches(1000)]
    assert _spool_files(spool) == []


def _bounded(directory: Path, limit: int, response: httpx.Response) -> SpooledDocumentSource:
    def rows(spooled):
        with httpx.Client() as client:
            with spooled(lambda: client.stream("GET", "http://doc.test/a")) as body:
                yield from json.load(body)

    respx.get("http://doc.test/a").mock(return_value=response)
    return SpooledDocumentSource(
        rows, [("id", "bigint")], table="src.big", directory=directory, limit=limit
    )


@respx.mock
async def test_a_declared_length_over_the_bound_is_refused_before_a_byte_is_written(tmp_path):
    body = json.dumps([{"id": i} for i in range(2000)]).encode()
    written: list[int] = []

    class _Watched(_Chunks):
        def __iter__(self):
            for chunk in super().__iter__():
                written.append(len(chunk))
                yield chunk

    response = httpx.Response(
        200, stream=_Watched(body), headers={"content-length": str(len(body))}
    )
    source = _bounded(tmp_path, 1000, response)
    with pytest.raises(ReplicaSpoolFull) as refused:
        [b async for b in source.batches(100)]
    assert (refused.value.table, refused.value.limit) == ("src.big", 1000)
    assert refused.value.needed == len(body)
    assert "replication.spool_max_bytes" in str(refused.value)
    assert written == []  # nothing was read from the source
    assert _spool_files(tmp_path) == []


@respx.mock
async def test_an_undeclared_length_is_refused_when_the_bound_is_reached(tmp_path):
    body = json.dumps([{"id": i} for i in range(2000)]).encode()
    source = _bounded(tmp_path, 10_000, _streamed(body))
    with pytest.raises(ReplicaSpoolFull) as refused:
        [b async for b in source.batches(100)]
    assert 10_000 < refused.value.needed <= len(body)  # refused at the chunk that passed it
    assert _spool_files(tmp_path) == []  # no partial file, and no rows from a truncated one


@respx.mock
async def test_the_bound_is_the_directory_total_not_one_build(tmp_path):
    other = tmp_path / f"other{SPOOL_SUFFIX}"
    other.write_bytes(b"x" * 9000)
    holder = os.open(other, os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)  # another running build's file: held, so not swept
    try:
        body = json.dumps([{"id": i} for i in range(200)]).encode()  # about 2.3 KB on its own
        source = _bounded(tmp_path, 10_000, _streamed(body))
        with pytest.raises(ReplicaSpoolFull):
            [b async for b in source.batches(100)]
        assert _spool_files(tmp_path) == [other]
    finally:
        os.close(holder)


def test_a_sweep_removes_what_a_crashed_build_left_and_nothing_a_build_holds(tmp_path):
    left = tmp_path / f"left{SPOOL_SUFFIX}"
    held = tmp_path / f"held{SPOOL_SUFFIX}"
    unrelated = tmp_path / "notes.txt"
    for path in (left, held, unrelated):
        path.write_bytes(b"x")
    holder = os.open(held, os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        assert sweep(tmp_path) == [left]
        assert sorted(p.name for p in tmp_path.iterdir()) == [held.name, unrelated.name]
    finally:
        os.close(holder)
    assert sweep(tmp_path) == [held]  # its holder is gone: it is a leftover now
    assert sweep(tmp_path / "absent") == []


@respx.mock
async def test_a_new_spool_first_removes_leftovers(tmp_path):
    left = tmp_path / f"left{SPOOL_SUFFIX}"
    left.write_bytes(b"x" * 9000)
    body = json.dumps([{"id": i} for i in range(200)]).encode()
    source = _bounded(tmp_path, 10_000, _streamed(body))  # fits only once the leftover is gone
    assert sum(b.num_rows for b in [b async for b in source.batches(100)]) == 200
    assert _spool_files(tmp_path) == []


def test_both_single_document_reads_are_streamed_and_marked_not_optimal():
    assert NOT_OPTIMAL_READS == {SourceRead.SINGLE_DOCUMENT, SourceRead.SINGLE_DOCUMENT_SPOOLED}
    target = TargetCaps(frozenset({TargetWrite.BULK_BATCH}), True, TargetLoad.BULK_STREAM)
    engine = EngineCaps(reaches_source=False, runs=frozenset())
    assert choose_method(SpooledDocumentSource.caps, target, engine) is Method.STREAM_BATCHES


def test_the_parser_is_the_c_backend_by_name():
    from provisa.federation.replica_spool import _json_parser

    assert _json_parser().__name__ == "ijson.backends.yajl2_c"


def test_a_kind_that_cannot_spool_stays_a_plain_single_document():
    # A checker's rows are one scan's results, produced by a subprocess: there is no answer to
    # spool. (An API table's own reader is covered in tests/unit/test_api_replica_read.py.)
    assert not hasattr(sl.make_dq_loader(SimpleNamespace()), "replica_source")


def test_the_spool_directory_is_under_the_nodes_data_directory_and_swept_at_start():
    import inspect

    from provisa.api import app_startup
    from provisa.federation.replica_spool import spool_directory, spool_limit

    assert spool_directory() == Path(os.environ["PROVISA_DATA_DIR"]) / "replica-spool"
    assert spool_limit() == 2 * 1024**3
    assert "_sweep_spool(spool_directory())" in inspect.getsource(
        app_startup._start_background_tasks
    )
