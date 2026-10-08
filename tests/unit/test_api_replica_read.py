# Copyright (c) 2026 Kenneth Stott
# Canary: f674080f-d5ec-43dc-9fb5-cec809a851e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: the whole collection of an API table is read for a replica build a page or a row
at a time — a paged endpoint one page per round trip, a one-document endpoint through a spool
file — and only an answer that has to be understood whole is read into memory."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx

from provisa.api_source import replica_read
from provisa.api_source.caller import ApiCallError, ApiNotFoundError
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint
from provisa.core.paging import PaginationConfig
from provisa.events import source_loader as sl
from provisa.federation.data_replicator import NOT_OPTIMAL_READS, SourceRead
from provisa.federation.replica_source import CursorSource, DocumentSource
from provisa.federation.replica_spool import SPOOL_SUFFIX, SpooledDocumentSource

_N = 5000
COLUMNS = [("id", "bigint"), ("name", "text")]
BASE = "http://api.test"


class _Chunks(httpx.SyncByteStream):
    """A response body the fake server streams in pieces, with no declared length."""

    def __init__(self, body: bytes, size: int = 4096) -> None:
        self._body, self._size = body, size

    def __iter__(self):
        for start in range(0, len(self._body), self._size):
            yield self._body[start : start + self._size]


def _streamed(body: object, status: int = 200) -> httpx.Response:
    return httpx.Response(status, stream=_Chunks(json.dumps(body).encode()))


def _endpoint(**kw) -> ApiEndpoint:
    base = dict(
        source_id="api",
        path="/pets",
        table_name="pets",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="name", type=ApiColumnType.string),
        ],
    )
    base.update(kw)
    return ApiEndpoint(**base)


def _api_source(auth=None):
    return SimpleNamespace(base_url=BASE, auth=auth)


def _reader(endpoint: ApiEndpoint, auth=None):
    return replica_read.replica_source(endpoint, _api_source(auth), COLUMNS, table="api.pets")


def _pets(n: int, start: int = 0) -> list[dict]:
    return [{"id": i, "name": f"n{i}", "extra": {"k": i}} for i in range(start, start + n)]


@pytest.fixture
def spool(tmp_path, monkeypatch):
    from provisa.federation import replica_spool

    directory = tmp_path / "replica-spool"
    monkeypatch.setattr(replica_spool, "spool_directory", lambda: directory)
    monkeypatch.setattr(replica_spool, "spool_limit", lambda: 2 * 1024**3)
    return directory


def _spool_files(directory: Path) -> list[Path]:
    return sorted(directory.glob(f"*{SPOOL_SUFFIX}"))


async def _rows(reader, batch_rows=1000) -> list[dict]:
    return [row async for batch in reader.batches(batch_rows) for row in batch.to_pylist()]


# -- a paged endpoint ----------------------------------------------------------------------------


@respx.mock
async def test_a_paged_endpoint_is_read_one_page_per_round_trip():
    """Each page is fetched only when the build asks for the next batch: the reader never holds
    more than the page it is handing over."""
    pages = [_pets(100), _pets(100, 100), _pets(40, 200)]
    route = respx.get(f"{BASE}/pets").mock(
        side_effect=[httpx.Response(200, json=page) for page in pages]
    )
    reader = _reader(
        _endpoint(pagination=PaginationConfig(type="offset", page_size=100, max_pages=10))
    )
    assert isinstance(reader, CursorSource)
    assert reader.caps.reads == {SourceRead.CURSOR}

    stream = reader.batches(1000)
    first = await stream.__anext__()
    assert first.num_rows == 100 and route.call_count == 1  # the second page is not asked for yet
    rest = [batch async for batch in stream]
    assert [b.num_rows for b in rest] == [100, 40] and route.call_count == 3
    assert first.slice(0, 1).to_pylist() == [{"id": 0, "name": "n0"}]
    assert [dict(c.request.url.params) for c in route.calls] == [
        {"limit": "100", "offset": "0"},
        {"limit": "100", "offset": "100"},
        {"limit": "100", "offset": "200"},
    ]


@respx.mock
async def test_a_page_wider_than_the_batch_bound_is_cut_to_it():
    respx.get(f"{BASE}/pets").mock(return_value=httpx.Response(200, json=_pets(40)))
    reader = _reader(
        _endpoint(pagination=PaginationConfig(type="offset", page_size=100, max_pages=10))
    )
    assert [b.num_rows async for b in reader.batches(15)] == [15, 15, 10]


@respx.mock
async def test_a_build_that_reaches_the_page_cap_with_more_to_read_fails_by_name():
    """A replica is served as the whole table: one cut at max_pages is never swapped in, and
    the build does not read past the cap the operator declared."""
    route = respx.get(f"{BASE}/pets").mock(
        side_effect=[httpx.Response(200, json=_pets(100, 100 * i)) for i in range(5)]
    )
    reader = _reader(
        _endpoint(pagination=PaginationConfig(type="offset", page_size=100, max_pages=3))
    )
    batches = []
    with pytest.raises(replica_read.PageLimitReached) as failed:
        async for batch in reader.batches(1000):
            batches.append(batch.num_rows)
    assert batches == [100, 100, 100] and route.call_count == 3  # nothing read past the cap
    assert failed.value.code == "replication.page_limit_reached"
    assert failed.value.params == {"table": "api.pets", "max_pages": 3, "rows": 300}
    assert "max_pages=3" in str(failed.value) and "api.pets" in str(failed.value)


@respx.mock
async def test_a_collection_that_ends_at_or_before_the_cap_is_whole():
    # Exactly max_pages pages, the last one short: the endpoint ended there.
    respx.get(f"{BASE}/pets").mock(
        side_effect=[httpx.Response(200, json=_pets(100)), httpx.Response(200, json=_pets(7, 100))]
    )
    paged = PaginationConfig(type="page_number", page_size=100, max_pages=2)
    assert len(await _rows(_reader(_endpoint(pagination=paged)))) == 107


@respx.mock
async def test_a_page_wrapped_in_an_object_ends_the_read_when_it_is_short():
    """The transport sees an object, not a list, and cannot tell a short page: the reader
    counts the rows, so it stops asking instead of running to the cap."""
    route = respx.get(f"{BASE}/pets").mock(
        side_effect=[
            httpx.Response(200, json={"items": _pets(100)}),
            httpx.Response(200, json={"items": _pets(3, 100)}),
            httpx.Response(200, json={"items": []}),
        ]
    )
    paged = PaginationConfig(type="offset", page_size=100, max_pages=10)
    rows = await _rows(_reader(_endpoint(response_root="items", pagination=paged)))
    assert len(rows) == 103 and route.call_count == 2


@respx.mock
async def test_cursor_and_link_paging_know_from_the_answer_whether_there_is_more():
    respx.get(f"{BASE}/pets").mock(
        side_effect=[
            httpx.Response(200, json={"items": _pets(2), "next_cursor": "a"}),
            httpx.Response(200, json={"items": _pets(2, 2), "next_cursor": "b"}),
        ]
    )
    cursor = PaginationConfig(type="cursor", page_size=2, max_pages=2)
    with pytest.raises(replica_read.PageLimitReached, match="max_pages=2"):
        await _rows(_reader(_endpoint(response_root="items", pagination=cursor)))

    respx.get(f"{BASE}/pets").mock(
        side_effect=[
            httpx.Response(200, json=_pets(2), headers={"link": f'<{BASE}/pets?p=2>; rel="next"'}),
            httpx.Response(200, json=_pets(2, 2)),
        ]
    )
    link = PaginationConfig(type="link_header", page_size=2, max_pages=2)
    assert len(await _rows(_reader(_endpoint(pagination=link)))) == 4  # no next link: the end


@respx.mock
async def test_paging_by_the_last_row_reads_to_the_short_page_and_fails_at_the_cap():
    route = respx.get(f"{BASE}/pets").mock(
        side_effect=[
            httpx.Response(200, json={"data": _pets(2)}),
            httpx.Response(200, json={"data": _pets(1, 2)}),
        ]
    )
    paged = PaginationConfig(type="last_row", page_size=2, max_pages=5, rows_field="data")
    assert len(await _rows(_reader(_endpoint(response_root="data", pagination=paged)))) == 3
    assert [dict(call.request.url.params) for call in route.calls] == [
        {"limit": "2"},
        {"limit": "2", "starting_after": "1"},
    ]

    respx.get(f"{BASE}/pets").mock(
        side_effect=[httpx.Response(200, json={"data": _pets(2, 2 * i)}) for i in range(2)]
    )
    capped = paged.model_copy(update={"max_pages": 2})
    with pytest.raises(replica_read.PageLimitReached, match="max_pages=2"):
        await _rows(_reader(_endpoint(response_root="data", pagination=capped)))


# -- a one-document endpoint ---------------------------------------------------------------------


@respx.mock
async def test_a_plain_list_answer_is_spooled_and_read_a_row_at_a_time(spool):
    route = respx.get(f"{BASE}/pets").mock(return_value=_streamed({"data": {"pets": _pets(_N)}}))
    reader = _reader(_endpoint(response_root="data.pets"))
    assert isinstance(reader, SpooledDocumentSource)
    assert reader.caps.reads == {SourceRead.SINGLE_DOCUMENT_SPOOLED}

    rows, during = 0, []
    async for batch in reader.batches(1000):
        if not rows:
            assert batch.slice(0, 1).to_pylist() == [{"id": 0, "name": "n0"}]
        rows += batch.num_rows
        during.append(len(_spool_files(spool)))
    assert rows == _N and during == [1] * (_N // 1000)
    assert _spool_files(spool) == [] and route.call_count == 1


@respx.mock
async def test_a_list_at_the_top_of_the_answer_is_read_the_same_way(spool):
    respx.get(f"{BASE}/pets").mock(return_value=_streamed(_pets(3)))
    assert await _rows(_reader(_endpoint())) == [{"id": i, "name": f"n{i}"} for i in range(3)]


@respx.mock
async def test_one_object_is_one_row_and_a_map_of_counts_is_a_row_per_key(spool):
    respx.get(f"{BASE}/pets").mock(return_value=_streamed({"id": 7, "name": "rex"}))
    assert await _rows(_reader(_endpoint())) == [{"id": 7, "name": "rex"}]

    respx.get(f"{BASE}/inventory").mock(return_value=_streamed({"available": 3, "sold": 12}))
    counts = _endpoint(
        path="/inventory",
        columns=[
            ApiColumn(name="status", type=ApiColumnType.string),
            ApiColumn(name="count", type=ApiColumnType.integer),
        ],
    )
    reader = replica_read.replica_source(
        counts, _api_source(), [("status", "text"), ("count", "bigint")], table="api.inventory"
    )
    assert await _rows(reader) == [
        {"status": "available", "count": 3},
        {"status": "sold", "count": 12},
    ]


@respx.mock
async def test_an_answer_without_its_root_path_fails_the_read_by_name(spool):
    respx.get(f"{BASE}/pets").mock(return_value=_streamed({"other": []}))
    with pytest.raises(KeyError, match="data.pets"):
        await _rows(_reader(_endpoint(response_root="data.pets")))
    respx.get(f"{BASE}/pets").mock(return_value=_streamed({"data": {"pets": "none"}}))
    with pytest.raises(ValueError, match="Expected dict or list at root path 'data.pets'"):
        await _rows(_reader(_endpoint(response_root="data.pets")))
    assert _spool_files(spool) == []


@respx.mock
async def test_neo4j_rows_are_read_from_the_spool_under_the_results_columns(spool):
    answer = {
        "results": [
            {
                "columns": ["id", "name"],
                "data": [{"row": [i, f"n{i}"], "meta": [None, None]} for i in range(_N)],
            }
        ],
        "errors": [],
    }
    route = respx.post(f"{BASE}/db/neo4j/tx/commit").mock(return_value=_streamed(answer))
    endpoint = _endpoint(
        path="/db/neo4j/tx/commit",
        method="POST",
        body_encoding="neo4j_tx",
        query_template="MATCH (p:Pet) RETURN p.id AS id, p.name AS name",
        response_normalizer="neo4j_tabular",
    )
    reader = _reader(endpoint)
    assert isinstance(reader, SpooledDocumentSource)
    rows = await _rows(reader)
    assert len(rows) == _N and rows[0] == {"id": 0, "name": "n0"} and rows[-1]["id"] == _N - 1
    assert json.loads(route.calls[0].request.content) == {
        "statements": [{"statement": endpoint.query_template, "parameters": {}}]
    }
    assert _spool_files(spool) == []


@respx.mock
async def test_a_neo4j_answer_with_errors_fails_the_read_and_yields_no_row(spool):
    answer = {"results": [], "errors": [{"code": "Neo.ClientError", "message": "bad"}]}
    respx.post(f"{BASE}/tx").mock(return_value=_streamed(answer))
    endpoint = _endpoint(
        path="/tx", method="POST", body_encoding="neo4j_tx", response_normalizer="neo4j_tabular"
    )
    with pytest.raises(ValueError, match="neo4j query failed: .*Neo.ClientError"):
        await _rows(_reader(endpoint))
    assert _spool_files(spool) == []


@respx.mock
async def test_sparql_bindings_are_read_from_the_spool_as_their_values(spool):
    answer = {
        "head": {"vars": ["id", "name"]},
        "results": {
            "bindings": [
                {"id": {"type": "literal", "value": str(i)}, "name": {"type": "uri", "value": "u"}}
                for i in range(3)
            ]
        },
    }
    route = respx.post(f"{BASE}/sparql").mock(return_value=_streamed(answer))
    endpoint = _endpoint(
        path="/sparql",
        method="POST",
        body_encoding="form",
        query_template="SELECT ?id ?name WHERE { ?s ?p ?o }",
        response_normalizer="sparql_bindings",
    )
    assert await _rows(_reader(endpoint)) == [{"id": i, "name": "u"} for i in range(3)]
    assert b"query=SELECT" in route.calls[0].request.content


# -- the call itself -----------------------------------------------------------------------------


@respx.mock
async def test_the_spooled_call_carries_the_sources_auth_and_default_parameters(spool):
    route = respx.get(f"{BASE}/pets").mock(return_value=_streamed(_pets(1)))
    endpoint = _endpoint(
        default_params={"status": "sold"},
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="name", type=ApiColumnType.string),
            ApiColumn(
                name="status", type=ApiColumnType.string, param_type="query", param_name="status"
            ),
        ],
    )
    await _rows(_reader(endpoint, auth={"bearer": "t0ken"}))
    sent = route.calls[0].request
    assert sent.headers["authorization"] == "Bearer t0ken"
    assert dict(sent.url.params) == {"status": "sold"}


@respx.mock
async def test_a_busy_or_failing_server_is_asked_again_and_then_fails_by_name(spool, monkeypatch):
    monkeypatch.setattr(replica_read.time, "sleep", lambda _s: None)
    route = respx.get(f"{BASE}/pets").mock(
        side_effect=[_streamed({}, status=429), _streamed({}, status=503), _streamed(_pets(2))]
    )
    assert len(await _rows(_reader(_endpoint()))) == 2 and route.call_count == 3

    route.mock(side_effect=[_streamed({}, status=503)] * 3)
    with pytest.raises(ApiCallError, match="failed after 3 retries: 503"):
        await _rows(_reader(_endpoint()))
    route.mock(side_effect=[_streamed({}, status=404)])
    with pytest.raises(ApiNotFoundError):
        await _rows(_reader(_endpoint()))
    route.mock(side_effect=[_streamed({}, status=401)])
    with pytest.raises(httpx.HTTPStatusError):
        await _rows(_reader(_endpoint()))
    assert _spool_files(spool) == []


# -- what is still read whole --------------------------------------------------------------------


def test_an_answer_that_must_be_understood_whole_is_a_single_document():
    for whole in (
        _endpoint(method="RPC"),
        _endpoint(response_normalizer="neo4j_graph_nodes"),
        _endpoint(response_root="results.0.items"),
    ):
        assert _reader(whole) is None

    endpoint = _endpoint(response_normalizer="neo4j_query_v2")
    loader = sl.make_openapi_loader(
        SimpleNamespace(
            api_endpoints={(endpoint.source_id, "pets"): endpoint},
            api_sources={"api": _api_source()},
        )
    )
    table = SimpleNamespace(schema_name="s", table_name="pets", columns=[])
    document = loader.replica_source(SimpleNamespace(id="api"), table, COLUMNS)
    assert isinstance(document, DocumentSource)
    assert document.caps.reads <= NOT_OPTIMAL_READS


def test_the_openapi_loader_hands_a_build_the_streamed_reader():
    loader = sl.make_openapi_loader(
        SimpleNamespace(
            api_endpoints={("api", "pets"): _endpoint()}, api_sources={"api": _api_source()}
        )
    )
    table = SimpleNamespace(schema_name="s", table_name="pets", columns=[])
    assert isinstance(
        loader.replica_source(SimpleNamespace(id="api"), table, COLUMNS), SpooledDocumentSource
    )
    with pytest.raises(sl.UnsupportedSourceFetch, match="no registered endpoint"):
        loader.replica_source(SimpleNamespace(id="other"), table, COLUMNS)
