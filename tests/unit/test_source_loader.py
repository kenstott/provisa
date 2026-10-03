# Copyright (c) 2026 Kenneth Stott
# Canary: 0af4a6cc-ea15-49ba-afa4-fc72c1afc2e1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-941/846: SourceRowLoader — read a MATERIALIZED source's rows via the engine terminal."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.events.source_loader import (
    SourceRowLoader,
    UnsupportedSourceFetch,
    make_graphql_remote_loader,
    make_openapi_loader,
)
from provisa.executor.result import QueryResult


class _Engine:
    def __init__(self, result):
        self._result = result
        self.sql: str | None = None

    def address_replicas(self, sql):
        return sql  # this stand-in's tables are all read where the statement names them

    async def execute_engine(self, sql, *a, **k):
        self.sql = sql
        return self._result


def _src(sid, stype):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value=stype))


def _tbl(schema, table, pagination=None):
    # A registry row (registry_view): it carries the table's paging, None when it sets none.
    return SimpleNamespace(schema_name=schema, table_name=table, pagination=pagination)


@pytest.mark.asyncio
async def test_engine_scan_returns_row_dicts():
    engine = _Engine(
        QueryResult(rows=[(1, "a"), (2, "b")], column_names=["id", "name"], column_types=None)
    )
    rows = await SourceRowLoader(engine).load(
        _src("pg-main", "postgresql"), _tbl("public", "orders")
    )
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    # hyphens in the source id become underscores in the engine catalog, quoted + schema-qualified.
    assert engine.sql == 'SELECT * FROM "pg_main"."public"."orders"'


@pytest.mark.asyncio
async def test_empty_result_is_empty_list():
    engine = _Engine(QueryResult(rows=[], column_names=["id"], column_types=None))
    rows = await SourceRowLoader(engine).load(_src("s1", "mysql"), _tbl("db", "t"))
    assert rows == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stype", ["openapi", "ingest", "websocket", "rss", "grpc_remote"])
async def test_adapter_only_sources_raise(stype):
    # API/push sources have no engine table — the loader is explicit, never a silent empty snapshot.
    engine = _Engine(QueryResult(rows=[], column_names=[], column_types=None))
    with pytest.raises(UnsupportedSourceFetch, match="no engine-scannable table"):
        await SourceRowLoader(engine).load(_src("api", stype), _tbl("default", "events"))
    assert engine.sql is None  # never issued a scan for a non-scannable source


@pytest.mark.asyncio
async def test_accepts_bare_string_source_type():
    engine = _Engine(QueryResult(rows=[(1,)], column_names=["id"], column_types=None))
    src = SimpleNamespace(id="s", type="postgresql")  # type as a plain string, not an enum
    assert await SourceRowLoader(engine).load(src, _tbl("public", "t")) == [{"id": 1}]


@pytest.mark.asyncio
async def test_adapter_loader_dispatch_for_openapi():
    # An injected adapter loader for a non-scannable type is used instead of an engine scan.
    async def _fake_openapi(source, table):
        return [{"id": 1, "name": "a"}]

    engine = _Engine(QueryResult(rows=[], column_names=[], column_types=None))
    loader = SourceRowLoader(engine, adapter_loaders={"openapi": _fake_openapi})
    rows = await loader.load(_src("api", "openapi"), _tbl("default", "events"))
    assert rows == [{"id": 1, "name": "a"}]
    assert engine.sql is None  # never scanned — the adapter loader handled it


@pytest.mark.asyncio
async def test_make_openapi_loader_calls_and_flattens(monkeypatch):
    # make_openapi_loader resolves the table's endpoint + api-source, calls the operation with its
    # default params, and flattens the response pages into rows.
    calls: dict = {}

    async def _fake_call_api(endpoint, params, *, base_url, auth):
        calls["endpoint"] = endpoint
        calls["params"] = params
        calls["base_url"] = base_url
        calls["auth"] = auth
        from provisa.api_source.caller import ApiAnswer

        return ApiAnswer([{"data": [{"id": 1}, {"id": 2}]}])

    def _fake_flatten(page, root, columns, normalizer):
        return list(page[root])  # trivial: root points at the row list

    monkeypatch.setattr("provisa.api_source.caller.call_api", _fake_call_api)
    monkeypatch.setattr("provisa.api_source.flattener.flatten_response", _fake_flatten)

    endpoint = SimpleNamespace(
        table_name="events",
        default_params={"limit": 100},
        response_root="data",
        columns=[],
        response_normalizer=None,
    )
    api_source = SimpleNamespace(id="api", base_url="https://x.test", auth=None)
    load = make_openapi_loader({"events": endpoint}, {"api": api_source})

    rows = await load(_src("api", "openapi"), _tbl("default", "events"))
    assert rows == [{"id": 1}, {"id": 2}]
    assert calls["base_url"] == "https://x.test" and calls["params"] == {"limit": 100}


@pytest.mark.asyncio
async def test_make_openapi_loader_missing_endpoint_raises():
    load = make_openapi_loader({}, {"api": SimpleNamespace(id="api", base_url="", auth=None)})
    with pytest.raises(UnsupportedSourceFetch, match="no registered endpoint"):
        await load(_src("api", "openapi"), _tbl("default", "events"))


@pytest.mark.asyncio
async def test_make_graphql_remote_loader_forwards_query(monkeypatch):
    captured: dict = {}

    async def _fake_execute_remote(
        *, url, auth, field_name, columns, rows_path, max_rows, error_policy
    ):
        captured.update(
            url=url,
            auth=auth,
            field_name=field_name,
            columns=columns,
            rows_path=rows_path,
            max_rows=max_rows,
        )
        from provisa.graphql_remote.executor import RemoteAnswer

        return RemoteAnswer([{"id": 1}, {"id": 2}])

    monkeypatch.setattr("provisa.graphql_remote.executor.execute_remote", _fake_execute_remote)

    gql_sources = {
        "gql": {
            "url": "https://gql.test/graphql",
            "auth": {"type": "bearer"},
            "tables": [
                {
                    "sql_name": "orders",
                    "name": "orders",
                    "field_name": "allOrders",
                    "columns": [
                        {"name": "id"},
                        {"name": "total", "gql_selection": "total { amount }"},
                    ],
                }
            ],
        }
    }
    load = make_graphql_remote_loader(gql_sources, max_rows=500)
    rows = await load(_src("gql", "graphql_remote"), _tbl("default", "orders"))
    assert rows == [{"id": 1}, {"id": 2}]
    assert captured["rows_path"] is None  # not a connection table
    assert captured["max_rows"] == 500
    assert captured["url"] == "https://gql.test/graphql"
    assert captured["field_name"] == "allOrders"  # field_name overrides the table name
    assert captured["columns"] == ["id", "total { amount }"]  # gql_selection overrides the name


@pytest.mark.asyncio
async def test_make_graphql_remote_loader_missing_registration_raises():
    load = make_graphql_remote_loader({}, max_rows=500)
    with pytest.raises(UnsupportedSourceFetch, match="no matching"):
        await load(_src("gql", "graphql_remote"), _tbl("default", "orders"))


@pytest.mark.asyncio
async def test_rss_loader_raises_when_the_feed_fetch_fails(monkeypatch):
    """REQ-1661 (amended 2026-09-30): a failed feed fetch fails the land (and so the query) --
    it is never read as an empty feed, which landed zero rows or left the stale replica served."""
    from provisa.events.source_loader import make_rss_loader
    from provisa.subscriptions.rss_provider import RSSNotificationProvider

    async def _down(self, url):
        raise ConnectionError("feed down")

    monkeypatch.setattr(RSSNotificationProvider, "_fetch", _down)
    source = SimpleNamespace(federation_hints={"feed_url": "http://feed.invalid/rss"})
    with pytest.raises(ConnectionError, match="feed down"):
        await make_rss_loader()(source, SimpleNamespace(table_name="items"))


@pytest.mark.asyncio
async def test_a_connection_tables_replica_source_reads_it_a_page_at_a_time(monkeypatch):
    """REQ-1915/REQ-1923: a connection table's build is a cursor over its pages (one held at a
    time), not a document read whole into memory; and a land of it is the whole table."""
    from provisa.federation.replica_source import CursorSource

    pulled: list[str] = []

    async def _pages(url, auth, field_name, columns, rows_path, *, table, max_rows, error_policy):
        assert (table, max_rows) == ("gql.issues", 500)
        for page in ([{"id": 1}, {"id": 2}], [{"id": 3}]):
            pulled.append("page")
            yield page

    monkeypatch.setattr("provisa.graphql_remote.executor.whole_connection", _pages)
    gql_sources = {
        "gql": {
            "url": "https://gql.test/graphql",
            "tables": [
                {
                    "sql_name": "issues",
                    "field_name": "issues",
                    "rows_path": ["nodes"],
                    "columns": [{"name": "id"}],
                }
            ],
        }
    }
    load = make_graphql_remote_loader(gql_sources, max_rows=500)
    source = load.replica_source(
        _src("gql", "graphql_remote"), _tbl("default", "issues"), [("id", "bigint")]
    )
    assert isinstance(source, CursorSource)
    batches = source.batches(10)
    first = await anext(batches)
    assert first.to_pylist() == [{"id": 1}, {"id": 2}] and pulled == ["page"]
    assert [b.to_pylist() async for b in batches] == [[{"id": 3}]]
    assert await load(_src("gql", "graphql_remote"), _tbl("default", "issues")) == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


@pytest.mark.asyncio
async def test_a_connection_table_is_read_up_to_its_own_bound_when_it_sets_one(monkeypatch):
    """REQ-318: a connection table's pagination.max_rows (which may only lower the operator's
    graphql_remote.max_rows) is the bound its build reads to; with none, the operator's."""
    from provisa.core.paging import PaginationConfig

    bounds: list[int] = []

    async def _pages(url, auth, field_name, columns, rows_path, *, table, max_rows, error_policy):
        bounds.append(max_rows)
        yield [{"id": 1}]

    monkeypatch.setattr("provisa.graphql_remote.executor.whole_connection", _pages)
    gql_sources = {
        "gql": {
            "url": "https://gql.test/graphql",
            "tables": [
                {"sql_name": "issues", "field_name": "issues", "rows_path": ["nodes"],
                 "columns": [{"name": "id"}]}
            ],
        }
    }  # fmt: skip
    load = make_graphql_remote_loader(gql_sources, max_rows=500)
    own = _tbl("default", "issues", PaginationConfig(max_rows=20))
    await load(_src("gql", "graphql_remote"), own)
    await load(_src("gql", "graphql_remote"), _tbl("default", "issues"))
    assert bounds == [20, 500]
