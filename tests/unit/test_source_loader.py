# Copyright (c) 2026 Kenneth Stott
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
    make_openapi_loader,
)
from provisa.executor.result import QueryResult


class _Engine:
    def __init__(self, result):
        self._result = result
        self.sql: str | None = None

    async def execute_engine(self, sql, *a, **k):
        self.sql = sql
        return self._result


def _src(sid, stype):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value=stype))


def _tbl(schema, table):
    return SimpleNamespace(schema_name=schema, table_name=table)


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
        return [{"data": [{"id": 1}, {"id": 2}]}]

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
