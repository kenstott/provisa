# Copyright (c) 2026 Kenneth Stott
# Canary: 9461ab8e-5d7e-4d4a-8c7d-4c2185065d71
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-943 - Live Data & Events: SourceRowLoader."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pytest_bdd import given, scenario, scenarios, then, when

from provisa.events.source_loader import (
    SourceRowLoader,
    UnsupportedSourceFetch,
    make_openapi_loader,
)
from provisa.executor.result import QueryResult


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-943.feature",
    "REQ-943 default behaviour",
)
def test_req_943_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeEngine:
    """Minimal engine stub that records the SQL issued and returns a preset QueryResult."""

    def __init__(self, result: QueryResult) -> None:
        self._result = result
        self.sql: str | None = None

    async def execute_engine(self, sql: str, *args, **kwargs) -> QueryResult:
        self.sql = sql
        return self._result


def _make_source(sid: str, stype: str) -> SimpleNamespace:
    return SimpleNamespace(id=sid, type=SimpleNamespace(value=stype))


def _make_table(schema: str, table: str) -> SimpleNamespace:
    return SimpleNamespace(schema_name=schema, table_name=table)


# ---------------------------------------------------------------------------
# Steps - materialized / engine-scannable path
# ---------------------------------------------------------------------------


@given("a materialized source with current rows in the federation catalog")
def given_materialized_source(shared_data):
    """Set up a SQL-federatable source and a fake engine that returns two rows."""
    engine = _FakeEngine(
        QueryResult(
            rows=[(42, "alice"), (99, "bob")],
            column_names=["id", "name"],
            column_types=None,
        )
    )
    source = _make_source("pg-main", "postgresql")
    table = _make_table("public", "orders")

    shared_data["engine"] = engine
    shared_data["source"] = source
    shared_data["table"] = table
    shared_data["loader"] = SourceRowLoader(engine)


@when("SourceRowLoader.load(source, table) is invoked", target_fixture="load_result")
def when_load_invoked(shared_data, monkeypatch):
    """Invoke SourceRowLoader.load and capture the result/exception. Two scenarios share this exact
    step text: the openapi adapter path (REQ-945) seeds ``call_api_calls`` and needs the adapter
    chain mocked + an adapter loader built; the engine-scannable path (REQ-943) uses the pre-built
    loader directly. Dispatch on the presence of the openapi setup so one step serves both."""
    import asyncio

    if "call_api_calls" in shared_data:
        import provisa.api_source.caller as caller_mod
        import provisa.api_source.flattener as flattener_mod

        call_api_calls = shared_data["call_api_calls"]
        flatten_calls = shared_data["flatten_calls"]
        fake_pages = [{"items": [{"id": 1, "title": "Widget"}, {"id": 2, "title": "Gadget"}]}]

        async def _fake_call_api(endpoint, params, *, base_url, auth):
            call_api_calls.append(
                {"endpoint": endpoint, "params": params, "base_url": base_url, "auth": auth}
            )
            return fake_pages

        def _fake_flatten(page, root, columns, normalizer):
            flatten_calls.append(
                {"page": page, "root": root, "columns": columns, "normalizer": normalizer}
            )
            return list(page[root])

        monkeypatch.setattr(caller_mod, "call_api", _fake_call_api)
        monkeypatch.setattr(flattener_mod, "flatten_response", _fake_flatten)

        openapi_loader = make_openapi_loader(
            shared_data["endpoints_by_table"], shared_data["sources_by_id"]
        )
        engine = _FakeEngine(QueryResult(rows=[], column_names=[], column_types=None))
        loader = SourceRowLoader(engine, adapter_loaders={"openapi": openapi_loader})
        shared_data["engine"] = engine
        shared_data["loader"] = loader

    loader = shared_data["loader"]
    source = shared_data["source"]
    table = shared_data["table"]
    try:
        rows = asyncio.run(loader.load(source, table))
        shared_data["result"] = rows
        shared_data["exception"] = None
    except UnsupportedSourceFetch as exc:
        shared_data["result"] = None
        shared_data["exception"] = exc

    return shared_data


@then(
    'engine.execute_engine issues SELECT * FROM "<catalog>"."<schema>"."<table>" and returns row dicts'
)
def then_engine_issues_qualified_select(shared_data):
    """Assert the SQL was correctly qualified and the return value is a list of dicts."""
    engine: _FakeEngine = shared_data["engine"]
    rows: list[dict] = shared_data["result"]

    # Hyphens in the source id become underscores in the catalog name.
    assert engine.sql == 'SELECT * FROM "pg_main"."public"."orders"', (
        f"unexpected SQL: {engine.sql!r}"
    )
    assert rows == [{"id": 42, "name": "alice"}, {"id": 99, "name": "bob"}], (
        f"unexpected rows: {rows!r}"
    )
    assert shared_data["exception"] is None, "unexpected exception raised"


# ---------------------------------------------------------------------------
# Steps - API/push (adapter-only) path
# ---------------------------------------------------------------------------


@given("a row-oriented API/push source type with no engine-scannable table")
def given_api_push_source(shared_data):
    """Set up an openapi source - one that must raise UnsupportedSourceFetch."""
    engine = _FakeEngine(QueryResult(rows=[], column_names=[], column_types=None))
    source = _make_source("my-api", "openapi")
    table = _make_table("default", "events")

    shared_data["engine"] = engine
    shared_data["source"] = source
    shared_data["table"] = table
    shared_data["loader"] = SourceRowLoader(engine)


@then("UnsupportedSourceFetch is raised and no scan is issued")
def then_unsupported_fetch_raised(shared_data):
    """Assert UnsupportedSourceFetch was raised and the engine was never queried."""
    exc = shared_data["exception"]
    engine: _FakeEngine = shared_data["engine"]

    assert exc is not None, "expected UnsupportedSourceFetch but no exception was raised"
    assert isinstance(exc, UnsupportedSourceFetch), (
        f"expected UnsupportedSourceFetch, got {type(exc).__name__}"
    )
    assert "no engine-scannable table" in str(exc), (
        f"exception message does not mention 'no engine-scannable table': {exc}"
    )
    assert engine.sql is None, (
        f"engine.execute_engine was called with SQL {engine.sql!r} - it must not be called "
        "for adapter-only sources"
    )


scenarios("../features/REQ-945.feature")


@given("an openapi source with a registered ApiEndpoint and ApiSource in live state")
def given_openapi_source_with_registered_endpoint(shared_data):
    """Set up an openapi source with a registered ApiEndpoint and ApiSource."""
    endpoint = SimpleNamespace(
        table_name="products",
        default_params={"page": 1, "limit": 100},
        response_root="items",
        columns=[{"name": "id"}, {"name": "title"}],
        response_normalizer=None,
    )
    api_source = SimpleNamespace(
        base_url="https://api.example.com",
        auth={"type": "bearer", "token": "tok-123"},
    )

    endpoints_by_table = {"products": endpoint}
    sources_by_id = {"openapi-shop": api_source}

    source = _make_source("openapi-shop", "openapi")
    table = _make_table("default", "products")

    shared_data["endpoint"] = endpoint
    shared_data["api_source"] = api_source
    shared_data["endpoints_by_table"] = endpoints_by_table
    shared_data["sources_by_id"] = sources_by_id
    shared_data["source"] = source
    shared_data["table"] = table
    shared_data["call_api_calls"] = []
    shared_data["flatten_calls"] = []


@then("make_openapi_loader resolves the ApiEndpoint and ApiSource from state")
def then_resolves_endpoint_and_api_source(shared_data):
    """Assert that call_api was invoked (meaning endpoint + api_source were resolved)."""
    assert shared_data["exception"] is None, f"unexpected exception: {shared_data['exception']}"
    call_api_calls = shared_data["call_api_calls"]
    assert len(call_api_calls) >= 1, "call_api was never called - endpoint/api_source not resolved"
    call = call_api_calls[0]
    assert call["endpoint"] is shared_data["endpoint"], "wrong endpoint resolved"
    assert call["base_url"] == shared_data["api_source"].base_url, (
        f"wrong base_url: {call['base_url']!r}"
    )
    assert call["auth"] == shared_data["api_source"].auth, f"wrong auth: {call['auth']!r}"


@then("calls the operation with default_params via api_source.caller.call_api")
def then_calls_with_default_params(shared_data):
    """Assert call_api was called with the endpoint's default_params."""
    call_api_calls = shared_data["call_api_calls"]
    assert len(call_api_calls) >= 1, "call_api was never called"
    call = call_api_calls[0]
    expected_params = dict(shared_data["endpoint"].default_params)
    assert call["params"] == expected_params, (
        f"expected params {expected_params!r}, got {call['params']!r}"
    )


@then("flattens the response pages via api_source.flattener.flatten_response")
def then_flattens_response_pages(shared_data):
    """Assert flatten_response was called for each page returned by call_api."""
    flatten_calls = shared_data["flatten_calls"]
    assert len(flatten_calls) >= 1, "flatten_response was never called"
    call = flatten_calls[0]
    assert call["root"] == shared_data["endpoint"].response_root, (
        f"wrong response_root: {call['root']!r}"
    )


@then("returns row dicts without issuing an engine SELECT")
def then_returns_rows_without_engine_select(shared_data):
    """Assert rows are returned and no engine SQL was issued."""
    rows = shared_data["result"]
    assert rows is not None, "expected row dicts but got None"
    assert isinstance(rows, list), f"expected list, got {type(rows)}"
    assert len(rows) > 0, "expected non-empty row list"
    for row in rows:
        assert isinstance(row, dict), f"expected dict rows, got {type(row)}"
    engine: _FakeEngine = shared_data["engine"]
    assert engine.sql is None, (
        f"engine.execute_engine was called with {engine.sql!r} - must not scan for openapi source"
    )


@given("an openapi source with no registered ApiEndpoint")
def given_openapi_source_with_no_endpoint(shared_data):
    """Set up an openapi source where no ApiEndpoint is registered for the table."""
    endpoints_by_table = {}  # empty - no endpoint registered
    sources_by_id = {
        "openapi-shop": SimpleNamespace(
            base_url="https://api.example.com",
            auth={"type": "bearer", "token": "tok-123"},
        )
    }

    source = _make_source("openapi-shop", "openapi")
    table = _make_table("default", "products")

    shared_data["endpoints_by_table"] = endpoints_by_table
    shared_data["sources_by_id"] = sources_by_id
    shared_data["source"] = source
    shared_data["table"] = table
    shared_data["call_api_calls"] = []
    shared_data["flatten_calls"] = []

    openapi_loader = make_openapi_loader(endpoints_by_table, sources_by_id)
    engine = _FakeEngine(QueryResult(rows=[], column_names=[], column_types=None))
    loader = SourceRowLoader(engine, adapter_loaders={"openapi": openapi_loader})
    shared_data["engine"] = engine
    shared_data["loader"] = loader


@then("UnsupportedSourceFetch is raised")
def then_unsupported_source_fetch_is_raised(shared_data):
    """Assert UnsupportedSourceFetch was raised for the no-endpoint case."""
    import asyncio

    loader: SourceRowLoader = shared_data["loader"]
    source = shared_data["source"]
    table = shared_data["table"]

    exc_raised = None
    try:
        asyncio.run(loader.load(source, table))
    except UnsupportedSourceFetch as exc:
        exc_raised = exc

    assert exc_raised is not None, (
        "expected UnsupportedSourceFetch to be raised but no exception was raised"
    )
    assert isinstance(exc_raised, UnsupportedSourceFetch), (
        f"expected UnsupportedSourceFetch, got {type(exc_raised).__name__}"
    )
