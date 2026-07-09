# Copyright (c) 2026 Kenneth Stott
# Canary: 9461ab8e-5d7e-4d4a-8c7d-4c2185065d71
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-943 - Live Data & Events: SourceRowLoader."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pytest_bdd import given, scenario, then, when

from provisa.events.source_loader import SourceRowLoader, UnsupportedSourceFetch
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
def when_load_invoked(shared_data):
    """Call SourceRowLoader.load and store the result (or exception) in shared_data."""
    import asyncio

    loader: SourceRowLoader = shared_data["loader"]
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
