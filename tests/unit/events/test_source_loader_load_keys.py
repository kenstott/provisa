# Copyright (c) 2026 Kenneth Stott
# Canary: 1043dcbd-2d6d-4bf5-afed-8b37ac4a5ccd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: SourceRowLoader.load_keys — the keyed IN-predicate builder, and the explicit
registration-gap error for a query-API (adapter-fetch-only) source type."""

from __future__ import annotations

import types

import pytest

from provisa.events.source_loader import (
    SourceRowLoader,
    UnsupportedSourceFetch,
    _pk_in_clause,
    _sql_literal,
)


def test_sql_literal_quotes_strings_and_escapes():
    assert _sql_literal(5) == "5"
    assert _sql_literal(5.5) == "5.5"
    assert _sql_literal(None) == "NULL"
    assert _sql_literal(True) == "TRUE"
    assert _sql_literal("o'brien") == "'o''brien'"


def test_pk_in_clause_single_column():
    clause = _pk_in_clause(["id"], [(1,), (2,)])
    assert clause == '"id" IN (1, 2)'


def test_pk_in_clause_composite():
    clause = _pk_in_clause(["id", "region"], [(1, "us"), (2, "eu")])
    assert clause == "(\"id\", \"region\") IN ((1, 'us'), (2, 'eu'))"


@pytest.mark.asyncio
async def test_load_keys_empty_returns_empty_without_touching_engine():
    loader = SourceRowLoader(engine=None)
    result = await loader.load_keys(types.SimpleNamespace(type="postgresql"), None, ["id"], [])
    assert result == []


@pytest.mark.asyncio
async def test_load_keys_raises_for_adapter_fetch_only_source():
    loader = SourceRowLoader(engine=None)
    source = types.SimpleNamespace(type="neo4j", id="neo")
    with pytest.raises(UnsupportedSourceFetch, match="no keyed-fetch translation"):
        await loader.load_keys(source, None, ["id"], [(1,)])


@pytest.mark.asyncio
async def test_load_keys_runs_bounded_select_through_engine_terminal():
    calls = []

    class _FakeResult:
        column_names = ["id", "status"]
        rows = [(1, "new")]

    class _FakeEngine:
        async def execute_engine(self, sql):
            calls.append(sql)
            return _FakeResult()

    loader = SourceRowLoader(engine=_FakeEngine())
    source = types.SimpleNamespace(type="postgresql", id="pg1")
    table = types.SimpleNamespace(schema_name="public", table_name="orders")
    rows = await loader.load_keys(source, table, ["id"], [(1,)])
    assert rows == [{"id": 1, "status": "new"}]
    assert len(calls) == 1
    assert "IN (1)" in calls[0]
    assert "public" in calls[0] and "orders" in calls[0]
