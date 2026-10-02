# Copyright (c) 2026 Kenneth Stott
# Canary: 12b0d5b3-591b-4e51-8ca5-e96bb51a33f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source's catalog is listed through the source's own driver (REQ-1912).

A source the operator floors has no live attach on any engine — on Trino no catalog is registered
for it — so the admin discovery reads (schemas, tables, columns, primary keys) cannot go through
an engine catalog. They ask the source's driver first; the engine's catalog is queried only for a
source the driver cannot list AND the engine holds a live attach of, and is otherwise not queried
at all. A source with neither answers with an error naming it.
"""

# Requirements: REQ-1912, REQ-826

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.admin.introspect import (
    SourceNotListable,
    native_columns,
    native_primary_keys,
    require_live_attach,
    unattached_source,
)
from provisa.core.models import Source, SourceType
from provisa.executor.result import QueryResult
from provisa.federation.engine import build_engine
from provisa.federation.replica_routing import has_live_attach

pytestmark = pytest.mark.unit


def _source(
    sid: str = "orders-pg", stype: SourceType = SourceType.postgresql, **settings
) -> Source:
    return Source(
        id=sid,
        type=stype,
        host="orders-db.internal",
        port=5432,
        database="shop",
        username="reader",
        password="secret",
        **settings,
    )


class _Pool:
    """The source's own driver: answers each statement from ``answers`` (first key the SQL
    contains) and records what it was asked."""

    def __init__(self, answers: dict[str, list[tuple]]) -> None:
        self._answers = answers
        self.asked: list[tuple[str, str, list | None]] = []

    def has(self, source_id: str) -> bool:
        del source_id
        return True

    async def execute(self, source_id: str, sql: str, params: list | None = None) -> QueryResult:
        self.asked.append((source_id, sql, params))
        for marker, rows in self._answers.items():
            if marker in sql:
                return QueryResult(rows=rows, column_names=[])
        raise AssertionError(f"unexpected driver statement: {sql}")


class _Engine:
    """The bound engine. Any statement sent to it is the defect under test."""

    def __init__(self, name: str = "trino") -> None:
        self.engine = build_engine(name)
        self.statements: list[str] = []

    async def execute_engine(self, sql: str, *args: Any, **kwargs: Any) -> QueryResult:
        del args, kwargs
        self.statements.append(sql)
        raise AssertionError(
            f"the engine's catalog was asked about a source it does not attach: {sql}"
        )

    def introspect_columns(self, *args: Any) -> dict:
        raise AssertionError("the engine was asked to attach a source it does not attach")

    introspect_schemas = introspect_tables = introspect_columns


class _Acquire:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *exc: Any) -> bool:
        del exc
        return False


_PG_COLUMNS = [
    ("id", "integer"),
    ("name", "character varying"),
    ("placed_at", "timestamp without time zone"),
    ("tags", "ARRAY"),
    ("status", "USER-DEFINED"),
]


def _state(monkeypatch, source: Source, pool: _Pool, engine: _Engine) -> SimpleNamespace:
    state = SimpleNamespace(
        source_types={source.id: source.type.value},
        source_pools=pool,
        federation_engine=engine,
        config=SimpleNamespace(sources=[source]),
        catalog_for=lambda sid: sid.replace("-", "_"),
    )

    async def _registered(_state, _conn=None):
        return [source]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _registered)
    monkeypatch.setattr("provisa.api.app.state", state)

    async def _pool() -> Any:
        return SimpleNamespace(acquire=lambda: _Acquire())

    monkeypatch.setattr("provisa.api.admin.schema_query._get_pool", _pool)
    return state


# -- the driver listings ----------------------------------------------------------------------------


async def test_postgres_columns_come_from_the_driver_in_the_ir_vocabulary():
    pool = _Pool({"information_schema.columns": _PG_COLUMNS})
    columns = await native_columns("orders-pg", "postgresql", "public", "orders", pool)  # type: ignore[arg-type]
    assert columns == [
        ("id", "integer"),
        ("name", "text"),
        ("placed_at", "timestamp"),
        ("tags", "text"),  # an array collapses to text in the IR
        ("status", "text"),  # so does an enum / domain / composite
    ]
    ((_sid, _sql, params),) = pool.asked
    assert params == ["public", "orders"]


async def test_sqlserver_columns_come_from_the_driver_through_its_ir_overlay():
    rows = [
        ("id", "int"),
        ("name", "nvarchar"),
        ("placed_at", "datetime2"),
        ("open", "bit"),
        ("ref", "uniqueidentifier"),
        ("total", "money"),
    ]
    pool = _Pool({"INFORMATION_SCHEMA.COLUMNS": rows})
    columns = await native_columns("erp", "sqlserver", "dbo", "orders", pool)  # type: ignore[arg-type]
    assert columns == [
        ("id", "integer"),
        ("name", "text"),
        ("placed_at", "timestamp"),
        ("open", "boolean"),
        ("ref", "uuid"),
        ("total", "numeric"),
    ]


@pytest.mark.parametrize(("stype", "placeholders"), [("postgresql", "$1"), ("sqlserver", "?")])
async def test_primary_keys_come_from_the_driver(stype, placeholders):
    pool = _Pool({"PRIMARY KEY": [("tenant",), ("id",)]})
    keys = await native_primary_keys("s", stype, "public", "orders", pool)  # type: ignore[arg-type]
    assert keys == ["tenant", "id"]
    ((_sid, sql, params),) = pool.asked
    assert placeholders in sql and params == ["public", "orders"]


async def test_a_type_with_no_driver_listing_of_keys_answers_none():
    pool = _Pool({})
    assert await native_primary_keys("s", "mongodb", "db", "c", pool) is None  # type: ignore[arg-type]
    assert pool.asked == []


# -- the live-attach decision -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("settings", "attached"),
    [
        ({}, True),
        ({"prefer_materialized": True, "cache_ttl": 60}, False),
        ({"load_protected": True, "cache_ttl": 60}, False),
    ],
    ids=["live", "prefer_materialized", "load_protected"],
)
def test_a_floored_source_has_no_live_attach(settings, attached):
    for name in ("trino", "duckdb"):
        assert has_live_attach(_source(**settings), build_engine(name)) is attached, name


def test_a_source_the_engine_only_replicates_has_no_live_attach():
    api = Source(id="pets", type=SourceType.openapi, path="https://example.test/openapi.json")
    assert has_live_attach(api, build_engine("duckdb")) is False


async def test_an_engine_listing_of_an_unattached_source_is_refused_naming_it(monkeypatch):
    source = _source(prefer_materialized=True, cache_ttl=60)
    state = _state(monkeypatch, source, _Pool({}), _Engine())
    assert await unattached_source(state, "orders-pg") is source
    with pytest.raises(SourceNotListable) as refused:
        await require_live_attach(state, "orders-pg", "tables")
    message = str(refused.value)
    assert "'orders-pg'" in message and "tables" in message and "no live attach" in message
    # An id that is not a registered source is Provisa's own catalog: the engine lists it.
    await require_live_attach(state, "provisa-admin", "tables")


async def test_an_attached_source_is_still_listed_by_the_engine(monkeypatch):
    state = _state(monkeypatch, _source(), _Pool({}), _Engine())
    assert await unattached_source(state, "orders-pg") is None
    await require_live_attach(state, "orders-pg", "tables")


# -- the callers ------------------------------------------------------------------------------------


async def test_column_metadata_of_a_floored_source_never_asks_the_engine(monkeypatch):
    from provisa.api.admin.schema_query import resolve_available_columns_metadata

    pool = _Pool({"information_schema.columns": _PG_COLUMNS, "PRIMARY KEY": [("id",)]})
    engine = _Engine()
    _state(monkeypatch, _source(prefer_materialized=True, cache_ttl=60), pool, engine)

    columns = await resolve_available_columns_metadata("orders-pg", "public", "orders")

    assert [(c.name, c.data_type, c.is_primary_key) for c in columns] == [
        ("id", "integer", True),
        ("name", "text", False),
        ("placed_at", "timestamp", False),
        ("tags", "text", False),
        ("status", "text", False),
    ]
    assert engine.statements == []


async def test_a_floored_table_the_driver_cannot_list_is_refused_not_answered_empty(monkeypatch):
    from provisa.api.admin import schema_query

    engine = _Engine()
    _state(monkeypatch, _source(prefer_materialized=True, cache_ttl=60), _Pool({}), engine)

    async def _no_listing(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    monkeypatch.setattr("provisa.api.admin.introspect.native_columns", _no_listing)
    with pytest.raises(SourceNotListable):
        await schema_query.resolve_available_columns_metadata("orders-pg", "public", "orders")
    assert engine.statements == []


async def test_table_search_of_a_floored_source_never_asks_the_engine(monkeypatch):
    from provisa.api.admin import table_search_router

    pool = _Pool(
        {
            "information_schema.tables": [("orders", "Orders placed")],
            "information_schema.columns": _PG_COLUMNS,
        }
    )
    engine = _Engine()
    state = _state(monkeypatch, _source(prefer_materialized=True, cache_ttl=60), pool, engine)

    async def _pool() -> Any:
        return SimpleNamespace(acquire=lambda: _Acquire())

    monkeypatch.setattr("provisa.api.admin.schema._get_pool", _pool)
    candidates = await table_search_router._candidates_live("orders-pg", "public", state)

    assert [(c.name, c.columns) for c in candidates] == [
        ("orders", ["id", "name", "placed_at", "tags", "status"])
    ]
    assert engine.statements == []


async def test_the_catalog_index_of_a_floored_source_never_asks_the_engine(monkeypatch):
    from provisa.discovery import catalog_cache

    pool = _Pool(
        {
            "information_schema.schemata": [("public",)],
            "information_schema.tables": [("orders", None)],
            "information_schema.columns": _PG_COLUMNS,
            "pg_proc": [],
        }
    )
    engine = _Engine()
    source = _source(prefer_materialized=True, cache_ttl=60)
    state = _state(monkeypatch, source, pool, engine)
    written: list[tuple[str, str, list]] = []

    async def _write(_pool, source_id, schema_name, tables):
        written.append((source_id, schema_name, [(t.table_name, t.column_names) for t in tables]))

    async def _no_routines(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    monkeypatch.setattr(catalog_cache, "write_cache", _write)
    monkeypatch.setattr(catalog_cache, "_index_source_routines", _no_routines)
    control_plane = SimpleNamespace(acquire=lambda: _Acquire())

    await catalog_cache.index_source(
        "orders-pg", control_plane, engine, pool, state.source_types, state
    )

    assert written == [
        ("orders-pg", "public", [("orders", ["id", "name", "placed_at", "tags", "status"])])
    ]
    assert engine.statements == []
