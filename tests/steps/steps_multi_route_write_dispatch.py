# Copyright (c) 2026 Kenneth Stott
# Canary: 80822517-3eb9-48ba-9d38-71c36ce538b0
#
# This source code is licensed under the Business Source License 1.1

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.executor.writable import WritePath, resolve_write_path


@scenario(
    "../features/REQ-910.feature",
    "REQ-910 default behaviour",
)
def test_req_910_default_behaviour():
    pass


@pytest.fixture
def shared_data():
    return {}


@given(
    "a federation engine with connectors for multiple source types (postgresql, sqlite, cassandra)"
)
def given_federation_engine_with_connectors(shared_data):
    """Build a minimal fake FederationEngine with three connectors of varying capability."""

    # postgresql connector: write=True (native driver + dialect available)
    pg_capability = MagicMock()
    pg_capability.write = True
    pg_connector = MagicMock()
    pg_connector.capability.return_value = pg_capability

    # sqlite connector: write=True (SQLAlchemy fallback + dialect available)
    sqlite_capability = MagicMock()
    sqlite_capability.write = True
    sqlite_connector = MagicMock()
    sqlite_connector.capability.return_value = sqlite_capability

    # cassandra connector: write=True (no direct driver, no SQLGlot dialect - ENGINE only)
    cassandra_capability = MagicMock()
    cassandra_capability.write = True
    cassandra_connector = MagicMock()
    cassandra_connector.capability.return_value = cassandra_capability

    engine = MagicMock()
    engine.connectors = {
        "postgresql": pg_connector,
        "sqlite": sqlite_connector,
        "cassandra": cassandra_connector,
    }

    shared_data["engine"] = engine
    shared_data["source_types"] = ["postgresql", "sqlite", "cassandra"]


@when("resolve_write_path is called for each source with the engine")
def when_resolve_write_path_called(shared_data):
    engine = shared_data["engine"]
    results = {}
    for source_type in shared_data["source_types"]:
        results[source_type] = resolve_write_path(source_type, engine)
    shared_data["results"] = results


@then("postgresql returns NATIVE (native asyncpg driver + dialect available)")
def then_postgresql_returns_native(shared_data):
    result = shared_data["results"]["postgresql"]
    assert result == WritePath.NATIVE, (
        f"Expected WritePath.NATIVE for postgresql, got {result!r}"
    )


@then("sqlite returns SQLALCHEMY (no native driver, SQLAlchemy fallback + dialect available)")
def then_sqlite_returns_sqlalchemy(shared_data):
    result = shared_data["results"]["sqlite"]
    assert result == WritePath.SQLALCHEMY, (
        f"Expected WritePath.SQLALCHEMY for sqlite, got {result!r}"
    )


@then("cassandra returns ENGINE (no direct driver/dialect, only connector write=True)")
def then_cassandra_returns_engine(shared_data):
    result = shared_data["results"]["cassandra"]
    assert result == WritePath.ENGINE, (
        f"Expected WritePath.ENGINE for cassandra, got {result!r}"
    )


@then("if engine is None, only NATIVE and SQLALCHEMY remain as possible routes")
def then_engine_none_excludes_engine_route(shared_data):
    # With engine=None, cassandra (no driver, no dialect) must return None
    cassandra_no_engine = resolve_write_path("cassandra", engine=None)
    assert cassandra_no_engine is None, (
        f"Expected None for cassandra without engine, got {cassandra_no_engine!r}"
    )

    # postgresql still resolves to NATIVE without the engine
    pg_no_engine = resolve_write_path("postgresql", engine=None)
    assert pg_no_engine == WritePath.NATIVE, (
        f"Expected WritePath.NATIVE for postgresql without engine, got {pg_no_engine!r}"
    )

    # sqlite still resolves to SQLALCHEMY without the engine
    sqlite_no_engine = resolve_write_path("sqlite", engine=None)
    assert sqlite_no_engine == WritePath.SQLALCHEMY, (
        f"Expected WritePath.SQLALCHEMY for sqlite without engine, got {sqlite_no_engine!r}"
    )

    # Confirm ENGINE is not returned for any source when engine is None
    for source_type in shared_data["source_types"]:
        path = resolve_write_path(source_type, engine=None)
        assert path != WritePath.ENGINE, (
            f"ENGINE route returned for {source_type!r} even though engine=None"
        )
