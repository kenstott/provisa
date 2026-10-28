# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's temporary tables are dropped from an embedded DuckDB store through its broker
(REQ-615, REQ-989): under the store's lock, as every other writer of the file. Opened on a
connection of its own, the file was held while the next session's CREATE TEMP TABLE attached it
("Unique file handle conflict: Cannot attach mat_store ... already attached by database
materialize"; pgspec TestCopy/BinaryIn and Fail)."""

from __future__ import annotations

import pytest

from provisa.federation import store_scope
from provisa.federation.materialize_broker import get_broker
from provisa.federation.replica_address import temp_schema


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = tmp_path / "materialize.duckdb"
    schema = temp_schema("conformance", None, "ab12")

    def no_connection_of_its_own(dsn):
        raise AssertionError(f"the store file was opened on a connection of its own: {dsn}")

    monkeypatch.setattr(
        "provisa.federation.store_writer.store_connection", no_connection_of_its_own
    )
    broker = get_broker(str(path))
    broker.execute(f'CREATE SCHEMA mat_store."{schema}"')
    broker.execute(f'CREATE TABLE mat_store."{schema}".t (id INTEGER)')
    broker.execute(f'CREATE TABLE mat_store."{schema}".u (id INTEGER)')
    return f"duckdb:///{path}", broker, schema


def _tables(broker, schema):
    return sorted(broker.tables(schema))


@pytest.mark.asyncio
async def test_a_temporary_table_is_dropped_through_the_stores_broker(store):
    dsn, broker, schema = store
    await store_scope.drop_temp_table(dsn, schema, "t")
    assert _tables(broker, schema) == ["u"]


@pytest.mark.asyncio
async def test_a_sessions_temporary_schema_is_dropped_through_the_stores_broker(store):
    dsn, broker, schema = store
    await store_scope.drop_temp_schema(dsn, schema)
    assert _tables(broker, schema) == []
