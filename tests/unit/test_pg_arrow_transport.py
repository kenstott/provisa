# Copyright (c) 2026 Kenneth Stott
# Canary: 2f81a94c-6d03-4b57-8e12-73c95a0b4e6d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1222: the Postgres runtime's Arrow transport, and what it must not hold onto.

Two properties carry the requirement, and neither needs a live Postgres to observe: the read runs
on its OWN short-lived ADBC connection rather than the engine's cache/write connection, and the
streaming form pulls batches lazily, closing that connection when the consumer stops — including
when it stops early, which is the case a happy-path integration test never reaches.

The ADBC driver is substituted at its import site. What is under test is the runtime's contract
with the driver (one connection per read, closed exactly once, batches not materialized), not the
driver's own Arrow decoding.
"""

# Requirements: REQ-1220, REQ-1222

from __future__ import annotations

import sys
import types

import pytest


class _FakeReader:
    """Stands in for ADBC's RecordBatchReader — yields batches only as they are pulled."""

    def __init__(self, batches, schema="arrow-schema"):
        self.schema = schema
        self._batches = list(batches)
        self.pulled = 0

    def __iter__(self):
        for batch in self._batches:
            self.pulled += 1
            yield batch


class _FakeCursor:
    def __init__(self, con, reader, table):
        self._con = con
        self._reader = reader
        self._table = table
        self.executed: list[tuple] = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetch_arrow_table(self):
        return self._table

    def fetch_record_batch(self):
        return self._reader

    def close(self):
        self.closed = True


class _FakeConnection:
    def __init__(self, dsn, reader, table):
        self.dsn = dsn
        self.closed = 0
        self.cursor_obj = _FakeCursor(self, reader, table)

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed += 1


@pytest.fixture
def adbc(monkeypatch):
    """Install a fake ``adbc_driver_postgresql.dbapi`` and record every connection it opens."""
    opened: list[_FakeConnection] = []
    state = types.SimpleNamespace(
        opened=opened, reader=_FakeReader(["b1", "b2", "b3"]), table="arrow-table"
    )

    def _connect(dsn):
        con = _FakeConnection(dsn, state.reader, state.table)
        opened.append(con)
        return con

    module = types.ModuleType("adbc_driver_postgresql")
    module.dbapi = types.SimpleNamespace(connect=_connect)  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setitem(sys.modules, "adbc_driver_postgresql", module)
    monkeypatch.setitem(sys.modules, "adbc_driver_postgresql.dbapi", module.dbapi)
    return state


def _runtime(dsn="postgresql://provisa@localhost:5432/warehouse"):
    """A runtime with its DSN set and no real engine behind it.

    ``__init__`` opens a psycopg2 connection for the cache/write path, which these reads must not
    touch — so the instance is built without it and only the DSN the Arrow path reads is set.
    """
    from provisa.federation.pg_runtime import PgFederationRuntime

    runtime = PgFederationRuntime.__new__(PgFederationRuntime)
    runtime._engine_dsn = dsn  # pyright: ignore[reportAttributeAccessIssue]
    return runtime


def test_run_arrow_returns_the_drivers_table_without_building_python_rows(adbc):
    runtime = _runtime()

    table = runtime.run_arrow("SELECT id FROM orders", ["p"])

    assert table == "arrow-table"
    con = adbc.opened[0]
    assert con.cursor_obj.executed == [("SELECT id FROM orders", ["p"])]


def test_run_arrow_opens_its_own_connection_and_closes_it(adbc):
    """The engine's psycopg2 connection is the cache/write terminal; a read that borrowed it
    would serialize behind writes and outlive the query."""
    runtime = _runtime()

    runtime.run_arrow("SELECT 1")

    assert len(adbc.opened) == 1
    assert adbc.opened[0].dsn == "postgresql://provisa@localhost:5432/warehouse"
    assert adbc.opened[0].closed == 1


def test_run_arrow_closes_its_connection_even_when_the_read_raises(adbc):
    runtime = _runtime()

    def _boom(self, sql, params=None):
        raise RuntimeError("relation does not exist")

    original = _FakeCursor.execute
    try:
        _FakeCursor.execute = _boom  # pyright: ignore[reportAttributeAccessIssue]
        with pytest.raises(RuntimeError, match="relation does not exist"):
            runtime.run_arrow("SELECT 1")
    finally:
        _FakeCursor.execute = original  # pyright: ignore[reportAttributeAccessIssue]

    assert adbc.opened[0].closed == 1


def test_run_arrow_stream_hands_back_the_schema_before_any_batch_is_pulled(adbc):
    """The Flight/airport transport advertises the schema first; pulling a batch to learn it
    would defeat the streaming the requirement is about."""
    runtime = _runtime()

    schema, batches = runtime.run_arrow_stream("SELECT id FROM orders")

    assert schema == "arrow-schema"
    assert adbc.reader.pulled == 0
    assert adbc.opened[0].closed == 0

    assert next(batches) == "b1"
    assert adbc.reader.pulled == 1


def test_run_arrow_stream_closes_the_connection_when_the_stream_drains(adbc):
    runtime = _runtime()

    _, batches = runtime.run_arrow_stream("SELECT id FROM orders")
    assert list(batches) == ["b1", "b2", "b3"]

    assert adbc.opened[0].cursor_obj.closed is True
    assert adbc.opened[0].closed == 1


def test_run_arrow_stream_closes_the_connection_when_the_consumer_stops_early(adbc):
    """A client that reads one batch and disconnects is the ordinary case for a paged Flight
    reader. Leaking the connection there exhausts the pool under exactly the load the streaming
    path was built for."""
    runtime = _runtime()

    _, batches = runtime.run_arrow_stream("SELECT id FROM orders")
    assert next(batches) == "b1"
    batches.close()

    assert adbc.opened[0].cursor_obj.closed is True
    assert adbc.opened[0].closed == 1
    assert adbc.reader.pulled == 1
