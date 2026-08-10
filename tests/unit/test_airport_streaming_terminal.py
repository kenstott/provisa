# Copyright (c) 2026 Kenneth Stott
# Canary: 7e51c093-2a84-4b6f-90d3-c48b1725ae60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1218 / REQ-1231 / REQ-1190: the airport is a streaming transport, on both routes.

Three properties decide that, and all three fail quietly:

* the advertised schema must be BYTE-STABLE — derived from the query's typed output columns, so a
  scan returning zero rows advertises exactly what the same scan returns with a million. A schema
  inferred from rows makes an empty table a different table.
* advertising must not open a data cursor, or a client listing schemas holds a pooled connection
  per table it looked at.
* a DIRECT scan must stream through the source's own cursor in bounded batches, never materialize.

A live Flight client would show rows arriving; it would not show how many were held to produce
them, which is the thing under test.
"""

# Requirements: REQ-1190, REQ-1218, REQ-1231

from __future__ import annotations

import asyncio
import threading
import types

import pyarrow as pa
import pytest

from provisa.api.airport.query import (
    _direct_typed_schema,
    _typed_batches_from_rows,
    governed_table_scan_schema,
    governed_table_scan_stream,
)


class _RowStream:
    """A lazy row stream that reports how much of itself was consumed."""

    def __init__(self, rows, column_names, column_types):
        self._rows = rows
        self.column_names = column_names
        self.column_types = column_types
        self.consumed = 0
        self.closed = False

    def iter_rows(self):
        for row in self._rows:
            self.consumed += 1
            yield row

    def close(self):
        self.closed = True


class _Reader:
    """An engine record-batch reader, closable without a drain."""

    def __init__(self, batches):
        self._batches = batches
        self.pulled = 0
        self.closed = False

    def __iter__(self):
        for batch in self._batches:
            self.pulled += 1
            yield batch

    def close(self):
        self.closed = True


def _plan(route, **kwargs):
    # audit/audit_written: the governors open the audit record and the terminal finalizes it
    # (REQ-074/REQ-1386). No identity is bound here, so the record is None and no row is written —
    # the fields still have to exist, because the terminal finalizes on every path.
    return types.SimpleNamespace(
        route=route,
        sql="SELECT id FROM orders",
        source_id="wh",
        exec_params=[],
        audit=None,
        audit_written=False,
        **kwargs,
    )


@pytest.fixture
def main_loop():
    """The app's event loop, running in its own thread — the airport calls into it from the Flight
    worker thread (governing the scan, and writing its audit row at the terminal)."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=2)
    loop.close()


@pytest.fixture
def airport(monkeypatch, main_loop):
    """State and plan wiring for the two routes, with the governed pipeline stubbed at the seam."""
    from provisa.transpiler.router import Route

    engine_schema = pa.schema([pa.field("id", pa.int64())])
    reader = _Reader([pa.RecordBatch.from_arrays([pa.array([1, 2])], schema=engine_schema)])
    direct_stream = _RowStream([(1,), (2,), (3,)], ["id"], ["integer"])

    engine = types.SimpleNamespace(
        execute_engine_stream=lambda sql, params: (engine_schema, reader),
        execute_native_stream=lambda pools, source_id, sql, params, loop=None: direct_stream,
    )
    pools = types.SimpleNamespace(has=lambda s: True, supports_stream=lambda s: True)
    state = types.SimpleNamespace(federation_engine=engine, source_pools=pools)

    holder = types.SimpleNamespace(
        state=state,
        reader=reader,
        direct=direct_stream,
        engine_schema=engine_schema,
        route=Route,
        plan=_plan(Route.ENGINE, physical_sql="SELECT id FROM orders"),
    )
    monkeypatch.setattr(
        "provisa.api.airport.query._plan_for_scan",
        lambda state, loop, sql, role_id: holder.plan,
    )
    return holder


# --- schema advertisement (REQ-1218, REQ-1231) ------------------------------------------------


def test_advertising_an_engine_scan_closes_the_reader_without_pulling_a_batch(airport):
    """flight_info is called per table by a client listing a catalog; leaving a lazy reader open
    per call holds an engine cursor for each one."""
    schema = governed_table_scan_schema(airport.state, None, "SELECT id FROM orders", "analyst")

    assert schema == airport.engine_schema
    assert airport.reader.closed is True
    assert airport.reader.pulled == 0


def test_advertising_a_direct_scan_releases_the_source_cursor_without_fetching(airport):
    airport.plan = _plan(airport.route.DIRECT)

    schema = governed_table_scan_schema(airport.state, None, "SELECT id FROM orders", "analyst")

    assert schema.names == ["id"]
    assert airport.direct.closed is True
    assert airport.direct.consumed == 0


def test_the_direct_schema_comes_from_the_drivers_types_not_from_rows():
    """Byte-stability is the requirement: a 0-row scan and an N-row scan advertise the same
    schema, so a client caching it does not see the table change shape when it empties."""
    empty = _direct_typed_schema(["id", "amount"], ["integer", "numeric"])
    populated = _direct_typed_schema(["id", "amount"], ["integer", "numeric"])

    assert empty == populated
    assert empty.field("id").type == pa.int32()
    assert empty.field("amount").type == pa.float64()


def test_a_direct_scan_that_reports_no_column_types_is_refused():
    """Inventing a type here is what produced the empty-scan retyping this requirement removed;
    the honest answer is that the driver did not report enough to advertise."""
    with pytest.raises(ValueError, match="byte-stable"):
        _direct_typed_schema(["id", "amount"], ["integer"])

    with pytest.raises(ValueError, match="byte-stable"):
        _direct_typed_schema(["id"], None)


# --- streaming (REQ-1190, REQ-1231) -----------------------------------------------------------


def test_an_engine_scan_streams_the_engines_own_reader(airport, main_loop):
    schema, batches = governed_table_scan_stream(
        airport.state, main_loop, "SELECT id FROM orders", "analyst"
    )

    assert schema == airport.engine_schema
    assert airport.reader.pulled == 0  # nothing pulled until the client reads
    assert len(list(batches)) == 1


def test_a_direct_scan_streams_through_the_sources_cursor(airport, main_loop):
    airport.plan = _plan(airport.route.DIRECT)

    schema, batches = governed_table_scan_stream(
        airport.state, main_loop, "SELECT id FROM orders", "analyst"
    )

    assert schema.names == ["id"]
    assert airport.direct.consumed == 0
    assert sum(b.num_rows for b in batches) == 3


def test_row_batches_are_bounded_rather_than_materialized():
    """The bound is what makes this a streaming transport: without it a large DIRECT scan is one
    batch the size of the table."""
    stream = _RowStream([(i,) for i in range(2500)], ["id"], ["integer"])
    typed = _direct_typed_schema(["id"], ["integer"])

    batches = list(_typed_batches_from_rows(stream, typed, batch_rows=1000))

    assert [b.num_rows for b in batches] == [1000, 1000, 500]
    assert all(b.schema == typed for b in batches)


def test_a_client_that_stops_early_does_not_drain_the_source():
    stream = _RowStream([(i,) for i in range(2500)], ["id"], ["integer"])
    typed = _direct_typed_schema(["id"], ["integer"])

    batches = _typed_batches_from_rows(stream, typed, batch_rows=1000)
    next(batches)
    batches.close()

    assert stream.consumed == 1000


def test_an_unroutable_scan_is_refused_rather_than_materialized(airport):
    airport.plan = _plan("SOMETHING_ELSE")

    with pytest.raises(ValueError, match="not supported for the airport service"):
        governed_table_scan_stream(airport.state, None, "SELECT 1", "analyst")
