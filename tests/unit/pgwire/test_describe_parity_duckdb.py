# Copyright (c) 2026 Kenneth Stott
# Canary: 1d9a4f6c-8e2b-4c75-b3a1-7f0e5d2c9b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The DuckDB engine's Describe agrees with its Execute, type for type (REQ-589)."""

# Requirements: REQ-589

from __future__ import annotations

import datetime
import json
from decimal import Decimal

import pytest
import pytest_asyncio

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from tests.pgwire_describe_parity import (
    RuntimeEngine,
    assert_runtime_parity,
    fetch_through_pgwire,
)
from tests.unit.pgwire.test_wire_protocol import _free_port, _make_server

_SQL = "SELECT i, b, d, f, s, flag, day, ts, j FROM parity ORDER BY i"
# The table as the registry records it (the types the pgwire catalog advertises).
_REGISTRY = {
    "parity": [
        ("i", "INTEGER"),
        ("b", "BIGINT"),
        ("d", "DECIMAL(18,2)"),
        ("f", "DOUBLE"),
        ("s", "VARCHAR"),
        ("flag", "BOOLEAN"),
        ("day", "DATE"),
        ("ts", "TIMESTAMP"),
        ("j", "JSON"),
    ]
}


@pytest.fixture
def runtime():
    rt = DuckDBFederationRuntime()
    rt.connection.execute(
        "CREATE TABLE parity (i INTEGER, b BIGINT, d DECIMAL(18,2), f DOUBLE, s VARCHAR, "
        "flag BOOLEAN, day DATE, ts TIMESTAMP, j JSON)"
    )
    rt.connection.execute(
        "INSERT INTO parity VALUES (1, 9000000000, 12.34, 1.5, 'x', true, DATE '2026-01-02', "
        "TIMESTAMP '2026-01-02 03:04:05', '{\"k\": 1}')"
    )
    return rt


@pytest_asyncio.fixture(scope="module")
async def pgwire_port():
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()


def test_describe_and_execute_report_the_same_declared_types(runtime):
    types = assert_runtime_parity(runtime, _SQL)
    assert types == [
        "INTEGER",
        "BIGINT",
        "DECIMAL(18,2)",
        "DOUBLE",
        "VARCHAR",
        "BOOLEAN",
        "DATE",
        "TIMESTAMP",
        "JSON",
    ]


def test_a_zero_row_statement_still_describes(runtime):
    types = assert_runtime_parity(runtime, "SELECT i, d FROM parity WHERE false")
    assert types == ["INTEGER", "DECIMAL(18,2)"]


def test_describe_keeps_duplicate_column_names(runtime):
    shape = runtime.describe_sync("SELECT i, i FROM parity")
    assert shape.column_names == ["i", "i"]


@pytest.mark.asyncio
async def test_every_type_reads_back_exactly_through_pgwire_once(runtime, pgwire_port):
    engine = RuntimeEngine(runtime, "duckdb")
    rows = await fetch_through_pgwire(pgwire_port, engine, _SQL, _REGISTRY)
    ((i, b, d, f, s, flag, day, ts, j),) = rows
    assert (i, b, d, f, s, flag) == (1, 9000000000, Decimal("12.34"), 1.5, "x", True)
    assert day == datetime.date(2026, 1, 2)
    assert ts == datetime.datetime(2026, 1, 2, 3, 4, 5)
    assert json.loads(j) == {"k": 1}
    assert engine.executed == [_SQL]
    assert engine.described == []  # the Describe came from the registry, not from the engine
