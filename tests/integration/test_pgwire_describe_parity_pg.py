# Copyright (c) 2026 Kenneth Stott
# Canary: 3b6e9d1f-7a2c-4e58-91d4-0c8f6a3e2b75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Postgres engine's Describe agrees with its Execute, type for type, against a real Postgres
(REQ-589): the describe plans the governed SQL behind a constant-false filter, so nothing runs."""

# Requirements: REQ-589

from __future__ import annotations

import datetime
import tempfile
import uuid
from decimal import Decimal

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration]

pgserver = pytest.importorskip("pgserver")
psycopg2 = pytest.importorskip("psycopg2")

from provisa.federation.pg_runtime import PgFederationRuntime  # noqa: E402
from tests.pgwire_describe_parity import (  # noqa: E402
    RuntimeEngine,
    assert_runtime_parity,
    fetch_through_pgwire,
)
from tests.unit.pgwire.test_wire_protocol import _free_port, _make_server  # noqa: E402

_SQL = "SELECT i, b, d, f, s, flag, day, ts, j FROM parity ORDER BY i"
# The tables as the registry records them (the types the pgwire catalog advertises).
_REGISTRY = {
    "parity": [
        ("i", "int4"),
        ("b", "int8"),
        ("d", "numeric(18,2)"),
        ("f", "float8"),
        ("s", "text"),
        ("flag", "bool"),
        ("day", "date"),
        ("ts", "timestamp"),
        ("j", "jsonb"),
    ],
    "wide": [
        ("si", "int2"),
        ("r", "float4"),
        ("jb", "jsonb"),
        ("tz", "timestamptz"),
        ("ttz", "timetz"),
        ("u", "uuid"),
        ("iv", "interval"),
        ("ba", "bytea"),
        ("old_ts", "timestamp"),
        ("old_day", "date"),
    ],
}


@pytest.fixture(scope="module")
def runtime():
    server = pgserver.get_server(
        tempfile.mkdtemp(prefix="provisa_pgw_parity_"), cleanup_mode="stop"
    )
    con = psycopg2.connect(server.get_uri())
    con.autocommit = True
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS parity")
    cur.execute(
        "CREATE TABLE parity (i int4, b int8, d numeric(18,2), f float8, s text, flag bool, "
        "day date, ts timestamp, j jsonb)"
    )
    cur.execute(
        "INSERT INTO parity VALUES (1, 9000000000, 12.34, 1.5, 'x', true, '2026-01-02', "
        "'2026-01-02 03:04:05', '{\"k\": 1}')"
    )
    con.close()
    rt = PgFederationRuntime(engine_dsn=server.get_uri())
    yield rt
    rt.close()
    server.cleanup()


@pytest_asyncio.fixture(scope="module")
async def pgwire_port():
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()


def test_describe_and_execute_report_the_same_declared_types(runtime):
    types = assert_runtime_parity(runtime, _SQL)
    assert types == [
        "int4",
        "int8",
        "numeric",
        "float8",
        "text",
        "bool",
        "date",
        "timestamp",
        "jsonb",
    ]


def test_a_zero_row_statement_still_describes(runtime):
    assert assert_runtime_parity(runtime, "SELECT i, d FROM parity WHERE false") == [
        "int4",
        "numeric",
    ]


def test_describe_keeps_duplicate_column_names(runtime):
    assert runtime.describe_sync("SELECT i, i FROM parity").column_names == ["i", "i"]


@pytest.mark.asyncio
async def test_every_type_reads_back_exactly_through_pgwire_once(runtime, pgwire_port):
    engine = RuntimeEngine(runtime, "duckdb")  # not "postgres": skip the REQ-1863 passthrough
    rows = await fetch_through_pgwire(pgwire_port, engine, _SQL, _REGISTRY)
    ((i, b, d, f, s, flag, day, ts, j),) = rows
    assert (i, b, d, f, s, flag) == (1, 9000000000, Decimal("12.34"), 1.5, "x", True)
    assert day == datetime.date(2026, 1, 2)
    assert ts == datetime.datetime(2026, 1, 2, 3, 4, 5)
    assert j == '{"k": 1}'
    assert engine.executed == [_SQL]
    assert engine.described == []  # the Describe came from the registry, not from the engine


# Every exact-width Postgres type, read by asyncpg in BINARY: once with the source's raw bytes
# forwarded by the REQ-1863 passthrough (the Describe's advertised OID must match the source
# layout exactly), once decoded and re-encoded by pgwire's own encoders.
_WIDE_SQL = "SELECT si, r, jb, tz, ttz, u, iv, ba, old_ts, old_day FROM wide ORDER BY si"
_WIDE_EXPECTED = (
    7,
    1.5,
    '{"k": [1, 2]}',
    datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
    datetime.time(3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=2))),
    uuid.UUID("12345678-1234-5678-1234-567812345678"),
    datetime.timedelta(days=3, seconds=4, microseconds=5),
    b"\x00\x01\xff",
    datetime.datetime(1999, 12, 31, 23, 59, 59),
    datetime.date(1970, 1, 1),
)


@pytest.fixture(scope="module")
def wide_table(runtime):
    con = psycopg2.connect(runtime_dsn(runtime))
    con.autocommit = True
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS wide")
    cur.execute(
        "CREATE TABLE wide (si int2, r float4, jb jsonb, tz timestamptz, ttz timetz, u uuid, "
        "iv interval, ba bytea, old_ts timestamp, old_day date)"
    )
    cur.execute(
        "INSERT INTO wide VALUES (7, 1.5, '{\"k\": [1, 2]}', '2026-01-02 03:04:05+00', "
        "'03:04:05+02', '12345678-1234-5678-1234-567812345678', "
        "'3 days 00:00:04.000005', '\\x0001ff', '1999-12-31 23:59:59', '1970-01-01')"
    )
    con.close()
    return runtime


def runtime_dsn(runtime) -> str:
    return runtime._engine_dsn


@pytest.mark.asyncio
async def test_every_wide_type_reads_back_exactly_through_the_passthrough(wide_table, pgwire_port):
    from tests.pgwire_describe_parity import PassthroughEngine

    engine = PassthroughEngine(wide_table, runtime_dsn(wide_table))
    rows = await fetch_through_pgwire(pgwire_port, engine, _WIDE_SQL, _REGISTRY)
    assert rows == [_WIDE_EXPECTED]
    assert engine.passthrough == [_WIDE_SQL]  # forwarded raw, not refused
    assert engine.executed == []  # no decode/re-encode fallback ran


@pytest.mark.asyncio
async def test_every_wide_type_reads_back_exactly_when_re_encoded(wide_table, pgwire_port):
    engine = RuntimeEngine(wide_table, "duckdb")  # not "postgres": no passthrough
    rows = await fetch_through_pgwire(pgwire_port, engine, _WIDE_SQL, _REGISTRY)
    assert rows == [_WIDE_EXPECTED]
    assert engine.executed == [_WIDE_SQL]
