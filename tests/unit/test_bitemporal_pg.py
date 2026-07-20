# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1159: cross-engine behavioral validation on POSTGRES.

The DuckDB exec suite proves the approach on one engine; this runs the SAME generated SQL against
PostgreSQL to catch dialect drift (Postgres is strict where DuckDB is lenient — e.g. it refused a
CAST(NULL AS varchar) into an int column, which this suite is here to keep honest). Self-provisions
the postgres container via the shared fixture; skips only if it truly cannot be reached.
"""

from __future__ import annotations

import time

import pytest

from provisa.mv.bitemporal import (
    MODE_DELTA,
    MODE_SNAPSHOT,
    BitemporalSpec,
    append_sql,
    create_sql,
    reconstruct_as_of_sql,
)

from tests.unit.test_bitemporal_exec import STEP1, STEP2, STEP3, STEP4, T1, T2, T3, T4

psycopg2 = pytest.importorskip("psycopg2")

COLS = ["id", "region", "amount"]
SCHEMA = "bitemporal_test"
BASE = f'"{SCHEMA}"."base"'
TARGET = f'"{SCHEMA}"."mv_t"'
SELECT = f"SELECT id, region, amount FROM {BASE}"


def _connect_ready(dsn: str, timeout: float = 30.0):
    """Connect once Postgres actually accepts queries. A freshly-started container has its TCP port
    open before the server is ready, which surfaces as 'server closed the connection unexpectedly';
    retry a real SELECT until it answers rather than treating that startup window as a failure."""
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = psycopg2.connect(dsn, connect_timeout=3)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except psycopg2.OperationalError as exc:
            last = exc
            time.sleep(1.0)
    raise RuntimeError(f"postgres not ready within {timeout}s: {last}")


class PgDriver:
    """Same contract as the DuckDB Driver, executed on Postgres in an isolated schema."""

    def __init__(self, dsn: str, spec: BitemporalSpec):
        self.spec = spec
        self.conn = _connect_ready(dsn)  # already in autocommit
        self.exists = False
        with self.conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{SCHEMA}"')
            cur.execute(f"CREATE TABLE {BASE} (id INTEGER, region VARCHAR, amount INTEGER)")

    def refresh(self, rows: list[tuple], ts: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {BASE}")
            for r in rows:
                cur.execute(f"INSERT INTO {BASE} VALUES (%s, %s, %s)", r)
            now = f"TIMESTAMP '{ts}'"
            if not self.exists:
                cur.execute(create_sql(TARGET, SELECT, self.spec, now))
                self.exists = True
            else:
                for stmt in append_sql(TARGET, SELECT, self.spec, COLS, now, "postgres"):
                    cur.execute(stmt)

    def state(self, ts: str | None = None) -> set[tuple]:
        ts_sql = f"TIMESTAMP '{ts}'" if ts else None
        with self.conn.cursor() as cur:
            cur.execute(reconstruct_as_of_sql(TARGET, self.spec, COLS, ts_sql))
            return set(cur.fetchall())

    def close(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        self.conn.close()


@pytest.fixture()
def pg_conn(docker_postgres, pg_dsn):
    _ = docker_postgres  # depend on the fixture for its side effect (container up)
    return pg_dsn


def _story(dsn: str, mode: str) -> PgDriver:
    d = PgDriver(dsn, BitemporalSpec(key=("id",), mode=mode))
    for rows, ts in [(STEP1, T1), (STEP2, T2), (STEP3, T3), (STEP4, T4)]:
        d.refresh(rows, ts)
    return d


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_pg_current_state_after_full_story(pg_conn, mode):
    d = _story(pg_conn, mode)
    try:
        assert d.state() == {(1, "west", 15), (2, "east", 99), (3, "north", 30)}
    finally:
        d.close()


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_pg_as_of_history(pg_conn, mode):
    d = _story(pg_conn, mode)
    try:
        assert d.state(T1) == {(1, "west", 10), (2, "east", 20)}
        assert d.state(T2) == {(1, "west", 15), (2, "east", 20), (3, "north", 30)}
        assert d.state(T3) == {(1, "west", 15), (3, "north", 30)}
        assert d.state(T4) == {(1, "west", 15), (2, "east", 99), (3, "north", 30)}
    finally:
        d.close()


def test_pg_delta_tombstone_and_null_safe(pg_conn):
    """The two things Postgres is strict about: tombstone NULLs into typed columns, and NULL-safe
    equality. Both must work here (they exercise the bare-NULL tombstone and IS NOT DISTINCT FROM)."""
    d = PgDriver(pg_conn, BitemporalSpec(key=("id",), mode=MODE_DELTA))
    try:
        d.refresh([(1, "west", None)], T1)
        d.refresh([(1, "west", None)], T2)  # NULL stays NULL → no new version
        assert d.state() == {(1, "west", None)}
        d.refresh([], T3)  # id=1 removed → tombstone (bare NULL into int/varchar columns)
        assert d.state() == set()
        assert d.state(T1) == {(1, "west", None)}
    finally:
        d.close()
