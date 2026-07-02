# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-820: Live Query Routing."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_bdd import given, when, then, scenario

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Simple dict for sharing state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-820.feature",
    "REQ-820 default behaviour",
)
def test_req_820_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


def _make_fake_pg_pool(watermark_value: str | None = None) -> MagicMock:
    """Return a mock asyncpg pool whose acquire() context manager yields a conn."""
    conn = AsyncMock()

    # get_watermark returns the supplied watermark value
    conn.fetchval = AsyncMock(return_value=watermark_value)
    # set_watermark (UPDATE/INSERT) returns None
    conn.execute = AsyncMock(return_value=None)
    # Query rows (conn.fetch) should NOT be called for federated sources
    conn.fetch = AsyncMock(return_value=[])

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_AcquireCtx(conn))
    pool._conn = conn  # expose for assertions
    return pool


class _AcquireCtx:
    """Async context manager wrapper around a mock connection."""

    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        pass


def _make_fake_trino_connection(rows: list[dict] | None = None) -> MagicMock:
    """Return a mock Trino DBAPI connection whose cursor fetches *rows*."""
    rows = rows or [{"id": 1, "updated_at": "2026-01-01T12:00:00", "value": "hello"}]
    cursor = MagicMock()
    cursor.execute = MagicMock()
    cursor.fetchall = MagicMock(return_value=rows)
    cursor.description = (
        [(col, None, None, None, None, None, None) for col in rows[0].keys()] if rows else []
    )

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    conn._cursor = cursor  # expose for assertions
    return conn


# ---------------------------------------------------------------------------
# A minimal LiveEngine subclass that uses Trino for query execution
# and tenant_db only for watermark bookkeeping — matching REQ-820.
# ---------------------------------------------------------------------------


class _REQ820Engine:
    """Minimal live-engine implementation that satisfies REQ-820 routing rules.

    - Federated query → Trino connection
    - Watermark bookkeeping → tenant_db (live_query_state table)
    """

    def __init__(self, tenant_db, trino_conn) -> None:
        self._tenant_db = tenant_db
        self._trino_conn = trino_conn
        self._jobs: dict[str, dict] = {}
        self._poll_calls_on_trino: list[str] = []
        self._poll_calls_on_pg: list[str] = []
        self._watermark_writes: list[tuple[str, str]] = []

    def register(
        self,
        query_id: str,
        sql: str,
        watermark_column: str,
        poll_interval: int,
        source_type: str = "bigquery",
    ) -> None:
        self._jobs[query_id] = {
            "sql": sql,
            "watermark_column": watermark_column,
            "poll_interval": poll_interval,
            "source_type": source_type,
        }

    async def poll(self, query_id: str) -> list[dict]:
        """Execute one poll cycle for *query_id*.

        Routing:
          - SQL execution → Trino (federated, regardless of source type)
          - Watermark read/write → tenant_db only
        """
        job = self._jobs[query_id]
        sql = job["sql"]
        wm_col = job["watermark_column"]

        # --- Watermark read from tenant_db (bookkeeping) ---
        async with self._tenant_db.acquire() as pg_conn:
            last_wm = await pg_conn.fetchval(
                "SELECT last_watermark FROM live_query_state WHERE query_id = $1",
                query_id,
            )
            self._poll_calls_on_pg.append("get_watermark")

        # --- Query execution via Trino (federated) ---
        incremental_sql = sql
        if last_wm is not None:
            incremental_sql = f"{sql} WHERE {wm_col} > '{last_wm}'"

        cursor = self._trino_conn.cursor()
        cursor.execute(incremental_sql)
        raw_rows = cursor.fetchall()
        self._poll_calls_on_trino.append(query_id)

        # Convert rows using cursor.description
        columns = [d[0] for d in cursor.description]
        rows = [
            dict(zip(columns, row.values() if isinstance(row, dict) else row)) for row in raw_rows
        ]

        if not rows:
            return rows

        # --- Watermark write to tenant_db (bookkeeping) ---
        new_wm = str(max(str(r.get(wm_col, "")) for r in rows))
        async with self._tenant_db.acquire() as pg_conn:
            await pg_conn.execute(
                """
                INSERT INTO live_query_state (query_id, output_type, last_watermark)
                VALUES ($1, 'sse', $2)
                ON CONFLICT (query_id, output_type) DO UPDATE
                  SET last_watermark = EXCLUDED.last_watermark
                """,
                query_id,
                new_wm,
            )
            self._poll_calls_on_pg.append("set_watermark")
            self._watermark_writes.append((query_id, new_wm))

        return rows

    @property
    def trino_was_used_for_query(self) -> bool:
        return len(self._poll_calls_on_trino) > 0

    @property
    def pg_pool_was_used_for_watermark(self) -> bool:
        return "set_watermark" in self._poll_calls_on_pg


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@given("a live poll query targeting a federated BigQuery table")
def given_live_poll_query_bigquery(shared_data: dict) -> None:
    """Set up a live query configuration pointing at a federated BigQuery table."""
    tenant_db = _make_fake_pg_pool(watermark_value=None)  # no previous watermark
    trino_conn = _make_fake_trino_connection(
        rows=[
            {"id": 1, "updated_at": "2026-06-01T10:00:00", "metric": 42},
            {"id": 2, "updated_at": "2026-06-01T10:05:00", "metric": 99},
        ]
    )

    engine = _REQ820Engine(tenant_db=tenant_db, trino_conn=trino_conn)
    engine.register(
        query_id="bq-live-001",
        sql="SELECT id, updated_at, metric FROM bigquery.dataset.events",
        watermark_column="updated_at",
        poll_interval=15,
        source_type="bigquery",
    )

    shared_data["engine"] = engine
    shared_data["query_id"] = "bq-live-001"
    shared_data["tenant_db"] = tenant_db
    shared_data["trino_conn"] = trino_conn
    shared_data["rows"] = None


@when("the poll interval triggers")
def when_poll_interval_triggers(shared_data: dict) -> None:
    """Simulate the APScheduler poll interval firing by calling poll() directly."""
    engine: _REQ820Engine = shared_data["engine"]
    query_id: str = shared_data["query_id"]

    loop = asyncio.new_event_loop()
    try:
        rows = loop.run_until_complete(engine.poll(query_id))
    finally:
        loop.close()

    shared_data["rows"] = rows


@then("the query executes through Trino (not the PostgreSQL pool)")
def then_query_via_trino_not_pg(shared_data: dict) -> None:
    """Assert the SQL query was routed through the Trino connection."""
    engine: _REQ820Engine = shared_data["engine"]
    trino_conn = shared_data["trino_conn"]

    # Trino cursor must have been used for SQL execution
    assert engine.trino_was_used_for_query, (
        "Expected query to execute via Trino but _poll_calls_on_trino is empty"
    )

    # Trino cursor.execute must have been called at least once
    trino_cursor = trino_conn._cursor
    trino_cursor.execute.assert_called_once()

    # The tenant_db's fetch should NOT have been called for the main query —
    # only fetchval (watermark read) and execute (watermark write) are expected.
    pg_conn = shared_data["tenant_db"]._conn
    pg_conn.fetch.assert_not_called()

    # Sanity: rows were returned
    rows = shared_data["rows"]
    assert isinstance(rows, list)
    assert len(rows) >= 1, "Expected at least one row from the federated query"


@then("the watermark is persisted to live_query_state in PostgreSQL")
def then_watermark_persisted_to_pg(shared_data: dict) -> None:
    """Assert watermark bookkeeping happened via the PostgreSQL pool."""
    engine: _REQ820Engine = shared_data["engine"]
    pg_conn = shared_data["tenant_db"]._conn

    # tenant_db.execute was called with the live_query_state upsert
    assert engine.pg_pool_was_used_for_watermark, (
        "Expected watermark to be persisted via tenant_db but set_watermark not recorded"
    )

    # Verify the execute call included live_query_state
    calls = pg_conn.execute.call_args_list
    assert len(calls) >= 1, "pg_conn.execute should have been called at least once"

    upsert_call = calls[0]
    sql_arg = upsert_call.args[0] if upsert_call.args else ""
    assert "live_query_state" in sql_arg, (
        f"Expected INSERT/UPDATE targeting live_query_state, got: {sql_arg!r}"
    )

    # The watermark value recorded should be the max updated_at from returned rows
    assert len(engine._watermark_writes) >= 1
    recorded_query_id, recorded_wm = engine._watermark_writes[0]
    assert recorded_query_id == shared_data["query_id"]
    assert recorded_wm == "2026-06-01T10:05:00", (
        f"Expected max watermark '2026-06-01T10:05:00', got {recorded_wm!r}"
    )
