# Copyright (c) 2026 Kenneth Stott
# Canary: 89dcf133-c814-4b97-8688-f54f8fa76f7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for auto-installed PostgreSQL notify triggers and
Debezium CDC subscriptions.

REQ-565 — At startup, Provisa idempotently installs ``AFTER INSERT OR UPDATE OR
DELETE`` triggers on all registered PostgreSQL subscription tables. Each trigger
calls ``pg_notify('provisa_{table}', json_build_object('op', lower(TG_OP),
'row', ...))`` so that raw DML from any writer is picked up by SSE subscriptions.

REQ-566 — When PostgreSQL trigger installation fails (e.g. insufficient
privilege), Provisa logs a warning and falls back to watermark-based polling for
that table, provided ``watermark_column`` is configured. Tables where
installation succeeds are tracked in-memory to select the LISTEN/NOTIFY path.

REQ-567 — When a subscription field selects columns from joined tables via
registered relationships, the subscription engine collects all physical table
names referenced by the join walk and calls ``watch_many(all_watch_tables)`` on
the PG notification provider. A change to any joined physical table re-fires the
subscription query.

REQ-260 — Polling-based subscription provider for sources without native CDC. A
``watermark_column`` (monotonic timestamp or sequence, e.g. ``updated_at``) must
be declared on the table config. Without a ``watermark_column``, poll
subscriptions are unavailable for that source. New or updated rows since the last
watermark value are delivered to the subscriber on each poll interval.

REQ-261 — Debezium CDC subscription provider for non-PG RDBMS sources (MySQL,
MariaDB, SQL Server, Oracle). Debezium captures changes from the source's
transaction log and publishes to Kafka topics. Provisa's Kafka notification
provider consumes these CDC events and streams them as SSE subscriptions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from graphql import parse as gql_parse
from pytest_bdd import given, scenarios, then, when

from provisa.api.data.subscription_sse import _collect_related_tables
from provisa.subscriptions.base import ChangeEvent
from provisa.subscriptions.debezium_provider import (
    _OP_MAP,
    DebeziumNotificationProvider,
)
from provisa.subscriptions.pg_provider import CHANNEL_PREFIX, PgNotificationProvider
from provisa.subscriptions.pg_triggers import (
    _trigger_sql,
    ensure_pg_notify_triggers,
)

scenarios("../features/REQ-565.feature")
scenarios("../features/REQ-566.feature")
scenarios("../features/REQ-567.feature")
scenarios("../features/REQ-260.feature")
scenarios("../features/REQ-261.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


def _pg_env() -> dict:
    return dict(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )


async def _make_pool():
    import asyncpg  # noqa: PLC0415

    env = _pg_env()
    return await asyncio.wait_for(
        asyncpg.create_pool(
            host=env["host"],
            port=env["port"],
            database=env["database"],
            user=env["user"],
            password=env["password"],
            min_size=1,
            max_size=3,
            command_timeout=10,
        ),
        timeout=5.0,
    )


# ---------------------------------------------------------------------------
# Unit-level verification: trigger SQL is well-formed and idempotent
# ---------------------------------------------------------------------------

def test_trigger_sql_is_idempotent_and_notifies() -> None:
    """The generated trigger SQL must use CREATE OR REPLACE / DROP IF EXISTS
    and call pg_notify on the provisa channel for INSERT/UPDATE/DELETE."""
    sql = _trigger_sql("public", "orders")
    channel = f"{CHANNEL_PREFIX}orders"

    # Idempotency primitives.
    assert "CREATE OR REPLACE FUNCTION" in sql
    assert "DROP TRIGGER IF EXISTS" in sql

    # Fires on all DML.
    assert "AFTER INSERT OR UPDATE OR DELETE ON public.orders" in sql
    assert "FOR EACH ROW" in sql

    # Notifies on the table-derived channel with op + row payload.
    assert f"pg_notify(\n    '{channel}'" in sql
    assert "lower(TG_OP)" in sql
    assert "json_build_object" in sql
    assert "row_to_json(OLD)" in sql
    assert "row_to_json(NEW)" in sql


# ---------------------------------------------------------------------------
# REQ-565 — Given
# ---------------------------------------------------------------------------

@given("Provisa has started and registered a PostgreSQL subscription table")
@pytest.mark.integration
def given_provisa_registered_pg_table(shared_data: dict) -> None:
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _setup() -> None:
        pool = await _make_pool()
        schema = "public"
        table = f"provisa_req565_{uuid.uuid4().hex[:8]}"

        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE TABLE {schema}.{table} "
                f"(id integer PRIMARY KEY, amount numeric)"
            )

            source_id = "src-pg"
            tables = [
                {
                    "source_id": source_id,
                    "schema_name": schema,
                    "table_name": table,
                }
            ]
            source_types = {source_id: "postgresql"}

            installed = await ensure_pg_notify_triggers(conn, tables, source_types)
            assert table in installed, "trigger must be installed on the registered table"

            # Idempotent: installing again must succeed and remain installed.
            installed_again = await ensure_pg_notify_triggers(
                conn, tables, source_types
            )
            assert table in installed_again

        shared_data["pool"] = pool
        shared_data["schema"] = schema
        shared_data["table"] = table

    asyncio.get_event_loop().run_until_complete(_setup())


# ---------------------------------------------------------------------------
# REQ-565 — When
# ---------------------------------------------------------------------------

@when("an external process inserts a row directly into the table")
@pytest.mark.integration
def when_external_insert(shared_data: dict) -> None:
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    pool = shared_data["pool"]
    schema = shared_data["schema"]
    table = shared_data["table"]
    provider = PgNotificationProvider(pool)
    received: list[ChangeEvent] = []

    async def _run() -> None:
        async def _consume() -> None:
            async for event in provider.watch(table):
                received.append(event)
                break

        task = asyncio.create_task(_consume())
        await asyncio.sleep(0.3)  # allow LISTEN to register

        # External writer: plain INSERT, no manual pg_notify call.
        async with pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {schema}.{table} (id, amount) VALUES ($1, $2)",
                7,
                42.5,
            )

        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            pytest.fail("SSE subscriber did not receive the change event in time")

    asyncio.get_event_loop().run_until_complete(_run())
    shared_data["received"] = received


# ---------------------------------------------------------------------------
# REQ-565 — Then
# ---------------------------------------------------------------------------

@then("the trigger fires pg_notify and the SSE subscriber receives the change event")
@pytest.mark.integration
def then_subscriber_receives_event(shared_data: dict) -> None:
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    received = shared_data["received"]
    table = shared_data["table"]
    pool = shared_data["pool"]
    schema = shared_data["schema"]

    try:
        assert len(received) == 1, "exactly one change event expected"
        evt = received[0]
        assert evt.operation == "insert"
        assert evt.table == table
        assert evt.row["id"] == 7
        assert float(evt.row["amount"]) == 42.5
    finally:
        async def _cleanup() -> None:
            async with pool.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            await pool.close()

        asyncio.get_event_loop().run_until_complete(_cleanup())


# ---------------------------------------------------------------------------
# REQ-566 — Graceful fallback to watermark polling on trigger install failure
# ---------------------------------------------------------------------------

class _PrivilegeError(Exception):
    """Stand-in for an asyncpg InsufficientPrivilegeError."""


class _FailingConn:
    """Connection whose execute() raises, simulating lack of CREATE privilege."""

    def __init__(self) -> None:
        self.attempts: list[str] = []

    async def execute(self, sql: str, *args) -> None:
        self.attempts.append(sql)
        raise _PrivilegeError(
            "permission denied: must be owner of relation to create trigger"
        )


class _LogCapture(logging.Handler):
    """Capture log records emitted by the pg_triggers logger."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@given(
    "a PostgreSQL table where Provisa lacks trigger creation privileges"
)
def given_pg_table_without_privileges(shared_data: dict) -> None:
    source_id = "src-pg-restricted"
    schema = "public"
    table = f"provisa_req566_{uuid.uuid4().hex[:8]}"

    # The table is configured with a watermark_column so polling is possible.
    shared_data["conn"] = _FailingConn()
    shared_data["tables"] = [
        {
            "source_id": source_id,
            "schema_name": schema,
            "table_name": table,
            "watermark_column": "updated_at",
        }
    ]
    shared_data["source_types"] = {source_id: "postgresql"}
    shared_data["schema"] = schema
    shared_data["table"] = table


@when("Provisa starts up")
def when_provisa_starts_up(shared_data: dict) -> None:
    log = logging.getLogger("provisa.subscriptions.pg_triggers")
    capture = _LogCapture()
    prev_level = log.level
    log.setLevel(logging.DEBUG)
    log.addHandler(capture)

    async def _run() -> set[str]:
        return await ensure_pg_notify_triggers(
            shared_data["conn"],
            shared_data["tables"],
            shared_data["source_types"],
        )

    try:
        installed = asyncio.get_event_loop().run_until_complete(_run())
    finally:
        log.removeHandler(capture)
        log.setLevel(prev_level)

    shared_data["installed"] = installed
    shared_data["log_records"] = capture.records


@then(
    "it logs a warning and uses watermark-based polling for that table "
    "instead of LISTEN/NOTIFY"
)
def then_warning_logged_and_polling_used(shared_data: dict) -> None:
    table = shared_data["table"]
    installed: set[str] = shared_data["installed"]
    records: list[logging.LogRecord] = shared_data["log_records"]

    # The failed table is NOT tracked as trigger-installed -> not on LISTEN path.
    assert table not in installed

    # A warning (or higher) must have been logged about the failure.
    warnings = [r for r in records if r.levelno >= logging.WARNING]
    assert warnings, "expected a warning log record on trigger install failure"

    # The warning message must reference the fallback to polling.
    assert any(
        "fall back to polling" in (r.getMessage() or "").lower()
        for r in warnings
    ), "warning must mention polling fallback"

    # The conn was actually exercised (an execute attempt was made and failed).
    assert shared_data["conn"].attempts, "trigger install should have been attempted"

    # Watermark-based polling is available because watermark_column is configured.
    assert shared_data["tables"][0]["watermark_column"] == "updated_at"


# ---------------------------------------------------------------------------
# REQ-567 — Joined subscription tables are all watched
# ---------------------------------------------------------------------------

@given("a subscription that selects columns from a joined relationship")
def given_subscription_with_join(shared_data: dict) -> None:
    # GraphQL subscription selecting fields from a related table via relationship.
    query = """
    subscription {
      orders {
        id
        amount
        customer {
          id
          name
        }
      }
    }
    """
    shared_data["document"] = gql_parse(query)
    # Registered relationships mapping a logical field to a physical table.
    shared_data["root_table"] = "orders"
    shared_data["relationships"] = {
        ("orders", "customer"): "customers",
    }


@when("the subscription engine resolves the physical tables to watch")
def when_resolve_watch_tables(shared_data: dict) -> None:
    related = _collect_related_tables(
        shared_data["document"],
        shared_data["root_table"],
        shared_data["relationships"],
    )
    watch_tables = set(related) | {shared_data["root_table"]}
    shared_data["watch_tables"] = watch_tables


@then(
    "it watches every physical table referenced by the join so any change "
    "re-fires the query"
)
def then_all_join_tables_watched(shared_data: dict) -> None:
    watch_tables = shared_data["watch_tables"]
    assert "orders" in watch_tables, "root table must be watched"
    assert "customers" in watch_tables,
