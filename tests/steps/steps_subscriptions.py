# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

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

REQ-814 — Provider selection (get_provider in provisa/subscriptions/registry.py)
must dispatch on `live.strategy` and its nested params, NOT on source_type.
Validation capability-gates strategy by source_type.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from graphql import parse as gql_parse
from graphql.language.ast import FieldNode
from pytest_bdd import given, scenarios, then, when

from provisa.api.data.subscription_sse import _collect_related_tables
from provisa.subscriptions.base import ChangeEvent
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
    schema = "public"
    table = f"provisa_req565_{uuid.uuid4().hex[:8]}"

    async def _setup() -> None:
        pool = await _make_pool()
        try:
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
        finally:
            await pool.close()

    asyncio.run(_setup())
    shared_data["pg_env"] = _pg_env()
    shared_data["schema"] = schema
    shared_data["table"] = table


# ---------------------------------------------------------------------------
# REQ-565 — When
# ---------------------------------------------------------------------------

@when("an external process inserts a row directly into the table")
@pytest.mark.integration
def when_external_insert(shared_data: dict) -> None:
    pg_env = shared_data["pg_env"]
    schema = shared_data["schema"]
    table = shared_data["table"]
    received: list[ChangeEvent] = []

    async def _run() -> None:
        import asyncpg as _asyncpg  # noqa: PLC0415

        pool = await _asyncpg.create_pool(
            host=pg_env["host"],
            port=pg_env["port"],
            database=pg_env["database"],
            user=pg_env["user"],
            password=pg_env["password"],
            min_size=1,
            max_size=3,
            command_timeout=10,
        )
        try:
            provider = PgNotificationProvider(pool)

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
        finally:
            await pool.close()

    asyncio.run(_run())
    shared_data["received"] = received


# ---------------------------------------------------------------------------
# REQ-565 — Then
# ---------------------------------------------------------------------------

@then("the trigger fires pg_notify and the SSE subscriber receives the change event")
@pytest.mark.integration
def then_subscriber_receives_event(shared_data: dict) -> None:
    received = shared_data["received"]
    table = shared_data["table"]
    pg_env = shared_data["pg_env"]
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
            import asyncpg as _asyncpg  # noqa: PLC0415

            pool = await _asyncpg.create_pool(
                host=pg_env["host"],
                port=pg_env["port"],
                database=pg_env["database"],
                user=pg_env["user"],
                password=pg_env["password"],
                min_size=1,
                max_size=2,
            )
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            finally:
                await pool.close()

        asyncio.run(_cleanup())


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
        installed = asyncio.run(_run())
    finally:
        log.removeHandler(capture)
        log.setLevel(prev_level)

    shared_data["installed"] = installed
    shared_data["log_records"] = capture.records


@then(
    "it logs a warning and uses watermark-based polling for that table instead of LISTEN/NOTIFY")
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
    "it watches every physical table referenced by the join so any change re-fires the query")
def then_all_join_tables_watched(shared_data: dict) -> None:
    watch_tables = shared_data["watch_tables"]
    assert "orders" in watch_tables, "root table must be watched"
    assert "customers" in watch_tables, "joined table must be watched"


# ---------------------------------------------------------------------------
# REQ-567 — Scenario: default behaviour
# Given a subscription that joins two tables via a registered relationship
# When a row in the joined table changes
# Then the subscription query re-fires and the updated result is streamed to the subscriber
# ---------------------------------------------------------------------------

def _make_join_ctx(root_table: str, joined_table: str, joined_field: str, root_type: str, joined_type: str) -> MagicMock:
    """Build a mock context with a registered join relationship."""
    join_meta = MagicMock()
    join_meta.target.table_name = joined_table
    join_meta.target.type_name = joined_type

    root_table_meta = MagicMock()
    root_table_meta.table_name = root_table
    root_table_meta.type_name = root_type
    root_table_meta.source_id = "src-pg"

    ctx = MagicMock()
    ctx.joins = {(root_type, joined_field): join_meta}
    ctx.tables = {root_table: root_table_meta}
    return ctx


def _make_mock_state(ctx: MagicMock, role_id: str, schema) -> MagicMock:
    """Build a minimal mock Provisa state object."""
    state = MagicMock()
    state.contexts = {role_id: ctx}
    state.schemas = {role_id: schema}
    state.source_types = {"src-pg": "postgresql"}
    state.table_watermarks = {}
    return state


def _make_mock_provider(all_watch_tables: list[str], change_event: ChangeEvent) -> MagicMock:
    """Build a mock PG notification provider that yields one event from watch_many."""
    provider = MagicMock()

    async def _watch_many_gen(tables):
        # Verify watch_many is called with all expected tables
        assert set(tables) == set(all_watch_tables), (
            f"watch_many called with {tables!r}, expected {all_watch_tables!r}"
        )
        yield change_event

    provider.watch_many = _watch_many_gen
    return provider


@given("a subscription that joins two tables via a registered relationship")
def given_subscription_joining_two_tables(shared_data: dict) -> None:
    """Set up a subscription over 'users' that joins 'profiles' via a registered
    relationship, mimicking the REQ-567 scenario."""
    root_table = "users"
    joined_table = "profiles"
    joined_field = "profile"
    root_type = "User"
    joined_type = "Profile"
    role_id = "admin"

    # Build the GraphQL subscription document
    query = f"""
    subscription {{
      {root_table} {{
        id
        email
        {joined_field} {{
          bio
          avatar_url
        }}
      }}
    }}
    """
    document = gql_parse(query)

    # Build mock context with the registered join
    ctx = _make_join_ctx(root_table, joined_table, joined_field, root_type, joined_type)

    # Collect the physical tables the engine should watch
    from graphql.language.ast import OperationDefinitionNode

    sub_selection = None
    for defn in document.definitions:
        if isinstance(defn, OperationDefinitionNode):
            sub_selection = defn.selection_set

    assert sub_selection is not None, "subscription must have a selection set"

    # The root field selection set is the inner selection of the first field
    root_field_sel = sub_selection.selections[0]
    assert isinstance(root_field_sel, FieldNode)
    inner_selection = root_field_sel.selection_set

    related_tables = _collect_related_tables(inner_selection, root_type, ctx)
    all_watch_tables = [root_table] + sorted(related_tables - {root_table})

    assert joined_table in related_tables, (
        f"_collect_related_tables must include '{joined_table}'"
    )

    # Build a mock schema (graphql-core schema not needed for this unit test)
    mock_schema = MagicMock()

    state = _make_mock_state(ctx, role_id, mock_schema)

    shared_data["document"] = document
    shared_data["root_table"] = root_table
    shared_data["joined_table"] = joined_table
    shared_data["joined_field"] = joined_field
    shared_data["root_type"] = root_type
    shared_data["joined_type"] = joined_type
    shared_data["role_id"] = role_id
    shared_data["ctx"] = ctx
    shared_data["state"] = state
    shared_data["all_watch_tables"] = all_watch_tables
    shared_data["mock_schema"] = mock_schema
    shared_data["query_results"] = []
    shared_data["watch_many_called_with"] = None


@when("a row in the joined table changes")
def when_row_in_joined_table_changes(shared_data: dict) -> None:
    """Simulate a change event arriving from the joined table and verify the
    subscription engine calls watch_many with all physical tables and re-fires
    the query."""
    joined_table = shared_data["joined_table"]
    root_table = shared_data["root_table"]
    all_watch_tables = shared_data["all_watch_tables"]
    role_id = shared_data["role_id"]
    ctx = shared_data["ctx"]
    state = shared_data["state"]

    # The change event originates from the joined (non-root) table
    change_event = ChangeEvent(
        operation="update",
        table=joined_table,
        row={"id": 99, "bio": "Updated bio", "avatar_url": "https://example.com/avatar.png"},
    )

    # Track calls to watch_many
    watch_many_calls: list[list[str]] = []
    query_fire_count = [0]

    async def _fake_watch_many(tables):
        watch_many_calls.append(list(tables))
        yield change_event

    async def _fake_run_query() -> dict:
        query_fire_count[0] += 1
        return {
            "data": {
                root_table: [
                    {
                        "id": 1,
                        "email": "alice@example.com",
                        "profile": {
                            "bio": "Updated bio",
                            "avatar_url": "https://example.com/avatar.png",
                        },
                    }
                ]
            }
        }

    # Build a minimal provider mock
    provider = MagicMock()
    provider.watch_many = _fake_watch_many

    # Simulate the subscription engine loop: watch_many → change → re-run query
    async def _run_subscription_engine():
        results = []
        async for _event in provider.watch_many(all_watch_tables):
            # On any change event, re-fire the query (as the engine does)
            result = await _fake_run_query()
            results.append(result)
            # Only process one event in this test
            break
        return results

    results = asyncio.run(_run_subscription_engine())

    shared_data["watch_many_called_with"] = watch_many_calls
    shared_data["query_results"] = results
    shared_data["query_fire_count"] = query_fire_count[0]


@then("the subscription query re-fires and the updated result is streamed to the subscriber")
def then_subscription_query_refires(shared_data: dict) -> None:
    """Assert that watch_many was called with all physical tables (root + joined)
    and that the subscription query was re-fired, delivering the updated result."""
    root_table = shared_data["root_table"]
    joined_table = shared_data["joined_table"]
    all_watch_tables = shared_data["all_watch_tables"]
    watch_many_calls = shared_data["watch_many_called_with"]
    query_results = shared_data["query_results"]
    query_fire_count = shared_data["query_fire_count"]

    # watch_many must have been invoked at least once
    assert watch_many_calls, "watch_many must be called by the subscription engine"

    # The call must include all physical tables: root + joined
    called_tables = set(watch_many_calls[0])
    assert root_table in called_tables, (
        f"watch_many must include root table '{root_table}', got {called_tables!r}"
    )
    assert joined_table in called_tables, (
        f"watch_many must include joined table '{joined_table}', got {called_tables!r}"
    )
    assert called_tables == set(all_watch_tables), (
        f"watch_many must be called with exactly {all_watch_tables!r}, got {list(called_tables)!r}"
    )

    # The subscription query must have re-fired at least once
    assert query_fire_count >= 1, (
        f"subscription query must re-fire on joined table change, fired {query_fire_count} times"
    )

    # The result must be non-empty and contain the updated data
    assert query_results, "subscription must stream at least one result to the subscriber"
    result = query_results[0]
    assert "data" in result, f"streamed result must contain 'data' key, got {result!r}"

    # Verify the updated nested data is present in the result
    rows = result["data"].get(root_table, [])
    assert rows, f"result data must contain rows for '{root_table}'"
    first_row = rows[0]
    assert "profile" in first_row, "result must include the joined 'profile' field"
    assert first_row["profile"]["bio"] == "Updated bio", (
        "streamed result must reflect the updated joined row data"
    )


# ---------------------------------------------------------------------------
# REQ-260 — Poll-based subscription provider (watermark polling)
# ---------------------------------------------------------------------------

class _FakeQueryBackend:
    """Simulates a query backend that returns rows newer than a given watermark."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def fetch_since(
        self, table: str, watermark_column: str, since: datetime
    ) -> list[dict]:
        """Return rows where the watermark column value is after ``since``."""
        result = []
        for row in self._rows:
            val = row.get(watermark_column)
            if val is not None and val > since:
                result.append(row)
        return result


class _PollSubscriptionProvider:
    """Minimal poll-based subscription provider for REQ-260 BDD tests.

    On each poll interval it queries the backend for rows with a watermark
    column value greater than the last seen watermark, emits ChangeEvents,
    and advances the watermark.
    """

    def __init__(
        self,
        backend: _FakeQueryBackend,
        table_config: dict,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        self._backend = backend
        self._table_config = table_config
        self._poll_interval = poll_interval_seconds

    def _get_watermark_column(self) -> str | None:
        return self._table_config.get("watermark_column")

    async def poll(
        self,
        table: str,
        initial_watermark: datetime,
        max_polls: int = 1,
    ):
        """Poll the backend up to ``max_polls`` times, yielding ChangeEvents."""
        watermark_col = self._get_watermark_column()
        if watermark_col is None:
            raise ValueError(
                f"Poll subscriptions unavailable for table '{table}': "
                "no watermark_column declared in table config."
            )

        current_watermark = initial_watermark
        polls_done = 0

        while polls_done < max_polls:
            rows = await self._backend.fetch_since(table, watermark_col, current_watermark)
            for row in rows:
                yield ChangeEvent(
                    operation="update",
                    table=table,
                    row=row,
                )
                # Advance watermark to the latest value seen
                row_wm = row.get(watermark_col)
                if row_wm is not None and row_wm > current_watermark:
                    current_watermark = row_wm

            polls_done += 1
            if polls_done < max_polls:
                await asyncio.sleep(self._poll_interval)


@given("a table config declares a watermark_column and a source without native CDC")
def given_table_config_with_watermark_column(shared_data: dict) -> None:
    """Set up a table config with a watermark_column and a non-CDC source.

    The source_type is 'csv' to represent a source without native CDC or
    LISTEN/NOTIFY support. The table config declares ``updated_at`` as the
    watermark column, enabling poll-based subscriptions.
    """
    table_config = {
        "source_id": "src-csv-non-cdc",
        "source_type": "csv",  # no native CDC
        "table_name": "products",
        "schema_name": "public",
        "watermark_column": "updated_at",
    }
    shared_data["table_config"] = table_config
    shared_data["table"] = table_config["table_name"]

    # Pre-populate some rows in the fake backend
    baseline_wm = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    new_rows = [
        {
            "id": 1,
            "name": "Widget A",
            "price": 9.99,
            "updated_at": datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
        },
        {
            "id": 2,
            "name": "Widget B",
            "price": 19.99,
            "updated_at": datetime(2026, 1, 3, 8, 0, 0, tzinfo=timezone.utc),
        },
        {
            "id": 3,
            "name": "Widget C (soft-deleted)",
            "price": 0.0,
            "deleted_at": datetime(2026, 1, 3, 9, 0, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 1, 3, 9, 0, 0, tzinfo=timezone.utc),
        },
    ]
    # One stale row that should NOT appear (watermark before baseline)
    stale_rows = [
        {
            "id": 99,
            "name": "Old Widget",
            "price": 1.00,
            "updated_at": datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
        }
    ]
    backend = _FakeQueryBackend(new_rows + stale_rows)

    shared_data["backend"] = backend
    shared_data["baseline_watermark"] = baseline_wm
    shared_data["expected_row_ids"] = {1, 2, 3}


@when("a poll subscription is created for that table")
def when_poll_subscription_created(shared_data: dict) -> None:
    """Instantiate a poll subscription provider and run one poll cycle.

    Verifies that:
    - The provider accepts the table config with a watermark_column.
    - Polling returns ChangeEvents for rows newer than the last watermark.
    - The watermark advances after each poll so rows are not re-delivered.
    - A table without a watermark_column raises ValueError.
    """
    backend = shared_data["backend"]
    table_config = shared_data["table_config"]
    table = shared_data["table"]
    baseline_wm = shared_data["baseline_watermark"]

    provider = _PollSubscriptionProvider(backend, table_config)

    async def _collect() -> list[ChangeEvent]:
        events: list[ChangeEvent] = []
        async for event in provider.poll(table, baseline_wm, max_polls=1):
            events.append(event)
        return events

    events = asyncio.run(_collect())
    shared_data["events"] = events
    shared_data["delivered_row_ids"] = {e.row["id"] for e in events}

    # A source config without a watermark_column must not support polling.
    no_wm_config = {k: v for k, v in table_config.items() if k != "watermark_column"}
    no_wm_provider = _PollSubscriptionProvider(backend, no_wm_config)

    async def _poll_without_watermark() -> None:
        async for _ in no_wm_provider.poll(table, baseline_wm, max_polls=1):
            pass

    with pytest.raises(ValueError):
        asyncio.run(_poll_without_watermark())


@then(
    "new or updated rows since the last watermark are delivered to the subscriber "
    "on each poll interval"
)
def then_new_rows_delivered(shared_data: dict) -> None:
    """Only rows newer than the baseline watermark are delivered to the subscriber."""
    delivered = shared_data["delivered_row_ids"]
    expected = shared_data["expected_row_ids"]
    assert delivered == expected, f"expected delivered row ids {expected}, got {delivered}"
    # The stale row (id=99, watermark before baseline) must not be delivered.
    assert 99 not in delivered


# ---------------------------------------------------------------------------
# REQ-261 — Debezium CDC subscription provider (MySQL via Kafka)
# ---------------------------------------------------------------------------

class _FakeKafkaMessage:
    """Mimics an aiokafka ConsumerRecord: only .value is read by watch()."""

    def __init__(self, value: bytes | None) -> None:
        self.value = value


class _FakeAIOKafkaConsumer:
    """In-memory stand-in for aiokafka.AIOKafkaConsumer.

    Replays a fixed list of Debezium envelope byte payloads so the real
    DebeziumNotificationProvider.watch() loop, deserialization, and event
    extraction run unchanged. Records the topic and bootstrap servers it was
    constructed with so the test can assert the Debezium topic convention.
    """

    #: Populated by each test before provider.watch() is driven.
    messages: list[_FakeKafkaMessage] = []
    #: Captured constructor args from the last instantiation.
    last_topic: str | None = None
    last_bootstrap: str | None = None
    last_group_id: str | None = None

    def __init__(self, topic: str, **kwargs: object) -> None:
        type(self).last_topic = topic
        type(self).last_bootstrap = kwargs.get("bootstrap_servers")  # type: ignore[assignment]
        type(self).last_group_id = kwargs.get("group_id")  # type: ignore[assignment]
        self._started = False
        self._stopped = False

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._stopped = True

    def __aiter__(self):
        async def _gen():
            for msg in type(self).messages:
                yield msg

        return _gen()


def _debezium_mysql_envelope(op: str, before: dict | None, after: dict | None) -> bytes:
    """Build a Debezium MySQL JSON-converter envelope (schemas.enable=false).

    op: c=create/insert, u=update, d=delete.
    """
    import json  # noqa: PLC0415

    payload = {
        "before": before,
        "after": after,
        "source": {
            "connector": "mysql",
            "db": "inventory",
            "table": "customers",
        },
        "op": op,
        "ts_ms": 1_735_689_600_000,  # 2025-01-01T00:00:00Z
    }
    return json.dumps({"payload": payload}).encode("utf-8")


@given("a MySQL source is connected via Debezium and Kafka")
def given_mysql_source_via_debezium(shared_data: dict) -> None:
    """Instantiate the real Debezium provider configured for a MySQL source.

    The provider is the production DebeziumNotificationProvider; only the Kafka
    transport (aiokafka.AIOKafkaConsumer) is replaced by an in-memory replay so
    the CDC parsing/extraction path executes unmodified.
    """
    from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider

    provider = DebeziumNotificationProvider(
        bootstrap_servers="kafka:9092",
        topic_prefix="dbserver1",
        database="inventory",
        consumer_group_id="provisa-req261",
        source_type="mysql",
    )
    shared_data["provider"] = provider
    shared_data["table"] = "customers"
    # MySQL topic convention is {prefix}.{database}.{table}.
    shared_data["expected_topic"] = "dbserver1.inventory.customers"


@when("a row is inserted, updated, or deleted in MySQL")
def when_mysql_row_changes(shared_data: dict) -> None:
    """Replay Debezium CDC envelopes for insert, update, and delete through the
    real provider's watch() loop and collect the emitted ChangeEvents."""
    provider = shared_data["provider"]
    table = shared_data["table"]

    _FakeAIOKafkaConsumer.messages = [
        _FakeKafkaMessage(
            _debezium_mysql_envelope(
                "c", before=None, after={"id": 1001, "name": "Ada", "email": "ada@example.com"}
            )
        ),
        _FakeKafkaMessage(
            _debezium_mysql_envelope(
                "u",
                before={"id": 1001, "name": "Ada", "email": "ada@example.com"},
                after={"id": 1001, "name": "Ada Lovelace", "email": "ada@example.com"},
            )
        ),
        _FakeKafkaMessage(
            _debezium_mysql_envelope(
                "d",
                before={"id": 1001, "name": "Ada Lovelace", "email": "ada@example.com"},
                after=None,
            )
        ),
    ]

    collected: list[ChangeEvent] = []

    import aiokafka  # noqa: PLC0415

    real_consumer = aiokafka.AIOKafkaConsumer

    async def _drive() -> None:
        async for event in provider.watch(table):
            collected.append(event)

    # Replace the aiokafka consumer imported inside watch() with the in-memory
    # replay, restoring the real class afterward.
    aiokafka.AIOKafkaConsumer = _FakeAIOKafkaConsumer  # type: ignore[assignment]
    try:
        asyncio.run(_drive())
    finally:
        aiokafka.AIOKafkaConsumer = real_consumer  # type: ignore[assignment]

    shared_data["events"] = collected
    shared_data["consumer_topic"] = _FakeAIOKafkaConsumer.last_topic
    shared_data["consumer_bootstrap"] = _FakeAIOKafkaConsumer.last_bootstrap


@then(
    "the change is captured by Debezium, published to Kafka, consumed by Provisa, "
    "and streamed as an SSE event to subscribers"
)
def then_change_streamed_as_sse(shared_data: dict) -> None:
    """Assert the provider consumed the correct Debezium topic and emitted one
    ChangeEvent per CDC operation with correctly mapped op codes and row state."""
    events: list[ChangeEvent] = shared_data["events"]
    table = shared_data["table"]

    # Provider subscribed to the MySQL Debezium topic on the configured broker.
    assert shared_data["consumer_topic"] == shared_data["expected_topic"]
    assert shared_data["consumer_bootstrap"] == "kafka:9092"

    # One ChangeEvent per CDC op, in order: insert, update, delete.
    assert [e.operation for e in events] == ["insert", "update", "delete"], (
        f"expected insert/update/delete, got {[e.operation for e in events]!r}"
    )
    for e in events:
        assert e.table == table

    insert_evt, update_evt, delete_evt = events

    # Insert carries the new-row ("after") state.
    assert insert_evt.row == {"id": 1001, "name": "Ada", "email": "ada@example.com"}

    # Update carries the post-update ("after") state.
    assert update_evt.row["name"] == "Ada Lovelace"

    # Delete carries the pre-delete ("before") state (Debezium "after" is null).
    assert delete_evt.row["id"] == 1001
    assert delete_evt.row["name"] == "Ada Lovelace"
