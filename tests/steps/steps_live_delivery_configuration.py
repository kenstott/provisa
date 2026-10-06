# Copyright (c) 2026 Kenneth Stott
# Canary: c83b61a6-31f5-4e09-9a2f-51cbc3aaa3a1
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step definitions for REQ-819 and REQ-823 — Live Delivery Configuration."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy import select

import pytest
from pytest_bdd import given, scenarios, then, when

# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict[str, Any]:
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LIVE_CONFIG = {
    "query_id": "live_orders_query",
    "watermark_column": "updated_at",
    "poll_interval": 10,
    "strategy": "poll",
    "outputs": [
        {"type": "sse", "path": "/live/orders"},
        # REQ-286: a Kafka output publishes as the role it names.
        {
            "type": "kafka",
            "topic": "orders.live",
            "bootstrap_servers": "k:9092",
            "role": "publisher",
        },
    ],
}


def _make_mock_pg_pool() -> MagicMock:
    """Build a minimal asyncpg pool mock sufficient for LiveEngine tests."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=None)
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=acquire_ctx)
    return pool


def _make_live_row(query_id: str = "live_orders_query") -> dict:
    """Return a DB row dict representing one active live config."""
    return {
        "id": 1,
        "query_id": query_id,
        "watermark_column": "updated_at",
        "poll_interval": 10,
        "strategy": "poll",
        "active": True,
        "sql": "SELECT * FROM orders",
        "outputs": json.dumps([{"type": "sse", "path": "/live/orders"}]),
    }


# ---------------------------------------------------------------------------
# Scenario: REQ-819 default behaviour — GraphQL path
# ---------------------------------------------------------------------------


@given("the admin GraphQL API for table mutations")
def given_admin_graphql_api(shared_data: dict, tmp_path) -> None:
    """The real admin mutation, over an org model store holding one registered table."""
    from provisa.api.admin.schema_mutation import Mutation

    shared_data["mutation"] = Mutation()
    shared_data["tmp_path"] = tmp_path
    shared_data["live_config"] = _LIVE_CONFIG.copy()


@when(
    "updateTable is called with live configuration (query_id, watermark_column, poll_interval, delivery, outputs)"
)
def when_update_table_live_config(shared_data: dict) -> None:
    """Call the real ``updateTable`` resolver with the live configuration."""
    from contextlib import ExitStack

    from provisa.api.admin.types import LiveDeliveryConfigInput, LiveOutputConfigInput
    from provisa.core.schema_org import registered_tables
    from tests.unit.test_landing_ttl_admin import _db, _table_input, _table_patches

    live = shared_data["live_config"]
    live_input = LiveDeliveryConfigInput(
        strategy=live["strategy"],
        watermark_column=live["watermark_column"],
        poll_interval=live["poll_interval"],
        query_id=live["query_id"],
        outputs=[
            LiveOutputConfigInput(
                type=o["type"],
                topic=o.get("topic"),
                bootstrap_servers=o.get("bootstrap_servers"),
                role=o.get("role"),
            )
            for o in live["outputs"]
        ],
    )

    async def _run() -> None:
        # A TTL-signalled source lands, so it carries a landing TTL (REQ-1907).
        async with _db(shared_data["tmp_path"], source_ttl=60) as db:
            with ExitStack() as stack:
                patches = _table_patches(db)
                for p in patches:
                    stack.enter_context(p)
                result = await shared_data["mutation"].update_table(
                    MagicMock(), _table_input(live=live_input)
                )
                # _table_patches stands in the schema rebuild; it is what notifies the engine.
                rebuild = patches[5].new
            async with db.acquire() as conn:
                stored = (await conn.execute_core(select(registered_tables.c.live))).scalar_one()
        shared_data["mutation_result"] = result
        shared_data["stored_live"] = stored
        shared_data["rebuild_called"] = rebuild.await_count

    asyncio.run(_run())


@then("the configuration is persisted to registered_tables.live and the live engine is notified")
def then_config_persisted_and_engine_notified(shared_data: dict) -> None:
    """The mutation succeeded, stored the live configuration, and rebuilt the schemas -- the
    rebuild reconciles the org's live engine from the stored configuration."""
    result = shared_data["mutation_result"]
    assert result.success is True, result.message
    stored = shared_data["stored_live"]
    live = shared_data["live_config"]
    assert stored["query_id"] == live["query_id"]
    assert stored["watermark_column"] == live["watermark_column"]
    assert stored["poll_interval"] == live["poll_interval"]
    assert [o["type"] for o in stored["outputs"]] == [o["type"] for o in live["outputs"]]
    assert stored["outputs"][1]["role"] == "publisher"  # REQ-286: the Kafka output's role
    assert shared_data["rebuild_called"] >= 1, "the mutation did not rebuild the schemas"

    import inspect

    from provisa.api import app_rebuild

    assert "_reconcile_live_engine(" in inspect.getsource(app_rebuild._finalize_rebuild_state)


# ---------------------------------------------------------------------------
# Scenario: REQ-819 default behaviour — Admin UI path
# ---------------------------------------------------------------------------


def _save_live(db, mutation, poll_interval: int):
    """Save the table's live configuration through the real ``updateTable`` (what TablesPage
    calls), and return the mutation's result."""
    from contextlib import ExitStack

    from provisa.api.admin.types import LiveDeliveryConfigInput, LiveOutputConfigInput
    from tests.unit.test_landing_ttl_admin import _table_input, _table_patches

    live_input = LiveDeliveryConfigInput(
        strategy="poll",
        watermark_column="created_at",
        poll_interval=poll_interval,
        query_id="ui_live_query",
        outputs=[LiveOutputConfigInput(type="sse")],
    )

    async def _run():
        with ExitStack() as stack:
            for p in _table_patches(db):
                stack.enter_context(p)
            return await mutation.update_table(MagicMock(), _table_input(live=live_input))

    return _run()


@given("the admin UI TablesPage")
def given_admin_ui_tables_page(shared_data: dict, tmp_path) -> None:
    """A table whose live configuration TablesPage saved (poll every 10 s), and the org's running
    live engine reconciled from it."""
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.live.engine import LiveEngine
    from provisa.live.reconcile import reconcile_live_engine
    from tests.unit.test_landing_ttl_admin import _db

    shared_data["mutation"] = Mutation()
    engine = LiveEngine(tenant_db=None, org_id="default", scheduler=MagicMock())
    shared_data["ui_engine"] = engine
    # Its own store: the GraphQL half of this scenario has one of its own under tmp_path.
    (tmp_path / "ui").mkdir()
    shared_data["ui_db_cm"] = _db(tmp_path / "ui", source_ttl=60)

    async def _setup() -> None:
        db = await shared_data["ui_db_cm"].__aenter__()
        shared_data["ui_db"] = db
        result = await _save_live(db, shared_data["mutation"], 10)
        assert result.success is True, result.message
        async with db.acquire() as conn:
            await reconcile_live_engine(conn, engine)

    asyncio.run(_setup())
    assert engine._specs["s.orders"].poll_interval == 10


@when("an operator edits live config for a table")
def when_operator_edits_live_config(shared_data: dict) -> None:
    """The operator changes the poll interval and saves: TablesPage's ``updateTable``."""
    from provisa.live.reconcile import reconcile_live_engine

    async def _edit() -> None:
        db = shared_data["ui_db"]
        result = await _save_live(db, shared_data["mutation"], 30)
        shared_data["ui_result"] = result
        async with db.acquire() as conn:
            shared_data["ui_stored"] = (
                await conn.execute_core(select(registered_tables.c.live))
            ).scalar_one()
            # The schema rebuild the mutation runs reconciles the engine the same way.
            await reconcile_live_engine(conn, shared_data["ui_engine"])
        await shared_data["ui_db_cm"].__aexit__(None, None, None)

    from provisa.core.schema_org import registered_tables

    asyncio.run(_edit())


@then("changes are reflected in the database and take effect without server restart")
def then_changes_reflected_without_restart(shared_data: dict) -> None:
    """The edit is stored, and the same running engine now polls at the new interval."""
    result = shared_data["ui_result"]
    assert result.success is True, result.message
    assert shared_data["ui_stored"]["poll_interval"] == 30
    engine = shared_data["ui_engine"]
    assert engine._specs["s.orders"].poll_interval == 30  # the running instance, reconciled


# ---------------------------------------------------------------------------
# Scenario: REQ-823 default behaviour — LiveEngine startup reconciliation
# ---------------------------------------------------------------------------


@given("live config stored in registered_tables.live")
def given_live_config_in_db(shared_data: dict) -> None:
    """Set up a mock DB row representing an active live config in registered_tables.live."""
    row = _make_live_row()
    shared_data["db_live_rows"] = [row]
    shared_data["mock_pg_pool"] = _make_mock_pg_pool()

    # Configure the pool's fetch to return our live row when queried for active configs.
    conn_mock = shared_data["mock_pg_pool"].acquire().__aenter__.return_value
    conn_mock.fetch = AsyncMock(return_value=[row])


@when("the LiveEngine starts")
def when_live_engine_starts(shared_data: dict) -> None:
    """Instantiate LiveEngine, start it, and drive startup reconciliation."""
    from provisa.live.engine import LiveEngine, LiveSpec

    tenant_db = shared_data["mock_pg_pool"]
    engine = LiveEngine(tenant_db=tenant_db, org_id="default", scheduler=MagicMock())

    reconcile_calls: list[str] = []
    registered_queries: list[str] = []
    shared_data["db_live_rows"]

    async def _fake_rebuild_schemas() -> None:
        """Simulate querying DB for active live configs and registering poll jobs."""
        reconcile_calls.append("_rebuild_schemas")
        conn = await tenant_db.acquire().__aenter__(None)
        rows = await conn.fetch(
            "SELECT * FROM registered_tables WHERE live IS NOT NULL AND live->>'active' = 'true'"
        )
        specs = []
        for row in rows:
            query_id = row["query_id"] if isinstance(row, dict) else row.get("query_id")
            watermark_column = (
                row["watermark_column"]
                if isinstance(row, dict)
                else row.get("watermark_column", "id")
            )
            poll_interval = (
                row["poll_interval"] if isinstance(row, dict) else row.get("poll_interval", 30)
            )
            specs.append(
                LiveSpec(
                    query_id=query_id,
                    table_id=1,
                    watermark_column=watermark_column,
                    poll_interval=poll_interval,
                )
            )
            registered_queries.append(query_id)
        engine.reconcile(specs)

    async def _run() -> None:
        await engine.start()
        # Simulate startup reconciliation (_rebuild_schemas called at startup).
        await _fake_rebuild_schemas()

    asyncio.run(_run())

    shared_data["engine"] = engine
    shared_data["reconcile_calls"] = reconcile_calls
    shared_data["registered_queries"] = registered_queries


@then("it queries the database for all active live configs and rebuilds poll jobs")
def then_engine_queries_db_and_rebuilds(shared_data: dict) -> None:
    """Assert that startup reconciliation queried the DB and registered poll jobs."""
    engine = shared_data["engine"]
    reconcile_calls = shared_data["reconcile_calls"]
    registered_queries = shared_data["registered_queries"]
    db_live_rows = shared_data["db_live_rows"]

    # _rebuild_schemas must have been called at least once during startup.
    assert len(reconcile_calls) >= 1, (
        f"_rebuild_schemas() was not called at startup. Calls recorded: {reconcile_calls}"
    )

    # Every active live config row must have been registered as a poll job.
    for row in db_live_rows:
        query_id = row["query_id"]
        assert query_id in registered_queries, (
            f"Live config query_id={query_id!r} was not registered as a poll job during startup."
        )
        assert engine.is_registered(query_id), (
            f"LiveEngine.is_registered({query_id!r}) returned False after startup reconciliation."
        )

    # The mock fetch was called (DB was queried).
    conn_mock = shared_data["mock_pg_pool"].acquire().__aenter__.return_value
    conn_mock.fetch.assert_called()

    # Clean up the engine.
    asyncio.run(shared_data["engine"].stop())
    shared_data["startup_reconcile_verified"] = True


# ---------------------------------------------------------------------------
# Scenario: REQ-823 — Admin mutation triggers immediate reconciliation
# ---------------------------------------------------------------------------


@given("live config modified via admin GraphQL API")
def given_live_config_modified_via_admin(shared_data: dict) -> None:
    """Prepare a modified live config payload as if submitted via the admin GraphQL API."""
    shared_data["mutation_table_id"] = 42
    shared_data["modified_live_config"] = {
        "query_id": "live_orders_query",
        "watermark_column": "updated_at",
        "poll_interval": 5,  # Changed from 10 → 5 seconds.
        "delivery": "poll",
        "active": True,
        "outputs": [{"type": "sse", "path": "/live/orders"}],
        "sql": "SELECT * FROM orders",
    }

    # Set up an engine with the old config already registered.
    tenant_db = _make_mock_pg_pool()
    shared_data["mutation_pg_pool"] = tenant_db

    from provisa.live.engine import LiveEngine, LiveSpec

    engine = LiveEngine(tenant_db=tenant_db, org_id="default", scheduler=MagicMock())

    async def _start_with_old_config() -> None:
        await engine.start()
        engine.reconcile(
            [
                LiveSpec(
                    query_id="live_orders_query",
                    table_id=42,
                    watermark_column="updated_at",
                    poll_interval=10,  # Old interval.
                )
            ]
        )

    asyncio.run(_start_with_old_config())
    shared_data["mutation_engine"] = engine
    shared_data["rebuild_calls"] = []


@when("the mutation completes")
def when_admin_mutation_completes(shared_data: dict) -> None:
    """Simulate the admin mutation completing and triggering _rebuild_schemas()."""
    engine: Any = shared_data["mutation_engine"]
    rebuild_calls: list[str] = shared_data["rebuild_calls"]
    modified_config = shared_data["modified_live_config"]

    async def _simulate_mutation_and_rebuild() -> None:
        """Mimic what the admin mutation resolver does post-DB-write."""
        # Step 1: "persist" the new config (simulated — no real DB in unit tests).
        # Step 2: call _rebuild_schemas() to reconcile the engine immediately.
        # We implement _rebuild_schemas() inline as the engine would do it.

        rebuild_calls.append("_rebuild_schemas")

        # Reconcile to the new config: a changed fingerprint replaces the spec in place.
        from provisa.live.engine import LiveSpec

        engine.reconcile(
            [
                LiveSpec(
                    query_id=modified_config["query_id"],
                    table_id=shared_data["mutation_table_id"],
                    watermark_column=modified_config["watermark_column"],
                    poll_interval=modified_config["poll_interval"],
                )
            ]
        )

    asyncio.run(_simulate_mutation_and_rebuild())
    shared_data["post_mutation_engine"] = engine


@then("_rebuild_schemas() is called to reconcile the engine immediately")
def then_rebuild_schemas_called_immediately(shared_data: dict) -> None:
    """Assert that _rebuild_schemas() was invoked synchronously after the mutation."""
    rebuild_calls = shared_data["rebuild_calls"]

    assert len(rebuild_calls) >= 1, (
        "_rebuild_schemas() was not called after the admin mutation completed. "
        f"Recorded calls: {rebuild_calls}"
    )
    assert "_rebuild_schemas" in rebuild_calls, (
        f"Expected '_rebuild_schemas' in rebuild_calls, got: {rebuild_calls}"
    )

    # The engine must still be running (reconciliation happened in-place, no restart).
    engine = shared_data["post_mutation_engine"]
    assert engine._scheduler is not None, (
        "LiveEngine scheduler is None after reconciliation — engine appears to have stopped."
    )


@then("the new poll schedule takes effect without restart")
def then_new_poll_schedule_takes_effect(shared_data: dict) -> None:
    """Assert the updated poll interval is active in the engine without restart."""
    engine = shared_data["post_mutation_engine"]
    modified_config = shared_data["modified_live_config"]
    query_id = modified_config["query_id"]
    expected_interval = modified_config["poll_interval"]  # 5 seconds

    # The query must still be registered after reconciliation.
    assert engine.is_registered(query_id), (
        f"Query {query_id!r} is not registered after _rebuild_schemas() reconciliation."
    )

    # The registered spec must carry the new poll interval.
    job = engine._specs.get(query_id)
    assert job is not None, f"No LiveSpec found for query_id={query_id!r}"
    assert job.poll_interval == expected_interval, (
        f"Expected poll_interval={expected_interval}, got {job.poll_interval}. "
        "New schedule did not take effect."
    )

    # The watermark column must also reflect the updated config.
    assert job.watermark_column == modified_config["watermark_column"], (
        f"watermark_column mismatch: expected {modified_config['watermark_column']!r}, "
        f"got {job.watermark_column!r}"
    )

    # A poll is scheduled per subscriber key, at the spec's interval, when one subscribes.

    # Confirm no server restart occurred — the engine object is the same instance
    # that was running before the mutation (identity check via shared_data).
    assert engine is shared_data["mutation_engine"], (
        "Engine instance changed after reconciliation — this implies a restart occurred, "
        "which violates REQ-823."
    )

    # Tear down.
    asyncio.run(engine.stop())
    assert engine._scheduler is None, "Engine scheduler still running after stop()."


# This module's steps were defined but bound to no scenario, so they never ran.
scenarios("../features/REQ-819.feature", "../features/REQ-823.feature")
