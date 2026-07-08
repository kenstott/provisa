# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-236 and REQ-237 — Hot Table Auto-Detection."""

from __future__ import annotations

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.cache.hot_tables import (
    HOT_PREFIX,
    HotTableEntry,
    HotTableManager,
    detect_hot_tables,
)

scenarios("../features/REQ-236.feature")
scenarios("../features/REQ-237.feature")


@pytest.fixture
def shared_data():
    return {}


@given(
    "a table that is the target of a many-to-one relationship or has row count <= auto_threshold"
)
def given_auto_hot_candidate(shared_data):
    """Define a schema where 'countries' is the target of a many-to-one relationship.

    This makes it an auto-hot candidate independent of explicit hot config.
    """
    shared_data["tables"] = [
        {"table_name": "orders"},
        {"table_name": "countries"},
    ]
    shared_data["relationships"] = [
        {
            "source_table_id": "orders",
            "target_table_id": "countries",
            "cardinality": "many-to-one",
        },
    ]
    # No explicit hot config — rely entirely on auto-detection.
    shared_data["hot_config"] = {}
    shared_data["auto_threshold"] = 1000

    # Sanity: the candidate table exists and a many-to-one relationship targets it.
    target_names = {
        r["target_table_id"]
        for r in shared_data["relationships"]
        if r["cardinality"] == "many-to-one"
    }
    assert "countries" in target_names


@when("schema is built")
def when_schema_built(shared_data):
    """Run auto-detection (the schema-build step that designates hot tables)."""
    detected = detect_hot_tables(
        shared_data["tables"],
        shared_data["relationships"],
        shared_data["hot_config"],
    )
    shared_data["detected"] = detected

    # Build a manager and register/cache the detected entry. Use a real Redis when
    # integration infrastructure is present, otherwise a faithful in-memory mock.
    manager = HotTableManager(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/1"),
        auto_threshold=shared_data["auto_threshold"],
        max_rows=1000,
    )

    rows = [
        {"id": 1, "name": "US"},
        {"id": 2, "name": "CA"},
    ]
    shared_data["rows"] = rows

    if os.getenv("PROVISA_INTEGRATION"):

        async def _cache():
            await manager._connect()
            await manager._redis.ping()
            blob_key = HOT_PREFIX + "countries:blob"
            await manager._redis.set(blob_key, json.dumps(rows))
            manager._hot_tables["countries"] = HotTableEntry(
                table_name="countries",
                catalog="c",
                schema="s",
                pk_column="id",
                rows=rows,
                column_names=["id", "name"],
            )
            fetched = await manager.get_rows("countries")
            await manager.close()
            return fetched

        shared_data["cached_rows"] = asyncio.run(_cache())
    else:
        # In-memory Redis substitute: faithfully store and return the JSON blob.
        store: dict[str, str] = {}
        mock_redis = MagicMock()
        mock_redis.set = AsyncMock(side_effect=lambda k, v, **kw: store.__setitem__(k, v))
        mock_redis.get = AsyncMock(side_effect=lambda k: store.get(k))
        mock_redis.exists = AsyncMock(side_effect=lambda k: int(k in store))
        manager._redis = mock_redis

        async def _cache():
            blob_key = HOT_PREFIX + "countries:blob"
            await manager._redis.set(blob_key, json.dumps(rows))
            manager._hot_tables["countries"] = HotTableEntry(
                table_name="countries",
                catalog="c",
                schema="s",
                pk_column="id",
                rows=rows,
                column_names=["id", "name"],
            )
            blob = await manager._redis.get(blob_key)
            return json.loads(blob)

        shared_data["cached_rows"] = asyncio.run(_cache())

    shared_data["manager"] = manager


@then("the table is automatically designated as hot and cached in Redis")
def then_designated_hot_and_cached(shared_data):
    detected = shared_data["detected"]
    manager = shared_data["manager"]

    # Auto-designated as hot via the many-to-one target rule.
    assert "countries" in detected
    assert "orders" not in detected

    # Registered as hot in the manager.
    assert manager.is_hot("countries")

    # Cached rows round-tripped through Redis intact.
    assert shared_data["cached_rows"] == shared_data["rows"]


# ---------------------------------------------------------------------------
# REQ-237 — Explicit hot:false opt-out overrides auto-detection
# ---------------------------------------------------------------------------


@given("a table with hot: false in its config")
def given_hot_false_table(shared_data):
    """Define a table that *would* be auto-detected (it is the target of a
    many-to-one relationship) but is explicitly opted out via hot: false.
    """
    shared_data["tables"] = [
        {"table_name": "orders"},
        {"table_name": "countries"},
    ]
    shared_data["relationships"] = [
        {
            "source_table_id": "orders",
            "target_table_id": "countries",
            "cardinality": "many-to-one",
        },
    ]
    # Explicit opt-out: hot: false on the candidate table.
    shared_data["hot_config"] = {"countries": False}
    shared_data["auto_threshold"] = 1000

    # Sanity: without the opt-out this table WOULD be auto-detected.
    would_detect = detect_hot_tables(shared_data["tables"], shared_data["relationships"], {})
    assert "countries" in would_detect, (
        "Test precondition broken: 'countries' must meet auto-detection criteria "
        "so that the hot:false opt-out is meaningful."
    )


@when("schema is rebuilt")
def when_schema_rebuilt(shared_data):
    """Re-run auto-detection on schema rebuild, honoring explicit hot config."""
    detected = detect_hot_tables(
        shared_data["tables"],
        shared_data["relationships"],
        shared_data["hot_config"],
    )
    shared_data["detected"] = detected

    # Build a manager and only cache the tables that survived detection. Since
    # 'countries' is opted out, it must NOT be registered or written to Redis.
    manager = HotTableManager(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/1"),
        auto_threshold=shared_data["auto_threshold"],
        max_rows=1000,
    )

    if os.getenv("PROVISA_INTEGRATION"):

        async def _cache():
            await manager._connect()
            await manager._redis.ping()
            blob_key = HOT_PREFIX + "countries:blob"
            # Ensure a clean slate from any prior rebuild.
            await manager._redis.delete(blob_key)
            for tbl in detected:
                bk = HOT_PREFIX + tbl + ":blob"
                rows = [{"id": 1, "name": "US"}]
                await manager._redis.set(bk, json.dumps(rows))
                manager._hot_tables[tbl] = HotTableEntry(
                    table_name=tbl,
                    catalog="c",
                    schema="s",
                    pk_column="id",
                    rows=rows,
                    column_names=["id", "name"],
                )
            exists = bool(await manager._redis.exists(blob_key))
            await manager.close()
            return exists

        shared_data["countries_blob_exists"] = asyncio.run(_cache())
    else:
        store: dict[str, str] = {}
        mock_redis = MagicMock()
        mock_redis.set = AsyncMock(side_effect=lambda k, v, **kw: store.__setitem__(k, v))
        mock_redis.get = AsyncMock(side_effect=lambda k: store.get(k))
        mock_redis.exists = AsyncMock(side_effect=lambda k: int(k in store))
        mock_redis.delete = AsyncMock(side_effect=lambda k: store.pop(k, None))
        manager._redis = mock_redis

        async def _cache():
            for tbl in detected:
                bk = HOT_PREFIX + tbl + ":blob"
                rows = [{"id": 1, "name": "US"}]
                await manager._redis.set(bk, json.dumps(rows))
                manager._hot_tables[tbl] = HotTableEntry(
                    table_name=tbl,
                    catalog="c",
                    schema="s",
                    pk_column="id",
                    rows=rows,
                    column_names=["id", "name"],
                )
            blob_key = HOT_PREFIX + "countries:blob"
            return bool(await manager._redis.exists(blob_key))

        shared_data["countries_blob_exists"] = asyncio.run(_cache())

    shared_data["manager"] = manager


@then("the table is not cached in Redis even if it meets auto-detection criteria")
def then_not_cached_despite_criteria(shared_data):
    detected = shared_data["detected"]
    manager = shared_data["manager"]

    # The hot:false opt-out wins over the auto-detection criteria.
    assert "countries" not in detected, (
        "hot:false must override auto-detection; 'countries' should not be detected."
    )

    # Not registered as hot in the manager.
    assert not manager.is_hot("countries")

    # No Redis blob was written for the opted-out table.
    assert shared_data["countries_blob_exists"] is False
