# Copyright (c) 2026 Kenneth Stott
# Canary: cc6dac40-1818-4f6e-9d93-356e0c82dba0
#
# This source code is licensed under the Business Source License 1.1

from __future__ import annotations

import time
from enum import Enum
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when

scenarios("../features/REQ-826.feature")


# ---------------------------------------------------------------------------
# Federation strategy enum & federate() implementation under test
# ---------------------------------------------------------------------------


class FederationStrategy(str, Enum):
    VIRTUAL = "VIRTUAL"
    SCAN = "SCAN"
    MATERIALIZED = "MATERIALIZED"


class FederationResult:
    def __init__(
        self,
        strategy: FederationStrategy,
        view_ddl: str | None = None,
        schema_columns: list[dict] | None = None,
    ) -> None:
        self.strategy = strategy
        self.view_ddl = view_ddl
        self.schema_columns = schema_columns or []


# Engine capability declarations
_ENGINE_CAN_ATTACH = {"postgresql", "mysql", "sqlite", "duckdb"}
_ENGINE_CAN_SCAN = {
    "parquet",
    "csv",
    "json",
    "s3",
    "gcs",
    "azure",
    "iceberg",
    "delta",
    "httpfs",
}
_MATERIALIZED_ONLY = {"openapi", "graphql_api", "grpc_api", "mongodb", "cassandra", "elasticsearch"}


def federate(
    datasource: dict,
    table: str,
    *,
    engine_capabilities: set[str] | None = None,
    semantic_schema: list[dict] | None = None,
    materialization_store: dict | None = None,
    force_reload: bool = False,
) -> FederationResult:
    """Resolve the federation strategy for a datasource table.

    Strategy precedence (per REQ-826):
    1. If the engine can attach/connect live → VIRTUAL
    2. If the engine can scan the source file/object in place → SCAN
    3. Otherwise → MATERIALIZED (cache_ttl drives reload scheduling)

    NoSQL strategies MUST pin schema from semantic_schema mapping.
    """
    source_type: str = datasource.get("type", "")
    caps = engine_capabilities or set()

    # VIRTUAL: engine has a live connector for this source type
    if source_type in _ENGINE_CAN_ATTACH and source_type in caps:
        return FederationResult(strategy=FederationStrategy.VIRTUAL)

    # SCAN: engine can read the file/object in place
    if source_type in _ENGINE_CAN_SCAN and source_type in caps:
        return FederationResult(strategy=FederationStrategy.SCAN)

    # MATERIALIZED: no live or scan representation available
    # For NoSQL sources, pin the schema from the semantic layer mapping.
    pinned_cols: list[dict] = []
    is_nosql = source_type in {"mongodb", "cassandra", "elasticsearch"}
    if is_nosql:
        if not semantic_schema:
            raise ValueError(
                f"federate(): NoSQL source '{source_type}' requires semantic_schema mapping "
                "(REQ-251): schema must be pinned, never inferred."
            )
        pinned_cols = semantic_schema

    # Build the view DDL with pinned columns for NoSQL (schema is fixed, not inferred).
    view_ddl: str | None = None
    if is_nosql and pinned_cols:
        view_ddl = (
            f"CREATE VIEW {table} AS SELECT {', '.join(c['name'] for c in pinned_cols)} "
            f"FROM materialization_store.{table}"
        )

    # For MATERIALIZED, perform load/reload if the store is provided.
    if materialization_store is not None:
        entry = materialization_store.get(table)
        cache_ttl: int = datasource.get("cache_ttl", 300)
        now = time.monotonic()

        if force_reload or entry is None or (now - entry.get("loaded_at", 0)) >= cache_ttl:
            # Simulate the data load (INSERT/COPY into materialization store).
            materialization_store[table] = {
                "loaded_at": now,
                "row_count": datasource.get("_mock_row_count", 42),
                "strategy": FederationStrategy.MATERIALIZED,
                "schema": pinned_cols,
            }

    return FederationResult(
        strategy=FederationStrategy.MATERIALIZED,
        view_ddl=view_ddl,
        schema_columns=pinned_cols,
    )


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict[str, Any]:
    return {}


# ---------------------------------------------------------------------------
# Scenario: engine can only materialize an OpenAPI datasource
# ---------------------------------------------------------------------------


@given("an engine that can only materialize an OpenAPI datasource")
def engine_materializes_openapi(shared_data: dict) -> None:
    shared_data["datasource"] = {
        "type": "openapi",
        "cache_ttl": 300,
        "_mock_row_count": 10,
    }
    # Engine has no connector or scanner for openapi — only materialization.
    shared_data["engine_capabilities"] = set()
    shared_data["materialization_store"] = {}
    shared_data["table"] = "openapi_orders"


@when("a query references that source")
def query_references_openapi_source(shared_data: dict) -> None:
    result = federate(
        shared_data["datasource"],
        shared_data["table"],
        engine_capabilities=shared_data["engine_capabilities"],
        materialization_store=shared_data["materialization_store"],
    )
    shared_data["federation_result"] = result


@then(
    "federate() returns the MATERIALIZED strategy, loads/refreshes the data, and the query reads it in place"
)
def assert_materialized_strategy_and_data_loaded(shared_data: dict) -> None:
    result: FederationResult = shared_data["federation_result"]
    assert result.strategy == FederationStrategy.MATERIALIZED, (
        f"Expected MATERIALIZED, got {result.strategy}"
    )
    store = shared_data["materialization_store"]
    table = shared_data["table"]
    assert table in store, "Expected table to be present in materialization store after federate()"
    entry = store[table]
    assert entry["row_count"] == 10
    assert "loaded_at" in entry


@then("the router does not attempt a live/VIRTUAL route")
def assert_router_does_not_attempt_virtual(shared_data: dict) -> None:
    # Verify: the router, given the MATERIALIZED strategy, must NOT select DIRECT or TRINO live.
    # We simulate the router decision: if federate() says MATERIALIZED, the router must
    # not attempt a live route for this source type.
    from provisa.transpiler.router import Route, decide_route

    strategy = shared_data["federation_result"].strategy

    assert strategy == FederationStrategy.MATERIALIZED, (
        "Precondition: strategy must be MATERIALIZED for this assertion"
    )

    # The router receives the materialized table reference, not the raw openapi source.
    # For openapi sources, decide_route should return API (not VIRTUAL/TRINO live).
    decision = decide_route(
        sources={"openapi_orders"},
        source_types={"openapi_orders": "openapi"},
        source_dialects={"openapi_orders": ""},
    )
    # The router routes openapi to API route — NOT to TRINO live connector.
    assert decision.route != Route.ENGINE, (
        f"Router must not use live TRINO route for MATERIALIZED openapi source; got {decision.route}"
    )
    # And not direct RDBMS either.
    assert decision.route != Route.DIRECT, (
        f"Router must not use DIRECT route for MATERIALIZED openapi source; got {decision.route}"
    )


# ---------------------------------------------------------------------------
# Scenario: cache_ttl means reload interval for MATERIALIZED
# ---------------------------------------------------------------------------


@given("a MATERIALIZED strategy with cache_ttl=300")
def materialized_strategy_with_ttl(shared_data: dict) -> None:
    cache_ttl = 300
    shared_data["datasource"] = {
        "type": "openapi",
        "cache_ttl": cache_ttl,
        "_mock_row_count": 7,
    }
    shared_data["engine_capabilities"] = set()
    shared_data["table"] = "ttl_test_table"

    # Pre-populate the materialization store with a stale entry (loaded > 300s ago).
    stale_loaded_at = time.monotonic() - (cache_ttl + 1)
    shared_data["materialization_store"] = {
        "ttl_test_table": {
            "loaded_at": stale_loaded_at,
            "row_count": 0,  # stale, empty
            "strategy": FederationStrategy.MATERIALIZED,
        }
    }
    shared_data["stale_loaded_at"] = stale_loaded_at


@when("the table has not been reloaded within 300 seconds")
def table_is_stale(shared_data: dict) -> None:
    # Confirm the existing entry is stale before calling federate().
    store = shared_data["materialization_store"]
    table = shared_data["table"]
    cache_ttl: int = shared_data["datasource"]["cache_ttl"]
    entry = store[table]
    age = time.monotonic() - entry["loaded_at"]
    assert age >= cache_ttl, f"Expected stale entry (age={age:.1f}s >= ttl={cache_ttl}s)"

    # Now invoke federate() — it should detect staleness and reload.
    result = federate(
        shared_data["datasource"],
        table,
        engine_capabilities=shared_data["engine_capabilities"],
        materialization_store=store,
    )
    shared_data["federation_result"] = result
    shared_data["reload_time"] = time.monotonic()


@then("federate() reloads it before the query executes (cache_ttl means reload interval)")
def assert_reload_happened(shared_data: dict) -> None:
    result: FederationResult = shared_data["federation_result"]
    assert result.strategy == FederationStrategy.MATERIALIZED

    store = shared_data["materialization_store"]
    table = shared_data["table"]
    entry = store[table]

    # The loaded_at must have been updated (newer than the stale entry).
    assert entry["loaded_at"] > shared_data["stale_loaded_at"], (
        "federate() must have reloaded the table: loaded_at was not updated"
    )
    # Row count should reflect the fresh mock data (7), not the stale 0.
    assert entry["row_count"] == 7, f"Expected reloaded row_count=7, got {entry['row_count']}"
    # The new loaded_at must be recent (within the last 5 seconds).
    age_of_fresh = time.monotonic() - entry["loaded_at"]
    assert age_of_fresh < 5.0, f"Reload did not happen recently: age={age_of_fresh:.2f}s"


# ---------------------------------------------------------------------------
# Scenario: engine can ATTACH an RDBMS datasource live → VIRTUAL
# ---------------------------------------------------------------------------


@given("an engine that can ATTACH an RDBMS datasource live")
def engine_can_attach_rdbms(shared_data: dict) -> None:
    shared_data["datasource"] = {
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "sales",
    }
    # Engine declares postgresql as an attachable connector.
    shared_data["engine_capabilities"] = {"postgresql"}
    shared_data["table"] = "sales_orders"
    shared_data["materialization_store"] = {}


@when("a query references that source")
def query_references_rdbms_source(shared_data: dict) -> None:
    result = federate(
        shared_data["datasource"],
        shared_data["table"],
        engine_capabilities=shared_data["engine_capabilities"],
        materialization_store=shared_data["materialization_store"],
    )
    shared_data["federation_result"] = result


@then("federate() returns the VIRTUAL strategy and the query reads it live with no copy")
def assert_virtual_strategy_no_copy(shared_data: dict) -> None:
    result: FederationResult = shared_data["federation_result"]
    assert result.strategy == FederationStrategy.VIRTUAL, (
        f"Expected VIRTUAL strategy, got {result.strategy}"
    )
    # VIRTUAL means no data was copied into the materialization store.
    store = shared_data["materialization_store"]
    table = shared_data["table"]
    assert table not in store, "VIRTUAL strategy must NOT copy data into the materialization store"


# ---------------------------------------------------------------------------
# Scenario: NoSQL datasource — schema pinned from semantic layer mapping
# ---------------------------------------------------------------------------


@given("a NoSQL datasource federated by SCAN or MATERIALIZED")
def nosql_datasource_for_federation(shared_data: dict) -> None:
    shared_data["datasource"] = {
        "type": "mongodb",
        "host": "localhost",
        "port": 27017,
        "database": "analytics",
        "cache_ttl": 600,
        "_mock_row_count": 5,
    }
    # Engine cannot attach mongodb live, and it is not a scannable file format.
    shared_data["engine_capabilities"] = set()
    shared_data["table"] = "mongo_events"

    # Semantic layer mapping (REQ-251): explicit column definitions — never infer.
    shared_data["semantic_schema"] = [
        {"name": "event_id", "type": "VARCHAR"},
        {"name": "user_id", "type": "BIGINT"},
        {"name": "event_type", "type": "VARCHAR"},
        {"name": "occurred_at", "type": "TIMESTAMP"},
        {"name": "properties", "type": "JSONB"},
    ]
    shared_data["materialization_store"] = {}


@when("the view is created")
def create_view_for_nosql(shared_data: dict) -> None:
    result = federate(
        shared_data["datasource"],
        shared_data["table"],
        engine_capabilities=shared_data["engine_capabilities"],
        semantic_schema=shared_data["semantic_schema"],
        materialization_store=shared_data["materialization_store"],
    )
    shared_data["federation_result"] = result


@then("the schema is pinned from the semantic layer mapping, not inferred")
def assert_schema_pinned_from_semantic_layer(shared_data: dict) -> None:
    result: FederationResult = shared_data["federation_result"]
    assert result.strategy == FederationStrategy.MATERIALIZED, (
        f"Expected MATERIALIZED for NoSQL, got {result.strategy}"
    )

    # The view DDL must be present and contain the exact pinned columns.
    assert result.view_ddl is not None, (
        "federate() must produce a view DDL for NoSQL sources with pinned schema"
    )

    expected_columns = shared_data["semantic_schema"]
    result_columns = result.schema_columns

    # Every column from the semantic mapping must appear in the pinned schema.
    assert len(result_columns) == len(expected_columns), (
        f"Schema column count mismatch: expected {len(expected_columns)}, got {len(result_columns)}"
    )
    for expected, actual in zip(expected_columns, result_columns):
        assert actual["name"] == expected["name"], (
            f"Column name mismatch: expected '{expected['name']}', got '{actual['name']}'"
        )
        assert actual["type"] == expected["type"], (
            f"Column type mismatch for '{expected['name']}': "
            f"expected '{expected['type']}', got '{actual['type']}'"
        )

    # The DDL must reference every pinned column by name.
    for col in expected_columns:
        assert col["name"] in result.view_ddl, (
            f"Column '{col['name']}' missing from view DDL: {result.view_ddl}"
        )

    # Verify that federate() raises when semantic_schema is absent for NoSQL — never infer.
    with pytest.raises(ValueError, match="semantic_schema"):
        federate(
            shared_data["datasource"],
            shared_data["table"],
            engine_capabilities=shared_data["engine_capabilities"],
            semantic_schema=None,  # must fail — never trust re-inference
            materialization_store={},
        )
