# Copyright (c) 2026 Kenneth Stott
# Canary: dd9a1414-5b05-423d-a8bc-2ea3f6a683ae
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-903: PostgreSQL Deployment - connector availability validation."""

from __future__ import annotations

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.core.models import Source, SourceType
from provisa.federation.engine import FederationEngine, UnreachableSource, build_duckdb_engine

scenarios("../features/REQ-903.feature")


@pytest.fixture
def shared_data():
    return {}


@given("a source requiring an unavailable connector")
def given_source_requiring_unavailable_connector(shared_data):
    # Build a DuckDB engine, which has no MySQL connector (unavailable for mysql source_type).
    engine = build_duckdb_engine()
    assert not engine.reachable("mysql"), "Expected mysql to be unreachable on DuckDB engine"

    # Create a MySQL source - DuckDB has no connector for it.
    source = Source(
        id="mysql_source_unavailable",
        type=SourceType.mysql,
        host="db.example.com",
        port=3306,
        database="inventory",
        username="reader",
    )

    shared_data["engine"] = engine
    shared_data["source"] = source


@when("query planning occurs")
def when_query_planning_occurs(shared_data):
    engine: FederationEngine = shared_data["engine"]
    source: Source = shared_data["source"]

    caught_exception: UnreachableSource | None = None
    try:
        # resolve() calls connector_for() which raises UnreachableSource for missing connectors.
        engine.resolve(source)
    except UnreachableSource as exc:
        caught_exception = exc

    shared_data["exception"] = caught_exception


@then("the source resolves to UnreachableSource with explicit error.")
def then_source_resolves_to_unreachable_source(shared_data):
    exc = shared_data.get("exception")

    assert exc is not None, (
        "Expected UnreachableSource to be raised during query planning, but no exception was caught"
    )
    assert isinstance(exc, UnreachableSource), (
        f"Expected UnreachableSource, got {type(exc).__name__}"
    )
    assert exc.engine == "duckdb", (
        f"Expected engine name 'duckdb' in error, got {exc.engine!r}"
    )
    assert exc.source_type == "mysql", (
        f"Expected source_type 'mysql' in error, got {exc.source_type!r}"
    )
    error_message = str(exc)
    assert "duckdb" in error_message, (
        f"Expected engine name in error message, got: {error_message!r}"
    )
    assert "mysql" in error_message, (
        f"Expected source_type in error message, got: {error_message!r}"
    )


import asyncio
from unittest.mock import AsyncMock

from provisa.federation.connector import ProbeResult
from provisa.federation.engine import build_pg_engine

scenarios("../features/REQ-904.feature")


@pytest.fixture
def shared_data():
    return {}


@given("a FederationEngine with pg_duckdb not in shared_preload_libraries")
def given_federation_engine_without_pg_duckdb_preloaded(shared_data):
    # Build the Postgres federation engine with all prebuilt connectors as candidates.
    engine = build_pg_engine()
    shared_data["engine"] = engine

    # Track call count so we can simulate two discovery rounds with different results.
    shared_data["probe_call_count"] = 0

    async def fetch_without_preload(sql: str):
        """Simulate a Postgres instance where pg_duckdb is NOT in shared_preload_libraries."""
        sql_lower = sql.lower()
        # shared_preload_libraries check: pg_duckdb absent
        if "shared_preload_libraries" in sql_lower:
            return [{"setting": "pg_stat_statements"}]
        # postgres_fdw extension check: present and works
        if "postgres_fdw" in sql_lower:
            return [{"extname": "postgres_fdw", "installed_version": "1.0"}]
        # file_fdw extension check: present and works
        if "file_fdw" in sql_lower:
            return [{"extname": "file_fdw", "installed_version": "1.0"}]
        return []

    shared_data["fetch_without_preload"] = fetch_without_preload


@when("discover() is called")
def when_discover_is_called(shared_data):
    engine: FederationEngine = shared_data["engine"]
    fetch = shared_data["fetch_without_preload"]

    report = asyncio.get_event_loop().run_until_complete(
        engine.discover(fetch, disabled=frozenset())
    )
    shared_data["report"] = report
    shared_data["connectors_after_discover"] = dict(engine.connectors)


@then("pg_duckdb's probe reports unavailable")
def then_pg_duckdb_probe_reports_unavailable(shared_data):
    report: dict = shared_data["report"]

    # At least one pg_duckdb connector key must be present and report unavailable.
    pg_duckdb_keys = [k for k in report if "pg_duckdb" in k]
    assert pg_duckdb_keys, (
        f"Expected at least one pg_duckdb connector in probe report, got keys: {list(report.keys())}"
    )
    for key in pg_duckdb_keys:
        result: ProbeResult = report[key]
        assert not result.available, (
            f"Expected pg_duckdb connector '{key}' to be unavailable without shared_preload_libraries, "
            f"but probe reported available=True. Reason: {result.reason}"
        )


@then("CSV sources fall back to file_fdw")
def then_csv_sources_fall_back_to_file_fdw(shared_data):
    connectors: dict = shared_data["connectors_after_discover"]

    # CSV source_type must be reachable (file_fdw fallback is active).
    assert "csv" in connectors, (
        f"Expected 'csv' source_type to be reachable via file_fdw fallback after discover(), "
        f"but active connectors are: {list(connectors.keys())}"
    )
    active_csv_connector = connectors["csv"]
    # The active connector for csv should be file_fdw, not pg_duckdb_csv.
    assert "file_fdw" in active_csv_connector.key or active_csv_connector.__class__.__name__ == "FileFdwCsvConnector", (
        f"Expected file_fdw connector to be active for CSV after pg_duckdb unavailable, "
        f"but got: {active_csv_connector.__class__.__name__!r} (key={active_csv_connector.key!r})"
    )


@then("post-preload, discover() re-probes and activates pg_duckdb.")
def then_post_preload_discover_activates_pg_duckdb(shared_data):
    engine: FederationEngine = shared_data["engine"]

    async def fetch_with_preload(sql: str):
        """Simulate a Postgres instance where pg_duckdb IS in shared_preload_libraries."""
        sql_lower = sql.lower()
        if "shared_preload_libraries" in sql_lower:
            return [{"setting": "pg_stat_statements,pg_duckdb"}]
        if "postgres_fdw" in sql_lower:
            return [{"extname": "postgres_fdw", "installed_version": "1.0"}]
        if "file_fdw" in sql_lower:
            return [{"extname": "file_fdw", "installed_version": "1.0"}]
        if "pg_duckdb" in sql_lower:
            return [{"extname": "pg_duckdb", "installed_version": "1.0.0"}]
        return []

    report = asyncio.get_event_loop().run_until_complete(
        engine.discover(fetch_with_preload, disabled=frozenset())
    )

    # After preload, at least one pg_duckdb connector must now be available.
    pg_duckdb_keys = [k for k in report if "pg_duckdb" in k]
    assert pg_duckdb_keys, (
        f"Expected pg_duckdb keys in post-preload report, got: {list(report.keys())}"
    )
    any_pg_duckdb_available = any(report[k].available for k in pg_duckdb_keys)
    assert any_pg_duckdb_available, (
        f"Expected at least one pg_duckdb connector to be available after preload, "
        f"but all reported unavailable: { {k: report[k] for k in pg_duckdb_keys} }"
    )

    # CSV source_type should now be served by a pg_duckdb connector (richer, preferred).
    connectors_after_preload = dict(engine.connectors)
    assert "csv" in connectors_after_preload, (
        "Expected 'csv' source_type to remain reachable after post-preload discover()"
    )
    active_csv = connectors_after_preload["csv"]
    assert "pg_duckdb" in active_csv.key or "duckdb" in active_csv.__class__.__name__.lower(), (
        f"Expected pg_duckdb CSV connector to win after preload (richer preferred over file_fdw), "
        f"but active connector is: {active_csv.__class__.__name__!r} (key={active_csv.key!r})"
    )
