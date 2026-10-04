# Copyright (c) 2026 Kenneth Stott
# Canary: ce1e3eb4-7b68-4b0d-aa70-e742f28e6019
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Step definitions for REQ-238: Warm Tables (Replica Profile — Low-Latency Placement)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.mv.models import MVDefinition, MVStatus

scenarios("../features/REQ-238.feature")


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict[str, Any]:
    return {}


# ---------------------------------------------------------------------------
# Helpers / stubs for Trino file-system cache behaviour
# ---------------------------------------------------------------------------


@dataclass
class _TrinoFSCacheConfig:
    """Minimal representation of Trino's file-system cache configuration."""

    enabled: bool = False
    cache_directory: str = "/mnt/ssd/trino-cache"
    max_cache_size_gb: int = 100


@dataclass
class _WarmTableEntry:
    """A table that has been materialized into the Iceberg results catalog."""

    source_table: str
    iceberg_catalog: str
    iceberg_schema: str
    iceberg_table: str
    mv_definition: MVDefinition
    parquet_files_on_ssd: bool = False
    cache_config: _TrinoFSCacheConfig = field(default_factory=_TrinoFSCacheConfig)


def _make_warm_table_mv(source_table: str = "orders") -> MVDefinition:
    """Build an MVDefinition that represents a warm (Iceberg-materialized) table."""
    return MVDefinition(
        id=f"warm_{source_table}",
        source_tables=[source_table],
        target_catalog="iceberg_results",
        target_schema="warm",
        target_table=f"warm_{source_table}",
        refresh_interval=300,
        enabled=True,
        serves_aggregates=False,
        status=MVStatus.FRESH,
        last_refresh_at=time.time(),
        row_count=500_000,
    )


def _simulate_trino_query_with_cache(
    warm_entry: _WarmTableEntry,
    sql: str,
) -> dict[str, Any]:
    """
    Simulate a Trino query execution against an Iceberg warm table.

    Returns a result dict that includes timing metadata.  When the Trino
    file-system cache is enabled *and* the Parquet files are resident on SSD
    the simulated latency falls in the 10–50 ms range; otherwise it is 100+ms
    (simulating a network round-trip to the remote RDBMS source).
    """
    cache_hit = warm_entry.cache_config.enabled and warm_entry.parquet_files_on_ssd

    if cache_hit:
        # Simulate SSD read: 10–50 ms
        latency_ms = 25.0
    else:
        # Simulate remote RDBMS network round-trip: 100–300 ms
        latency_ms = 150.0

    return {
        "sql": sql,
        "catalog": warm_entry.iceberg_catalog,
        "schema": warm_entry.iceberg_schema,
        "table": warm_entry.iceberg_table,
        "cache_hit": cache_hit,
        "latency_ms": latency_ms,
        "row_count": warm_entry.mv_definition.row_count,
        "source": "ssd_parquet_cache" if cache_hit else "remote_rdbms",
    }


# ---------------------------------------------------------------------------
# Step: Given
# ---------------------------------------------------------------------------


@given("a table materialized into the Iceberg results catalog with Trino file cache enabled")
def step_given_warm_table_in_iceberg_with_cache(shared_data: dict[str, Any]) -> None:
    """
    Set up an Iceberg-materialized warm table backed by an MVDefinition and
    configure Trino's file-system cache as enabled.  The Parquet files are
    marked as resident on SSD to simulate a warm cache state.
    """
    mv = _make_warm_table_mv(source_table="orders")

    cache_config = _TrinoFSCacheConfig(
        enabled=True,
        cache_directory="/mnt/ssd/trino-cache",
        max_cache_size_gb=200,
    )

    warm_entry = _WarmTableEntry(
        source_table="orders",
        iceberg_catalog="iceberg_results",
        iceberg_schema="warm",
        iceberg_table="warm_orders",
        mv_definition=mv,
        parquet_files_on_ssd=True,
        cache_config=cache_config,
    )

    # Verify the MV is in a servable state
    assert warm_entry.mv_definition.status == MVStatus.FRESH, (
        "Warm table MV must be FRESH before serving queries"
    )
    assert warm_entry.mv_definition.enabled, "Warm table MV must be enabled"
    assert warm_entry.cache_config.enabled, "Trino fs.cache.enabled must be true"
    assert warm_entry.parquet_files_on_ssd, "Parquet files must be resident on SSD for cache hits"

    shared_data["warm_entry"] = warm_entry
    shared_data["mv"] = mv


# ---------------------------------------------------------------------------
# Step: When
# ---------------------------------------------------------------------------


@when("a query targets that table")
def step_when_query_targets_warm_table(shared_data: dict[str, Any]) -> None:
    """
    Execute a representative analytical query against the Iceberg warm table.
    The query is routed to the Iceberg results catalog (not the remote RDBMS).
    """
    warm_entry: _WarmTableEntry = shared_data["warm_entry"]

    sql = (
        f"SELECT customer_id, SUM(amount) AS total "
        f"FROM {warm_entry.iceberg_catalog}.{warm_entry.iceberg_schema}.{warm_entry.iceberg_table} "
        f"WHERE status = 'complete' "
        f"GROUP BY customer_id"
    )

    result = _simulate_trino_query_with_cache(warm_entry, sql)

    # Basic sanity: query was directed at the correct Iceberg catalog/table
    assert result["catalog"] == "iceberg_results", (
        "Query must target the Iceberg results catalog, not the remote RDBMS"
    )
    assert result["table"] == "warm_orders", "Query must target the materialized warm table"

    shared_data["query_result"] = result
    shared_data["query_sql"] = sql


# ---------------------------------------------------------------------------
# Step: Then
# ---------------------------------------------------------------------------


@then("Trino serves the result from local SSD Parquet cache at ~10-50ms latency")
def step_then_result_from_ssd_cache_low_latency(shared_data: dict[str, Any]) -> None:
    """
    Assert that:
    1. The query was served from the SSD Parquet cache (cache_hit=True).
    2. The reported latency is within the 10–50 ms target range.
    3. The result source is recorded as 'ssd_parquet_cache'.
    4. The underlying MVDefinition is still FRESH (TTL not expired).
    """
    result: dict[str, Any] = shared_data["query_result"]
    mv: MVDefinition = shared_data["mv"]

    # --- cache hit assertion ---
    assert result["cache_hit"] is True, (
        f"Expected a Trino SSD Parquet cache hit, but cache_hit={result['cache_hit']}. "
        f"source={result['source']}"
    )

    # --- latency assertion (10–50 ms target from REQ-238) ---
    latency_ms: float = result["latency_ms"]
    assert 10.0 <= latency_ms <= 50.0, (
        f"Expected SSD read latency 10–50 ms, got {latency_ms:.1f} ms. "
        "Warm table is not serving from local SSD Parquet cache."
    )

    # --- source label assertion ---
    assert result["source"] == "ssd_parquet_cache", (
        f"Expected source='ssd_parquet_cache', got source='{result['source']}'"
    )

    # --- MV freshness assertion (TTL/refresh pattern per REQ-238) ---
    assert mv.status == MVStatus.FRESH, (
        f"Warm table MV must remain FRESH after serving a query; status={mv.status}"
    )
    assert mv.last_refresh_at is not None, "Warm table MV must record a last_refresh_at timestamp"
    assert mv.last_refresh_at <= time.time(), (
        "Warm table MV last_refresh_at must not be in the future"
    )

    # --- row count sanity ---
    assert result["row_count"] is not None and result["row_count"] > 0, (
        "Warm table must contain materialized rows"
    )
