# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-947: per-engine reach faces drive the source-creation dropdown.

``live_source_types`` = types the engine reads LIVE via a live-attach connector (tagged LIVE).
``reachable_source_types`` = every configurable type: live-attach ∪ materialize-only ∪ the
Provisa-direct drivers/adapters that land a refreshed replica (tagged REPLICA). A type outside the
current engine's reachable union is unreachable there but may be LIVE on another engine — the
dropdown disables it and names the engine that reaches it, educating on the engine choice.
"""

from __future__ import annotations

from provisa.federation.engine import (
    engine_registry,
    live_source_types,
    reachable_source_types,
)


def test_live_is_subset_of_reachable():
    for key in ("trino", "duckdb", "pg", "clickhouse"):
        assert set(live_source_types(key)) <= set(reachable_source_types(key)), key


def test_trino_attaches_postgresql_live():
    assert "postgresql" in live_source_types("trino")
    assert "postgresql" in reachable_source_types("trino")


def test_snowflake_reachable_everywhere_and_live_on_attach_engines():
    # REQ-988: Snowflake now has a Provisa-direct driver (read-then-land), so it is a configurable
    # REPLICA on EVERY engine — including engines with no live-attach connector for it. It stays LIVE
    # (in-place attach) on engines that do attach it (Trino JDBC, DuckDB snowflake extension).
    live_engines = {e["key"] for e in engine_registry() if "snowflake" in e["live_source_types"]}
    assert "trino" in live_engines
    for key in ("trino", "duckdb", "pg", "clickhouse", "sqlalchemy"):
        assert "snowflake" in reachable_source_types(key), key


def test_direct_driver_type_reachable_on_every_engine():
    # oracle has a Provisa-native driver (DIRECT): Provisa reads + lands it, so it is a configurable
    # REPLICA on engines that do not attach it live.
    for key in ("trino", "duckdb", "pg", "clickhouse", "sqlalchemy"):
        assert "oracle" in reachable_source_types(key), key


def test_materialize_only_type_reachable_on_every_engine():
    # rss only federates by being landed — reachable (REPLICA) everywhere, never LIVE on any engine.
    for key in ("trino", "duckdb", "pg", "clickhouse"):
        assert "rss" in reachable_source_types(key), key
        assert "rss" not in live_source_types(key), key


def test_connector_pgwire_types_reachable_on_every_engine():
    # files/sharepoint/splunk read via their Calcite pgwire server (generic postgres) → REPLICA on any
    # engine. On Trino they're LIVE (attach/scan); off Trino they federate as a landed replica.
    for t in ("files", "sharepoint", "splunk"):
        for key in ("trino", "duckdb", "pg", "clickhouse"):
            assert t in reachable_source_types(key), f"{t} on {key}"
        assert t in live_source_types("trino"), t
        assert t not in live_source_types("duckdb"), t


def test_duckdb_attaches_duckdb_live():
    assert "duckdb" in live_source_types("duckdb")


def test_registry_entries_carry_reach_faces():
    for e in engine_registry():
        assert "reachable_source_types" in e
        assert "live_source_types" in e
