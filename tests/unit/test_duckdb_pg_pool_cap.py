# Copyright (c) 2026 Kenneth Stott
# Canary: 75aaf7b8-ebee-476f-8509-ec4a9af4a569
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A capped PostgreSQL source's DuckDB pool holds its share of the cap and waits (REQ-1909).

The postgres extension gives each attached database a pool of up to 18 connections that connects
past its limit ('force'), so a worker could hold far more connections to a source than its
max_live_concurrency allows: the live permits cap queries, not connections. A capped source's pool
holds ``max_live_concurrency // workers`` and waits for a free connection; a cap below the worker
count is refused by name, at save and at attach.
"""

# Requirements: REQ-1909

from __future__ import annotations

import pytest

from provisa.api.admin.schema_mutation import _validate_source_load_management
from provisa.api.admin.types import SourceInput
from provisa.core.models import Source, SourceType
from provisa.federation.connector_duckdb import (
    DuckDBPostgresConnector,
    PoolCapBelowWorkers,
    pg_pool_max_connections,
)


@pytest.fixture
def workers(monkeypatch):
    def _set(n: int) -> None:
        monkeypatch.setattr("provisa.core.boot_lock.expected_workers", lambda: n)

    return _set


def _pg(cap: int | None) -> Source:
    return Source(
        id="orders-pg",
        type=SourceType.postgresql,
        host="db",
        port=5432,
        database="orders",
        username="reader",
        max_live_concurrency=cap,
    )


def test_the_cap_is_divided_over_the_workers(workers):
    workers(4)
    assert pg_pool_max_connections("orders-pg", 8) == 2
    assert pg_pool_max_connections("orders-pg", 9) == 2  # never more than the cap in total


def test_an_uncapped_source_has_no_pool_size_of_its_own(workers):
    workers(4)
    assert pg_pool_max_connections("orders-pg", None) is None


def test_a_cap_below_the_worker_count_is_refused_by_name(workers):
    workers(4)
    with pytest.raises(
        PoolCapBelowWorkers, match=r"'orders-pg'.*max_live_concurrency 3.*4 workers"
    ):
        pg_pool_max_connections("orders-pg", 3)


def test_a_capped_source_is_attached_with_its_share_and_waits(workers):
    workers(2)
    details = DuckDBPostgresConnector().details(_pg(6))
    assert details["configure_pool"] == (
        "FROM postgres_configure_pool(catalog_name='_src_orders-pg', "
        "max_connections=3, acquire_mode='wait')"
    )


def test_an_uncapped_source_is_attached_as_the_extension_builds_it(workers):
    workers(2)
    assert "configure_pool" not in DuckDBPostgresConnector().details(_pg(None))


def test_the_attach_refuses_a_cap_below_the_worker_count(workers):
    workers(4)
    with pytest.raises(PoolCapBelowWorkers, match="'orders-pg'"):
        DuckDBPostgresConnector().details(_pg(2))


def test_the_runtime_configures_the_pool_right_after_the_attach(workers):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    workers(2)
    executed: list[str] = []
    runtime = DuckDBFederationRuntime.__new__(DuckDBFederationRuntime)
    runtime._con = type("_Con", (), {"execute": lambda _self, sql: executed.append(sql)})()
    runtime._pg_ext_loaded = False
    runtime._raw_attached = set()
    runtime._raw_attach_ddl = {}
    runtime._unreachable = {}
    runtime._attach_source = {}
    source = _pg(4)
    runtime._attach_raw(source, DuckDBPostgresConnector().details(source))
    attach = next(i for i, sql in enumerate(executed) if sql.startswith("ATTACH "))
    assert executed[attach + 1 :] == [
        "FROM postgres_configure_pool(catalog_name='_src_orders-pg', "
        "max_connections=2, acquire_mode='wait')"
    ]


def test_an_environments_own_attach_has_its_own_pool_configured(workers):
    """REQ-1529, REQ-1909: a source an environment binds to another database is attached a second
    time under a catalog of its own; the pool configured is that catalog's, not the first one's."""
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    workers(2)
    executed: list[str] = []
    runtime = DuckDBFederationRuntime.__new__(DuckDBFederationRuntime)
    runtime._con = type("_Con", (), {"execute": lambda _self, sql: executed.append(sql)})()
    runtime._pg_ext_loaded = True
    runtime._raw_attached = set()
    runtime._raw_attach_ddl = {}
    runtime._unreachable = {}
    runtime._attach_source = {}
    runtime._catalog_of = lambda _source: "dev"  # type: ignore[method-assign]
    runtime._ext_loaded = set()
    runtime._engine = type("_Engine", (), {"connector_for": lambda _self, _t: object()})()
    source = _pg(4)
    runtime._attach_raw(source, DuckDBPostgresConnector().details(source))
    source.database = "orders_dev"
    alias = runtime._attach_raw(source, DuckDBPostgresConnector().details(source))
    assert alias == "_src_orders-pg__dev"
    assert executed[-1] == (
        "FROM postgres_configure_pool(catalog_name='_src_orders-pg__dev', "
        "max_connections=2, acquire_mode='wait')"
    )


def _input(cap: int | None) -> SourceInput:
    return SourceInput(id="orders-pg", type="postgresql", max_live_concurrency=cap)


def test_saving_a_cap_below_the_worker_count_is_refused_on_duckdb(workers, monkeypatch):
    workers(4)
    monkeypatch.setattr("provisa.api.admin.schema_mutation._engine_name", lambda: "duckdb")
    result = _validate_source_load_management(_input(3))
    assert result is not None and result.success is False
    assert result.code == "schema.max_live_concurrency_below_workers"
    assert "4 workers" in result.message


def test_saving_a_cap_the_workers_can_share_is_accepted(workers, monkeypatch):
    workers(4)
    monkeypatch.setattr("provisa.api.admin.schema_mutation._engine_name", lambda: "duckdb")
    assert _validate_source_load_management(_input(4)) is None


def test_another_engine_does_not_divide_the_cap(workers, monkeypatch):
    workers(4)
    monkeypatch.setattr("provisa.api.admin.schema_mutation._engine_name", lambda: "trino")
    assert _validate_source_load_management(_input(3)) is None


def _walk_attaches(monkeypatch, row_cap: int | None) -> list[dict]:
    """Walk the native backend's registry with one postgres source registered at runtime (a
    sources row, not the config file), attaching through the real DuckDB postgres connector:
    the details each attach was given."""
    from types import SimpleNamespace

    from provisa.federation.replica_address import ReplicaRoutes

    from provisa.federation.native_backend import NativeEngineBackend

    monkeypatch.setattr("provisa.core.operator_floor.floor_setting", lambda _src: None)
    details: list[dict] = []

    class _Runtime:
        def attach_source(self, source) -> None:
            details.append(DuckDBPostgresConnector().details(source))

    class _Backend(NativeEngineBackend):
        def __init__(self) -> None:  # the walk and its shims alone are under test
            self._runtime = _Runtime()
            self.engine = SimpleNamespace(
                name="duckdb",
                connector_for_source=lambda _src: SimpleNamespace(reads_in_place=True),
            )
            self._attached = set()
            self._attached_fields = {}
            self._detached = set()
            self._walks = {}
            self._failed = {}
            self._starting = set()

    row = {
        "id": "shop-db",
        "type": "postgresql",
        "region": None,
        "host": "db",
        "port": 5432,
        "database": "shop",
        "username": "reader",
        "password_ref": "reader",
        "path": None,
        "base_url": None,
        "mapping": {},
        "replicate": None,
        "load_protected": False,
        "max_live_concurrency": row_cap,
        "cache_ttl": None,
    }
    config = SimpleNamespace(
        sources=[],
        tables=[
            SimpleNamespace(
                source_id="shop-db",
                schema_name="shop",
                table_name="orders",
                region=None,
                cache_ttl=None,
            )
        ],
    )
    state = SimpleNamespace(
        replica_routes=ReplicaRoutes(),
        runtime_sources={"shop-db": row},
        tables=[],
        model_db=None,
        tenant_db=None,
        source_catalogs={"shop-db": "shop_db"},
    )
    assert _Backend()._walk_registry(state, config) is True
    return details


def test_a_runtime_registered_capped_source_is_attached_with_its_pool_capped(workers, monkeypatch):
    """REQ-1909: a source registered after startup reaches the attach through the native
    backend's shims, which carry its live-read cap; its pool is its share of the cap."""
    workers(2)
    [attached] = _walk_attaches(monkeypatch, 4)
    assert attached["configure_pool"] == (
        "FROM postgres_configure_pool(catalog_name='_src_shop-db', "
        "max_connections=2, acquire_mode='wait')"
    )


def test_a_runtime_registered_uncapped_source_is_attached_as_the_extension_builds_it(
    workers, monkeypatch
):
    workers(2)
    [attached] = _walk_attaches(monkeypatch, None)
    assert "configure_pool" not in attached
