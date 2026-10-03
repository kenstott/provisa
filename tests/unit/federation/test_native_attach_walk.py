# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8b1c94-2a7f-4d63-9b0e-6c3f1a8d7e25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A native engine walks its registered tables once per registry state, not once per query.

``NativeEngineBackend._runtime_for`` runs before every ENGINE-route statement. It used to rebuild
the source map and re-walk every registered table each time, and — because a table whose attach is
refused for a declared reason (a source type the engine lands instead of attaching) was never
remembered — re-attempt those attaches on the engine connection on every query (measured on the
perf config: 22 of 36 tables, ``KeyError('attach')``, about a quarter of a millisecond each)."""

# Requirements: REQ-825

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from provisa.core.request_context import current_org
from provisa.federation.native_backend import NativeEngineBackend


class _Runtime:
    """Stands in for an engine runtime: records every attach; refuses land-only source types the
    way the DuckDB runtime does (their connector details carry no ``attach`` entry)."""

    def __init__(self) -> None:
        self.attempts: list[str] = []
        self._lock = threading.Lock()

    def attach_source(self, source) -> None:
        with self._lock:
            self.attempts.append(f"{source.schema_name}.{source.table_name}")
        if source.type.value == "openapi":
            raise KeyError("attach")


class _Backend(NativeEngineBackend):
    def __init__(self) -> None:
        super().__init__(SimpleNamespace(name="test-engine"))
        self.runtime = _Runtime()

    def _new_runtime(self):
        return self.runtime


class _Config:
    """A config whose ``sources`` reads are counted: one read per walk."""

    def __init__(self, sources, tables) -> None:
        self._sources = sources
        self.tables = tables
        self.walks = 0

    @property
    def sources(self):
        self.walks += 1
        return self._sources


def _source(source_id: str, kind: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=source_id,
        type=SimpleNamespace(value=kind),
        host="h",
        port=1,
        base_url=None,
        database="d",
        username="u",
        password="p",
        path=None,
        federation_hints={},
        mapping={},
        replicate=None,
        load_protected=False,
        cache_ttl=None,
        off_peak_window=None,
        change_signal="ttl",
    )


def _table(source_id: str, name: str) -> SimpleNamespace:
    return SimpleNamespace(source_id=source_id, schema_name="public", table_name=name)


def _state() -> SimpleNamespace:
    config = _Config(
        sources=[_source("pg", "postgresql"), _source("api", "openapi")],
        tables=[_table("pg", "orders"), _table("pg", "customers"), _table("api", "pets")],
    )
    return SimpleNamespace(
        config=config,
        runtime_sources={},
        tables=[
            {"source_id": "pg", "schema_name": "public", "table_name": "orders"},
            {"source_id": "api", "schema_name": "public", "table_name": "pets"},
        ],
        model_db=None,
        tenant_db=None,
        org_id="default",
        active_isolated_org=None,
    )


def test_repeated_queries_walk_the_registry_once():
    backend, state = _Backend(), _state()
    for _ in range(25):
        assert backend._runtime_for(state) is backend.runtime
    assert state.config.walks == 1, "the source map was rebuilt on a later query"
    attempts = backend.runtime.attempts
    assert sorted(set(attempts)) == ["public.customers", "public.orders", "public.pets"]
    assert attempts.count("public.orders") == 1
    assert attempts.count("public.pets") == 1, "a refused attach was re-attempted on a later query"


def test_a_registry_change_triggers_exactly_one_more_walk():
    backend, state = _Backend(), _state()
    for _ in range(5):
        backend._runtime_for(state)
    # A rebuild publishes a NEW table list (state.tables is replaced, never mutated in place).
    state.tables = [
        *state.tables,
        {"source_id": "pg", "schema_name": "public", "table_name": "invoices"},
    ]
    for _ in range(5):
        backend._runtime_for(state)
    assert state.config.walks == 2
    attempts = backend.runtime.attempts
    assert attempts.count("public.invoices") == 1, "the newly registered table was not attached"
    assert attempts.count("public.orders") == 1, "an attached table was attached again"
    assert attempts.count("public.pets") == 2, "a refused attach is retried once per registry state"


def test_a_source_registered_after_boot_is_reachable_on_the_next_query():
    backend, state = _Backend(), _state()
    backend._runtime_for(state)
    state.runtime_sources = {
        "pg2": {
            "type": "postgresql",
            "host": "h2",
            "port": 2,
            "database": "d",
            "username": "u",
            "password_ref": "p",
            "path": None,
            "base_url": None,
            "mapping": {},
        }
    }
    state.tables = [
        *state.tables,
        {"source_id": "pg2", "schema_name": "sales", "table_name": "leads"},
    ]
    backend._runtime_for(state)
    assert "sales.leads" in backend.runtime.attempts


def test_a_replaced_config_is_walked_again():
    backend, state = _Backend(), _state()
    backend._runtime_for(state)
    replacement = _Config(
        sources=[_source("pg", "postgresql")],
        tables=[_table("pg", "orders"), _table("pg", "shipments")],
    )
    state.config = replacement
    backend._runtime_for(state)
    backend._runtime_for(state)
    assert replacement.walks == 1
    assert backend.runtime.attempts.count("public.shipments") == 1


def test_concurrent_first_queries_share_one_walk():
    backend, state = _Backend(), _state()
    start = threading.Barrier(12)

    def _query() -> None:
        start.wait(timeout=10)
        backend._runtime_for(state)

    threads = [threading.Thread(target=_query) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
    assert state.config.walks == 1
    assert backend.runtime.attempts.count("public.orders") == 1


def test_the_single_org_guard_still_runs_on_every_query():
    """REQ-1266: the walk is skipped for an unchanged registry; the refusal of a foreign org is
    not part of the walk and must not be skipped with it."""
    backend, state = _Backend(), _state()
    backend._runtime_for(state)
    token = current_org.set("other-org")
    try:
        with pytest.raises(RuntimeError, match="single-org"):
            backend._runtime_for(state)
    finally:
        current_org.reset(token)


class _DriverDown(Exception):
    """A driver error: the source is offline, not unreachable by design."""


def test_a_driver_error_is_retried_on_the_next_query():
    """Only a DECLARED refusal is remembered for the registry state. A source that was offline
    must become queryable when it comes back, without waiting for a registry change."""

    class _FlakyRuntime(_Runtime):
        down = True

        def attach_source(self, source) -> None:
            super().attach_source(source)
            if source.table_name == "orders" and self.down:
                raise _DriverDown("connection refused")

    class _DriverBackend(_Backend):
        _attach_errors = (_DriverDown, *NativeEngineBackend._attach_errors)

    backend, state = _DriverBackend(), _state()
    backend.runtime = _FlakyRuntime()
    backend._runtime_for(state)
    backend._runtime_for(state)
    assert backend.runtime.attempts.count("public.orders") == 2
    backend.runtime.down = False
    backend._runtime_for(state)
    backend._runtime_for(state)
    attempts = backend.runtime.attempts
    assert attempts.count("public.orders") == 3, "the recovered source was not attached once"
    assert attempts.count("public.customers") == 1
    assert attempts.count("public.pets") == 1


def test_a_sqlite_control_plane_snapshot_is_checked_on_every_query():
    """The SQLite control plane is read through a snapshot the runtime re-takes when something was
    committed (a table registered after startup is visible to the next query). That check is not
    part of the registry walk."""

    class _SnapshotRuntime(_Runtime):
        def __init__(self) -> None:
            super().__init__()
            self.snapshots: list[tuple[str, str, str]] = []

        def attach_control_plane(self, db_path, schema_name, *, dialect) -> None:
            self.snapshots.append((db_path, schema_name, dialect))

    backend, state = _Backend(), _state()
    backend.runtime = _SnapshotRuntime()
    state.tenant_db = SimpleNamespace(
        dialect="sqlite", engine=SimpleNamespace(url=SimpleNamespace(database="/tmp/cp.db"))
    )
    state.model_db = state.tenant_db
    for _ in range(3):
        backend._runtime_for(state)
    assert backend.runtime.snapshots == [("/tmp/cp.db", "org_default", "sqlite")] * 3
    assert state.config.walks == 1
