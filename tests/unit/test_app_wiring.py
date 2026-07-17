# Copyright (c) 2026 Kenneth Stott
# Canary: e122d5f0-ff2a-46b6-a5e6-4bd94f9fbcdb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-941: wire_event_loop — best-effort boot wiring of the event loop onto the scheduler."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from provisa.events.app_wiring import wire_event_loop
from provisa.federation.engine import build_duckdb_engine

_LOG = logging.getLogger("test")


@pytest.fixture(autouse=True)
def _patch_fetch_tables(monkeypatch):
    # wire_event_loop drives off the REGISTERED tables (control plane); the fake conn carries them.
    async def _fetch(conn):
        return getattr(conn, "registered", [])

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch)


class _Sched:
    def __init__(self):
        self.jobs: list[str] = []

    def add_job(self, fn, trigger=None, id="", replace_existing=None):
        self.jobs.append(id)


class _Conn:
    def __init__(self, registered):
        self.registered = registered

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def _fake_db(registered):
    return SimpleNamespace(acquire=lambda: _Conn(registered))


def _rcol(name, dt="bigint", pk=False):
    return {"column_name": name, "data_type": dt, "is_primary_key": pk, "native_filter_type": None}


def _state(*, ready=True):
    if not ready:
        return SimpleNamespace(tenant_db=None, federation_engine=None, config=None)
    engine = SimpleNamespace(
        engine=build_duckdb_engine(),
        materialize_store_dsn=lambda: "sqlite://",
    )
    config = SimpleNamespace(
        sources=[
            SimpleNamespace(id="api", type=SimpleNamespace(value="openapi"), change_signal="ttl")
        ],
        tables=[
            SimpleNamespace(
                source_id="api",
                schema_name="default",
                table_name="events",
                change_signal=None,
                watermark_column=None,
                live=None,
                cache_ttl=300,
            )
        ],
    )
    registered = [
        {
            "source_id": "api",
            "schema_name": "default",
            "table_name": "events",
            "columns": [_rcol("id", "bigint", pk=True)],
        }
    ]
    registry = SimpleNamespace(get_enabled=lambda: [])
    return SimpleNamespace(
        tenant_db=_fake_db(registered),
        federation_engine=engine,
        config=config,
        mv_registry=registry,
    )


@pytest.mark.asyncio
async def test_skips_when_prerequisites_missing():
    sched = _Sched()
    n = await wire_event_loop(sched, state=_state(ready=False), log=_LOG)
    assert n == 0 and sched.jobs == []  # no db/engine/config → no-op, boot unharmed


@pytest.mark.asyncio
async def test_registers_source_node_and_runtime_jobs():
    sched = _Sched()
    n = await wire_event_loop(sched, state=_state(), log=_LOG)
    assert n == 1  # the one MATERIALIZED source table (openapi) → a source node
    assert "events:tick" in sched.jobs and "events:reaper" in sched.jobs


def _state_with_mv(*, column_types):
    """State with one enabled MV; its engine probe returns typed output columns."""
    from provisa.executor.result import QueryResult

    async def _execute_engine(sql, *a, **k):
        return QueryResult(rows=[], column_names=["d", "n"], column_types=column_types)

    engine = SimpleNamespace(
        engine=build_duckdb_engine(),
        materialize_store_dsn=lambda: "sqlite://",
        execute_engine=_execute_engine,
    )
    mv = SimpleNamespace(
        target_schema="analytics",
        target_table="daily",
        sql="SELECT day AS d, count(*) AS n FROM orders GROUP BY day",
        freshness_mode="ttl",
        refresh_interval=600,
        debounce_quiet=0.0,
        debounce_max_delay=None,
    )
    return SimpleNamespace(
        tenant_db=_fake_db([]),
        federation_engine=engine,
        config=SimpleNamespace(sources=[], tables=[]),
        mv_registry=SimpleNamespace(get_enabled=lambda: [mv]),
    )


@pytest.mark.asyncio
async def test_mv_columns_introspected_and_node_registered():
    # LIMIT-0 probe yields typed columns → translated native→IR → the MV node registers.
    n = await wire_event_loop(
        _Sched(), state=_state_with_mv(column_types=["date", "bigint"]), log=_LOG
    )
    assert n == 1  # the one MV node


@pytest.mark.asyncio
async def test_mv_with_unmapped_output_type_skipped():
    # an unmapped output type is an IR vocabulary gap → the MV is skipped (not silently defaulted).
    n = await wire_event_loop(
        _Sched(), state=_state_with_mv(column_types=["date", "geometry"]), log=_LOG
    )
    assert n == 0  # no node — its columns did not resolve to IR


@pytest.mark.asyncio
async def test_never_raises_into_boot():
    # a malformed state (missing attrs) must be swallowed, not propagated
    n = await wire_event_loop(
        _Sched(),
        state=SimpleNamespace(
            tenant_db=object(), federation_engine=SimpleNamespace(), config=SimpleNamespace()
        ),
        log=_LOG,
    )
    assert n == 0
