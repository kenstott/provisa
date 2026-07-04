# Copyright (c) 2026 Kenneth Stott
# Canary: 7e4f2b83-6d2b-4f97-a034-5e9c2f6d1e39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-825: EngineRuntime — terminal-route dispatch + capability-gated engine transports."""

from __future__ import annotations

import types

import pytest

from provisa.executor.trino import QueryResult
from provisa.federation.engine import (
    build_duckdb_engine,
    build_snowflake_engine,
    build_trino_engine,
)
from provisa.federation.runtime import (
    EngineCapability,
    EngineRuntime,
    UnsupportedCapabilityError,
)
from provisa.transpiler.router import Route, RouteDecision


class _FakePools:
    def __init__(self, present: set[str]) -> None:
        self._present = present

    def has(self, source_id: str) -> bool:
        return source_id in self._present


def _state(*, trino_conn=object(), flight_client=None, present=()):
    return types.SimpleNamespace(
        trino_conn=trino_conn,
        flight_client=flight_client,
        source_pools=_FakePools(set(present)),
    )


# ---- capability advertisement (REQ-825) -------------------------------------


def test_trino_advertises_all_transports():
    rt = EngineRuntime(build_trino_engine(), _state())
    assert rt.capabilities == frozenset(
        {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
    )
    assert rt.supports(EngineCapability.ARROW_STREAM) is True


def test_duckdb_omits_flight_stream():
    rt = EngineRuntime(build_duckdb_engine(), _state())
    assert rt.supports(EngineCapability.ARROW) is True
    assert rt.supports(EngineCapability.ARROW_STREAM) is False


def test_require_fails_closed_for_unsupported_transport():
    rt = EngineRuntime(build_snowflake_engine(), _state())
    with pytest.raises(UnsupportedCapabilityError):
        rt.require(EngineCapability.ARROW_STREAM)


# ---- terminal-route dispatch (REQ-825) --------------------------------------


@pytest.mark.asyncio
async def test_direct_route_uses_native_terminal(monkeypatch):
    seen = {}

    async def _fake_direct(pools, source_id, sql, params):
        seen.update(pools=pools, source_id=source_id, sql=sql, params=params)
        return QueryResult(rows=[(1,)], column_names=["x"])

    monkeypatch.setattr("provisa.executor.direct.execute_direct", _fake_direct)
    rt = EngineRuntime(build_trino_engine(), _state(present={"pg"}))
    decision = RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="")

    result = await rt.execute(decision, "SELECT 1", [], source_pools=rt._state.source_pools)

    assert result.rows == [(1,)]
    assert seen["source_id"] == "pg"


@pytest.mark.asyncio
async def test_engine_route_uses_engine_terminal(monkeypatch):
    seen = {}

    def _fake_trino(conn, sql, **kwargs):
        seen.update(conn=conn, sql=sql, kwargs=kwargs)
        return QueryResult(rows=[(2,)], column_names=["y"])

    monkeypatch.setattr("provisa.executor.trino.execute_trino", _fake_trino)
    conn = object()
    rt = EngineRuntime(build_trino_engine(), _state(trino_conn=conn, present={"pg"}))
    # Multi-source / non-direct → ENGINE terminal even though a direct source exists.
    decision = RouteDecision(route=Route.TRINO, source_id=None, dialect=None, reason="")

    result = await rt.execute(decision, "SELECT 2", [], source_pools=rt._state.source_pools)

    assert result.rows == [(2,)]
    assert seen["conn"] is conn


@pytest.mark.asyncio
async def test_direct_route_falls_to_engine_when_source_absent(monkeypatch):
    def _fake_trino(conn, sql, **kwargs):
        return QueryResult(rows=[(3,)], column_names=["z"])

    monkeypatch.setattr("provisa.executor.trino.execute_trino", _fake_trino)
    rt = EngineRuntime(build_trino_engine(), _state(present=frozenset()))
    # DIRECT decision but the source has no pool → ENGINE terminal.
    decision = RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="")

    result = await rt.execute(decision, "SELECT 3", [], source_pools=rt._state.source_pools)

    assert result.rows == [(3,)]


# ---- flight transport fail-closed (REQ-825) ---------------------------------


def test_arrow_transport_fails_closed_when_proxy_unconfigured():
    rt = EngineRuntime(build_trino_engine(), _state(flight_client=None))
    with pytest.raises(RuntimeError, match="Arrow Flight transport is not configured"):
        rt.execute_engine_arrow("SELECT 1")
