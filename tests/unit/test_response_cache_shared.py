# Copyright (c) 2026 Kenneth Stott
# Canary: 89740bf5-6efa-4a56-9c9a-4fe3ad1ffb1a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the shared response-cache HIT short circuit (REQ-1897).

``provisa.pgwire._pipeline.check_response_cache`` is the one implementation every raw-SQL
surface uses for a cache HIT: the chokepoint (``_execute_plan_in_org``, reached by Bolt and
pgwire's non-COPY path) and the three direct ``execute_engine_sync`` call sites that bypass it
(Flight SQL, gRPC's streaming RPC, pgwire's COPY-binary sink). These tests exercise the shared
helper directly rather than each transport's wiring, plus GraphQL's own read/write round trip
through the same typed encoding (REQ-1896) and its audit/egress fix.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import pytest

from provisa.audit.pipeline import PendingAudit
from provisa.cache.codec import encode_cache_payload
from provisa.cache.store import CachedResult, CacheStore
from provisa.pgwire._pipeline import _Plan, check_response_cache


class FakeCacheStore(CacheStore):
    """In-memory CacheStore -- no Redis dependency for a unit test."""

    def __init__(self) -> None:
        self._data: dict[str, CachedResult] = {}

    async def get(self, key: str, tenant_id: str | None = None) -> CachedResult | None:
        return self._data.get(self._k(key, tenant_id))

    async def set(
        self,
        key: str,
        data: bytes,
        ttl: int,
        tenant_id: str | None = None,
        table_ids: set[int] | None = None,
    ) -> None:
        self._data[self._k(key, tenant_id)] = CachedResult(
            data=data, cached_at=time.time(), ttl=ttl
        )

    async def invalidate_by_pattern(self, pattern: str, tenant_id: str | None = None) -> int:
        return 0

    async def invalidate_by_table(self, table_id: int, tenant_id: str | None = None) -> int:
        return 0

    async def close(self) -> None:
        pass

    @staticmethod
    def _k(key: str, tenant_id: str | None) -> str:
        return f"{tenant_id}:{key}" if tenant_id else key


@dataclass
class FakeState:
    response_cache_store: CacheStore
    tenant_db: Any = "fake-tenant-db"
    org_id: str | None = None


def _make_plan(sql: str = "SELECT id FROM t", role_id: str = "role-1") -> _Plan:
    audit = PendingAudit(
        user_id="user-1",
        surface="bolt",
        role_id=role_id,
        query_text=sql,
        table_ids=[42],
        started=time.time(),
    )
    return _Plan(route=object(), sql=sql, source_id="engine", dialect="postgres", audit=audit)


async def _seed_hit(
    store: FakeCacheStore, plan: _Plan, *, rows, column_names, column_types=None
) -> None:
    """Write a cache entry keyed and encoded exactly the way ``store_result`` does, so
    ``check_response_cache``'s ``decode_cached_result`` unwraps it the same way it would a
    real production write."""
    from provisa.cache.key import cache_key

    assert plan.audit is not None
    ck = cache_key(plan.sql, plan.exec_params or [], plan.audit.role_id, {})
    payload = {"data": {"rows": rows, "column_names": column_names}, "column_types": column_types}
    await store.set(ck, encode_cache_payload(payload), ttl=60)


async def _fake_write_audit_noop(pending, status_code, state=None) -> None:
    """A write_audit stand-in that just discards the row -- used by tests that only care about
    the returned QueryResult, not the audit call itself."""
    return None


@pytest.mark.asyncio
async def test_miss_returns_none(monkeypatch):
    plan = _make_plan()
    state = FakeState(response_cache_store=FakeCacheStore())
    result = await check_response_cache(plan, state)
    assert result is None


@pytest.mark.asyncio
async def test_hit_serves_rows_without_touching_engine(monkeypatch):
    plan = _make_plan()
    store = FakeCacheStore()
    await _seed_hit(store, plan, rows=[(1, "a"), (2, "b")], column_names=["id", "name"])
    state = FakeState(response_cache_store=store)

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _fake_write_audit_noop)

    result = await check_response_cache(plan, state)
    assert result is not None
    assert result.rows == [(1, "a"), (2, "b")]
    assert result.column_names == ["id", "name"]


@pytest.mark.asyncio
async def test_hit_writes_audit_row_at_200(monkeypatch):
    plan = _make_plan()
    store = FakeCacheStore()
    await _seed_hit(store, plan, rows=[(1,)], column_names=["id"])
    state = FakeState(response_cache_store=store)

    calls: list[tuple[Any, int]] = []

    async def _fake_write_audit(pending, status_code, state=None):
        calls.append((pending, status_code))

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _fake_write_audit)

    assert plan.audit_written is False
    result = await check_response_cache(plan, state)
    assert result is not None
    assert plan.audit_written is True
    assert calls == [(plan.audit, 200)]


@pytest.mark.asyncio
async def test_hit_runs_tier_egress_accounting(monkeypatch):
    """A cache hit must go through the SAME egress-cap enforcement a live execution would --
    this is the exact gap the maintainer flagged (cache hits skipping tier/egress accounting)."""
    plan = _make_plan()
    plan.tier_caps = object()
    plan.tier_plan = "starter"
    store = FakeCacheStore()
    await _seed_hit(store, plan, rows=[(1,)], column_names=["id"])
    state = FakeState(response_cache_store=store)

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _fake_write_audit_noop)

    enforce_calls: list[tuple[Any, Any]] = []

    class _FakePlugin:
        def enforce_output_cap(self, result, caps, plan_name):
            enforce_calls.append((caps, plan_name))
            return result

    monkeypatch.setattr("provisa.core.commerce.load", lambda: _FakePlugin())

    result = await check_response_cache(plan, state)
    assert result is not None
    assert enforce_calls == [(plan.tier_caps, "starter")]


@pytest.mark.asyncio
async def test_hit_egress_rejection_audits_402_not_200(monkeypatch):
    plan = _make_plan()
    plan.tier_caps = object()
    plan.tier_plan = "starter"
    store = FakeCacheStore()
    await _seed_hit(store, plan, rows=[(1,)], column_names=["id"])
    state = FakeState(response_cache_store=store)

    calls: list[int] = []

    async def _fake_write_audit(pending, status_code, state=None):
        calls.append(status_code)

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _fake_write_audit)

    class _FakePlugin:
        def enforce_output_cap(self, result, caps, plan_name):
            raise RuntimeError("egress cap exceeded")

    monkeypatch.setattr("provisa.core.commerce.load", lambda: _FakePlugin())

    with pytest.raises(RuntimeError):
        await check_response_cache(plan, state)
    assert calls == [402]


@pytest.mark.asyncio
async def test_unresolved_session_state_is_never_cacheable(monkeypatch):
    """REQ-866 fail-closed: a governed SQL string depending on current_setting() must never be
    read from (or, by the same gate, written to) the shared cache."""
    plan = _make_plan(sql="SELECT * FROM t WHERE tenant_id = current_setting('app.tenant')")
    store = FakeCacheStore()
    await _seed_hit(store, plan, rows=[(1,)], column_names=["id"])
    state = FakeState(response_cache_store=store)

    result = await check_response_cache(plan, state)
    assert result is None


@pytest.mark.asyncio
async def test_no_store_configured_is_a_miss(monkeypatch):
    plan = _make_plan()
    state = FakeState(response_cache_store=None)  # type: ignore[arg-type]
    result = await check_response_cache(plan, state)
    assert result is None


@pytest.mark.asyncio
async def test_different_roles_get_different_cache_entries(monkeypatch):
    """Two personas issuing the identical SQL text must never share a cache entry (REQ-866)."""
    plan_a = _make_plan(role_id="role-a")
    plan_b = _make_plan(role_id="role-b")
    store = FakeCacheStore()
    await _seed_hit(store, plan_a, rows=[(1,)], column_names=["id"])
    state = FakeState(response_cache_store=store)

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _fake_write_audit_noop)

    hit_for_a = await check_response_cache(plan_a, state)
    miss_for_b = await check_response_cache(plan_b, state)
    assert hit_for_a is not None
    assert miss_for_b is None
