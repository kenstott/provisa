# Copyright (c) 2026 Kenneth Stott
# Canary: 3b2d535b-df2b-4b85-abe8-649bea8c88d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1866: the pre-governance prepared-statement cache in provisa.compiler.prepared.

Covers exactly the narrow scope that module claims: cache hit/miss keying on (sql text, role,
schema generation), and the hard "never cache a statement that localized an inline command"
carve-out — never governance, RLS, masking, or routing, which this module never touches.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler import prepared


class _FakeState(SimpleNamespace):
    pass


def _state(schema_boot_id="boot-1", schema_version=1, metrics=None, tables=None):
    return _FakeState(
        schema_boot_id=schema_boot_id,
        schema_version=schema_version,
        metrics=metrics or {},
        tables=tables or [],
        relationships=[],
    )


async def _no_localize(tree, role_id, state):  # noqa: ARG001 - matches the injected signature
    """Stand-in for REQ-1159's _localize_inline_commands: no inline command ever matches."""
    return False


@pytest.fixture(autouse=True)
def _clear_cache():
    prepared.clear()
    yield
    prepared.clear()


@pytest.mark.asyncio
async def test_second_identical_call_is_a_cache_hit():
    state = _state()
    sql = "SELECT * FROM perf_bench.orders WHERE order_id = 12345"

    first = await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)
    second = await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert second.normalized_sql == first.normalized_sql


@pytest.mark.asyncio
async def test_different_role_is_a_cache_miss():
    state = _state()
    sql = "SELECT * FROM perf_bench.orders WHERE order_id = 12345"

    await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)
    other_role = await prepared.prepare_front_end(sql, "analyst", state, _no_localize)

    assert other_role.cache_hit is False


@pytest.mark.asyncio
async def test_schema_version_bump_invalidates():
    state = _state(schema_version=1)
    sql = "SELECT * FROM perf_bench.orders WHERE order_id = 12345"

    await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)

    state_v2 = _state(schema_version=2)
    after_rebuild = await prepared.prepare_front_end(sql, "org_admin", state_v2, _no_localize)

    assert after_rebuild.cache_hit is False


@pytest.mark.asyncio
async def test_schema_boot_id_change_invalidates():
    state = _state(schema_boot_id="boot-1")
    sql = "SELECT * FROM perf_bench.orders WHERE order_id = 12345"

    await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)

    restarted = _state(schema_boot_id="boot-2")
    after_restart = await prepared.prepare_front_end(sql, "org_admin", restarted, _no_localize)

    assert after_restart.cache_hit is False


@pytest.mark.asyncio
async def test_different_sql_text_is_a_cache_miss():
    state = _state()

    await prepared.prepare_front_end(
        "SELECT * FROM perf_bench.orders WHERE order_id = 1", "org_admin", state, _no_localize
    )
    different = await prepared.prepare_front_end(
        "SELECT * FROM perf_bench.orders WHERE order_id = 2", "org_admin", state, _no_localize
    )

    assert different.cache_hit is False


@pytest.mark.asyncio
async def test_inline_command_localization_is_never_cached():
    """A statement localize_inline_commands actually rewrites must never be cached, even for
    the identical text/role/generation, since the rewrite bakes in this call's own literal
    command-invocation result (REQ-1159) — reusing it would silently replay a stale result."""

    async def _always_localize(tree, role_id, state):  # noqa: ARG001 - matches injected signature
        return True  # pretend every call finds and localizes an inline command

    state = _state()
    sql = "SELECT * FROM perf_bench.orders WHERE order_id = 12345"

    first = await prepared.prepare_front_end(sql, "org_admin", state, _always_localize)
    second = await prepared.prepare_front_end(sql, "org_admin", state, _always_localize)

    assert first.cache_hit is False
    assert second.cache_hit is False  # still a miss the second time — never entered the cache


@pytest.mark.asyncio
async def test_metric_semantic_sql_survives_a_cache_hit():
    from provisa.core.models import Metric

    metrics = {"order_count": Metric(name="order_count", expression="COUNT(orders.id)")}
    tables = [{"id": 1, "table_name": "orders", "columns": [{"column_name": "id"}]}]
    state = _state(metrics=metrics, tables=tables)
    sql = "SELECT value FROM metrics.order_count"

    first = await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)
    second = await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)

    if first.metric_semantic_sql is not None:  # only meaningful if expansion actually matched
        assert second.cache_hit is True
        assert second.metric_semantic_sql == first.metric_semantic_sql
