# Copyright (c) 2026 Kenneth Stott
# Canary: 3b2d535b-df2b-4b85-abe8-649bea8c88d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1866/REQ-1885: the pre-governance prepared-statement cache in provisa.compiler.prepared.

Covers the narrow scope that module claims: cache hit/miss keying on (SQL shape, role, schema
generation), the no-stale-literal correctness guarantee a shape-based key requires, and the hard
"never cache a statement that localized an inline command" carve-out — never governance, RLS,
masking, or routing, which this module never touches.
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
async def test_different_sql_shape_is_a_cache_miss():
    """Different STRUCTURE (not just a different literal) must still miss."""
    state = _state()

    await prepared.prepare_front_end(
        "SELECT * FROM perf_bench.orders WHERE order_id = 1", "org_admin", state, _no_localize
    )
    different = await prepared.prepare_front_end(
        "SELECT id, status FROM perf_bench.orders WHERE order_id = 1 AND status = 'open'",
        "org_admin",
        state,
        _no_localize,
    )

    assert different.cache_hit is False


@pytest.mark.asyncio
async def test_same_shape_different_literal_is_a_cache_hit_with_correct_literal():
    """REQ-1885: the dominant point-lookup traffic shape (same query, different id per call) must
    now actually hit — and the returned SQL must reflect the SECOND call's own literal, never the
    first call's stale one. This is the single most important correctness property of the
    shape-keyed cache: a hit must never silently replay a prior call's literal value."""
    state = _state()

    first = await prepared.prepare_front_end(
        "SELECT * FROM perf_bench.orders WHERE order_id = 1", "org_admin", state, _no_localize
    )
    second = await prepared.prepare_front_end(
        "SELECT * FROM perf_bench.orders WHERE order_id = 999999", "org_admin", state, _no_localize
    )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert "999999" in second.normalized_sql
    assert "999999" in second.parsed.sql(dialect="postgres")
    assert "order_id = 1" not in second.normalized_sql
    assert second.normalized_sql != first.normalized_sql


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


@pytest.mark.asyncio
async def test_metric_query_template_hit_uses_current_calls_own_literal():
    """REQ-1885: a metric-expanded shape is cached as a template (skipping the join-plan rebuild
    on a hit) and its WHERE-clause literal is spliced from the CURRENT call, never replayed from
    the call that built the template. Proves the template+splice path (not just the plain path)
    honors the no-stale-literal correctness property."""
    from provisa.core.models import Metric

    metrics = {"order_count": Metric(name="order_count", expression="COUNT(orders.id)")}
    tables = [
        {
            "id": 1,
            "table_name": "orders",
            "columns": [{"column_name": "id"}, {"column_name": "status"}],
        }
    ]
    state = _state(metrics=metrics, tables=tables)

    first = await prepared.prepare_front_end(
        "SELECT status, value FROM metrics.order_count WHERE status = 'open'",
        "org_admin",
        state,
        _no_localize,
    )
    second = await prepared.prepare_front_end(
        "SELECT status, value FROM metrics.order_count WHERE status = 'closed'",
        "org_admin",
        state,
        _no_localize,
    )

    assert first.cache_hit is False
    assert first.metric_semantic_sql is not None
    assert second.cache_hit is True
    assert second.metric_semantic_sql is not None
    assert "'closed'" in second.metric_semantic_sql
    assert "'open'" not in second.metric_semantic_sql
    assert "'closed'" in second.parsed.sql(dialect="postgres")
    assert second.metric_semantic_sql != first.metric_semantic_sql


@pytest.mark.asyncio
async def test_wrapped_sampling_metric_query_still_reflects_current_literal():
    """The UI-sampling wrapper (`SELECT * FROM (<inner>) _sample LIMIT n`) is excluded from
    template-splicing (module docstring) and reruns `expand_metric_query` fresh on every hit —
    still must never leak a prior call's literal."""
    from provisa.core.models import Metric

    metrics = {"order_count": Metric(name="order_count", expression="COUNT(orders.id)")}
    tables = [
        {
            "id": 1,
            "table_name": "orders",
            "columns": [{"column_name": "id"}, {"column_name": "status"}],
        }
    ]
    state = _state(metrics=metrics, tables=tables)

    def _sql(status: str) -> str:
        return (
            "SELECT * FROM (SELECT status, value FROM metrics.order_count "
            f"WHERE status = '{status}') _sample LIMIT 100"
        )

    first = await prepared.prepare_front_end(_sql("open"), "org_admin", state, _no_localize)
    second = await prepared.prepare_front_end(_sql("closed"), "org_admin", state, _no_localize)

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert second.metric_semantic_sql is not None
    assert "'closed'" in second.metric_semantic_sql
    assert "'open'" not in second.metric_semantic_sql


@pytest.mark.asyncio
async def test_three_successive_calls_never_leak_a_stale_literal():
    """The cached template must never be mutated in place — each hit splices into a fresh copy.
    A bug that mutated the shared template (or returned it directly) would only surface once a
    THIRD call proved the second call's literal didn't linger."""
    from provisa.core.models import Metric

    metrics = {"order_count": Metric(name="order_count", expression="COUNT(orders.id)")}
    tables = [
        {
            "id": 1,
            "table_name": "orders",
            "columns": [{"column_name": "id"}, {"column_name": "status"}],
        }
    ]
    state = _state(metrics=metrics, tables=tables)

    def _sql(status: str) -> str:
        return f"SELECT status, value FROM metrics.order_count WHERE status = '{status}'"

    results = []
    for status in ("open", "closed", "pending"):
        results.append(
            await prepared.prepare_front_end(_sql(status), "org_admin", state, _no_localize)
        )

    assert results[0].cache_hit is False
    assert results[1].cache_hit is True
    assert results[2].cache_hit is True
    for status, result in zip(("open", "closed", "pending"), results):
        assert f"'{status}'" in result.metric_semantic_sql
        for other in ("open", "closed", "pending"):
            if other != status:
                assert f"'{other}'" not in result.metric_semantic_sql


def test_splice_raises_loudly_on_literal_count_mismatch():
    """A literal-count mismatch between a cached template and the current call must never be
    silently papered over — it must raise, per this project's no-silent-fallback rule."""
    import sqlglot

    template = sqlglot.parse_one("SELECT a FROM t WHERE x = 1 AND y = 2", read="postgres")
    current = sqlglot.parse_one("SELECT a FROM t WHERE x = 1", read="postgres")

    with pytest.raises(RuntimeError, match="literal count drifted"):
        prepared._splice_current_literals(template, current, cache_key="k")
