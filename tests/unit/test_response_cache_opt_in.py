# Copyright (c) 2026 Kenneth Stott
# Canary: 1e6b9d42-7a3c-4f85-b0d2-9c4e8a1f6b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-544 (amended 2026-09-30): the response cache is per-request OPT-IN.

The end user trades recency for speed only by asking: GraphQL ``@cached(ttl)``, SQL
``-- @provisa cache=true`` / ``-- @provisa cache_ttl=N``. No hint — no cache read, no cache write.
The operator's source/table settings stay the permission and TTL: a disabled source (or a TTL of 0)
keeps even an opted-in result out. ``@noCache`` / ``no_cache`` are gone (caching is off by default).
A failed invalidation raises instead of leaving stale entries behind.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from graphql import parse

from provisa.audit.pipeline import PendingAudit
from provisa.cache.policy import opt_in_ttl
from provisa.compiler.directives import (
    extract_directives,
    extract_directives_from_sql_comments,
    merge_directives,
)
from provisa.executor.result import StreamingQueryResult
from provisa.pgwire._pipeline import (
    _Plan,
    _response_cache_key,
    check_response_cache,
    response_cache_tee,
)
from tests.unit.test_response_cache_shared import FakeCacheStore

# -- directives -----------------------------------------------------------------------------------


def test_no_hint_means_no_opt_in():
    d = extract_directives(parse("query { orders { id } }"))
    assert d.cache_opt_in is False and d.cache_ttl is None
    s = extract_directives_from_sql_comments("SELECT 1")
    assert s.cache_opt_in is False and s.cache_ttl is None


def test_cached_directive_opts_in_with_or_without_ttl():
    bare = extract_directives(parse("query @cached { orders { id } }"))
    assert bare.cache_opt_in is True and bare.cache_ttl is None
    ttl = extract_directives(parse("query @cached(ttl: 60) { orders { id } }"))
    assert ttl.cache_opt_in is True and ttl.cache_ttl == 60


def test_sql_comment_hints_opt_in():
    on = extract_directives_from_sql_comments("-- @provisa cache=true\nSELECT 1")
    assert on.cache_opt_in is True and on.cache_ttl is None
    ttl = extract_directives_from_sql_comments("-- @provisa cache_ttl=30\nSELECT 1")
    assert ttl.cache_opt_in is True and ttl.cache_ttl == 30
    merged = merge_directives(on, extract_directives_from_sql_comments("SELECT 1"))
    assert merged.cache_opt_in is True


def test_the_removed_bypass_knobs_are_gone():
    from provisa.compiler.directives import QueryDirectives
    from provisa.compiler.schema_directives import PROVISA_DIRECTIVES

    assert not hasattr(QueryDirectives(), "no_cache")
    assert "noCache" not in {d.name for d in PROVISA_DIRECTIVES}
    legacy = extract_directives_from_sql_comments("-- @provisa no_cache=true\nSELECT 1")
    assert legacy.cache_opt_in is False  # not a recognized key: no effect at all


def test_a_malformed_ttl_fails_instead_of_being_ignored():
    with pytest.raises(ValueError):
        extract_directives_from_sql_comments("-- @provisa cache_ttl=soon\nSELECT 1")


# -- policy: operator settings are the permission + TTL ---------------------------------------------


def test_opt_in_ttl_is_gated_by_the_operator():
    assert opt_in_ttl(None, [300, 60]) == 60  # no request ttl: the shortest operator TTL
    assert opt_in_ttl(5, [300, 60]) == 5  # the request chooses its own staleness
    assert opt_in_ttl(3600, [300]) == 3600  # any ttl — never fresher than the replica read
    assert opt_in_ttl(60, [300, 0]) == 0  # a table the operator keeps out of the cache
    assert opt_in_ttl(None, []) == 0


# -- raw-SQL plans: no hint, no read, no write ------------------------------------------------------


def _plan(**kw) -> _Plan:
    audit = PendingAudit(
        user_id="u",
        surface="pgwire",
        role_id="analyst",
        query_text="SELECT id FROM t",
        table_ids=[7],
        started=time.time(),
    )
    return _Plan(
        route=object(),
        sql="SELECT id FROM t",
        source_id="engine",
        dialect="duckdb",
        audit=audit,
        role_id="analyst",
        table_ids=(7,),
        response_cacheable=True,
        **kw,
    )


def _state(store, *, source_cache=None):
    return SimpleNamespace(
        response_cache_store=store,
        tenant_db="fake",
        org_id="org-a",
        contexts={
            "analyst": SimpleNamespace(tables={"t": SimpleNamespace(table_id=7, source_id="pg")})
        },
        source_cache=source_cache if source_cache is not None else {},
        table_cache={},
        response_cache_default_ttl=300,
    )


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    monkeypatch.setattr("provisa.pgwire._pipeline._response_cache_bound", lambda: 100)


def _drain(tee) -> None:
    stream = StreamingQueryResult(iter([[(1,)]]), column_names=["id"], column_types=["BIGINT"])
    for _ in tee.rows(stream).batches():
        pass


def test_an_unhinted_plan_has_no_key_and_no_tee():
    plan = _plan()
    assert _response_cache_key(plan, wire_formats=None) is None
    assert response_cache_tee(plan, _state(FakeCacheStore()), run=None) is None


@pytest.mark.asyncio
async def test_an_unhinted_plan_never_reads_an_existing_entry():
    store = FakeCacheStore()
    state = _state(store)
    hinted = response_cache_tee(_plan(cache_opt_in=True), state, run=None)
    assert hinted is not None
    _drain(hinted)
    await hinted.commit()
    assert await check_response_cache(_plan(cache_opt_in=True), state) is not None
    assert await check_response_cache(_plan(), state) is None  # same statement, no hint: MISS


@pytest.mark.asyncio
async def test_a_disabled_source_blocks_a_hint(monkeypatch):
    captured: list[int] = []
    store = FakeCacheStore()

    async def _set(key, data, ttl, tenant_id=None, table_ids=None):
        captured.append(ttl)

    monkeypatch.setattr(store, "set", _set)
    off = _state(store, source_cache={"pg": {"cache_enabled": False}})
    assert response_cache_tee(_plan(cache_opt_in=True, cache_ttl=60), off, run=None) is None
    tee = response_cache_tee(_plan(cache_opt_in=True, cache_ttl=45), _state(store), run=None)
    assert tee is not None
    _drain(tee)
    await tee.commit()
    assert captured == [45]  # the request's ttl, permitted by an enabled source


@pytest.mark.asyncio
async def test_the_raw_sql_pipeline_reads_the_hint_off_the_statement(monkeypatch):
    """The plan the one pipeline mints carries the statement's `-- @provisa cache` hint."""
    from unittest.mock import patch

    from provisa.pgwire import _pipeline
    from tests.unit.test_governed_sql_engine_internals import _state as gov_state

    import provisa.api.app as app_mod

    seen: list = []

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        from provisa.transpiler.router import Route, RouteDecision

        seen.append(1)
        return (
            exec_sql,
            RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="t"),
            "pg",
            False,
            {"pg"},
            (),
        )

    monkeypatch.setattr(app_mod, "state", gov_state(["*"]), raising=False)
    monkeypatch.setattr(app_mod.state, "source_dialects", {"pg": "postgres"}, raising=False)
    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        hinted = await _pipeline._govern_and_route(
            "-- @provisa cache_ttl=30\nSELECT o.id FROM sales.orders o", "analyst"
        )
        plain = await _pipeline._govern_and_route("SELECT o.id FROM sales.orders o", "analyst")
    assert (hinted.cache_opt_in, hinted.cache_ttl) == (True, 30)
    assert (plain.cache_opt_in, plain.cache_ttl) == (False, None)


# -- invalidation -----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_failed_invalidation_raises():
    from redis.exceptions import ConnectionError as RedisConnectionError

    from provisa.cache.store import RedisCacheStore

    store = RedisCacheStore("redis://127.0.0.1:1/0")
    store._redis = SimpleNamespace(smembers=AsyncMock(side_effect=RedisConnectionError("down")))
    store._connect = AsyncMock(return_value=None)  # type: ignore[method-assign]
    with pytest.raises(RedisConnectionError):
        await store.invalidate_by_table(7, tenant_id="org-a")


# -- caching disabled: no tee, no buffering -------------------------------------------------------


def test_a_noop_store_means_no_tee_even_for_an_opted_in_plan():
    """With caching disabled (AppState's NoopCacheStore) cacheability is decided before any
    stream is wrapped: no tee, so nothing is ever buffered."""
    from provisa.cache.store import NoopCacheStore
    from provisa.pgwire._pipeline import serve_stream_through_cache

    state = _state(NoopCacheStore())
    assert response_cache_tee(_plan(cache_opt_in=True), state, run=None) is None
    stream = StreamingQueryResult(iter([[(1,)]]), column_names=["id"], column_types=["BIGINT"])
    served = serve_stream_through_cache(
        _plan(cache_opt_in=True),
        state,
        run=lambda coro: coro.close(),  # a read against the no-op store is never dispatched
        check_rows=False,
        passthrough=None,
        open_rows=lambda: stream,
    )
    assert served is stream  # handed back unwrapped


# -- every transport: the request's own hint, parsed once per language, reaches the plan ----------


def test_each_language_parses_its_own_hint_syntax():
    from provisa.compiler.directives import NO_CACHE_HINT, CacheHint, cache_hint_for

    assert cache_hint_for("graphql", "query @cached(ttl: 60) { orders { id } }") == CacheHint(
        True, 60
    )
    assert cache_hint_for("graphql", "query { orders { id } }") == NO_CACHE_HINT
    assert cache_hint_for("cypher", "// @provisa cache=true\nMATCH (n) RETURN n") == CacheHint(
        True, None
    )
    assert cache_hint_for("cypher", "// @provisa cache_ttl=45\nMATCH (n) RETURN n") == CacheHint(
        True, 45
    )
    assert cache_hint_for("cypher", "MATCH (n) RETURN n") == NO_CACHE_HINT
    assert cache_hint_for("sql", "-- @provisa cache=true\nSELECT 1") == CacheHint(True, None)
    with pytest.raises(ValueError):
        cache_hint_for("cypher", "// @provisa cache_ttl=soon\nMATCH (n) RETURN n")
    with pytest.raises(ValueError):
        cache_hint_for("rest", "anything")


def test_grpc_metadata_carries_the_hint():
    from provisa.compiler.directives import NO_CACHE_HINT, CacheHint, cache_hint_from_grpc_metadata

    assert cache_hint_from_grpc_metadata((("x-provisa-role", "analyst"),)) == NO_CACHE_HINT
    assert cache_hint_from_grpc_metadata((("x-provisa-cache", "true"),)) == CacheHint(True, None)
    assert cache_hint_from_grpc_metadata((("x-provisa-cache-ttl", "20"),)) == CacheHint(True, 20)
    with pytest.raises(ValueError):
        cache_hint_from_grpc_metadata((("x-provisa-cache-ttl", "soon"),))


@pytest.mark.asyncio
async def test_the_compiled_pipeline_carries_the_hint_onto_the_plan(monkeypatch):
    """GraphQL/Cypher/gRPC reach the plan through _govern_and_route_compiled: the hint each
    transport parsed is on the plan it mints — no per-surface cache logic downstream."""
    from unittest.mock import patch

    from provisa.compiler.directives import NO_CACHE_HINT, CacheHint
    from provisa.pgwire import _pipeline
    from tests.unit.test_governed_sql_engine_internals import _state as gov_state

    import provisa.api.app as app_mod

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        from provisa.transpiler.router import Route, RouteDecision

        return (
            exec_sql,
            RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="t"),
            "pg",
            False,
            {"pg"},
            (),
        )

    monkeypatch.setattr(app_mod, "state", gov_state(["*"]), raising=False)
    monkeypatch.setattr(app_mod.state, "source_dialects", {"pg": "postgres"}, raising=False)
    sql = "SELECT o.id FROM sales.orders o"
    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        hinted = await _pipeline._govern_and_route_compiled(
            sql, "analyst", cache_hint=CacheHint(True, 30)
        )
        plain = await _pipeline._govern_and_route_compiled(sql, "analyst", cache_hint=NO_CACHE_HINT)
    assert (hinted.cache_opt_in, hinted.cache_ttl) == (True, 30)
    assert (plain.cache_opt_in, plain.cache_ttl) == (False, None)


@pytest.mark.asyncio
async def test_cypher_dict_rows_round_trip_through_the_cache():
    """The HTTP Cypher and Bolt DIRECT dispatchers return dict rows; an opted-in plan stores them
    and serves them back identically, an unhinted one does neither."""
    from provisa.api.rest.cypher_router import cached_cypher_rows, store_cypher_rows

    state = _state(FakeCacheStore())
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    await store_cypher_rows(_plan(), state, rows)
    assert await cached_cypher_rows(_plan(), state) is None
    assert await cached_cypher_rows(_plan(cache_opt_in=True), state) is None
    await store_cypher_rows(_plan(cache_opt_in=True), state, rows)
    assert await cached_cypher_rows(_plan(cache_opt_in=True), state) == rows
