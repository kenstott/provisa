# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c6e-52d9-4b07-9e4a-d1c7b2f60a85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Encoded responses over the Arrow result cache (REQ-1867, amended 2026-11-23, ENCODED RESPONSES).

A response-cache hit answers the exact bytes its surface sent for the same shape, kept beside the
Arrow entry; a shape not yet kept is encoded from the Arrow entry once and kept. A hit's bytes are
the miss's bytes. Encoded responses expire and are invalidated with their entry, and are never kept
beside an entry dropped or written again since it was read."""

# Requirements: REQ-1867

from __future__ import annotations

import json
import statistics
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import pytest

from provisa.cache import encoded
from provisa.cache.middleware import check_cache
from provisa.cache.store import CachedResult, RedisCacheStore
from provisa.core.redis_factory import SharedRedis
from provisa.compiler.directives import CacheHint
from provisa.compiler.sql_types import ColumnRef, CompiledQuery
from provisa.core.statement_warnings import ServerWarning, collecting, raised
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline, governed_plan
from provisa.transpiler.router import Route
from tests.role_state import RoleState
from tests.unit.test_response_cache_shared import FakeCacheStore

# -- the store: kept beside the entry, gone with it -------------------------------------------------


def _redis_store() -> RedisCacheStore:
    store = RedisCacheStore(redis_url="redis://unused")
    # As make_redis builds it: the shared sync client behind the awaitable call surface.
    store._redis = SharedRedis(fakeredis.FakeRedis())
    return store


async def _hit(store, key: str = "k", tenant: str | None = "org-1:m1") -> CachedResult:
    cached = await check_cache(store, key, tenant)
    assert cached is not None
    return cached


async def test_a_hit_names_the_key_and_tenant_it_was_read_under():
    store = _redis_store()
    await store.set("k", b"arrow", 60, tenant_id="org-1:m1")
    hit = await _hit(store)
    assert (hit.key, hit.tenant_id, hit.data) == ("k", "org-1:m1", b"arrow")


async def test_an_encoded_response_is_kept_beside_its_entry_and_read_back():
    store = _redis_store()
    await store.set("k", b"arrow", 60, tenant_id="org-1:m1", table_ids={7})
    hit = await _hit(store)
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-1:m1") is None
    assert await store.set_encoded(hit, "graphql:x", b"[1]") is True
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-1:m1") == b"[1]"
    # Another tenant's same key holds nothing.
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-2:m1") is None


async def test_invalidating_a_table_drops_the_entry_and_every_encoded_response():
    store = _redis_store()
    await store.set("k", b"arrow", 60, tenant_id="org-1:m1", table_ids={7})
    hit = await _hit(store)
    await store.set_encoded(hit, "graphql:x", b"[1]")
    await store.set_encoded(hit, "cypher_http:y", b"[2]")
    assert await store.invalidate_by_table(7, tenant_id="org-1:m1") == 1
    assert await store.get("k", tenant_id="org-1:m1") is None
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-1:m1") is None
    assert await store.get_encoded("k", "cypher_http:y", tenant_id="org-1:m1") is None


async def test_purging_a_place_drops_encoded_responses_and_counts_only_entries():
    store = _redis_store()
    await store.set("k", b"arrow", 60, tenant_id="org-1:m1")
    await store.set_encoded(await _hit(store), "graphql:x", b"[1]")
    assert await store.purge_place("org-1") == 1
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-1:m1") is None


async def test_writing_the_entry_again_drops_the_encoded_responses_of_the_old_one():
    store = _redis_store()
    await store.set("k", b"arrow", 60, tenant_id="org-1:m1")
    await store.set_encoded(await _hit(store), "graphql:x", b"[1]")
    await store.set("k", b"arrow2", 60, tenant_id="org-1:m1")
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-1:m1") is None


async def test_nothing_is_kept_beside_an_entry_dropped_or_replaced_since_it_was_read():
    store = _redis_store()
    await store.set("k", b"arrow", 60, tenant_id="org-1:m1", table_ids={7})
    stale = await _hit(store)
    await store.invalidate_by_table(7, tenant_id="org-1:m1")
    assert await store.set_encoded(stale, "graphql:x", b"[1]") is False
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-1:m1") is None

    await store.set("k", b"arrow2", 60, tenant_id="org-1:m1")
    newer = await _hit(store)
    replaced = CachedResult(
        data=b"arrow", cached_at=newer.cached_at - 5, ttl=60, key="k", tenant_id="org-1:m1"
    )
    assert await store.set_encoded(replaced, "graphql:x", b"[1]") is False
    assert await store.get_encoded("k", "graphql:x", tenant_id="org-1:m1") is None


async def test_an_encoded_response_expires_when_its_entry_does():
    store = _redis_store()
    await store.set("k", b"arrow", 60, tenant_id="org-1:m1")
    await store.set_encoded(await _hit(store), "graphql:x", b"[1]")
    redis = store._redis
    assert redis is not None
    entry_ttl = redis.client().pttl("provisa:cache:org-1:m1:k")
    encoded_ttl = redis.client().pttl("provisa:cache:org-1:m1:k:enc")
    assert 0 < encoded_ttl <= 60_000
    assert abs(entry_ttl - encoded_ttl) < 2_000


async def test_an_entry_that_never_expires_keeps_encoded_responses_that_never_do():
    from provisa.core.cache_ttl import NEVER_EXPIRES

    store = _redis_store()
    await store.set("k", b"arrow", NEVER_EXPIRES, tenant_id="org-1:m1")
    await store.set_encoded(await _hit(store), "graphql:x", b"[1]")
    assert store._redis is not None
    assert store._redis.client().pttl("provisa:cache:org-1:m1:k:enc") == -1


# -- the kept form -----------------------------------------------------------------------------------


def test_what_is_kept_reads_back_as_it_was_written():
    warning = ServerWarning(code="statement.x", message="m", params={"limit": 3})
    kept = encoded.Encoded((b'{"a":1}', b"", b"[2]"), {"rows": 1}, (warning,))
    assert encoded.unpack(encoded.pack(kept)) == kept


async def test_a_response_past_the_size_cap_is_not_kept(monkeypatch):
    store = FakeCacheStore()
    await store.set("k", b"arrow", 60)
    hit = await _hit(store, tenant=None)
    monkeypatch.setattr(encoded, "MAX_ENCODED_BYTES", 64)
    assert (
        await encoded.keep(
            SimpleNamespace(response_cache_store=store), hit, "f", encoded.Encoded((b"x" * 100,))
        )
        is False
    )
    assert (
        await encoded.keep(
            SimpleNamespace(response_cache_store=store), hit, "f", encoded.Encoded((b"x" * 10,))
        )
        is True
    )


async def test_a_hit_raises_the_warnings_encoding_raised():
    store = FakeCacheStore()
    await store.set("k", b"arrow", 60)
    hit = await _hit(store, tenant=None)
    warning = ServerWarning(code="statement.x", message="m")
    await encoded.keep(
        SimpleNamespace(response_cache_store=store),
        hit,
        "f",
        encoded.Encoded((b"[]",), {}, (warning,)),
    )
    with collecting():
        assert (
            await encoded.served(SimpleNamespace(response_cache_store=store), hit, "f") is not None
        )
        assert raised() == [warning]


def test_shape_fields_differ_by_surface_and_by_shape():
    a = encoded.shape_field("graphql", "json", [("id",)])
    assert a == encoded.shape_field("graphql", "json", [("id",)])
    assert a != encoded.shape_field("graphql", "json", [("orderId",)])
    assert a != encoded.shape_field("sql_http", "json", [("id",)])


# -- surfaces: a hit is the miss's bytes, encoded once ----------------------------------------------


class _Counted:
    """Wraps a surface's encode step and counts its calls."""

    def __init__(self, monkeypatch, owner, name: str) -> None:
        self.calls = 0
        real = getattr(owner, name)
        if hasattr(real, "__wrapped__") or not callable(real):
            raise AssertionError(f"{name} is not a plain function")

        import inspect

        async def counted_async(*a, **k):
            self.calls += 1
            return await real(*a, **k)

        def counted_sync(*a, **k):
            self.calls += 1
            return real(*a, **k)

        counted = counted_async if inspect.iscoroutinefunction(real) else counted_sync
        monkeypatch.setattr(owner, name, counted)


async def _held_entry(store) -> CachedResult:
    """The Arrow entry a miss wrote, read back as the pipeline reads a hit."""
    await store.set("k", b"arrow", 300, tenant_id=None)
    return await _hit(store, tenant=None)


# GraphQL --------------------------------------------------------------------------------------------


def _compiled() -> CompiledQuery:
    return CompiledQuery(
        sql='SELECT "id", "name" FROM "sales"."orders"',
        params=[5],
        root_field="orders",
        columns=[ColumnRef(None, "id", "id", None), ColumnRef(None, "name", "label", None)],
        sources={"sales-pg"},
        canonical_field="orders",
    )


@pytest.fixture
def graphql(monkeypatch):
    from provisa.api.data import endpoint

    results: list[QueryResult] = []

    async def _govern(sql, role_id, **kwargs):
        return SimpleNamespace(
            route=Route.DIRECT,
            cache_hit=None,
            materialize=None,
            sources=frozenset({"sales-pg"}),
            source_id="sales-pg",
            dialect="postgres",
            route_reason="t",
            sql=sql,
        )

    async def _execute(plan, state):
        return results.pop(0)

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    monkeypatch.setattr(endpoint, "note_request_route", lambda route: None)
    monkeypatch.setattr(endpoint, "_record_per_source_stats", lambda *a, **k: None)
    store = FakeCacheStore()
    encode = _Counted(monkeypatch, endpoint, "_format_response")
    return SimpleNamespace(
        results=results,
        store=store,
        state=SimpleNamespace(response_cache_store=store),
        encode=encode,
    )


async def _graphql_body(h, entry) -> bytes:
    from provisa.api.data import endpoint
    from provisa.observability.request_facts import DocumentResponse

    h.results.append(
        QueryResult(rows=[(1, "Ana"), (2, "Bo")], column_names=["id", "name"], cache_entry=entry)
    )
    root, answer, _redirect, _hit_record = await endpoint._execute_one_field(
        _compiled(),
        SimpleNamespace(tables={}),
        h.state,
        "analyst",
        "json",
        delivery=None,
        steward_hint=None,
        query_session_props=None,
        cache_hint=CacheHint(opt_in=True, ttl=30),
    )
    return bytes(DocumentResponse({"data": {root: endpoint._field_value(answer)}}).body)


async def test_graphql_a_hit_answers_the_miss_bytes_encoding_once(graphql):
    miss = await _graphql_body(graphql, None)
    assert json.loads(miss) == {
        "data": {"orders": [{"id": 1, "label": "Ana"}, {"id": 2, "label": "Bo"}]}
    }
    entry = await _held_entry(graphql.store)
    first = await _graphql_body(graphql, entry)
    assert graphql.encode.calls == 2  # the miss, and the first hit of this shape
    second = await _graphql_body(graphql, entry)
    assert graphql.encode.calls == 2  # the kept bytes: nothing encoded
    assert miss == first == second


# SQL over HTTP --------------------------------------------------------------------------------------


@pytest.fixture
def sql_http(monkeypatch):
    import provisa.api.app as app_mod
    from provisa.api.data import endpoint_dev

    store = FakeCacheStore()
    state = RoleState(schemas={"analyst": object()}, roles={}, response_cache_store=store)
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    encode = _Counted(monkeypatch, endpoint_dev, "_answer_values")
    raw = MagicMock()
    raw.state.role = "analyst"
    raw.headers = {}
    return SimpleNamespace(
        state=state, store=store, raw=raw, encode=encode, monkeypatch=monkeypatch
    )


async def _sql_body(h, entry, *, stats: bool = False):
    from provisa.api.data.endpoint_dev import SQLRequest, sql_endpoint

    result = QueryResult(
        rows=[(1, "Ana"), (2, "Bo")],
        column_names=["id", "name"],
        column_types=["integer", "varchar"],
        cache_entry=entry,
    )
    h.monkeypatch.setattr(_pipeline, "execute_sql_batch", AsyncMock(return_value=result))
    h.monkeypatch.setattr("provisa.api.app.state", h.state, raising=False)
    return await sql_endpoint(
        h.raw,
        SQLRequest(sql="SELECT id, name FROM orders", role="analyst"),
        x_provisa_role=None,
        accept=None,
        x_provisa_stats="true" if stats else None,
    )


async def test_sql_over_http_a_hit_answers_the_miss_bytes_encoding_once(sql_http):
    miss = await _sql_body(sql_http, None)
    entry = await _held_entry(sql_http.store)
    first = await _sql_body(sql_http, entry)
    assert sql_http.encode.calls == 2
    second = await _sql_body(sql_http, entry)
    assert sql_http.encode.calls == 2
    assert miss.body == first.body == second.body
    assert json.loads(bytes(miss.body))["data"] == {
        "sql": [{"id": 1, "name": "Ana"}, {"id": 2, "name": "Bo"}]
    }
    assert (miss.headers["X-Provisa-Cache"], second.headers["X-Provisa-Cache"]) == ("MISS", "HIT")


# Cypher over HTTP -----------------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _graph_sessions(monkeypatch):
    """REQ-2176: every Cypher statement reads its principal's graph session; the stand-in state
    has no control plane to hold one, so each is given an empty one."""
    from tests.unit.graph_session_double import empty_graph_sessions

    empty_graph_sessions(monkeypatch)


@pytest.fixture
def cypher(monkeypatch):
    from provisa.api.rest import cypher_router
    from tests.unit.test_cypher_kept_plan import _Pipeline, _state

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    state = _state()
    store = FakeCacheStore()
    state.response_cache_store = store
    _Pipeline(monkeypatch, state)
    monkeypatch.setattr("provisa.pgwire.governed_plan.role_governance", MagicMock())
    monkeypatch.setattr("provisa.compiler.sql_validator.validate_sql", MagicMock(return_value=[]))
    encode = _Counted(monkeypatch, cypher_router, "_answered_rows")
    request = MagicMock()
    request.state.role = "analyst"
    request.headers = {}
    return SimpleNamespace(
        state=state, store=store, request=request, encode=encode, monkeypatch=monkeypatch
    )


async def _cypher_body(h, entry, *, query=None):
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query
    from tests.unit.test_cypher_kept_plan import _CYPHER

    served = QueryResult(rows=[(10.5,), (20.0,)], column_names=["amount"], cache_entry=entry)
    h.monkeypatch.setattr(_pipeline, "_execute_plan", AsyncMock(return_value=served))
    h.monkeypatch.setattr("provisa.api.app.state", h.state, raising=False)
    body = CypherRequest(query=query or _CYPHER, params={"min": 1})
    return await cypher_query(body, h.request, query_id=None, x_provisa_stats=None)


async def test_cypher_over_http_a_hit_answers_the_miss_bytes_encoding_once(cypher):
    miss = await _cypher_body(cypher, None)
    assert miss.status_code == 200, miss.body
    entry = await _held_entry(cypher.store)
    first = await _cypher_body(cypher, entry)
    assert cypher.encode.calls == 2
    second = await _cypher_body(cypher, entry)
    assert cypher.encode.calls == 2
    assert miss.body == first.body == second.body
    assert json.loads(bytes(miss.body)) == {
        "columns": ["amount"],
        "rows": [{"amount": 10.5}, {"amount": 20.0}],
        "type": "cypher",
        "graph_columns": json.loads(bytes(miss.body))["graph_columns"],
    }


async def test_another_surface_on_the_same_entry_encodes_once_then_is_kept(cypher, sql_http):
    """One Arrow entry, two surfaces: each encodes its own answer once from the entry and keeps
    it; neither is answered with the other's bytes."""
    sql_http.state.response_cache_store = sql_http.store = cypher.store  # one store, both read it
    entry = await _held_entry(cypher.store)
    cypher_bytes = (await _cypher_body(cypher, entry)).body
    sql_bytes = (await _sql_body(sql_http, entry)).body
    assert (cypher.encode.calls, sql_http.encode.calls) == (1, 1)
    assert len(cypher.store._encoded["k"]) == 2
    assert (await _cypher_body(cypher, entry)).body == cypher_bytes
    assert (await _sql_body(sql_http, entry)).body == sql_bytes
    assert (cypher.encode.calls, sql_http.encode.calls) == (1, 1)


async def test_invalidation_drops_both_and_the_next_hit_encodes_again(cypher):
    entry = await _held_entry(cypher.store)
    await _cypher_body(cypher, entry)
    assert cypher.encode.calls == 1
    # What invalidation does to a store: the entry and what is kept beside it are gone; the next
    # miss writes the entry again.
    cypher.store._data.clear()
    cypher.store._encoded.clear()
    fresh = await _held_entry(cypher.store)
    await _cypher_body(cypher, fresh)
    assert cypher.encode.calls == 2


# -- the micro benchmark: a one-row hit encodes nothing --------------------------------------------


async def test_a_one_row_hit_does_no_encoding_and_answers_within_the_latency_target(graphql):
    """REQ-1867 target: a cached one-row response p50 under 5 ms, p95 under 10 ms. In process,
    the kept bytes are answered with no encoder call."""
    from provisa.api.data import endpoint
    from provisa.observability.request_facts import DocumentResponse

    entry = await _held_entry(graphql.store)

    async def _one() -> bytes:
        graphql.results.append(
            QueryResult(rows=[(1, "Ana")], column_names=["id", "name"], cache_entry=entry)
        )
        root, answer, _r, _h = await endpoint._execute_one_field(
            _compiled(),
            SimpleNamespace(tables={}),
            graphql.state,
            "analyst",
            "json",
            delivery=None,
            steward_hint=None,
            query_session_props=None,
            cache_hint=CacheHint(opt_in=True, ttl=30),
        )
        return bytes(DocumentResponse({"data": {root: endpoint._field_value(answer)}}).body)

    first = await _one()
    calls = graphql.encode.calls
    timings: list[float] = []
    for _ in range(300):
        t0 = time.perf_counter()
        assert await _one() == first
        timings.append((time.perf_counter() - t0) * 1000)
    assert graphql.encode.calls == calls  # no hit after the first encoded anything
    timings.sort()
    p50 = statistics.median(timings)
    p95 = timings[int(len(timings) * 0.95)]
    print(f"one-row hit: p50={p50:.3f} ms p95={p95:.3f} ms")
    assert p50 < 5 and p95 < 10
