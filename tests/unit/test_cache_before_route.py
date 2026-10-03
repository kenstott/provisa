# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3f6a52-1c7e-4b08-a4d9-5e2b8c0f7a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The response cache is consulted before routing (REQ-1897, amended 2026-10-01).

A request that opted in to the response cache used to be governed, lowered, optimized, routed and
prepared for residency first; only then did its terminal look in the cache. On a hit all of that
was thrown away (measured: 1.6 ms of a 3.3 ms pgwire hit, 2.2 ms plus a 3.6 ms control-plane read
of a 9.5 ms /data/sql hit). The entry's key is the governed statement, its bound values and the
role — all known once the kept governed plan is found — so the cache is read there. A hit is
served with no route work, no control-plane read and no API-table lookup."""

# Requirements: REQ-1897, REQ-544, REQ-1877

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.audit.context import audit_identity_scope
from provisa.compiler.directives import NO_CACHE_HINT, CacheHint
from provisa.transpiler.router import Route
from tests.unit.test_buffered_auto_threshold import _ControlPlane, _decision, _Source
from tests.unit.test_response_cache_shared import FakeCacheStore

pytestmark = pytest.mark.asyncio

_CACHED = "-- @provisa cache=true\nSELECT o.id FROM sales.orders o WHERE o.id = $1"
_PLAIN = "SELECT o.id FROM sales.orders o WHERE o.id = $1"
_OPT_IN = CacheHint(opt_in=True, ttl=None)


class _CountingStore(FakeCacheStore):
    def __init__(self) -> None:
        super().__init__()
        self.gets = 0

    async def get(self, key, tenant_id=None):
        self.gets += 1
        return await super().get(key, tenant_id)


@pytest.fixture
def pipe(monkeypatch):
    """The one pipeline over a stand-in state: a counting cache store, a control plane that fails
    on any statement, and every route-stage function counted."""
    import provisa.api.app as app_mod
    import sqlglot.parser
    from provisa.api.data import materialization
    from provisa.pgwire import _pipeline, governed_plan
    from tests.unit.test_governed_sql_engine_internals import _state as gov_state

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    audits: list[dict] = []

    async def _write_audit(pending, status_code, state=None, **outcome):
        audits.append({"status": status_code, **outcome})

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _write_audit)
    state = gov_state(["*"])
    state.source_dialects = {"pg": "postgres"}
    state.source_catalogs = {"pg": "pg"}
    state.source_pools = SimpleNamespace(source_ids=["pg"], has=lambda sid: sid == "pg")
    state.response_cache_store = _CountingStore()
    state.response_cache_default_ttl = 300
    state.source_cache = {}
    state.table_cache = {}
    state.settings_overrides = {}
    state.org_id = "org-a"
    state.model_stamp = 1
    state.tenant_db = None
    state.federation_engine = _Source(lambda sql, params: [(params[0],)])
    monkeypatch.setattr(app_mod, "state", state, raising=False)

    counts = {"route": 0, "api_lookup": 0, "parses": 0}
    route = _decision("DIRECT")

    # The registered tables each statement reads, as the pipeline handed them to the API-table
    # lookup: the same ids it then routes on.
    looked_up: list[tuple[int, ...]] = []

    async def _counted_route(*args, **kwargs):
        counts["route"] += 1
        assert tuple(kwargs["table_ids"]) == looked_up[-1], "routed on other tables than looked up"
        return await route(*args, **kwargs)

    real_wmo = materialization.would_materialize_optimize

    def _counted_wmo(exec_sql, st, *, table_ids):
        counts["api_lookup"] += 1
        ids = tuple(table_ids)
        assert all(isinstance(i, int) for i in ids), f"table ids are registered ids, got {ids!r}"
        looked_up.append(ids)
        return real_wmo(exec_sql, st, table_ids=table_ids)

    monkeypatch.setattr(materialization, "would_materialize_optimize", _counted_wmo)
    real_parse = sqlglot.parser.Parser.parse

    def _counting_parse(self, *args, **kwargs):
        counts["parses"] += 1
        return real_parse(self, *args, **kwargs)

    monkeypatch.setattr(sqlglot.parser.Parser, "parse", _counting_parse)
    with (
        patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_counted_route)),
        audit_identity_scope("u-1", "analyst"),
    ):
        yield SimpleNamespace(state=state, mod=_pipeline, counts=counts, audits=audits)


async def _raw(p, sql, params, **kw):
    plan = await p.mod._govern_and_route(sql, "analyst", params=params, serve_cached=True, **kw)
    return plan, await p.mod._execute_plan(plan, p.state)


async def test_a_raw_sql_hit_does_no_route_work(pipe):
    plan, first = await _raw(pipe, _CACHED, [7])
    assert plan.route == Route.DIRECT and first.rows == [(7,)]
    assert pipe.counts["route"] == 1 and len(pipe.state.federation_engine.statements) == 1
    before = dict(pipe.counts)
    pipe.state.tenant_db = _ControlPlane()  # any control-plane statement fails the request

    for _ in range(3):
        plan, again = await _raw(pipe, _CACHED, [7])
        assert plan.route == Route.CACHE
        assert again.rows == [(7,)] and again.column_names == first.column_names
        assert again.cache_entry is not None
    assert pipe.counts == before, "a cache hit routed, looked up API tables or parsed SQL"
    assert len(pipe.state.federation_engine.statements) == 1, "a cache hit reached the source"
    assert pipe.state.tenant_db.acquires == 0
    assert [a["route"] for a in pipe.audits] == ["direct", "cache", "cache", "cache"]
    assert [a["row_count"] for a in pipe.audits] == [1, 1, 1, 1]


async def test_a_hit_is_one_cache_read_and_a_miss_reads_once(pipe):
    store = pipe.state.response_cache_store
    await _raw(pipe, _CACHED, [7])
    assert store.gets == 1, "a miss read the cache again at the terminal"
    await _raw(pipe, _CACHED, [7])
    assert store.gets == 2, "a hit read the cache more than once"


async def test_other_bound_values_and_roles_miss(pipe):
    await _raw(pipe, _CACHED, [7])
    plan, result = await _raw(pipe, _CACHED, [8])
    assert plan.route == Route.DIRECT and result.rows == [(8,)]
    key = pipe.mod._raw_cache_key
    assert key(plan.cache_sql, [7], "analyst", None) != key(plan.cache_sql, [7], "auditor", None)


async def test_a_request_that_did_not_opt_in_reads_no_cache(pipe):
    for _ in range(2):
        plan, _result = await _raw(pipe, _PLAIN, [7])
        assert plan.route == Route.DIRECT
    assert pipe.state.response_cache_store.gets == 0
    assert len(pipe.state.federation_engine.statements) == 2


async def test_a_caller_that_does_not_serve_cached_plans_gets_a_routed_plan(pipe):
    """The planner answers Route.CACHE only to a caller that said it serves one."""
    await _raw(pipe, _CACHED, [7])
    plan = await pipe.mod._govern_and_route(_CACHED, "analyst", params=[7])
    assert plan.route == Route.DIRECT and plan.sql and plan.source_id == "pg"


async def test_explain_delivery_and_writes_are_never_answered_from_the_cache(pipe):
    from provisa.executor.redirect import Delivery, RedirectConfig

    await _raw(pipe, _CACHED, [7])
    gets = pipe.state.response_cache_store.gets
    plan = await pipe.mod._govern_and_route(
        _CACHED, "analyst", params=[7], serve_cached=True, explain=False
    )
    assert plan.route != Route.CACHE
    config = RedirectConfig(
        enabled=True, threshold=5, bucket="b", endpoint_url="", access_key="", secret_key="", ttl=60
    )
    with patch.object(
        pipe.mod, "_optimize_and_route", new=AsyncMock(side_effect=_decision("ENGINE"))
    ):
        plan = await pipe.mod._govern_and_route(
            _CACHED,
            "analyst",
            params=[7],
            serve_cached=True,
            deliver=Delivery(output_format="parquet", config=config, role="analyst"),
        )
    assert plan.route == Route.ENGINE
    assert pipe.state.response_cache_store.gets == gets, "an excluded request read the cache"


async def test_a_compiled_hit_does_no_route_work(pipe):
    async def _compiled():
        plan = await pipe.mod._govern_and_route_compiled(
            _PLAIN,
            "analyst",
            exec_params=[7],
            state=pipe.state,
            cache_hint=_OPT_IN,
            serve_cached=True,
        )
        return plan, await pipe.mod._execute_plan(plan, pipe.state)

    plan, first = await _compiled()
    assert plan.route == Route.DIRECT and first.rows == [(7,)]
    before = dict(pipe.counts)
    pipe.state.tenant_db = _ControlPlane()
    plan, again = await _compiled()
    assert plan.route == Route.CACHE and again.rows == [(7,)]
    assert pipe.counts == before
    assert len(pipe.state.federation_engine.statements) == 1
    # no opt-in: the same statement is routed and read
    plan = await pipe.mod._govern_and_route_compiled(
        _PLAIN,
        "analyst",
        exec_params=[7],
        state=SimpleNamespace(**{**vars(pipe.state), "tenant_db": None}),
        cache_hint=NO_CACHE_HINT,
        serve_cached=True,
    )
    assert plan.route == Route.DIRECT


async def test_a_pgwire_passthrough_entry_is_replayed_for_its_format_codes(pipe):
    """A pg_datarows entry is keyed by the client's result format codes. pgwire states them when
    it plans, so a passthrough hit is found before routing too — and only for those codes."""
    from provisa.executor.result import StreamingQueryResult

    miss = await pipe.mod._govern_and_route(
        _CACHED, "analyst", params=[7], serve_cached=True, wire_formats=[1]
    )
    assert miss.route == Route.DIRECT
    tee = pipe.mod._cache_tee(miss, pipe.state, None, [1])
    assert tee is not None
    from buenavista.core import RawDataRowBytes

    stream = StreamingQueryResult(
        iter([[RawDataRowBytes(b"D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x017")]]),
        column_names=["id"],
        column_types=["int4"],
    )
    for _ in tee.datarows(stream, [1]).batches():
        pass
    await tee.commit()
    gets = pipe.state.response_cache_store.gets

    hit = await pipe.mod._govern_and_route(
        _CACHED, "analyst", params=[7], serve_cached=True, wire_formats=[1]
    )
    assert hit.route == Route.CACHE
    replay = await pipe.mod.cached_result(hit, pipe.state)
    assert replay is not None and pipe.state.response_cache_store.gets == gets + 1
    other = await pipe.mod._govern_and_route(
        _CACHED, "analyst", params=[7], serve_cached=True, wire_formats=[0]
    )
    assert other.route == Route.DIRECT, "text-format client was offered binary-format bytes"


async def test_reads_at_different_as_of_never_share_an_entry(pipe):
    """A request-level as-of changes what a statement over a bitemporal view reads (REQ-1163), so
    it is part of the entry's identity: the same statement at another as-of, or at none, is a
    miss — before routing and at the terminal alike."""
    jan, feb = "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"
    plan, _ = await _raw(pipe, _CACHED, [7], as_of=jan)
    assert plan.route == Route.DIRECT
    again, _ = await _raw(pipe, _CACHED, [7], as_of=jan)
    assert again.route == Route.CACHE
    other, _ = await _raw(pipe, _CACHED, [7], as_of=feb)
    assert other.route == Route.DIRECT, "a read as of February was served January's entry"
    current, _ = await _raw(pipe, _CACHED, [7])
    assert current.route == Route.DIRECT, "a current read was served an as-of entry"

    key = pipe.mod._response_cache_key
    routed = [
        await pipe.mod._govern_and_route(_CACHED, "analyst", params=[9], as_of=a)
        for a in (jan, feb, None)
    ]
    keys = {key(p, wire_formats=None) for p in routed}
    assert len(keys) == 3 and None not in keys
