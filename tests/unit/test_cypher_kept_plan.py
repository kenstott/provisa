# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e1c93-6a2d-4f58-9e03-d1c5a8f27b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher request does not rebuild what the registry already determines (REQ-1877).

The Cypher label map is a pure function of the registry, and the translation of a Cypher statement
a pure function of its text and that map. Both are kept through the one plan store every surface
uses (``provisa.pgwire.governed_plan``): built once per schema generation, rebuilt once when the
registry changes, never shared across roles or orgs. A DIRECT-routed Cypher statement is served
over Arrow Flight through the pipeline terminal, as on every other transport."""

# Requirements: REQ-1877, REQ-345

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.cypher.label_map import CypherLabelMap
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline, governed_plan
from provisa.transpiler.router import Route

_CYPHER = "MATCH (o:Orders) WHERE o.amount > $min RETURN o.amount AS amount"


@pytest.fixture(autouse=True)
def _no_rebuild(monkeypatch):
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)


def _ctx(table_id: int = 1) -> SimpleNamespace:
    table = SimpleNamespace(
        type_name="Sales_Orders",
        table_id=table_id,
        source_id="pg",
        catalog_name="postgresql",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )
    return SimpleNamespace(
        tables={"sales__orders": table},
        joins={},
        aggregate_columns={table_id: [("id", "integer"), ("amount", "float")]},
        pk_columns={},
        native_filter_columns={},
        physical_to_sql={},
        gql_governed_object_cols=set(),
    )


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        contexts={"analyst": _ctx(), "admin": _ctx()},
        rls_contexts={},
        roles={
            "analyst": {"id": "analyst", "domain_access": ["*"]},
            "admin": {"id": "admin", "domain_access": ["*"]},
        },
        masking_rules={},
        tables=[],
        relationships=[],
        schema_build_cache={"tables": [], "relationships": [], "column_types": {}},
        source_catalogs={},
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        cypher_label_maps={},
        model_db=None,
        tenant_db=None,
    )


def _rebuild(state: SimpleNamespace) -> None:
    """What a schema rebuild does: swap the governance objects, then bump the generation."""
    state.contexts = {"analyst": _ctx(), "admin": _ctx()}
    state.schema_build_cache = {"tables": [], "relationships": [], "column_types": {}}
    state.schema_version += 1


class _Builds:
    """Counts label-map builds while still running the real one."""

    def __init__(self) -> None:
        self.count = 0
        self._real = CypherLabelMap.from_schema

    def __call__(self, *args, **kwargs):
        self.count += 1
        return self._real(*args, **kwargs)


@pytest.fixture
def builds(monkeypatch) -> _Builds:
    counter = _Builds()
    monkeypatch.setattr(CypherLabelMap, "from_schema", counter)
    return counter


# -- the label map --------------------------------------------------------------------------------


def _bolt_map(state, role="analyst", include_ops=True):
    from provisa.bolt.session import _bolt_label_map

    return _bolt_label_map(state.contexts[role], role, include_ops, state)


def _http_map(state, role="analyst"):
    from provisa.api.rest.cypher_exec import _build_label_map

    return _build_label_map(state.contexts[role], role, state)


@pytest.mark.parametrize("build", [_bolt_map, _http_map])
def test_n_requests_build_the_label_map_once(builds, build):
    state = _state()
    maps = [build(state) for _ in range(5)]
    assert builds.count == 1
    assert all(m is maps[0] for m in maps)


@pytest.mark.parametrize("build", [_bolt_map, _http_map])
def test_a_registry_change_rebuilds_the_label_map_exactly_once(builds, build):
    state = _state()
    before = [build(state) for _ in range(3)][0]
    _rebuild(state)
    after = [build(state) for _ in range(3)]
    assert builds.count == 2
    assert after[0] is not before and all(m is after[0] for m in after)


def test_bolt_and_http_share_the_map_their_inputs_agree_on(builds):
    state = _state()
    assert _bolt_map(state) is _http_map(state)
    assert builds.count == 1


def test_roles_never_share_a_label_map(builds):
    state = _state()
    assert _bolt_map(state, "analyst") is not _bolt_map(state, "admin")
    assert builds.count == 2


def test_every_person_acting_as_a_role_shares_its_label_map(builds):
    """The map depends on the registry and the role's scope, not on who is asking."""
    from provisa.api.rest.cypher_plan import label_map_key
    from provisa.audit.context import audit_identity_scope

    state = _state()
    with audit_identity_scope("alice", "bolt"):
        alices = _bolt_map(state)
    with audit_identity_scope("bob", "bolt"):
        bobs = _bolt_map(state)
    assert bobs is alices
    assert builds.count == 1
    assert list(state.cypher_label_maps) == [label_map_key("analyst", ["*"], True, False)]
    assert label_map_key("analyst", ["*"], True, False) == ("analyst", ("*",), True, False)


def test_the_label_map_is_not_an_entry_the_plan_store_can_evict(builds):
    """The plan store is bounded and evicts; a label map lasts the schema generation."""
    state = _state()
    first = _bolt_map(state)
    state.compiled_query_cache.clear()  # every plan evicted
    assert _bolt_map(state) is first
    assert builds.count == 1


def test_a_new_generation_drops_the_previous_generations_label_maps(builds):
    state = _state()
    _bolt_map(state, "analyst")
    _bolt_map(state, "admin")
    _bolt_map(state, "analyst", include_ops=False)
    assert len(state.cypher_label_maps) == 3
    _rebuild(state)
    _bolt_map(state, "analyst")
    assert len(state.cypher_label_maps) == 1


def test_no_label_map_is_kept_while_the_schema_is_rebuilding(builds, monkeypatch):
    state = _state()
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: True)
    _bolt_map(state)
    _bolt_map(state)
    assert builds.count == 2 and state.cypher_label_maps == {}


def test_orgs_never_share_a_label_map(builds):
    org_a, org_b = _state(), _state()
    assert _bolt_map(org_a) is not _bolt_map(org_b)
    assert builds.count == 2


def test_the_business_view_is_not_the_ops_view(builds):
    state = _state()
    assert _bolt_map(state, include_ops=True) is not _bolt_map(state, include_ops=False)


def test_a_wider_acting_role_set_is_a_different_label_map(builds):
    """REQ-1620: the HTTP surface widens domain_access to every role the caller is acting as."""
    from provisa.core.request_context import current_role_claims

    state = _state()
    state.roles["analyst"]["domain_access"] = ["sales"]
    state.roles["admin"]["domain_access"] = ["sales", "hr"]
    alone = _http_map(state)
    token = current_role_claims.set(("analyst", "admin"))
    try:
        widened = _http_map(state)
    finally:
        current_role_claims.reset(token)
    assert widened is not alone
    assert builds.count == 2


# -- the translation ------------------------------------------------------------------------------


class _Pipeline:
    """Stands in for governance, routing and the terminal: records what each request governed."""

    def __init__(self, monkeypatch, state) -> None:
        import provisa.api.app as app_mod
        from provisa.cypher import parser, translator

        monkeypatch.setattr(app_mod, "state", state, raising=False)
        monkeypatch.setattr("provisa.compiler.sql_rewrite.make_semantic_sql", lambda sql, _c: sql)
        self.translate = MagicMock(wraps=translator.cypher_to_sql)
        monkeypatch.setattr(translator, "cypher_to_sql", self.translate)
        self.parse = MagicMock(wraps=parser.parse_cypher)
        monkeypatch.setattr(parser, "parse_cypher", self.parse)
        self.governed: list[tuple[str, list | None]] = []

        async def _govern(sql, _role_id, *, exec_params=None, **_kw):
            self.governed.append((sql, exec_params))
            # One reachable source: the DIRECT route every surface serves from the source's driver.
            return SimpleNamespace(
                route=Route.DIRECT,
                source_id="pg",
                sql=sql,
                exec_sql=sql,
                physical_sql=None,
                row_count=None,
                live_caps=(),  # REQ-1909: as _Plan — no capped live source bound at mint
                live_caps_org=None,
            )

        monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
        monkeypatch.setattr(_pipeline, "require_governed_plan", lambda _p: None)
        monkeypatch.setattr(_pipeline, "finalize_audit", AsyncMock(return_value=None))
        monkeypatch.setattr(_pipeline, "check_response_cache", AsyncMock(return_value=None))
        monkeypatch.setattr(_pipeline, "store_executed_result", AsyncMock(return_value=None))
        monkeypatch.setattr(
            "provisa.api.rest.cypher_router._dispatch_execution_direct",
            AsyncMock(return_value=[{"amount": 10.5}]),
        )


async def _bolt(role="analyst", minimum=1, cypher=_CYPHER):
    from provisa.bolt.session import _execute_cypher

    return await _execute_cypher(cypher, {"min": minimum}, role)


async def test_a_repeated_bolt_statement_is_translated_once(monkeypatch, builds):
    pipe = _Pipeline(monkeypatch, _state())
    for minimum in (1, 2, 3):
        columns, rows, _ = await _bolt(minimum=minimum)
        assert (columns, rows) == (["amount"], [[10.5]])
    assert pipe.translate.call_count == 1
    assert pipe.parse.call_count == 1
    assert builds.count == 1
    # Each request governs the same kept SQL with its OWN bound values.
    assert len({sql for sql, _ in pipe.governed}) == 1
    assert [params for _, params in pipe.governed] == [[1], [2], [3]]


async def test_another_statement_is_its_own_translation(monkeypatch):
    pipe = _Pipeline(monkeypatch, _state())
    await _bolt()
    await _bolt(cypher=_CYPHER.replace(">", "<"))
    assert pipe.translate.call_count == 2
    assert pipe.governed[0][0] != pipe.governed[1][0]


async def test_a_registry_change_retranslates_exactly_once(monkeypatch, builds):
    state = _state()
    pipe = _Pipeline(monkeypatch, state)
    await _bolt()
    await _bolt()
    _rebuild(state)
    await _bolt()
    await _bolt()
    assert pipe.translate.call_count == 2
    assert builds.count == 2


async def test_roles_never_share_a_translation(monkeypatch):
    pipe = _Pipeline(monkeypatch, _state())
    await _bolt("analyst")
    await _bolt("admin")
    assert pipe.translate.call_count == 2


async def test_orgs_never_share_a_translation(monkeypatch):
    org_a, org_b = _state(), _state()
    pipe = _Pipeline(monkeypatch, org_a)
    await _bolt()
    import provisa.api.app as app_mod

    monkeypatch.setattr(app_mod, "state", org_b, raising=False)
    await _bolt()
    assert pipe.translate.call_count == 2


async def test_a_kept_translation_still_refuses_an_unbound_parameter(monkeypatch):
    from provisa.bolt.session import _execute_cypher

    _Pipeline(monkeypatch, _state())
    await _bolt()
    with pytest.raises(ValueError, match="Unbound Cypher parameters"):
        await _execute_cypher(_CYPHER, {}, "analyst")


async def test_a_repeated_http_statement_is_translated_once(monkeypatch, builds):
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query

    state = _state()
    pipe = _Pipeline(monkeypatch, state)
    monkeypatch.setattr("provisa.compiler.stage2.build_governance_context", MagicMock())
    validate = MagicMock(return_value=[])
    monkeypatch.setattr("provisa.compiler.sql_validator.validate_sql", validate)
    request = MagicMock()
    request.state.role = "analyst"
    for minimum in (1, 2, 3):
        body = CypherRequest(query=_CYPHER, params={"min": minimum})
        response = await cypher_query(body, request, query_id=None, x_provisa_stats=None)
        assert response.status_code == 200, response.body
    assert pipe.translate.call_count == 1
    assert pipe.parse.call_count == 1
    assert validate.call_count == 1
    assert builds.count == 1
    assert [params for _, params in pipe.governed] == [[1], [2], [3]]


# -- Cypher over Arrow Flight, DIRECT route ---------------------------------------------------------


def test_flight_serves_a_direct_routed_cypher_statement(monkeypatch, builds):
    """A single-source statement routes DIRECT (no engine-physical SQL): Flight runs it through the
    pipeline terminal like every other transport, instead of refusing the route."""
    from provisa.api.flight.server import ProvisaFlightServer

    state = _state()
    state.federation_engine = SimpleNamespace()
    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    srv._state = state  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setattr(srv, "_run_on_loop", lambda coro, *, timeout=None: asyncio.run(coro))
    monkeypatch.setattr("provisa.compiler.sql_rewrite.make_semantic_sql", lambda sql, _ctx: sql)

    plan = SimpleNamespace(
        route=Route.DIRECT, source_id="pg", sql="SELECT 1", physical_sql=None, warnings=[]
    )
    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", AsyncMock(return_value=plan))
    monkeypatch.setattr(_pipeline, "require_governed_plan", lambda _p: None)
    terminal = AsyncMock(return_value=QueryResult(rows=[(10.5,)], column_names=["amount"]))
    monkeypatch.setattr(_pipeline, "_execute_plan", terminal)

    ticket = {"query": _CYPHER, "role": "analyst", "params": {"min": 1}}
    for _ in range(3):
        stream = srv._do_get_cypher(ticket)
        assert isinstance(stream, pa.flight.RecordBatchStream)
    assert terminal.await_count == 3
    assert all(call.args[0] is plan for call in terminal.await_args_list)
    assert builds.count == 1


# -- X-Provisa-Cache on the HTTP data endpoints (REQ-536) -------------------------------------------


def _stored(age_seconds: int = 7):
    import time

    from provisa.cache.store import CachedResult

    return CachedResult(data=b"", cached_at=time.time() - age_seconds, ttl=300)


async def test_a_pipeline_hit_names_the_entry_it_was_served_from(monkeypatch):
    """The one read every row terminal uses hands the surface the entry, so its age is reportable."""
    from provisa.cache.middleware import build_cache_headers
    from tests.unit.test_response_cache_opt_in import _plan, _state
    from tests.unit.test_response_cache_shared import FakeCacheStore

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    monkeypatch.setattr(_pipeline, "_response_cache_bound", lambda: 100)
    state = _state(FakeCacheStore())
    assert await _pipeline.check_response_cache(_plan(cache_opt_in=True), state) is None
    await _pipeline.store_executed_result(
        _plan(cache_opt_in=True), state, QueryResult(rows=[(1,)], column_names=["id"])
    )
    hit = await _pipeline.check_response_cache(_plan(cache_opt_in=True), state)
    assert hit is not None and hit.rows == [(1,)]
    headers = build_cache_headers(hit.cache_entry)
    assert headers["X-Provisa-Cache"] == "HIT" and "X-Provisa-Cache-Age" in headers
    assert build_cache_headers(QueryResult(rows=[], column_names=[]).cache_entry) == {
        "X-Provisa-Cache": "MISS"
    }


async def test_data_cypher_reports_hit_on_a_hit_and_miss_on_a_miss(monkeypatch):
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query

    _Pipeline(monkeypatch, _state())
    monkeypatch.setattr("provisa.compiler.stage2.build_governance_context", MagicMock())
    monkeypatch.setattr("provisa.compiler.sql_validator.validate_sql", MagicMock(return_value=[]))
    request = MagicMock()
    request.state.role = "analyst"
    body = CypherRequest(query=_CYPHER, params={"min": 1})

    miss = await cypher_query(body, request, query_id=None, x_provisa_stats=None)
    assert miss.headers["X-Provisa-Cache"] == "MISS"
    assert "X-Provisa-Cache-Age" not in miss.headers

    served = QueryResult(rows=[(10.5,)], column_names=["amount"], cache_entry=_stored(7))
    monkeypatch.setattr(_pipeline, "check_response_cache", AsyncMock(return_value=served))
    hit = await cypher_query(body, request, query_id=None, x_provisa_stats=None)
    assert hit.headers["X-Provisa-Cache"] == "HIT"
    assert hit.headers["X-Provisa-Cache-Age"] == "7"
    assert hit.body == miss.body


@pytest.mark.parametrize("accept", [None, "text/csv"])
async def test_data_sql_reports_hit_on_a_hit_and_miss_on_a_miss(monkeypatch, accept):
    import provisa.api.app as app_mod
    from provisa.api.data.endpoint_dev import SQLRequest, sql_endpoint

    state = SimpleNamespace(schemas={"analyst": object()}, roles={})
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    raw = MagicMock()
    raw.state.role = "analyst"

    async def _call(result):
        monkeypatch.setattr(_pipeline, "execute_sql_batch", AsyncMock(return_value=result))
        return await sql_endpoint(
            raw,
            SQLRequest(sql="SELECT 1 AS id", role="analyst"),
            x_provisa_role=None,
            accept=accept,
            x_provisa_stats=None,
            x_provisa_as_of=None,
        )

    miss = await _call(QueryResult(rows=[(1,)], column_names=["id"]))
    assert miss.headers["X-Provisa-Cache"] == "MISS"
    assert "X-Provisa-Cache-Age" not in miss.headers
    hit = await _call(QueryResult(rows=[(1,)], column_names=["id"], cache_entry=_stored(7)))
    assert hit.headers["X-Provisa-Cache"] == "HIT"
    assert hit.headers["X-Provisa-Cache-Age"] == "7"
    assert hit.body == miss.body
