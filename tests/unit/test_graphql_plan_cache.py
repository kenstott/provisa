# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1e8a52-7b3d-4f96-a0c5-2d9e6b1f8a34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A repeated GraphQL query reuses its governed plan (REQ-1877, amended 2026-09-30).

The cached-hit path is: role -> plan lookup -> response-cache key -> Redis GET -> respond. Parsing,
validation, compilation and governance run once per (schema generation, role, query,
variables, session variables, as-of) and are reused until the schema generation moves."""

# Requirements: REQ-1877

from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from provisa.api.data import graphql_plan
from provisa.api.data.graphql_plan import GraphQLPlan, PlanRequest
from provisa.pgwire import governed_plan
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.directives import QueryDirectives
from provisa.compiler.sql_types import CompiledQuery


def _cq(
    sql: str = 'SELECT "order_id" FROM "sales"."orders"', sources=("sales-pg",)
) -> CompiledQuery:
    return CompiledQuery(sql=sql, params=[1], root_field="orders", columns=[], sources=set(sources))


_ROLE = {"id": "analyst"}
_SCHEMA = object()
_CTX = object()
_RLS = object()
_DIRECTIVES = QueryDirectives()


def _state(**over) -> SimpleNamespace:
    base = dict(
        compiled_query_cache=CompiledQueryCache(),
        schema_boot_id="boot",
        schema_version=7,
        approval_hook=None,
        kafka_table_configs={},
        source_types={"sales-pg": "postgresql", "events-kafka": "kafka"},
        contexts={"analyst": _CTX},
        rls_contexts={"analyst": _RLS},
        roles={"analyst": _ROLE},
        masking_rules={},
        tables=[],
    )
    base.update(over)
    return SimpleNamespace(**base)


def _request(state, **over) -> PlanRequest:
    kwargs = dict(
        role_id="analyst",
        role=state.roles["analyst"],
        schema=_SCHEMA,
        query="{ orders { orderId } }",
        variables=None,
        as_of=None,
        fresh_mvs=[],
        eligible=True,
    )
    kwargs.update(over)
    return PlanRequest(state, **kwargs)


@pytest.fixture(autouse=True)
def _no_rebuild(monkeypatch):
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)


@pytest.mark.parametrize(
    "change",
    [
        {"query": "{ b }"},
        {"variables": {"x": 2}},
        {"as_of": "TIMESTAMP '2026-01-01'"},
    ],
)
def test_the_plan_is_partitioned_on_the_requests_own_inputs(change):
    state = _state()
    base = dict(query="{ a }", variables={"x": 1}, as_of=None)
    _request(state, **base).record(_DIRECTIVES, [_cq()])
    assert _request(state, **base).cached() is not None
    assert _request(state, **{**base, **change}).cached() is None, change


def test_variable_key_order_does_not_split_the_plan():
    state = _state()
    _request(state, variables={"a": 1, "b": 2}).record(_DIRECTIVES, [_cq()])
    assert _request(state, variables={"b": 2, "a": 1}).cached() is not None


def test_a_recorded_plan_is_served_to_the_next_identical_request():
    state = _state()
    first = _request(state)
    assert first.cached() is None
    first.record(_DIRECTIVES, [_cq()])

    plan = _request(state).cached()
    assert isinstance(plan, GraphQLPlan)
    assert plan.directives is _DIRECTIVES
    assert [c.sql for c in plan.prepared_copies()] == ['SELECT "order_id" FROM "sales"."orders"']


def test_each_hit_gets_its_own_copies():
    state = _state()
    req = _request(state)
    original = [_cq()]
    req.record(_DIRECTIVES, original)
    original[0].sql = "MUTATED AFTER RECORD"
    original[0].params.append(99)

    plan = _request(state).cached()
    assert plan is not None
    one = plan.prepared_copies()
    one[0].sql = "MUTATED BY A REQUEST"
    one[0].params.append(42)
    one[0].sources.add("other")
    two = plan.prepared_copies()
    assert two[0].sql == 'SELECT "order_id" FROM "sales"."orders"'
    assert two[0].params == [1]
    assert two[0].sources == {"sales-pg"}


def test_a_new_schema_generation_misses():
    state = _state()
    _request(state).record(_DIRECTIVES, [_cq()])
    state.schema_version += 1
    assert _request(state).cached() is None


@pytest.mark.parametrize("anchor", ["roles", "contexts", "rls_contexts", "schema"])
def test_a_replaced_governance_object_misses_even_under_the_same_generation(anchor):
    """The rebuild swaps the per-role objects before it bumps schema_version; a plan built from
    the old objects must not be served against the new ones."""
    state = _state()
    _request(state).record(_DIRECTIVES, [_cq()])
    if anchor == "schema":
        assert _request(state, schema=object()).cached() is None
        return
    setattr(state, anchor, {"analyst": {"id": "analyst"} if anchor == "roles" else object()})
    assert _request(state).cached() is None


def test_a_change_in_the_fresh_mv_set_misses():
    state = _state()
    mv = SimpleNamespace(id="mv1")
    _request(state, fresh_mvs=[mv]).record(_DIRECTIVES, [_cq()])
    assert _request(state, fresh_mvs=[mv]).cached() is not None
    assert _request(state, fresh_mvs=[]).cached() is None
    assert _request(state, fresh_mvs=[SimpleNamespace(id="mv1")]).cached() is None


def test_nothing_is_recorded_while_a_rebuild_is_in_progress(monkeypatch):
    state = _state()
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: True)
    _request(state).record(_DIRECTIVES, [_cq()])
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    assert _request(state).cached() is None


def test_nothing_is_recorded_when_the_generation_moved_during_the_compile():
    state = _state()
    req = _request(state)
    state.schema_version += 1  # a rebuild started and finished while this request compiled
    req.record(_DIRECTIVES, [_cq()])
    assert _request(state).cached() is None
    assert len(state.compiled_query_cache) == 0


def test_an_approval_hook_is_evaluated_per_request_so_no_plan_is_kept():
    state = _state(approval_hook=object())
    req = _request(state)
    req.record(_DIRECTIVES, [_cq()])
    assert _request(state).cached() is None
    assert len(state.compiled_query_cache) == 0


def test_a_kafka_windowed_query_is_not_kept():
    """inject_kafka_filters writes a CURRENT_TIMESTAMP-relative window chosen per table config."""
    state = _state(kafka_table_configs={"events": object()})
    _request(state).record(_DIRECTIVES, [_cq(sources=("events-kafka",))])
    assert _request(state).cached() is None
    _request(state).record(_DIRECTIVES, [_cq(sources=("sales-pg",))])
    assert _request(state).cached() is not None


def test_an_ineligible_request_neither_reads_nor_writes():
    state = _state()
    _request(state).record(_DIRECTIVES, [_cq()])
    off = _request(state, eligible=False)
    assert off.cached() is None
    off.record(_DIRECTIVES, [_cq("SELECT 2")])
    plan = _request(state).cached()
    assert plan is not None and plan.prepared_copies()[0].sql != "SELECT 2"


def test_session_variables_partition_the_plan(monkeypatch):
    state = _state()
    monkeypatch.setattr(graphql_plan, "session_vars_for", lambda role: {"tenant": "acme"})
    _request(state).record(_DIRECTIVES, [_cq()])
    assert _request(state).cached() is not None
    monkeypatch.setattr(graphql_plan, "session_vars_for", lambda role: {"tenant": "beta"})
    assert _request(state).cached() is None


def test_recorded_plan_is_detached_from_the_request_objects():
    state = _state()
    cq = _cq()
    snapshot = copy.deepcopy(cq)
    _request(state).record(_DIRECTIVES, [cq])
    plan = _request(state).cached()
    assert plan is not None
    assert plan.prepared_copies()[0] == snapshot
    assert plan.prepared_copies()[0] is not cq


# --------------------------------------------------------------------------------------------- #
# The endpoint: a repeated query skips parse / validate / compile / governance
# --------------------------------------------------------------------------------------------- #


def _endpoint_harness(monkeypatch, *, approval_hook=None):
    """graphql_endpoint over a stand-in state, with every compile stage counted."""
    import asyncio

    from fastapi.responses import JSONResponse

    import provisa.api.app as app_module
    from provisa.api.data import endpoint
    from provisa.compiler import limits

    calls = {"parse": 0, "limits": 0, "compile": 0, "prepare": 0, "execute": 0}
    document = SimpleNamespace(definitions=[])

    def _parse(schema, query, variables=None):
        calls["parse"] += 1
        return document

    def _compile(doc, ctx, variables):
        calls["compile"] += 1
        return [_cq()]

    async def _prepare(cq, ctx, rls, state, role_id, role, fresh_mvs, as_of=None):
        calls["prepare"] += 1
        cq.sql = cq.sql + " /* governed */"
        return cq, False

    seen_sql: list[str] = []

    async def _execute(compiled, *args, **kwargs):
        calls["execute"] += 1
        seen_sql.append(compiled.sql)
        compiled.sql = "MUTATED DOWNSTREAM"
        return compiled.root_field, [{"orderId": 1}], None, "ck", None

    async def _awake(state):
        return None

    def _limits(doc, **kwargs):
        calls["limits"] += 1

    state = SimpleNamespace(
        admin_db=None,  # as AppState without a control plane: no debug-trace settings
        schemas={"analyst": _SCHEMA},
        contexts={"analyst": _CTX},
        rls_contexts={"analyst": _RLS},
        roles={"analyst": _ROLE},
        mv_registry=SimpleNamespace(get_fresh=lambda: []),
        compiled_query_cache=CompiledQueryCache(),
        masking_rules={},
        tables=[],
        schema_boot_id="boot",
        schema_version=1,
        approval_hook=approval_hook,
        kafka_table_configs={},
        source_types={"sales-pg": "postgresql"},
        settings_overrides={},
    )
    monkeypatch.setattr(app_module, "state", state)
    monkeypatch.setattr(endpoint, "parse_query", _parse)
    monkeypatch.setattr(endpoint, "compile_query", _compile)
    monkeypatch.setattr(endpoint, "_prepare_compiled", _prepare)
    monkeypatch.setattr(endpoint, "_execute_one_field", _execute)
    monkeypatch.setattr(endpoint, "ensure_engine_awake", _awake)
    monkeypatch.setattr(endpoint, "_check_role_capability", lambda role, cap: None)
    monkeypatch.setattr(endpoint, "_split_action_fields", lambda doc, st: ([], ["orders"]))
    monkeypatch.setattr(endpoint, "_detect_introspection", lambda doc: False)
    monkeypatch.setattr(
        endpoint, "coerce_variable_defaults", lambda doc, variables: variables or {}
    )
    monkeypatch.setattr(
        endpoint, "_build_directives_with_legacy", lambda query, doc, hints: _DIRECTIVES
    )
    monkeypatch.setattr(endpoint, "cache_tenant", lambda st: "org")
    monkeypatch.setattr(limits, "enforce_limits", _limits)

    def call(query="{ orders { orderId } }", variables=None, **headers):
        raw = SimpleNamespace(state=SimpleNamespace(role="analyst", tenant_id=None))
        req = endpoint.GraphQLRequest(query=query, variables=variables)
        defaults = dict(
            x_provisa_role=None,
            accept=None,
            x_provisa_redirect=None,
            x_provisa_redirect_threshold=None,
            x_provisa_redirect_format=None,
            x_provisa_stats=None,
            x_provisa_normalized=None,
            x_provisa_as_of=None,
            x_provisa_trace=None,
        )
        defaults.update(headers)
        response = asyncio.run(endpoint.graphql_endpoint(raw, req, **defaults))
        assert isinstance(response, JSONResponse)
        return response

    return SimpleNamespace(call=call, calls=calls, state=state, seen_sql=seen_sql)


def test_a_repeated_query_compiles_and_governs_once(monkeypatch):
    h = _endpoint_harness(monkeypatch)
    h.call()
    assert h.calls == {"parse": 1, "limits": 1, "compile": 1, "prepare": 1, "execute": 1}
    for _ in range(3):
        h.call()
    assert h.calls == {"parse": 1, "limits": 1, "compile": 1, "prepare": 1, "execute": 4}
    governed = 'SELECT "order_id" FROM "sales"."orders" /* governed */'
    assert h.seen_sql == [governed] * 4, "a hit must execute the governed SQL the miss produced"


def test_different_variables_compile_separately(monkeypatch):
    h = _endpoint_harness(monkeypatch)
    h.call(variables={"id": 1})
    h.call(variables={"id": 2})
    h.call(variables={"id": 1})
    assert h.calls["parse"] == 2 and h.calls["prepare"] == 2 and h.calls["execute"] == 3


def test_a_schema_rebuild_recompiles(monkeypatch):
    h = _endpoint_harness(monkeypatch)
    h.call()
    h.state.schema_version += 1
    h.call()
    assert h.calls["parse"] == 2 and h.calls["prepare"] == 2


def test_an_approval_hook_deployment_governs_every_request(monkeypatch):
    h = _endpoint_harness(monkeypatch, approval_hook=object())
    h.call()
    h.call()
    assert h.calls["parse"] == 2 and h.calls["prepare"] == 2


def test_a_normalized_request_does_not_take_the_plan_path(monkeypatch):
    from fastapi.responses import JSONResponse

    from provisa.api.data import endpoint

    h = _endpoint_harness(monkeypatch)
    h.call()

    async def _normalized(*args, **kwargs):
        return JSONResponse({"data": {}})

    monkeypatch.setattr(endpoint, "_handle_normalized", _normalized)
    h.call(x_provisa_normalized="true")
    assert h.calls["parse"] == 2, "X-Provisa-Normalized needs the document; it must parse"


# --------------------------------------------------------------------------------------------- #
# Pure helpers on every surface's per-statement path are computed once per distinct input
# --------------------------------------------------------------------------------------------- #


def test_name_conversion_is_memoized(monkeypatch):
    import re

    from provisa.compiler import naming

    naming._to_snake_case.cache_clear()
    naming.domain_to_sql_name.cache_clear()
    real_sub = re.sub
    subs = {"n": 0}

    def _counting_sub(*args, **kwargs):
        subs["n"] += 1
        return real_sub(*args, **kwargs)

    monkeypatch.setattr(naming.re, "sub", _counting_sub)
    assert naming._to_snake_case("orderLineItems") == "order_line_items"
    assert naming.domain_to_sql_name("sales-analytics") == "sales_analytics"
    first = subs["n"]
    assert first > 0
    for _ in range(50):
        assert naming._to_snake_case("orderLineItems") == "order_line_items"
        assert naming.domain_to_sql_name("sales-analytics") == "sales_analytics"
    assert subs["n"] == first, "a repeated name ran the regexes again"


def test_cache_key_normalization_parses_a_statement_once(monkeypatch):
    import sqlglot

    from provisa.cache import key as cache_key_module

    cache_key_module._normalize_sql.cache_clear()
    real_parse = sqlglot.parse_one
    parses = {"n": 0}

    def _counting_parse(*args, **kwargs):
        parses["n"] += 1
        return real_parse(*args, **kwargs)

    monkeypatch.setattr(sqlglot, "parse_one", _counting_parse)
    sql = 'SELECT "order_id" FROM "sales"."orders" WHERE "region" = \'EU\' LIMIT 1'
    k1 = cache_key_module.cache_key(sql, [], "analyst", {})
    assert parses["n"] == 1
    for _ in range(20):
        assert cache_key_module.cache_key(sql, [], "analyst", {}) == k1
    assert parses["n"] == 1
    assert cache_key_module.cache_key(sql.replace("EU", "US"), [], "analyst", {}) != k1
