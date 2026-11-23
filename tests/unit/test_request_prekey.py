# Copyright (c) 2026 Kenneth Stott
# Canary: 4a7d2e91-3c5b-4f86-9e1a-8b0c6d2f5e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The request pre-key (REQ-1897, amended 2026-11-23, REQUEST PRE-KEY).

An exact repeat of a request -- its text, its values, the caller's role, session context and
tenant -- is answered from the response cache by the key its first compile produced: no
governance, no routing. The same text with new values skips the parse and governance (the
text-only kept plan) and is bound and run. The pre-key is dropped when the model or the caller's
rights change, and never crosses a role, session context or tenant."""

# Requirements: REQ-1897, REQ-1877

from __future__ import annotations

import copy
import statistics
import time
from unittest.mock import patch

import pytest

from provisa.compiler.directives import CacheHint
from provisa.compiler.rls import RLSContext
from provisa.transpiler.router import Route
from tests.unit.test_cache_before_route import _CACHED, _OPT_IN, _PLAIN, _raw, pipe  # noqa: F401

pytestmark = pytest.mark.asyncio


class _Counter:
    """Counts calls to a pipeline function, calling through to it."""

    def __init__(self, mod, name: str) -> None:
        self.calls = 0
        self._real = getattr(mod, name)
        self._patch = patch.object(mod, name, new=self)

    async def __call__(self, *args, **kwargs):
        self.calls += 1
        return await self._real(*args, **kwargs)

    def __enter__(self):
        self._patch.__enter__()
        return self

    def __exit__(self, *exc):
        return self._patch.__exit__(*exc)


async def _compiled(p, params, *, cache_hint: CacheHint = _OPT_IN, role_id: str = "analyst"):
    plan = await p.mod._govern_and_route_compiled(
        _PLAIN,
        role_id,
        exec_params=params,
        state=p.state,
        cache_hint=cache_hint,
        serve_cached=True,
        sdl_joins=True,
    )
    return plan, await p.mod._execute_plan(plan, p.state)


async def test_an_exact_sql_repeat_is_neither_governed_nor_routed(pipe):  # noqa: F811
    with _Counter(pipe.mod, "_govern_and_route_planned") as planned:
        plan, first = await _raw(pipe, _CACHED, [7])
        assert plan.route == Route.DIRECT and planned.calls == 1
        for _ in range(3):
            plan, again = await _raw(pipe, _CACHED, [7])
            assert plan.route == Route.CACHE and again.rows == first.rows == [(7,)]
    assert planned.calls == 1, "an exact repeat was governed again"
    assert len(pipe.state.federation_engine.statements) == 1
    # Accounted like any hit: one audit row each, as the full path's cache hit writes.
    assert [a["route"] for a in pipe.audits] == ["direct", "cache", "cache", "cache"]


async def test_an_exact_compiled_repeat_is_neither_governed_nor_routed(pipe):  # noqa: F811
    with _Counter(pipe.mod, "_govern_and_route_compiled_planned") as planned:
        plan, first = await _compiled(pipe, [7])
        assert plan.route == Route.DIRECT
        for _ in range(3):
            plan, again = await _compiled(pipe, [7])
            assert plan.route == Route.CACHE and again.rows == [(7,)]
    assert planned.calls == 1, "an exact compiled repeat was governed again"
    assert [a["route"] for a in pipe.audits] == ["direct", "cache", "cache", "cache"]


async def test_the_same_text_with_new_values_is_bound_and_run_not_parsed(pipe):  # noqa: F811
    """Values are part of the result's identity: the new values are a new answer, from the
    source, but the governed statement is the kept one (no parse, no governance)."""
    from provisa.compiler import prepared

    real = prepared.prepare_front_end
    front_ends = []

    def _counted(*args, **kwargs):
        front_ends.append(1)
        return real(*args, **kwargs)

    with patch.object(prepared, "prepare_front_end", new=_counted):
        _plan, first = await _raw(pipe, _CACHED, [7])
        plan, second = await _raw(pipe, _CACHED, [8])
    assert first.rows == [(7,)]
    assert plan.route == Route.DIRECT and second.rows == [(8,)], "a new value served the old answer"
    assert len(front_ends) == 1, "the same text with new values was parsed again"
    assert len(pipe.state.federation_engine.statements) == 2


async def test_a_model_change_drops_the_pre_key(pipe):  # noqa: F811
    with _Counter(pipe.mod, "_govern_and_route_planned") as planned:
        await _raw(pipe, _CACHED, [7])
        await _raw(pipe, _CACHED, [7])
        assert planned.calls == 1
        pipe.state.schema_version += 1  # every schema, RLS, masking and role change bumps it
        await _raw(pipe, _CACHED, [7])
    assert planned.calls == 2, "a pre-key outlived a model change"


async def test_a_rule_change_drops_the_pre_key(pipe):  # noqa: F811
    """A rebuild replaces the governance objects before it bumps the generation: a pre-key made
    under the old masking rules or role record never answers under the new ones."""
    with _Counter(pipe.mod, "_govern_and_route_planned") as planned:
        await _raw(pipe, _CACHED, [7])
        await _raw(pipe, _CACHED, [7])
        assert planned.calls == 1
        pipe.state.masking_rules = dict(pipe.state.masking_rules)
        await _raw(pipe, _CACHED, [7])
        assert planned.calls == 2, "a pre-key outlived a masking-rule change"
        pipe.state.roles["analyst"] = dict(pipe.state.roles["analyst"])
        await _raw(pipe, _CACHED, [7])
    assert planned.calls == 3, "a pre-key outlived a change to the caller's rights"


async def test_an_expired_entry_falls_through_to_the_full_path(pipe):  # noqa: F811
    with _Counter(pipe.mod, "_govern_and_route_planned") as planned:
        await _raw(pipe, _CACHED, [7])
        pipe.state.response_cache_store._data.clear()
        plan, again = await _raw(pipe, _CACHED, [7])
    assert plan.route == Route.DIRECT and again.rows == [(7,)]
    assert planned.calls == 2
    assert len(pipe.state.federation_engine.statements) == 2


async def test_another_role_never_hits_a_roles_pre_key(pipe):  # noqa: F811
    """The auditor's own governance answers the auditor (here: it sees no column of the table),
    never the analyst's pre-key or entry."""
    from provisa.compiler.sql_validator import GovernanceRefused

    state = pipe.state
    state.contexts["auditor"] = copy.deepcopy(state.contexts["analyst"])
    state.rls_contexts["auditor"] = RLSContext.empty()
    state.roles["auditor"] = {**state.roles["analyst"], "id": "auditor"}
    await _raw(pipe, _CACHED, [7])
    plan, _ = await _raw(pipe, _CACHED, [7])
    assert plan.route == Route.CACHE
    with _Counter(pipe.mod, "_govern_and_route_planned") as planned:
        with pytest.raises(GovernanceRefused):
            await pipe.mod._govern_and_route(_CACHED, "auditor", params=[7], serve_cached=True)
    assert planned.calls == 1, "a role was answered by another role's pre-key"


async def test_pre_keys_never_cross_session_context_tenant_or_values(pipe):  # noqa: F811
    from provisa.core.request_context import reset_session_vars, set_session_vars

    def key(**kw):
        args = {
            "state": pipe.state,
            "sql": _CACHED,
            "role_id": "analyst",
            "session_vars": None,
            "params": [7],
            "as_of": None,
            "wire_formats": None,
            **kw,
        }
        return pipe.mod._sql_pre_key(**args)._key

    base = key()
    assert key(role_id="analyst") == base
    assert key(params=["7"]) != base, "1 and '1' shared a pre-key"
    assert key(params=[8]) != base
    assert key(session_vars={"user.region": "eu"}) != base
    assert key(wire_formats=[1]) != base
    assert key(as_of="2026-01-01T00:00:00Z") != base
    token = set_session_vars({"user.region": "eu"})
    try:
        assert key() != base, "a session context shared another's pre-key"
    finally:
        reset_session_vars(token)
    pipe.state.model_stamp = 2  # the tenant carries the loaded model
    assert key() != base


async def test_a_request_that_did_not_opt_in_keeps_no_pre_key(pipe):  # noqa: F811
    before = len(pipe.state.compiled_query_cache)
    await _raw(pipe, _PLAIN, [7])
    await _raw(pipe, _PLAIN, [7])
    kept = len(pipe.state.compiled_query_cache) - before
    with _Counter(pipe.mod, "_govern_and_route_planned") as planned:
        await _raw(pipe, _PLAIN, [7])
    assert planned.calls == 1
    assert len(pipe.state.compiled_query_cache) - before == kept


async def _p50(fn, n: int = 200) -> float:
    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        await fn()
        samples.append((time.perf_counter() - t0) * 1000)
    return statistics.median(samples)


async def test_cached_one_row_p50_before_and_after(pipe, capsys):  # noqa: F811
    """The planning cost of a cached one-row read, through the pipeline's two entries, with the
    pre-key (after) and with it turned off (before: govern -> cache read before routing)."""
    await _raw(pipe, _CACHED, [7])
    await _compiled(pipe, [7])

    def _sql():
        return pipe.mod._govern_and_route(_CACHED, "analyst", params=[7], serve_cached=True)

    def _cmp():
        return pipe.mod._govern_and_route_compiled(
            _PLAIN,
            "analyst",
            exec_params=[7],
            state=pipe.state,
            cache_hint=_OPT_IN,
            serve_cached=True,
            sdl_joins=True,
        )

    after_sql, after_cmp = await _p50(_sql), await _p50(_cmp)

    async def _none(*_a, **_k):
        return None

    with patch.object(pipe.mod, "_pre_keyed_plan", new=_none):
        before_sql, before_cmp = await _p50(_sql), await _p50(_cmp)
    with capsys.disabled():
        print(
            f"\nprekey p50 ms: sql before={before_sql:.3f} after={after_sql:.3f}; "
            f"compiled before={before_cmp:.3f} after={after_cmp:.3f}"
        )
    assert after_sql < before_sql and after_cmp < before_cmp


async def test_graphql_text_is_validated_once_whatever_its_variables(monkeypatch):
    """The text-only kept form of a GraphQL request: parsing and validating read only the text and
    the schema, so the same text with new variables is not parsed or validated again."""
    from graphql import build_schema

    from provisa.api.data.graphql_plan import PlanRequest
    from provisa.compiler import parser
    from provisa.compiler.parser import GraphQLValidationError
    from provisa.pgwire import governed_plan
    from tests.unit.test_governed_sql_engine_internals import _state as gov_state

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    state = gov_state(["*"])
    ctx = state.contexts["analyst"]
    schema = build_schema("type Query { orders(id: Int): Int }")
    calls = []

    def _counted(*args, **kwargs):
        calls.append(1)
        return parser.parse_query(*args, **kwargs)

    def document(schema, text, variables):
        request = PlanRequest(
            state,
            role_id="analyst",
            role=state.roles["analyst"],
            schema=schema,
            query=text,
            variables=variables,
            eligible=True,
        )
        return request.document(schema, text, ctx, _counted)

    text = "query Q($id: Int) { orders(id: $id) }"
    first = document(schema, text, {"id": 1})
    again = document(schema, text, {"id": 2})
    assert again is first and len(calls) == 1
    # another schema object (a rebuild) is validated afresh
    document(build_schema("type Query { orders(id: Int): Int }"), text, {"id": 1})
    assert len(calls) == 2
    # a document that fails is refused on every request, never kept
    for _ in range(2):
        with pytest.raises(GraphQLValidationError):
            document(schema, "{ nope }", None)
    assert len(calls) == 4
