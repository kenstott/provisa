# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2d8f35-9b1c-4e70-8d3a-1f5c7e0b4a92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every surface reuses a repeated statement's governed plan through one mechanism (REQ-1877).

The raw-SQL stage (pgwire, Flight SQL, /data/sql, MCP) and the compiled stage (Cypher, gRPC, REST,
JSON:API, GraphQL over Flight, NL) keep their governed result in the same store under the same
rules as the GraphQL endpoint's plan: governed once until the schema generation or a governance
object changes, never shared across roles, persons or stages."""

# Requirements: REQ-1877

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.audit.context import audit_identity_scope
from provisa.pgwire import governed_plan
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.directives import NO_CACHE_HINT
from provisa.pgwire.governed_plan import PlanSlot
from tests.unit.test_governed_sql_engine_internals import _state as gov_state

_SQL = "SELECT o.id FROM sales.orders o"


@pytest.fixture(autouse=True)
def _no_rebuild(monkeypatch):
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)


# -- the shared slot ------------------------------------------------------------------------------


def _slot_state() -> SimpleNamespace:
    return SimpleNamespace(
        contexts={"analyst": object(), "admin": object()},
        rls_contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst"}, "admin": {"id": "admin"}},
        masking_rules={},
        tables=[],
        schema_boot_id="boot",
        schema_version=3,
        compiled_query_cache=CompiledQueryCache(),
    )


def test_a_kept_plan_is_served_to_the_same_stage_role_and_inputs():
    state = _slot_state()
    PlanSlot(state, "sql", "analyst", _SQL, {}).keep("plan")
    assert PlanSlot(state, "sql", "analyst", _SQL, {}).cached() == "plan"


@pytest.mark.parametrize(
    "stage, role, body",
    [
        ("compiled", "analyst", (_SQL, {})),  # another stage, same text
        ("sql", "admin", (_SQL, {})),  # another role
        ("sql", "analyst", (_SQL + " ", {})),  # another statement
        ("sql", "analyst", (_SQL, {"tenant": "acme"})),  # other session variables
    ],
)
def test_a_plan_is_not_shared_across_stage_role_or_inputs(stage, role, body):
    state = _slot_state()
    PlanSlot(state, "sql", "analyst", _SQL, {}).keep("plan")
    assert PlanSlot(state, stage, role, *body).cached() is None


def test_a_plan_is_not_shared_across_persons():
    state = _slot_state()
    with audit_identity_scope("alice", "test"):
        PlanSlot(state, "sql", "analyst", _SQL).keep("alice's")
        assert PlanSlot(state, "sql", "analyst", _SQL).cached() == "alice's"
    with audit_identity_scope("bob", "test"):
        assert PlanSlot(state, "sql", "analyst", _SQL).cached() is None


def test_two_orgs_keep_separate_plans():
    """The store is the org's own: the same statement governed in two orgs is two plans."""
    org_a, org_b = _slot_state(), _slot_state()
    PlanSlot(org_a, "sql", "analyst", _SQL).keep("a")
    assert PlanSlot(org_b, "sql", "analyst", _SQL).cached() is None


def test_a_new_generation_or_a_replaced_governance_object_misses():
    state = _slot_state()
    PlanSlot(state, "sql", "analyst", _SQL).keep("plan")
    state.schema_version += 1
    assert PlanSlot(state, "sql", "analyst", _SQL).cached() is None
    state.schema_version -= 1
    assert PlanSlot(state, "sql", "analyst", _SQL).cached() == "plan"
    for field, value in (
        ("contexts", {"analyst": object()}),
        ("rls_contexts", {"analyst": object()}),
        ("roles", {"analyst": {"id": "analyst"}}),
        ("masking_rules", {}),
        ("tables", []),
    ):
        original = getattr(state, field)
        setattr(state, field, value)
        assert PlanSlot(state, "sql", "analyst", _SQL).cached() is None, field
        setattr(state, field, original)


def test_nothing_is_kept_during_or_across_a_rebuild(monkeypatch):
    state = _slot_state()
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: True)
    PlanSlot(state, "sql", "analyst", _SQL).keep("plan")
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    assert PlanSlot(state, "sql", "analyst", _SQL).cached() is None

    slot = PlanSlot(state, "sql", "analyst", _SQL)
    state.schema_version += 1  # a rebuild ran while this request governed
    slot.keep("plan")
    assert len(state.compiled_query_cache) == 0


# -- the two pipeline stages ----------------------------------------------------------------------


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


@pytest.fixture
def pipeline(monkeypatch):
    """The one pipeline over a stand-in state, with the governance stages counted."""
    import provisa.api.app as app_mod
    from provisa.compiler import stage2
    from provisa.pgwire import _pipeline

    state = gov_state(["*"])
    state.source_dialects = {"pg": "postgres"}
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    calls = {"context": 0, "govern": 0}
    real_build, real_apply = stage2.build_governance_context, stage2.apply_governance

    def _build(*args, **kwargs):
        calls["context"] += 1
        return real_build(*args, **kwargs)

    def _apply(*args, **kwargs):
        calls["govern"] += 1
        return real_apply(*args, **kwargs)

    monkeypatch.setattr(stage2, "build_governance_context", _build)
    monkeypatch.setattr(stage2, "apply_governance", _apply)
    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        yield SimpleNamespace(state=state, calls=calls, mod=_pipeline)


async def _compiled(p, sql=_SQL, role="analyst", params=None):
    return await p.mod._govern_and_route_compiled(
        sql, role, exec_params=params, cache_hint=NO_CACHE_HINT
    )


async def test_the_compiled_stage_governs_a_repeated_statement_once(pipeline):
    first = await _compiled(pipeline, params=[1])
    assert pipeline.calls == {"context": 1, "govern": 1}
    for value in (2, 3, 4):
        again = await _compiled(pipeline, params=[value])
        assert again.sql == first.sql
    assert pipeline.calls == {"context": 1, "govern": 1}, "a repeat was governed again"


async def test_the_compiled_stage_regoverns_after_a_schema_change(pipeline):
    await _compiled(pipeline)
    pipeline.state.schema_version += 1
    await _compiled(pipeline)
    assert pipeline.calls["context"] == 2
    pipeline.state.masking_rules = {}  # replaced object, same generation
    await _compiled(pipeline)
    assert pipeline.calls["context"] == 3


async def test_the_compiled_stage_keeps_a_plan_per_role(pipeline):
    state = pipeline.state
    state.contexts["auditor"] = state.contexts["analyst"]
    state.roles["auditor"] = {"id": "auditor", "capabilities": [], "domain_access": ["*"]}
    await _compiled(pipeline, role="analyst")
    await _compiled(pipeline, role="auditor")
    assert pipeline.calls["context"] == 2
    await _compiled(pipeline, role="auditor")
    assert pipeline.calls["context"] == 2


async def test_the_compiled_stage_applies_rls_per_session_value(pipeline):
    """A plan governed for one session value is never served to another."""
    from provisa.compiler.rls import RLSContext
    from provisa.core.request_context import current_session_vars

    orders_id = pipeline.state.tables[0]["id"]
    pipeline.state.rls_contexts["analyst"] = RLSContext(
        rules={orders_id: "region = current_setting('provisa.region')"}
    )
    token = current_session_vars.set({"region": "EU"})
    try:
        eu = await _compiled(pipeline)
        current_session_vars.set({"region": "US"})
        us = await _compiled(pipeline)
    finally:
        current_session_vars.reset(token)
    assert "'EU'" in eu.sql and "'US'" in us.sql and "'EU'" not in us.sql


async def test_the_raw_stage_governs_a_repeated_statement_once(pipeline):
    await pipeline.mod._govern_and_route(_SQL, "analyst")
    assert pipeline.calls == {"context": 1, "govern": 1}
    await pipeline.mod._govern_and_route(_SQL, "analyst")
    assert pipeline.calls == {"context": 1, "govern": 1}


async def test_the_two_stages_never_share_a_plan_for_the_same_text(pipeline):
    await pipeline.mod._govern_and_route(_SQL, "analyst")
    await _compiled(pipeline)
    assert pipeline.calls["context"] == 2


def test_a_cache_key_parses_a_statement_text_once(monkeypatch):
    """Building a plan-cache key must not cost a parse per lookup."""
    from provisa.compiler import compiled_query_cache as cqc
    from provisa.observability import stage_trace

    cqc.sql_shape_digest.cache_clear()
    real = stage_trace.redact_sql
    parses = {"n": 0}

    def _counting(sql):
        parses["n"] += 1
        return real(sql)

    monkeypatch.setattr(stage_trace, "redact_sql", _counting)
    sql = "SELECT o.id FROM sales.orders o WHERE o.id = 7"
    first = cqc.routing_cache_key(sql, "analyst", "boot", 1)
    for _ in range(25):
        assert cqc.routing_cache_key(sql, "analyst", "boot", 1) == first
        cqc.compiled_query_cache_key(sql, "analyst", None, "boot", 1, False)
    assert parses["n"] == 1
    # The shape still collapses literal-only differences onto one entry.
    assert cqc.routing_cache_key(sql.replace("7", "8"), "analyst", "boot", 1) == first


async def test_a_plan_governed_for_a_wider_acting_role_set_is_not_served_to_a_narrower_one(
    pipeline,
):
    """REQ-1620: domain access follows the union of the roles a caller is acting as. That set is
    an input of governance, so it is part of the plan's identity: a statement admitted for
    (analyst + sales_reader) must be governed again — and refused — for analyst alone."""
    from provisa.core.request_context import reset_role_claims, set_role_claims

    state = pipeline.state
    state.roles["analyst"]["domain_access"] = ["hr"]  # not the statement's domain (sales)
    state.roles["sales_reader"] = {"id": "sales_reader", "capabilities": [], "domain_access": ["*"]}
    token = set_role_claims(["analyst", "sales_reader"])
    try:
        wide = await pipeline.mod._govern_and_route(_SQL, "analyst")
        assert wide.sql
    finally:
        reset_role_claims(token)
    with pytest.raises(PermissionError):
        await pipeline.mod._govern_and_route(_SQL, "analyst")
    token = set_role_claims(["sales_reader", "analyst"])  # the same set, another order: same plan
    try:
        before = dict(pipeline.calls)
        await pipeline.mod._govern_and_route(_SQL, "analyst")
        assert pipeline.calls == before
    finally:
        reset_role_claims(token)
