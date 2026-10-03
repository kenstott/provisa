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
        relationships=[],
        metrics={},
        source_types={},
        federation_engine=None,
        security_high=False,
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


def test_a_plan_is_shared_across_persons_of_one_role():
    """Governance reads nothing from the acting person: what differs per person reaches it as
    session variables, which are in the key. Keying on the person only multiplied the entries
    (statements x roles x persons against a bound of 4,096)."""
    state = _slot_state()
    with audit_identity_scope("alice", "test"):
        PlanSlot(state, "sql", "analyst", _SQL, {"tenant": "acme"}).keep("the role's plan")
    with audit_identity_scope("bob", "test"):
        assert PlanSlot(state, "sql", "analyst", _SQL, {"tenant": "acme"}).cached() == (
            "the role's plan"
        )
        # a per-person value arrives as a session variable — and stays its own plan
        assert PlanSlot(state, "sql", "analyst", _SQL, {"tenant": "globex"}).cached() is None


def test_a_kept_plan_has_no_time_expiry(monkeypatch):
    """A kept plan is invalidated by its generation and anchors, not by the clock: the org's store
    (``OrgRuntime.compiled_query_cache``) does not expire entries."""
    import time as _time

    from provisa.api.org_runtime import OrgRuntime
    from provisa.compiler import compiled_query_cache as cqc

    state = _slot_state()
    state.compiled_query_cache = OrgRuntime.__dataclass_fields__[
        "compiled_query_cache"
    ].default_factory()
    PlanSlot(state, "sql", "analyst", _SQL).keep("plan")
    real = _time.monotonic
    monkeypatch.setattr(cqc.time, "monotonic", lambda: real() + 7 * 24 * 3600)
    assert PlanSlot(state, "sql", "analyst", _SQL).cached() == "plan"


def test_the_store_drops_the_least_recently_used_plan(monkeypatch):
    from provisa.compiler import compiled_query_cache as cqc

    monkeypatch.setattr(cqc, "_MAX_ENTRIES", 3)
    state = _slot_state()
    state.compiled_query_cache = cqc.CompiledQueryCache(expires=False)
    for name in ("a", "b", "c"):
        PlanSlot(state, "sql", "analyst", name).keep(name)
    assert PlanSlot(state, "sql", "analyst", "a").cached() == "a"  # "a" is now the most recent
    PlanSlot(state, "sql", "analyst", "d").keep("d")
    assert len(state.compiled_query_cache) == 3
    assert PlanSlot(state, "sql", "analyst", "b").cached() is None, "the oldest-used was kept"
    assert [PlanSlot(state, "sql", "analyst", n).cached() for n in ("a", "c", "d")] == [
        "a",
        "c",
        "d",
    ]


@pytest.mark.parametrize(
    "change",
    [
        lambda state, monkeypatch: monkeypatch.setenv("PROVISA_DEFAULT_ROW_LIMIT", "7"),
        lambda state, monkeypatch: setattr(state, "security_high", True),
        lambda state, monkeypatch: setattr(state, "relationships", []),
        lambda state, monkeypatch: setattr(state, "metrics", {}),
        lambda state, monkeypatch: setattr(state, "source_types", {"pg": "postgresql"}),
        lambda state, monkeypatch: setattr(state, "federation_engine", object()),
    ],
    ids=[
        "default row limit",
        "security mode",
        "relationships",
        "metrics",
        "source types",
        "engine",
    ],
)
def test_a_governance_input_outside_the_statement_misses_at_once(change, monkeypatch):
    """Everything governance reads that is not the statement, the role or the session variables
    is an anchor: the two settings by value, the registry objects by identity. A changed setting
    used to reach kept plans only when their 60 s lifetime ran out."""
    monkeypatch.setenv("PROVISA_DEFAULT_ROW_LIMIT", "100")
    state = _slot_state()
    PlanSlot(state, "sql", "analyst", _SQL).keep("plan")
    assert PlanSlot(state, "sql", "analyst", _SQL).cached() == "plan"
    change(state, monkeypatch)
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
    first = cqc.routing_cache_key(sql, "analyst", "boot", 1, 0)
    for _ in range(25):
        assert cqc.routing_cache_key(sql, "analyst", "boot", 1, 0) == first
        cqc.compiled_query_cache_key(sql, "analyst", None, "boot", 1, False)
    assert parses["n"] == 1
    # The shape still collapses literal-only differences onto one entry.
    assert cqc.routing_cache_key(sql.replace("7", "8"), "analyst", "boot", 1, 0) == first
    # REQ-826: a route is decided under one set of replica-served tables; when that set changes
    # (a busy table promoted or demoted, a replica completed) the generation moves and the
    # route is decided again — with no schema build.
    assert cqc.routing_cache_key(sql, "analyst", "boot", 1, 1) != first


# -- the ENGINE route's statement-only work is kept with the plan ---------------------------------


async def _engine_route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
    from provisa.transpiler.router import Route, RouteDecision

    return (
        exec_sql,
        RouteDecision(route=Route.ENGINE, source_id=None, dialect=None, reason="t"),
        "pg",
        False,
        {"pg"},
        (),
    )


class _Engine:
    """The federation engine as the pipeline's ENGINE branch reads it: a real DuckDB transpile."""

    dialect = "duckdb"
    engine = SimpleNamespace(name="duckdb", catalog_qualified=True)

    def transpile_physical(self, pg_sql: str) -> str:
        from provisa.transpiler.transpile import transpile_to_duckdb

        return transpile_to_duckdb(pg_sql)


@pytest.fixture
def engine_pipeline(pipeline, monkeypatch):
    """``pipeline`` routed to the engine, with every sqlglot parse counted."""
    import sqlglot.parser

    pipeline.state.federation_engine = _Engine()
    pipeline.state.source_pools = SimpleNamespace(source_ids=["pg"])
    pipeline.state.source_catalogs = {"pg": "pg"}
    parses = {"n": 0}
    real_parse = sqlglot.parser.Parser.parse

    def _counting(self, *args, **kwargs):
        parses["n"] += 1
        return real_parse(self, *args, **kwargs)

    monkeypatch.setattr(sqlglot.parser.Parser, "parse", _counting)
    with (
        patch.object(pipeline.mod, "_optimize_and_route", new=AsyncMock(side_effect=_engine_route)),
        audit_identity_scope("u-1", "analyst"),
    ):
        yield SimpleNamespace(state=pipeline.state, mod=pipeline.mod, parses=parses)


_ENGINE_SQL = "SELECT o.id FROM sales.orders o WHERE o.id = $1"


async def test_a_repeated_engine_statement_on_the_compiled_stage_parses_once(engine_pipeline):
    """REQ-1877: lowering, the unknown-catalog check, the literal-predicate carry, the engine
    transpile and the span attributes are functions of the governed text — kept with the plan."""
    p = engine_pipeline
    first = await p.mod._govern_and_route_compiled(
        _ENGINE_SQL, "analyst", exec_params=[1], cache_hint=NO_CACHE_HINT
    )
    assert first.physical_sql and p.parses["n"] > 0
    p.parses["n"] = 0
    for value in (2, 3, 4):
        again = await p.mod._govern_and_route_compiled(
            _ENGINE_SQL, "analyst", exec_params=[value], cache_hint=NO_CACHE_HINT
        )
        assert again.physical_sql == first.physical_sql
        assert again.exec_sql == first.exec_sql
        assert again.span_attrs == first.span_attrs and again.span_attrs is not first.span_attrs
        assert again.exec_params == [value]
        assert again.stamp != first.stamp
    assert p.parses["n"] == 0, "a repeated ENGINE-route statement was parsed again"


async def test_a_repeated_engine_statement_on_the_raw_stage_parses_once(engine_pipeline):
    p = engine_pipeline
    first = await p.mod._govern_and_route("SELECT o.id FROM sales.orders o", "analyst")
    assert first.physical_sql and p.parses["n"] > 0
    p.parses["n"] = 0
    for _ in range(3):
        again = await p.mod._govern_and_route("SELECT o.id FROM sales.orders o", "analyst")
        assert again.physical_sql == first.physical_sql
        assert again.span_attrs == first.span_attrs
    assert p.parses["n"] == 0, "a repeated ENGINE-route statement was parsed again"


async def test_the_kept_engine_form_follows_the_optimized_text_and_the_engine(engine_pipeline):
    """The kept engine-physical form answers only for the exact text it was derived from and the
    engine it was transpiled for: an optimization that rewrites the statement (a hot-table inline
    changes with the data) or a swapped engine derives it again."""
    p = engine_pipeline
    first = await p.mod._govern_and_route_compiled(
        _ENGINE_SQL, "analyst", exec_params=[1], cache_hint=NO_CACHE_HINT
    )

    async def _rewritten(exec_sql, governed_sql, gov_ctx, ctx, state, role_id, **kwargs):
        out = await _engine_route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs)
        return (exec_sql.replace("$1", "$1 AND 1 = 1"), *out[1:3], True, *out[4:])

    with patch.object(p.mod, "_optimize_and_route_cached", new=AsyncMock(side_effect=_rewritten)):
        rewritten = await p.mod._govern_and_route_compiled(
            _ENGINE_SQL, "analyst", exec_params=[1], cache_hint=NO_CACHE_HINT
        )
    assert "1 = 1" in rewritten.physical_sql and "1 = 1" not in first.physical_sql

    class _Other(_Engine):
        def transpile_physical(self, pg_sql: str) -> str:
            return "/* other engine */ " + pg_sql

    p.state.federation_engine = _Other()
    swapped = await p.mod._govern_and_route_compiled(
        _ENGINE_SQL, "analyst", exec_params=[1], cache_hint=NO_CACHE_HINT
    )
    assert swapped.physical_sql.startswith("/* other engine */")


def test_kept_pk_bounds_parse_once_and_follow_the_bound_values(monkeypatch):
    """REQ-1865: the bounds are resolved per execution from that execution's values; the parsed
    statement and the row_materialize table set they are resolved against are kept."""
    import sqlglot.parser

    from provisa.pgwire import _pipeline

    table = SimpleNamespace(
        id=1,
        source_id="neo",
        schema_name="graph",
        table_name="customer_node",
        alias=None,
        row_materialize=True,
        columns=[
            SimpleNamespace(
                name="customer_id",
                data_type="integer",
                is_primary_key=True,
                native_filter_type=None,
            )
        ],
    )
    lookups = {"n": 0}

    def _row_tables(state):
        lookups["n"] += 1
        return {"customer_node": table}

    monkeypatch.setattr(_pipeline, "_row_materialize_tables_in_memory", _row_tables)
    parses = {"n": 0}
    real_parse = sqlglot.parser.Parser.parse

    def _counting(self, *args, **kwargs):
        parses["n"] += 1
        return real_parse(self, *args, **kwargs)

    monkeypatch.setattr(sqlglot.parser.Parser, "parse", _counting)
    state = SimpleNamespace(tables=[{"id": 1}], federation_engine=object())
    sql = 'SELECT c."customer_id" FROM "graph"."customer_node" c WHERE c."customer_id" = $1'
    memo: dict = {}
    seen = [_pipeline._kept_pk_bounds(memo, sql, state, [value]) for value in (7, 8, 9)]
    assert parses["n"] == 1 and lookups["n"] == 1
    assert len({repr(b) for b in seen}) == 3, "the bounds did not follow the bound values"
    assert all(b for b in seen)

    state.tables = [{"id": 1}]  # a rebuild republished the registry
    _pipeline._kept_pk_bounds(memo, sql, state, [7])
    assert lookups["n"] == 2


async def test_a_statement_governed_for_one_person_is_the_same_plan_for_another(pipeline):
    """The whole stage, not only the slot: bob is served the statement governed for alice — same
    governed SQL, governed once — and a per-person RLS value (a session variable) is not shared."""
    from provisa.compiler.rls import RLSContext
    from provisa.core.request_context import current_session_vars

    orders_id = pipeline.state.tables[0]["id"]
    pipeline.state.rls_contexts["analyst"] = RLSContext(
        rules={orders_id: "region = current_setting('provisa.region')"}
    )
    token = current_session_vars.set({"region": "EU"})
    try:
        with audit_identity_scope("alice", "test"):
            alice = await _compiled(pipeline)
        with audit_identity_scope("bob", "test"):
            bob = await _compiled(pipeline)
        assert pipeline.calls == {"context": 1, "govern": 1}, "bob's request was governed again"
        assert bob.sql == alice.sql and "'EU'" in bob.sql
        assert bob.audit is not alice.audit and bob.stamp != alice.stamp
        current_session_vars.set({"region": "US"})
        with audit_identity_scope("carol", "test"):
            carol = await _compiled(pipeline)
        assert "'US'" in carol.sql and "'EU'" not in carol.sql
        assert pipeline.calls["context"] == 2
    finally:
        current_session_vars.reset(token)


async def test_a_plan_governed_for_a_wider_acting_role_set_is_not_served_to_a_narrower_one(
    pipeline,
):
    """REQ-1620: a set of held roles acts as its meta-role, whose domain access is the union. A
    statement admitted for (analyst + sales_reader) is that role's plan; analyst alone is governed
    again — and refused."""
    state = pipeline.state
    state.roles["analyst"]["domain_access"] = ["hr"]  # not the statement's domain (sales)
    meta = "meta:analyst+sales_reader"
    state.roles[meta] = {**state.roles["analyst"], "id": meta, "domain_access": ["*"]}
    state.contexts[meta] = state.contexts["analyst"]
    # A child of both: every grant to a member reaches it (security/inheritance.py).
    from provisa.security.inheritance import expand_column_grants

    expand_column_grants(state.tables, {meta: [meta, "analyst", "sales_reader"]})
    if "analyst" in state.rls_contexts:
        state.rls_contexts[meta] = state.rls_contexts["analyst"]
    wide = await pipeline.mod._govern_and_route(_SQL, meta)
    assert wide.sql
    with pytest.raises(PermissionError):
        await pipeline.mod._govern_and_route(_SQL, "analyst")
    before = dict(pipeline.calls)
    await pipeline.mod._govern_and_route(_SQL, meta)  # the same set: the same plan
    assert pipeline.calls == before
