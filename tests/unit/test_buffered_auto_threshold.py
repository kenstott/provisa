# Copyright (c) 2026 Kenneth Stott
# Canary: fa8957d5-0716-41f5-bf4c-1d618392bae5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1224 (streaming-uniformity Defect 4): the AUTOMATIC stream↔CTAS threshold at the single
terminal (``_execute_plan``). A buffered transport (JSON:API, GraphQL, Bolt) carries an ``auto_deliver``
policy on its governed plan; the terminal DECIDES per-result — inline the body when it fits under the
config row threshold, land an engine-native CTAS (``run_materialize``) off Provisa's heap when it
exceeds. No caller side-channel, no transport-local branch. Opt-in: disabled → the policy is None and
the terminal returns rows inline exactly as before."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.executor.redirect import Delivery, RedirectConfig, auto_delivery_for_buffered


def _config(threshold: int) -> RedirectConfig:
    return RedirectConfig(
        enabled=True,
        threshold=threshold,
        bucket="b",
        endpoint_url="",
        access_key="",
        secret_key="",
        ttl=60,
    )


def test_auto_delivery_disabled_by_default(monkeypatch):
    monkeypatch.delenv("PROVISA_REDIRECT_ENABLED", raising=False)
    assert auto_delivery_for_buffered("role-x") is None


def test_auto_delivery_enabled_carries_config(monkeypatch):
    monkeypatch.setenv("PROVISA_REDIRECT_ENABLED", "true")
    monkeypatch.setenv("PROVISA_REDIRECT_FORMAT", "orc")
    d = auto_delivery_for_buffered("role-x")
    assert d is not None
    assert d.role == "role-x"
    assert d.output_format == "orc"
    assert d.config.enabled is True


class _FakeStream:
    """A minimal ResultStream whose row generator records early closure (the engine cursor's
    ``finally``) — so the test can assert the terminal does NOT drain past the threshold."""

    def __init__(self, rows: list[tuple], closed_flag: list[bool]) -> None:
        self.column_names = ["n"]
        self.column_types = ["bigint"]
        self._rows = rows
        self._closed = closed_flag

    def iter_rows(self):
        try:
            for r in self._rows:
                yield r
        finally:
            self._closed[0] = True


class _FakeEngine:
    dialect = "trino"

    def __init__(self, rows: list[tuple], closed_flag: list[bool]) -> None:
        self._rows = rows
        self._closed = closed_flag
        self.calls: list[str] = []

    def execute_engine_sync(self, sql, params=None, *, session_hints=None):
        self.calls.append(sql)
        return _FakeStream(self._rows, self._closed)


class _FakeState:
    def __init__(self, engine) -> None:
        from provisa.cache.store import NoopCacheStore

        self.federation_engine = engine
        self.response_cache_store = NoopCacheStore()  # AppState always holds a store


def _plan(auto_deliver):
    from provisa.pgwire import _pipeline as P
    from provisa.transpiler.router import Route

    return P._Plan(
        route=Route.ENGINE,
        sql="SELECT n FROM t",
        source_id="s",
        dialect="duckdb",
        physical_sql="SELECT n FROM t",
        auto_deliver=auto_deliver,
        stamp=P._mint_stamp(),  # governed-provenance: the terminal refuses an unstamped plan
    )


@pytest.mark.asyncio
async def test_terminal_inlines_below_threshold():
    from provisa.pgwire._pipeline import _execute_plan

    closed = [False]
    engine = _FakeEngine([(i,) for i in range(3)], closed)
    deliv = Delivery(output_format="parquet", config=_config(threshold=5), role="r")
    result = await _execute_plan(_plan(deliv), _FakeState(engine))

    assert result.redirect is None
    assert result.rows == [(0,), (1,), (2,)]
    assert result.column_names == ["n"]
    assert closed[0]  # stream drained to exhaustion (its finally ran)


@pytest.mark.asyncio
async def test_terminal_materializes_above_threshold(monkeypatch):
    from provisa.pgwire import _pipeline as P

    closed = [False]
    engine = _FakeEngine([(i,) for i in range(100)], closed)

    captured: dict = {}

    async def _fake_run_materialize(state, sql, deliv, params):
        captured["sql"] = sql
        captured["deliv"] = deliv
        return {"sink": "s3://b/x.parquet", "row_count": None, "redirect_url": "http://x"}

    import provisa.executor.redirect as R

    monkeypatch.setattr(R, "run_materialize", _fake_run_materialize)

    deliv = Delivery(output_format="parquet", config=_config(threshold=10), role="r")
    result = await P._execute_plan(_plan(deliv), _FakeState(engine))

    assert result.rows == []
    assert result.redirect == {
        "sink": "s3://b/x.parquet",
        "row_count": None,
        "redirect_url": "http://x",
    }
    assert captured["sql"] == "SELECT n FROM t"  # the engine-physical CTAS source
    assert closed[0]  # partial buffer abandoned — the stream was closed early, not fully drained


# -- a buffered transport routes like every other surface (REQ-1224 amended 2026-10-01) -----------
#
# The automatic threshold used to force every buffered read (REST, JSON:API, Bolt) onto the
# federation engine, so a primary-key lookup on one PostgreSQL source ran as an engine scan through
# an attached catalog instead of one indexed statement on the source. The route is now the
# router's; a single-source read stays DIRECT and is bounded by a threshold+1 probe, and only a
# result that does not fit is landed by the engine.


class _Source:
    """The DIRECT terminal and the engine's transpile, as the pipeline reads them."""

    dialect = "duckdb"
    engine = SimpleNamespace(name="duckdb", catalog_qualified=True)

    def __init__(self, rows_for) -> None:
        self.statements: list[tuple[str, str, list]] = []
        self.engine_statements: list[str] = []
        self._rows_for = rows_for

    async def execute_native(self, pools, source_id, sql, params=None, span_attrs=None):
        from provisa.executor.result import QueryResult

        self.statements.append((source_id, sql, list(params or [])))
        return QueryResult(rows=self._rows_for(sql, params), column_names=["id"])

    def execute_engine_sync(self, sql, params=None, *, session_hints=None):
        self.engine_statements.append(sql)
        raise AssertionError("a single-source buffered read reached the engine")

    def transpile_physical(self, pg_sql: str) -> str:
        from provisa.transpiler.transpile import transpile_to_duckdb

        return transpile_to_duckdb(pg_sql)


def _decision(route_name: str):
    from provisa.transpiler.router import Route, RouteDecision

    route = getattr(Route, route_name)
    direct = route == Route.DIRECT

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        return (
            exec_sql,
            RouteDecision(
                route=route,
                source_id="pg" if direct else None,
                dialect="postgres" if direct else None,
                reason="t",
            ),
            "pg",
            False,
            {"pg"},
            (),
        )

    return _route


@pytest.fixture
def buffered(monkeypatch):
    """The one pipeline over a stand-in state with redirect enabled at a threshold of 5 rows."""
    import provisa.api.app as app_mod
    from provisa.cache.store import NoopCacheStore
    from provisa.pgwire import _pipeline, governed_plan
    from tests.unit.test_governed_sql_engine_internals import _state as gov_state

    monkeypatch.setenv("PROVISA_REDIRECT_ENABLED", "true")
    monkeypatch.setenv("PROVISA_REDIRECT_THRESHOLD", "5")
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    state = gov_state(["*"])
    state.source_dialects = {"pg": "postgres"}
    state.source_catalogs = {"pg": "pg"}
    state.source_pools = SimpleNamespace(source_ids=["pg"], has=lambda sid: sid == "pg")
    state.response_cache_store = NoopCacheStore()
    state.settings_overrides = {}  # as AppState: no org override of the redirect settings
    state.roles["analyst"]["max_rows"] = 100  # the role's own row ceiling, above the threshold
    monkeypatch.setattr(app_mod, "state", state, raising=False)

    def _use(route_name: str, rows_for):
        state.federation_engine = _Source(rows_for)
        return patch.object(
            _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision(route_name))
        )

    return SimpleNamespace(state=state, mod=_pipeline, use=_use)


_LOOKUP = "SELECT o.id FROM sales.orders o WHERE o.id = $1 LIMIT $2"


async def _run(b, sql, params):
    from provisa.compiler.directives import NO_CACHE_HINT

    plan = await b.mod._govern_and_route_compiled(
        sql, "analyst", exec_params=params, state=b.state, buffered=True, cache_hint=NO_CACHE_HINT
    )
    return plan, await b.mod._execute_plan(plan, b.state)


@pytest.mark.asyncio
async def test_a_buffered_pk_lookup_is_one_statement_on_the_source(buffered):
    from provisa.transpiler.router import Route

    with buffered.use("DIRECT", lambda sql, params: [(7,)]):
        plan, result = await _run(buffered, _LOOKUP, [7, 1])
    src = buffered.state.federation_engine
    assert plan.route == Route.DIRECT and plan.auto_deliver is not None
    assert result.rows == [(7,)] and result.redirect is None
    assert len(src.statements) == 1, "a primary-key lookup is exactly one statement"
    source_id, sql, params = src.statements[0]
    assert source_id == "pg" and params == [7, 1]
    assert '"sales"."orders"' in sql and "WHERE" in sql and "$1" in sql
    assert src.engine_statements == []


@pytest.mark.asyncio
async def test_a_buffered_read_past_the_threshold_still_redirects(buffered, monkeypatch):
    """The probe is bounded at threshold+1 rows whatever the client's own limit; a result that
    fills it is landed by the engine from the engine-physical statement, and the handle returned."""
    import provisa.executor.redirect as R

    landed: dict = {}

    async def _fake_run_materialize(state, sql, deliv, params):
        landed["sql"] = sql
        landed["params"] = params
        return {"sink": "object-store", "redirect_url": "http://x", "row_count": None}

    monkeypatch.setattr(R, "run_materialize", _fake_run_materialize)
    sql = "SELECT o.id FROM sales.orders o LIMIT $1"
    with buffered.use("DIRECT", lambda sql, params: [(i,) for i in range(6)]):
        plan, result = await _run(buffered, sql, [50000])
    src = buffered.state.federation_engine
    assert len(src.statements) == 1
    assert src.statements[0][1].endswith("LIMIT 6"), "the probe reads threshold+1 rows at most"
    assert src.statements[0][2] == [50000] and plan.exec_params == [50000]
    assert result.rows == [] and result.redirect["redirect_url"] == "http://x"
    assert landed["sql"] == await plan.engine_landing()
    assert landed["params"] == [50000], "the landed statement lost its bound values"
    # The landing reads the whole governed result (the role's own row ceiling), not the probe.
    assert landed["sql"].endswith("LIMIT 100") and "LIMIT 6" not in landed["sql"]


@pytest.mark.asyncio
async def test_a_buffered_read_at_the_threshold_is_inlined(buffered):
    sql = "SELECT o.id FROM sales.orders o LIMIT $1"
    with buffered.use("DIRECT", lambda sql, params: [(i,) for i in range(5)]):
        _plan_, result = await _run(buffered, sql, [50000])
    assert len(result.rows) == 5 and result.redirect is None


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT o.id FROM sales.orders o", "LIMIT 6"),
        ("SELECT o.id FROM sales.orders o LIMIT 9000", "LIMIT 6"),
        ("SELECT o.id FROM sales.orders o LIMIT 3", "LIMIT 3"),
    ],
)
@pytest.mark.asyncio
async def test_a_buffered_read_with_no_bound_limit_is_probed_at_threshold_plus_one(
    buffered, sql, expected
):
    with buffered.use("DIRECT", lambda sql, params: [(1,)]):
        await _run(buffered, sql, None)
    assert buffered.state.federation_engine.statements[0][1].endswith(expected)


@pytest.mark.asyncio
async def test_a_buffered_read_the_router_sends_to_the_engine_stays_on_the_engine(buffered):
    """The router's ENGINE decision (several sources, a virtual source, the operator's floor) is
    never turned into a DIRECT read by the buffered threshold."""
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.transpiler.router import Route

    with buffered.use("ENGINE", lambda sql, params: []):
        plan = await buffered.mod._govern_and_route_compiled(
            _LOOKUP,
            "analyst",
            exec_params=[7, 1],
            state=buffered.state,
            buffered=True,
            cache_hint=NO_CACHE_HINT,
        )
    assert plan.route == Route.ENGINE and plan.auto_deliver is not None and plan.physical_sql


@pytest.mark.asyncio
async def test_an_unbuffered_direct_read_is_not_probed(buffered):
    from provisa.compiler.directives import NO_CACHE_HINT

    with buffered.use("DIRECT", lambda sql, params: [(1,)]):
        plan = await buffered.mod._govern_and_route_compiled(
            "SELECT o.id FROM sales.orders o",
            "analyst",
            state=buffered.state,
            cache_hint=NO_CACHE_HINT,
        )
    assert plan.auto_deliver is None and plan.engine_landing is None
    assert not plan.sql.endswith("LIMIT 6")


class _ControlPlane:
    """A tenant store that counts round trips and answers none: any statement is a failure."""

    def __init__(self) -> None:
        self.acquires = 0

    def acquire(self):
        self.acquires += 1
        raise AssertionError("the request path read the control plane")


@pytest.mark.asyncio
async def test_a_direct_read_issues_no_control_plane_statement(buffered, monkeypatch):
    """REQ-1661 (amended 2026-10-01): a plan that reads no landed table — one source the engine
    reads in place, routed to that source's own driver — is executed without a control-plane
    statement. The residency step used to read node_freshness_state once per table of the
    source before every statement."""
    from provisa.compiler.directives import NO_CACHE_HINT

    source = SimpleNamespace(
        id="pg",
        type=SimpleNamespace(value="postgresql"),
        prefer_materialized=False,
        load_protected=False,
        model_dump_json=lambda: "{}",
    )
    table = SimpleNamespace(
        source_id="pg", schema_name="sales", table_name="orders", row_materialize=False, columns=[]
    )

    async def _sources(state, conn=None):
        return [source]

    async def _tables(state, conn=None):
        return [table]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)

    class _LiveBackend:
        """The engine reads every source in place: nothing ever lands."""

        def require_reconciled(self, source_ids) -> None:
            del source_ids  # every replica here reconciled

        def pending_lands(self, sources, **kw):
            return []

        def is_first_touch(self, source_id):
            return True

    state = buffered.state
    state.tenant_db = _ControlPlane()
    state.config = SimpleNamespace(sources=[source], tables=[table])
    with buffered.use("DIRECT", lambda sql, params: [(7,)]):
        state.federation_engine.engine = SimpleNamespace(
            name="duckdb",
            catalog_qualified=True,
            backend=_LiveBackend(),
            materialize_store=lambda: "duckdb:///:memory:",
        )
        for _ in range(3):
            plan = await buffered.mod._govern_and_route_compiled(
                _LOOKUP, "analyst", exec_params=[7, 1], state=state, cache_hint=NO_CACHE_HINT
            )
            result = await buffered.mod._execute_plan(plan, state)
            assert result.rows == [(7,)]
    assert state.tenant_db.acquires == 0
    assert len(state.federation_engine.statements) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
