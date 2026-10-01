# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2b4d7f-1a6c-4f39-b5e0-3c9a7d1f6e28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extended-protocol portal lifecycle over a real asyncpg client (REQ-589).

A Bind replaces the portal it names, so a result a PREVIOUS query left suspended under that name
must never be served to the new one; a Describe(Statement) reports the governed plan's shape from
the engine's declared types without running the statement, so each fetch runs the engine once and
the described types are the executed types."""

# Requirements: REQ-589

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio

from provisa.executor.result import QueryResult as EngineResult
from tests.unit.pgwire.test_wire_protocol import _free_port, _make_server


@pytest_asyncio.fixture(scope="module")
async def pgwire_port():
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()


@pytest.fixture
def trust_state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.contexts = {"alice": ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    return state


def _pipeline(calls: list[str]):
    async def _stub(sql, role_id, params=None):
        calls.append(sql)
        if "FROM t" in sql:
            return EngineResult(rows=[(i,) for i in range(5)], column_names=["n"])
        return EngineResult(rows=[(1,)], column_names=["?column?"])

    return _stub


async def _connect(port: int):
    # statement_cache_size=0: every query is an UNNAMED statement + UNNAMED portal — the shape a
    # pooled asyncpg client (and the perf-bench harness) uses.
    return await asyncpg.connect(
        host="127.0.0.1",
        port=port,
        user="alice",
        password="x",
        database="provisa",
        statement_cache_size=0,
    )


def _engine_plan(sql: str):
    from provisa.pgwire._pipeline import _mint_stamp, _Plan
    from provisa.transpiler.router import Route

    return _Plan(
        route=Route.ENGINE,
        sql=sql,
        source_id="s",
        dialect="duckdb",
        physical_sql=sql,
        exec_params=[],
        stamp=_mint_stamp(),
    )


class _Engine:
    """A federation engine double with the real seam's contract: describe returns zero rows with
    declared types; execute returns the rows with the same declared types."""

    def __init__(self, names: list[str], types: list[str], rows: list[tuple], dialect="duckdb"):
        self.names, self.types, self.rows, self.dialect = names, types, rows, dialect
        self.executed: list[str] = []
        self.described: list[str] = []

    def execute_engine_sync(self, sql, params, *, session_hints=None):
        self.executed.append(sql)
        return EngineResult(rows=list(self.rows), column_names=self.names, column_types=self.types)

    def describe_engine_sync(self, sql, params=None):
        self.described.append(sql)
        return EngineResult(rows=[], column_names=self.names, column_types=self.types)


def _serving(trust_state, engine: _Engine, govern_sql: str):
    """Patches wiring the pgwire session to ``engine`` through a governed ENGINE plan."""
    from contextlib import ExitStack

    from provisa.pgwire.server import ProvisaSession

    async def _govern(sql, role_id, params=None):
        return _engine_plan(govern_sql)

    async def _no_cache(plan, state):
        return None

    async def _resident(state, plan):
        return None

    trust_state.federation_engine = engine
    stack = ExitStack()
    stack.enter_context(patch("provisa.api.app.state", trust_state))
    stack.enter_context(patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern))
    stack.enter_context(
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache)
    )
    stack.enter_context(
        patch("provisa.federation.query_residency.prepare_engine_residency", _resident)
    )
    stack.enter_context(
        patch.object(ProvisaSession, "_finalize_audit", lambda self, governed, status: None)
    )
    return stack


@pytest.mark.asyncio
async def test_a_suspended_portal_is_not_served_to_the_next_query(pgwire_port, trust_state):
    """fetchval sends Execute(limit=1); a 1-row result exactly fills it, so the portal suspends.
    The next query rebinds the unnamed portal — it must run, not get the drained SELECT 1."""
    calls: list[str] = []
    with (
        patch("provisa.api.app.state", trust_state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _pipeline(calls)),
    ):
        conn = await _connect(pgwire_port)
        try:
            assert await conn.fetchval("SELECT 1") == 1
            rows = await conn.fetch("SELECT n FROM t")
            assert [r[0] for r in rows] == [0, 1, 2, 3, 4]
            assert await conn.fetchval("SELECT 1") == 1
            assert len(await conn.fetch("SELECT n FROM t")) == 5
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_a_prepared_statement_executes_once_per_fetch(pgwire_port, trust_state):
    """Describe(Statement) + Bind + Execute runs the engine ONCE: the Describe asks the engine for
    the shape (describe_engine_sync), it never executes the statement."""
    engine = _Engine(["n"], ["BIGINT"], [(i,) for i in range(5)])
    with _serving(trust_state, engine, "SELECT n FROM t"):
        conn = await _connect(pgwire_port)
        try:
            for _ in range(3):
                assert [r[0] for r in await conn.fetch("SELECT n FROM t")] == [0, 1, 2, 3, 4]
        finally:
            await conn.close()
    assert engine.executed == ["SELECT n FROM t"] * 3
    assert engine.described == ["SELECT n FROM t"] * 3


@pytest.mark.asyncio
async def test_a_parameterized_prepared_statement_executes_once_per_fetch(pgwire_port, trust_state):
    """The Describe of a statement WITH parameters no longer runs it with placeholder values."""
    engine = _Engine(["n"], ["BIGINT"], [(7,)])
    with _serving(trust_state, engine, "SELECT n FROM t WHERE n = 7"):
        conn = await _connect(pgwire_port)
        try:
            for _ in range(3):
                assert [r[0] for r in await conn.fetch("SELECT n FROM t WHERE n = $1", 7)] == [7]
        finally:
            await conn.close()
    assert engine.executed == ["SELECT n FROM t WHERE n = 7"] * 3
    assert len(engine.described) == 3


@pytest.mark.asyncio
async def test_a_zero_row_result_is_described_and_returns_no_rows(pgwire_port, trust_state):
    engine = _Engine(["n", "amount"], ["BIGINT", "DECIMAL(18,2)"], [])
    with _serving(trust_state, engine, "SELECT n, amount FROM t WHERE false"):
        conn = await _connect(pgwire_port)
        try:
            stmt = await conn.prepare("SELECT n, amount FROM t WHERE false")
            assert [a.name for a in stmt.get_attributes()] == ["n", "amount"]
            assert [a.type.name for a in stmt.get_attributes()] == ["int8", "numeric"]
            assert await stmt.fetch() == []
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_a_governance_hidden_column_is_absent_from_the_describe(pgwire_port, trust_state):
    """The Describe is governed by the one pipeline: it describes the GOVERNED plan, never the
    client's text, so a column the role cannot see is not in the RowDescription."""
    engine = _Engine(["n"], ["BIGINT"], [(1,)])
    with _serving(trust_state, engine, "SELECT n FROM t"):  # governance dropped `secret`
        conn = await _connect(pgwire_port)
        try:
            stmt = await conn.prepare("SELECT n, secret FROM t")
            assert [a.name for a in stmt.get_attributes()] == ["n"]
        finally:
            await conn.close()
    assert engine.described == ["SELECT n FROM t"]


@pytest.mark.asyncio
async def test_a_passthrough_eligible_statement_is_rerun_for_its_execute(pgwire_port, trust_state):
    """On a Postgres engine the Execute takes the REQ-1863 raw-DataRow passthrough, which needs the
    Bind's result formats up front; the Describe still only describes."""
    engine = _Engine(["n"], ["int8"], [(i,) for i in range(5)], dialect="postgres")
    passthrough: list[str] = []

    def _passthrough(sql, params, result_fmt, *, run):
        from provisa.pgwire.pg_passthrough import PassthroughError

        passthrough.append(sql)
        raise PassthroughError("not a real Postgres in this test")

    engine.execute_pg_engine_passthrough = _passthrough  # type: ignore[attr-defined]
    with _serving(trust_state, engine, "SELECT n FROM t"):
        conn = await _connect(pgwire_port)
        try:
            assert [r[0] for r in await conn.fetch("SELECT n FROM t")] == [0, 1, 2, 3, 4]
        finally:
            await conn.close()
    assert passthrough == ["SELECT n FROM t"]
    assert engine.executed == ["SELECT n FROM t"]  # the passthrough fell back once, not twice


@pytest.mark.parametrize(
    "type_name, expected",
    [
        ("DECIMAL(18,2)", "DECIMAL"),
        ("decimal(18, 2)", "DECIMAL"),
        ("NUMERIC(10,2)", "DECIMAL"),
        ("VARCHAR(20)", "TEXT"),
        ("TIMESTAMP(3)", "TIMESTAMP"),
        ("INTEGER[]", "INTEGERARRAY"),
        ("BIGINT", "BIGINT"),
    ],
)
def test_parameterized_type_names_map_to_their_base_wire_type(type_name, expected):
    from buenavista.core import BVType

    from provisa.pgwire.server import _sql_type_to_bvtype

    assert _sql_type_to_bvtype(type_name) == BVType[expected]


@pytest.mark.asyncio
async def test_a_decimal_column_round_trips_exactly_in_binary(pgwire_port, trust_state):
    """DuckDB reports DECIMAL(18,2); asyncpg reads numerics in binary — the value must arrive
    as the exact Decimal, not crash the TEXT encoder or come back as a float."""
    engine = _Engine(["amount"], ["DECIMAL(18,2)"], [(Decimal("12.34"),), (Decimal("-0.05"),)])
    with _serving(trust_state, engine, "SELECT amount FROM t"):
        conn = await _connect(pgwire_port)
        try:
            rows = await conn.fetch("SELECT amount FROM t")
        finally:
            await conn.close()
    assert [r[0] for r in rows] == [Decimal("12.34"), Decimal("-0.05")]


def test_the_response_cache_key_separates_bound_values():
    """A bound value is not in the SQL text any more, so the cache key must carry it — results for
    one value are never served for another."""
    from provisa.pgwire._pipeline import _response_cache_key

    a = _engine_plan("SELECT n FROM t WHERE n = $1")
    b = _engine_plan("SELECT n FROM t WHERE n = $1")
    for p in (a, b):  # REQ-1897: a read the pipeline marked cacheable, with its governed role
        p.response_cacheable = True
        p.cache_opt_in = True  # REQ-544 (amended): the request opted in
        p.role_id = "r"
    assert _response_cache_key(a, wire_formats=None) is not None
    a.exec_params, b.exec_params = [1], [2]
    assert _response_cache_key(a, wire_formats=None) != _response_cache_key(b, wire_formats=None)
    b.exec_params = [1]
    assert _response_cache_key(a, wire_formats=None) == _response_cache_key(b, wire_formats=None)


@pytest.mark.asyncio
async def test_governance_receives_placeholders_and_bound_values(pgwire_port, trust_state):
    """pgwire hands governance the statement WITH its $N placeholders plus the values — it never
    splices a value into the SQL text."""
    seen: list[tuple[str, list | None]] = []
    engine = _Engine(["n"], ["BIGINT"], [(7,)])

    async def _govern(sql, role_id, params=None):
        seen.append((sql, params))
        return _engine_plan("SELECT n FROM t WHERE n = $1")

    with _serving(trust_state, engine, "unused"):
        with patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern):
            conn = await _connect(pgwire_port)
            try:
                for v in (7, 8):
                    await conn.fetch("SELECT n FROM t WHERE n = $1", v)
            finally:
                await conn.close()
    assert {s for s, _ in seen} == {"SELECT n FROM t WHERE n = $1"}
    assert [p for _, p in seen] == [[42], [7], [42], [8]]  # Describe (int8 example) + Execute


@pytest.mark.asyncio
async def test_an_asyncpg_bound_int_reaches_the_engine_as_an_int(pgwire_port, trust_state):
    """asyncpg re-Parses with unspecified types after its Describe; the Describe-resolved int8
    must survive so the binary Bind value decodes as 7, not as raw bytes."""
    engine = _Engine(["n"], ["BIGINT"], [(7,)])
    bound: list = []
    real = engine.execute_engine_sync

    def _capture(sql, params, *, session_hints=None):
        bound.append(params)
        return real(sql, params, session_hints=session_hints)

    engine.execute_engine_sync = _capture  # type: ignore[method-assign]

    async def _govern(sql, role_id, params=None):
        plan = _engine_plan("SELECT n FROM t WHERE n = $1")
        plan.exec_params = params
        return plan

    with _serving(trust_state, engine, "unused"):
        with patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern):
            conn = await _connect(pgwire_port)
            try:
                assert [r[0] for r in await conn.fetch("SELECT n FROM t WHERE n = $1", 7)] == [7]
            finally:
                await conn.close()
    assert bound == [[7]]
