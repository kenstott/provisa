# Copyright (c) 2026 Kenneth Stott
# Canary: 76f06c3c-901e-45f8-92db-075bd29b1cb3
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-1165 — per-input streaming preflight check.

Every collaborator is real: DuckDB runs the count probe and delivers Arrow record batches through a
lazy reader, and :func:`provisa.mv.preflight_eval.evaluate_streams` gates on per-input streams. The
scenarios prove the three runtime behaviors — SQL pushdown over the named input, Arrow-batch
streaming for a non-SQL check, and fail-loud on an engine without ARROW_STREAM.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import duckdb
import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.federation.runtime import EngineCapability, UnsupportedCapabilityError
from provisa.mv.preflight import CONTINUE, Decision, Verdict
from provisa.mv.preflight_eval import evaluate_streams

scenarios("../features/REQ-1165-preflight-streaming.feature")


class _Ctx(SimpleNamespace):
    def ok(self):
        return CONTINUE

    def abort(self, reason=None):
        return Verdict(Decision.ABORT, reason)

    def quarantine(self, reason=None):
        return Verdict(Decision.QUARANTINE, reason)


class _DuckEngine:
    dialect = "duckdb"

    def __init__(self, con, caps=frozenset({EngineCapability.ARROW_STREAM})):
        self.con = con
        self._caps = caps
        self.stream_calls: list[str] = []

    async def execute_engine(self, sql, *a, **k):
        from provisa.executor.result import QueryResult

        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return QueryResult(rows=[tuple(r) for r in cur.fetchall()], column_names=cols)

    def execute_engine_stream(self, sql, *a, **k):
        self.stream_calls.append(sql)
        reader = self.con.execute(sql).to_arrow_reader(1)
        return reader.schema, reader

    def supports(self, cap):
        return cap in self._caps

    def require(self, cap):
        if cap not in self._caps:
            raise UnsupportedCapabilityError("duckdb", cap)


@pytest.fixture()
def ctx_bag():
    return {}


@given('a real engine with an input node "orders" holding a negative quantity')
def _engine_with_negative(ctx_bag):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10),(2,-3),(3,7)) AS v(id, qty)")
    ctx_bag["engine"] = _DuckEngine(con)
    ctx_bag["inputs"] = ["orders"]


@given('a real engine with an input node "orders" whose quantities sum below the threshold')
def _engine_sum_low(ctx_bag):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10),(2,-3),(3,7)) AS v(id, qty)")
    ctx_bag["engine"] = _DuckEngine(con)  # sum = 14 < 20
    ctx_bag["inputs"] = ["orders"]


@given("a real engine that does not advertise ARROW_STREAM")
def _engine_no_stream(ctx_bag):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10)) AS v(id, qty)")
    ctx_bag["engine"] = _DuckEngine(con, caps=frozenset())  # ROWS only
    ctx_bag["inputs"] = ["orders"]


@given("a preflight check that aborts when any orders row has a negative quantity")
def _check_abort(ctx_bag):
    ctx_bag["source"] = (
        "def preflight(streams, ctx):\n"
        "    if any(r['qty'] < 0 for r in streams['orders']):\n"
        "        return ctx.abort('negative')\n"
        "    return ctx.ok()"
    )


@given("a preflight check that quarantines when the running sum is too low")
def _check_quarantine(ctx_bag):
    ctx_bag["source"] = (
        "def preflight(streams, ctx):\n"
        "    total = 0\n"
        "    for r in streams['orders']:\n"
        "        total = total + r['qty']\n"
        "    if total < 20:\n"
        "        return ctx.quarantine('sum too low')\n"
        "    return ctx.ok()"
    )


@given("a non-SQL preflight check over the input")
def _check_non_sql(ctx_bag):
    ctx_bag["source"] = (
        "def preflight(streams, ctx):\n"
        "    total = 0\n"
        "    for r in streams['orders']:\n"
        "        total = total + r['qty']\n"
        "    return ctx.ok()"
    )


@when("the preflight gate evaluates before landing")
def _evaluate(ctx_bag):
    async def _run():
        return await evaluate_streams(
            ctx_bag["engine"], ctx_bag["source"], ctx_bag["inputs"], _Ctx()
        )

    try:
        ctx_bag["verdict"] = asyncio.run(_run())
        ctx_bag["error"] = None
    except Exception as exc:  # captured for the fail-loud scenario
        ctx_bag["verdict"] = None
        ctx_bag["error"] = exc


@then("the verdict is abort")
def _then_abort(ctx_bag):
    assert ctx_bag["error"] is None
    assert ctx_bag["verdict"].decision is Decision.ABORT


@then("the verdict is quarantine")
def _then_quarantine(ctx_bag):
    assert ctx_bag["error"] is None
    assert ctx_bag["verdict"].decision is Decision.QUARANTINE


@then("no Arrow stream was opened for the input")
def _then_no_stream(ctx_bag):
    assert ctx_bag["engine"].stream_calls == []  # SQL pushdown, not streamed


@then("the input node was streamed as Arrow batches")
def _then_streamed(ctx_bag):
    assert ctx_bag["engine"].stream_calls and "orders" in ctx_bag["engine"].stream_calls[0]


@then("the gate raises an unsupported-capability error")
def _then_fails_loud(ctx_bag):
    assert isinstance(ctx_bag["error"], UnsupportedCapabilityError)


@given('a real engine with an input node "orders" holding only non-negative quantities')
def _engine_clean(ctx_bag):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10),(2,3),(3,7)) AS v(id, qty)")
    ctx_bag["engine"] = _DuckEngine(con)
    ctx_bag["inputs"] = ["orders"]


@given("a preflight check that quarantines when all orders rows are non-negative")
def _check_all(ctx_bag):
    ctx_bag["source"] = (
        "def preflight(streams, ctx):\n"
        "    if all(r['qty'] >= 0 for r in streams['orders']):\n"
        "        return ctx.quarantine('all non-negative')\n"
        "    return ctx.ok()"
    )


@given("no preflight check is declared")
def _no_check(ctx_bag):
    ctx_bag["source"] = None


@then("the verdict is continue")
def _then_continue(ctx_bag):
    assert ctx_bag["error"] is None
    assert ctx_bag["verdict"].decision is Decision.CONTINUE


@then("the verdict is none (continue)")
def _then_none(ctx_bag):
    assert ctx_bag["error"] is None
    assert ctx_bag["verdict"] is None
