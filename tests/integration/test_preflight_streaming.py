# Copyright (c) 2026 Kenneth Stott
# Canary: d3d89488-0305-4b2a-86ad-34fc63092a1c
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: per-input streaming preflight over a REAL DuckDB engine (REQ-1165).

Every collaborator is real — DuckDB runs the count probe and delivers Arrow record batches through a
lazy reader; :func:`provisa.mv.preflight_eval.evaluate_streams` opens one stream PER INPUT NODE and
gates on it. This proves the runtime contract, not a fake: a SQL-expressible check is pushed down to
an engine-side ``count(*)`` over the named input; a non-SQL check streams that input's Arrow batches
through the compiled hook and short-circuits; and a streaming check on an engine that does NOT
advertise ARROW_STREAM fails loud (no materialize fallback).
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest

from provisa.executor.result import QueryResult
from provisa.federation.runtime import EngineCapability, UnsupportedCapabilityError
from provisa.mv.preflight import Decision
from provisa.mv.preflight_eval import evaluate_streams, make_rows_evaluator, make_streams_evaluator

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _DuckEngine:
    """A real DuckDB engine shim: ``execute_engine`` runs the count probe; ``execute_engine_stream``
    hands back DuckDB's native lazy Arrow reader (``fetch_record_batch``) so nothing is materialized;
    ``supports`` / ``require`` advertise the declared capabilities."""

    dialect = "duckdb"

    def __init__(self, con, caps=frozenset({EngineCapability.ARROW_STREAM})):
        self.con = con
        self._caps = caps
        self.stream_calls: list[str] = []

    async def execute_engine(self, sql: str, *a, **k) -> QueryResult:
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return QueryResult(rows=[tuple(r) for r in cur.fetchall()], column_names=cols)

    def execute_engine_stream(self, sql: str, *a, **k):
        self.stream_calls.append(sql)
        reader = self.con.execute(sql).to_arrow_reader(1)  # chunk=1 → lazily yielded batches
        return reader.schema, reader

    def supports(self, cap) -> bool:
        return cap in self._caps

    def require(self, cap) -> None:
        if cap not in self._caps:
            raise UnsupportedCapabilityError("duckdb", cap)


class _Ctx(SimpleNamespace):
    def ok(self):
        from provisa.mv.preflight import CONTINUE

        return CONTINUE

    def abort(self, reason=None):
        from provisa.mv.preflight import Verdict

        return Verdict(Decision.ABORT, reason)

    def quarantine(self, reason=None):
        from provisa.mv.preflight import Verdict

        return Verdict(Decision.QUARANTINE, reason)


@pytest.fixture()
def con():
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10),(2,-3),(3,7)) AS v(id, qty)")
    c.execute("CREATE TABLE clean AS SELECT * FROM (VALUES (1,5),(2,6)) AS v(id, qty)")
    yield c
    c.close()


_ANY_NEG = (
    "def preflight(streams, ctx):\n"
    "    if any(r['qty'] < 0 for r in streams['orders']):\n"
    "        return ctx.abort('negative')\n"
    "    return ctx.ok()"
)
_NON_SQL_QUARANTINE = (
    "def preflight(streams, ctx):\n"
    "    total = 0\n"
    "    for r in streams['orders']:\n"
    "        total = total + r['qty']\n"
    "    if total < 20:\n"
    "        return ctx.quarantine('sum too low')\n"
    "    return ctx.ok()"
)


async def test_sql_pushdown_over_real_input_node(con):
    # A SQL-expressible check → engine-side count(*) over the INPUT NODE; no Arrow stream is opened.
    eng = _DuckEngine(con)
    verdict = await evaluate_streams(eng, _ANY_NEG, ["orders"], _Ctx())
    assert verdict.decision is Decision.ABORT and verdict.reason == "negative"
    assert eng.stream_calls == []  # pushed down — no per-row streaming needed

    # The same shape over an input with no negatives continues (probed engine-side over 'clean').
    clean_check = _ANY_NEG.replace("streams['orders']", "streams['clean']")
    verdict2 = await evaluate_streams(eng, clean_check, ["clean"], _Ctx())
    assert verdict2.decision is Decision.CONTINUE


async def test_non_sql_streams_real_arrow_batches(con):
    # A cross-row check (running sum) cannot push down → it streams the input's real Arrow batches.
    eng = _DuckEngine(con)
    verdict = await evaluate_streams(eng, _NON_SQL_QUARANTINE, ["orders"], _Ctx())
    # orders qty sum = 10 - 3 + 7 = 14 < 20 → quarantine
    assert verdict.decision is Decision.QUARANTINE and verdict.reason == "sum too low"
    assert eng.stream_calls and "orders" in eng.stream_calls[0]  # streamed the input node


async def test_non_sql_check_fails_loud_without_arrow_stream(con):
    # REQ-1165: a streaming check on an engine lacking ARROW_STREAM fails loud — no materialize fallback.
    eng = _DuckEngine(con, caps=frozenset())  # ROWS only, no ARROW_STREAM
    with pytest.raises(UnsupportedCapabilityError):
        await evaluate_streams(eng, _NON_SQL_QUARANTINE, ["orders"], _Ctx())


async def test_per_input_keying_isolates_streams(con):
    # The hook selects ONE input by name; the other input's stream is never opened (lazy per-node).
    eng = _DuckEngine(con)
    # A cross-row check over 'clean' only (sum = 11) → continue; 'orders' is present but untouched.
    check = (
        "def preflight(streams, ctx):\n"
        "    total = 0\n"
        "    for r in streams['clean']:\n"
        "        total = total + r['qty']\n"
        "    if total < 5:\n"
        "        return ctx.abort('low')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["orders", "clean"], _Ctx())
    assert verdict.decision is Decision.CONTINUE
    # 'clean' was scanned; 'orders' reader was opened lazily but its rows never pulled — both nodes
    # had a reader prepared, but only 'clean' is iterated by the hook.
    assert any("clean" in s for s in eng.stream_calls)


async def test_real_arrow_reader_is_lazy(con):
    # Guard the premise: DuckDB's Arrow reader yields batches lazily (not one materialized table).
    reader = con.execute("SELECT * FROM orders").to_arrow_reader(1)
    assert isinstance(reader, pa.RecordBatchReader)
    first = reader.read_next_batch()
    assert first.num_rows == 1  # one row at a time, streamed


# ── extended scenarios ──────────────────────────────────────────────────────


async def test_sql_pushdown_continue_when_no_row_matches(con):
    # any(qty < 0) over 'clean' (all non-negative) → count 0 → CONTINUE, still pushed down.
    eng = _DuckEngine(con)
    check = _ANY_NEG.replace("streams['orders']", "streams['clean']")
    verdict = await evaluate_streams(eng, check, ["clean"], _Ctx())
    assert verdict.decision is Decision.CONTINUE
    assert eng.stream_calls == []


async def test_all_quantifier_pushdown_fires_on_vacuous_empty(con):
    # all(P for r in empty) is vacuously TRUE → the 'all' branch fires. Real engine, empty input.
    con.execute("CREATE TABLE empty_t AS SELECT * FROM (VALUES (1,1)) AS v(id, qty) WHERE 1=0")
    eng = _DuckEngine(con)
    check = (
        "def preflight(streams, ctx):\n"
        "    if all(r['qty'] > 0 for r in streams['empty_t']):\n"
        "        return ctx.quarantine('all positive (vacuous)')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["empty_t"], _Ctx())
    assert verdict.decision is Decision.QUARANTINE  # count of violators == 0 → all() fires


async def test_all_quantifier_pushdown_continue_when_a_row_violates(con):
    # all(qty > 0) over orders (has -3) → a violator exists → 'all' does NOT fire → CONTINUE.
    eng = _DuckEngine(con)
    check = (
        "def preflight(streams, ctx):\n"
        "    if all(r['qty'] > 0 for r in streams['orders']):\n"
        "        return ctx.quarantine('all positive')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["orders"], _Ctx())
    assert verdict.decision is Decision.CONTINUE


async def test_sql_pushdown_quarantine_verdict(con):
    # A pushdown check can return quarantine (not only abort) — the verdict vocabulary is complete.
    eng = _DuckEngine(con)
    check = (
        "def preflight(streams, ctx):\n"
        "    if any(r['qty'] < 0 for r in streams['orders']):\n"
        "        return ctx.quarantine('some negative')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["orders"], _Ctx())
    assert verdict.decision is Decision.QUARANTINE and verdict.reason == "some negative"


async def test_boolean_arithmetic_null_predicate_parity_on_real_engine(con):
    # REQ-964: a compound boolean/arith/NULL predicate pushes down and matches the engine's own count.
    con.execute(
        "CREATE TABLE t2 AS SELECT * FROM (VALUES (1,10,5,'x'),(2,1,9,NULL)) AS v(id,a,b,c)"
    )
    eng = _DuckEngine(con)
    check = (
        "def preflight(streams, ctx):\n"
        "    if any((r['a'] + 1) > r['b'] and r['c'] != None for r in streams['t2']):\n"
        "        return ctx.abort('x')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["t2"], _Ctx())
    # row1: (10+1)>5 and 'x' is not null → True → abort fires
    assert verdict.decision is Decision.ABORT
    assert eng.stream_calls == []  # pushed down


async def test_non_sql_continue_when_predicate_not_met(con):
    # A cross-row (streaming) check whose condition is not met → CONTINUE, having streamed the input.
    eng = _DuckEngine(con)
    check = (
        "def preflight(streams, ctx):\n"
        "    total = 0\n"
        "    for r in streams['orders']:\n"
        "        total = total + r['qty']\n"
        "    if total > 100:\n"
        "        return ctx.abort('too big')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["orders"], _Ctx())
    assert verdict.decision is Decision.CONTINUE  # sum 14 < 100
    assert eng.stream_calls  # it did stream


async def test_multi_input_streaming_check_reads_both(con):
    # A non-SQL check over TWO inputs streams each and combines them.
    eng = _DuckEngine(con)
    check = (
        "def preflight(streams, ctx):\n"
        "    a = sum(r['qty'] for r in streams['orders'])\n"  # 14
        "    b = sum(r['qty'] for r in streams['clean'])\n"  # 11
        "    if a + b > 100:\n"
        "        return ctx.abort('combined too big')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["orders", "clean"], _Ctx())
    assert verdict.decision is Decision.CONTINUE
    assert any("orders" in s for s in eng.stream_calls)
    assert any("clean" in s for s in eng.stream_calls)


async def test_no_check_returns_none(con):
    eng = _DuckEngine(con)
    assert await evaluate_streams(eng, None, ["orders"], _Ctx()) is None
    assert await evaluate_streams(eng, "   ", ["orders"], _Ctx()) is None


async def test_wiring_allows_sql_check_on_non_streaming_engine(con):
    # A SQL-expressible check needs only ROWS — make_streams_evaluator must NOT reject it at wiring
    # even when the engine lacks ARROW_STREAM (only non-SQL checks require streaming).
    eng = _DuckEngine(con, caps=frozenset())  # no ARROW_STREAM
    evaluator = make_streams_evaluator(eng, _ANY_NEG, ["orders"])
    assert evaluator is not None
    verdict = await evaluator(["ignored", "output", "rows"], _Ctx())  # rows arg ignored
    assert verdict.decision is Decision.ABORT


async def test_wiring_rejects_streaming_check_on_non_streaming_engine(con):
    # A non-SQL (streaming) check on a non-ARROW_STREAM engine fails LOUD at wiring, not first fire.
    eng = _DuckEngine(con, caps=frozenset())
    with pytest.raises(ValueError, match="ARROW_STREAM"):
        make_streams_evaluator(eng, _NON_SQL_QUARANTINE, ["orders"])


async def test_source_rows_evaluator_gates_in_memory(con):
    # The LANDED-SOURCE evaluator runs the hook over its own fetched rows ({node: rows}) — no engine.
    evaluator = make_rows_evaluator(_NON_SQL_QUARANTINE.replace("streams['orders']", "streams['s.o']"), "s.o")
    assert evaluator is not None
    rows = [{"id": 1, "qty": 5}, {"id": 2, "qty": 6}]  # sum 11 < 20 → quarantine
    verdict = await evaluator(rows, _Ctx())
    assert verdict.decision is Decision.QUARANTINE


async def test_source_rows_evaluator_none_when_no_source():
    assert make_rows_evaluator(None, "s.o") is None
    assert make_rows_evaluator("  ", "s.o") is None


async def test_per_input_stream_is_single_pass(con):
    # A streamed input is a ONE-SHOT iterator (it must be, to stream): a hook that consumes it in a
    # first loop sees it EMPTY on a second pass. This is the contract — authors iterate each input once.
    eng = _DuckEngine(con)
    check = (
        "def preflight(streams, ctx):\n"
        "    first = sum(1 for _ in streams['orders'])\n"  # consumes the stream (3 rows)
        "    second = sum(1 for _ in streams['orders'])\n"  # same exhausted iterator → 0
        "    if first == 3 and second == 0:\n"
        "        return ctx.abort('single-pass confirmed')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(eng, check, ["orders"], _Ctx())
    assert verdict.decision is Decision.ABORT and verdict.reason == "single-pass confirmed"
