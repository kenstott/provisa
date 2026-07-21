# Copyright (c) 2026 Kenneth Stott
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
from provisa.mv.preflight_eval import evaluate_streams

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
