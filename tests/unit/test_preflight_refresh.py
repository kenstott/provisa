# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8d1e60-5a29-4c73-9f18-2d6e0a3c7b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1165: the scheduled-refresh preflight gate — SQL pushdown + Arrow streaming.

The gate runs before materialization: a SQL-expressible check is pushed down as a count probe
(no rows to Python), a non-SQL check streams the SELECT as Arrow batches. Anything but CONTINUE
skips the rebuild (ABORT → STALE + error; QUARANTINE → SKIPPED_PREFLIGHT hold).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from provisa.executor.result import QueryResult
from provisa.federation.runtime import EngineCapability
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.preflight import Decision
from provisa.mv.refresh import _evaluate_preflight, refresh_mv
from provisa.mv.registry import MVRegistry

_ANY_NEG = (
    "def preflight(rows, ctx):\n"
    "    if any(r['qty'] < 0 for r in rows):\n"
    "        return ctx.abort('negative')\n"
    "    return ctx.ok()"
)
_NON_SQL = (
    "def preflight(rows, ctx):\n"
    "    if len(list(rows)) > 2:\n"
    "        return ctx.quarantine('too many')\n"
    "    return ctx.ok()"
)


class _Engine:
    """A minimal engine: the preflight count probe (contains ``_preflight``) returns ``violations``;
    the source-size probe (contains ``_probe``) returns ``size``; an Arrow table backs the stream."""

    def __init__(self, *, violations=0, size=0, arrow_rows=None, caps=frozenset()):
        self.violations = violations
        self.size = size
        self.arrow_rows = arrow_rows or []
        self._caps = caps
        self.sqls: list[str] = []

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        if "_preflight" in sql:
            return QueryResult(rows=[(self.violations,)], column_names=["count"])
        if "_probe" in sql:
            return QueryResult(rows=[(self.size,)], column_names=["count"])
        return QueryResult(rows=[], column_names=[])

    def supports(self, cap) -> bool:
        return cap in self._caps

    def execute_engine_arrow(self, sql, *a, **k) -> pa.Table:
        return pa.Table.from_pylist(self.arrow_rows)


def _mv(mv_id="mv-x", preprocess=None):
    return MVDefinition(
        id=mv_id,
        source_tables=["orders"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        sql="SELECT id, qty FROM orders",
        refresh_interval=300,
        preprocess=preprocess,
    )


@pytest.mark.asyncio
async def test_no_check_returns_none() -> None:
    assert await _evaluate_preflight(_Engine(), _mv(), "SELECT id, qty FROM orders") is None


@pytest.mark.asyncio
async def test_sql_pushdown_abort_and_continue() -> None:
    eng = _Engine(violations=2)
    v = await _evaluate_preflight(eng, _mv(preprocess=_ANY_NEG), "SELECT id, qty FROM orders")
    assert v.decision is Decision.ABORT and v.reason == "negative"
    # the probe was pushed down (no per-row Python) — one count(*) over the SELECT
    assert any("_preflight" in s and "count(*)" in s for s in eng.sqls)

    eng2 = _Engine(violations=0)
    v2 = await _evaluate_preflight(eng2, _mv(preprocess=_ANY_NEG), "SELECT id, qty FROM orders")
    assert v2.decision is Decision.CONTINUE


@pytest.mark.asyncio
async def test_non_sql_streams_arrow_and_quarantines() -> None:
    eng = _Engine(arrow_rows=[{"id": 1, "qty": 5}, {"id": 2, "qty": 6}, {"id": 3, "qty": 7}])
    v = await _evaluate_preflight(eng, _mv(preprocess=_NON_SQL), "SELECT id, qty FROM orders")
    assert v.decision is Decision.QUARANTINE and v.reason == "too many"
    # never pushed down (no _preflight probe) — evaluated over Arrow batches instead
    assert not any("_preflight" in s for s in eng.sqls)


@pytest.mark.asyncio
async def test_refresh_mv_aborts_before_materializing() -> None:
    # A pushdown ABORT must skip the rebuild: no CREATE TABLE / INSERT is ever issued.
    eng = _Engine(violations=3, size=1)
    mv = _mv(preprocess=_ANY_NEG)
    registry = MVRegistry()
    registry.register(mv)
    await refresh_mv(eng, mv, registry)
    assert mv.status is MVStatus.STALE  # fatal reject leaves it stale with the reason
    assert "preflight abort" in (mv.last_error or "")
    assert not any(s.startswith("CREATE TABLE") or s.startswith("INSERT") for s in eng.sqls)


@pytest.mark.asyncio
async def test_refresh_mv_quarantine_holds() -> None:
    eng = _Engine(size=1, arrow_rows=[{"id": i, "qty": i} for i in range(4)])
    mv = _mv(preprocess=_NON_SQL)
    registry = MVRegistry()
    registry.register(mv)
    await refresh_mv(eng, mv, registry)
    assert mv.status is MVStatus.SKIPPED_PREFLIGHT
    assert "preflight quarantine" in (mv.last_error or "")


@pytest.mark.asyncio
async def test_arrow_stream_capability_preferred() -> None:
    # When ARROW_STREAM is advertised, the lazy reader is used (schema, batch-generator).
    batches = pa.Table.from_pylist([{"id": 1, "qty": 1}]).to_batches()

    class _StreamEngine(_Engine):
        def execute_engine_stream(self, sql, *a, **k):
            return object(), iter(batches)

    eng = _StreamEngine(caps=frozenset({EngineCapability.ARROW_STREAM}))
    v = await _evaluate_preflight(eng, _mv(preprocess=_NON_SQL), "SELECT id, qty FROM orders")
    assert v.decision is Decision.CONTINUE  # only 1 row → not "too many"
