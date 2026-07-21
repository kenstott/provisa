# Copyright (c) 2026 Kenneth Stott
# Canary: 4861e93c-02da-4d5c-8343-2d29e74bfb95
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-input streaming preflight evaluation (REQ-1165).

The preflight CHECK receives a ``dict[str, Iterable[dict]]`` keyed by INPUT NODE — one lazy Arrow
stream per MV input — NOT a fully-materialized row set. A gate over a billion-row input must never
buffer it whole; it iterates batches and short-circuits (``any``/``all`` stops at the first decisive
row). Two evaluation strategies, and they MUST reach the same verdict for a dataset (REQ-964):

- SQL-EXPRESSIBLE: a single quantified assertion over one ``streams["node"]`` is pushed down to an
  engine-side ``SELECT count(*)`` over that input node (:mod:`provisa.mv.preflight_sql`) — no rows
  enter Python. This path needs only ROWS, so a non-streaming engine can still run it.
- PYTHON+ARROW: everything else opens one lazily-streamed Arrow reader per input node
  (``execute_engine_stream``, ARROW_STREAM capability) and feeds the ``{node: rows}`` dict to the
  compiled hook. This path REQUIRES ARROW_STREAM; on an engine lacking it the evaluator fails loud
  (never a silent skip, never a materialize-the-whole-input fallback).

Capability is also checked at WIRING (:func:`make_streams_evaluator`) so a streaming preflight on a
non-streaming engine is rejected at boot, not on the first fire.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from provisa.mv.preflight import Verdict, run_preflight
from provisa.mv.preflight_sql import _quoted_from, translate
from provisa.mv.preprocess import compile_preprocess
from provisa.processors.arrow import rows_of


def _requires_stream(source: str) -> bool:
    """True when the check cannot be pushed down as SQL and so needs the Arrow-stream path."""
    return translate(source) is None


async def _open_input_streams(engine: Any, input_nodes: Iterable[str]) -> dict[str, Any]:
    """Open one lazily-streamed Arrow reader per input node → ``{node: row-iterator}``.

    Each ``execute_engine_stream`` is acquired off the event loop (the transport is synchronous,
    REQ-145); the returned reader is lazy, so the underlying scan runs only when the hook actually
    iterates that input's rows — an input the hook never touches is never scanned."""
    streams: dict[str, Any] = {}
    for node in input_nodes:
        sql = f"SELECT * FROM {_quoted_from(node)}"
        _schema, batches = await asyncio.to_thread(engine.execute_engine_stream, sql)
        del _schema  # the reader carries its own schema; we only stream batches
        streams[node] = rows_of(batches)
    return streams


async def evaluate_streams(
    engine: Any, source: str | None, input_nodes: Iterable[str], ctx: Any
) -> Verdict | None:
    """Evaluate a preflight check over per-input Arrow streams (REQ-1165). ``None`` when no check.

    SQL-expressible → engine-side count probe over the named input node. Otherwise → open a lazy
    Arrow stream per input and run the compiled hook against the ``{node: rows}`` dict. The Arrow
    path REQUIRES ARROW_STREAM and fails loud when the engine does not advertise it."""
    if source is None or not source.strip():
        return None
    sqlpf = translate(source)
    if sqlpf is not None:
        res = await engine.execute_engine(sqlpf.count_sql())
        return sqlpf.verdict_for(res.rows[0][0])
    from provisa.federation.runtime import EngineCapability  # noqa: PLC0415

    engine.require(EngineCapability.ARROW_STREAM)  # fail loud — never materialize the whole input
    fn = compile_preprocess(source)
    streams = await _open_input_streams(engine, input_nodes)
    return await run_preflight(fn, streams, ctx)


def make_streams_evaluator(
    engine: Any, source: str | None, input_nodes: Iterable[str]
) -> Callable[[Any, Any], Awaitable[Verdict | None]] | None:
    """Bind a preflight evaluator for the event loop (REQ-1165). ``None`` when the node declares no
    check. Fails LOUD at wiring when the check needs streaming (not SQL-expressible) and the engine
    lacks ARROW_STREAM — a streaming preflight on a non-streaming engine is rejected at boot.

    The returned callable has the gate signature ``(rows, ctx) -> Verdict | None``; ``rows`` (the
    produced OUTPUT) is ignored — a preflight's subject is its INPUTS, streamed, not the output."""
    if source is None or not source.strip():
        return None
    from provisa.federation.runtime import EngineCapability  # noqa: PLC0415

    nodes = tuple(input_nodes)
    if _requires_stream(source) and not engine.supports(EngineCapability.ARROW_STREAM):
        raise ValueError(
            f"preflight check requires ARROW_STREAM but engine {getattr(engine, 'name', '?')!r} "
            f"does not advertise it (REQ-1165: no materialize fallback)"
        )

    async def _eval(rows: Any, ctx: Any) -> Verdict | None:
        del rows  # the gate streams inputs; the produced output is not its subject
        return await evaluate_streams(engine, source, nodes, ctx)

    return _eval


def make_rows_evaluator(
    source: str | None, node: str
) -> Callable[[Any, Any], Awaitable[Verdict | None]] | None:
    """Bind a preflight evaluator for a LANDED SOURCE (REQ-1165). ``None`` when no check.

    A source has no engine-queryable lineage inputs — its own fetched rows ARE the input — and the
    adapter already materialized them to land. So the gate runs the compiled hook over the single
    ``{node: rows}`` input (no engine streaming, no SQL pushdown), keeping the same ``streams`` dict
    authoring shape as an MV check."""
    if source is None or not source.strip():
        return None
    fn = compile_preprocess(source)

    async def _eval(rows: Any, ctx: Any) -> Verdict | None:
        return await run_preflight(fn, {node: rows}, ctx)

    return _eval
