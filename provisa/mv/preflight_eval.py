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

from provisa.federation.execution_auth import system_auth

from provisa.mv.preflight import Verdict, run_preflight
from provisa.mv.preflight_sql import _quoted_from, translate
from provisa.mv.preflight_services import compile_check
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
        # REQ-1912: an input served from its replica is streamed from its replica's address; the
        # stream keeps the input's own name as its key, which is what the check names.
        sql = engine.address_replicas(f"SELECT * FROM {_quoted_from(node)}")
        _schema, batches = await asyncio.to_thread(engine.execute_engine_stream, sql)
        del _schema  # the reader carries its own schema; we only stream batches
        streams[node] = rows_of(batches)
    return streams


async def evaluate_streams(
    engine: Any,
    source: str | None,
    input_nodes: Iterable[str],
    ctx: Any,
    *,
    product_id: str | None = None,
    hold: bool = False,
    table_id: int | None = None,
) -> Verdict | None:
    """Evaluate a preflight check over per-input Arrow streams (REQ-1165). ``None`` when no check.

    SQL-expressible → engine-side count probe over the named input node. Otherwise → open a lazy
    Arrow stream per input and run the compiled hook against the ``{node: rows}`` dict. The Arrow
    path REQUIRES ARROW_STREAM and fails loud when the engine does not advertise it."""
    held = await _hold_on_failing_checks(ctx, hold, table_id, source, product_id)
    if held is not None and not held.is_continue:
        return held
    if source is None or not source.strip():
        return held
    sqlpf = translate(source)
    if sqlpf is not None:
        # REQ-1912: the probe counts the input where the engine reads it.
        res = await engine.execute_engine(
            engine.address_replicas(sqlpf.count_sql()), authorization=system_auth("view preflight")
        )
        return sqlpf.verdict_for(res.rows[0][0])
    from provisa.federation.runtime import EngineCapability  # noqa: PLC0415

    engine.require(EngineCapability.ARROW_STREAM)  # fail loud — never materialize the whole input
    # REQ-2133: a check that imports provisa_services runs with them bound, dq relating checkers
    # to the view's product.
    fn = compile_check(source, subject=f"view {ctx.node}", product_id=product_id)
    streams = await _open_input_streams(engine, input_nodes)
    return await run_preflight(fn, streams, ctx)


async def _hold_on_failing_checks(
    ctx: Any, hold: bool, table_id: int | None, source: str | None, product_id: str | None
) -> Verdict | None:
    """REQ-2133: the view's Hold on failing checks verdict (None when the option is off), run before
    its own preflight -- the refresh lands only if both continue. A view whose check asks dq is given
    its nearest physical inputs on ``ctx`` whichever check asks."""
    from provisa.mv import hold_checks
    from provisa.mv.preprocess import uses_services

    if table_id is not None and (hold or uses_services(source)):
        ctx.nearest_inputs = await hold_checks.nearest_inputs(table_id)
    if not hold:
        return None
    if table_id is None:
        raise ValueError(
            f"view {ctx.node} holds on failing checks, but it is no registered view table, so it "
            "has no inputs to check"
        )
    check = compile_check(hold_checks.SOURCE, subject=f"view {ctx.node}", product_id=product_id)
    return await run_preflight(check, {}, ctx)


def make_streams_evaluator(
    engine: Any,
    source: str | None,
    input_nodes: Iterable[str],
    *,
    product_id: str | None = None,
    hold: bool = False,
    table_id: int | None = None,
) -> Callable[[Any, Any], Awaitable[Verdict | None]] | None:
    """Bind a preflight evaluator for the event loop (REQ-1165). ``None`` when the node declares no
    check. Fails LOUD at wiring when the check needs streaming (not SQL-expressible) and the engine
    lacks ARROW_STREAM — a streaming preflight on a non-streaming engine is rejected at boot.

    The returned callable has the gate signature ``(rows, ctx) -> Verdict | None``; ``rows`` (the
    produced OUTPUT) is ignored — a preflight's subject is its INPUTS, streamed, not the output.
    A view that holds on failing checks (REQ-2133) is checked with no preflight of its own."""
    if (source is None or not source.strip()) and not hold:
        return None
    from provisa.federation.runtime import EngineCapability  # noqa: PLC0415

    nodes = tuple(input_nodes)
    if (
        source is not None
        and source.strip()
        and _requires_stream(source)
        and not engine.supports(EngineCapability.ARROW_STREAM)
    ):
        raise ValueError(
            f"preflight check requires ARROW_STREAM but engine {getattr(engine, 'name', '?')!r} "
            f"does not advertise it (there is no materialize fallback)"
        )

    async def _eval(rows: Any, ctx: Any) -> Verdict | None:
        del rows  # the gate streams inputs; the produced output is not its subject
        return await evaluate_streams(
            engine, source, nodes, ctx, product_id=product_id, hold=hold, table_id=table_id
        )

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
    fn = compile_check(source, subject=f"table {node}")

    async def _eval(rows: Any, ctx: Any) -> Verdict | None:
        return await run_preflight(fn, {node: rows}, ctx)

    return _eval
