# Copyright (c) 2026 Kenneth Stott
# Canary: 494015e2-41ec-4014-897c-da8d6f875ed3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's preflight check (REQ-1165) run over its replica build's own rows, as they stream.

The check is one hook over ``{node: rows}``, a lazy iterable of the whole table, so it may count,
sum or compare across every row. A build already holds each batch to write it; the batch is also
handed to the hook, which runs once in a worker thread and reads the rows through a queue of at
most :data:`QUEUED_BATCHES` batches. So the hook sees the whole table, lazily, and the build holds
no more than that beyond what it held before. The verdict is awaited when the read has ended,
before the swap: ABORT (or a hook that raises) and QUARANTINE leave the replica as it was.

A hook that decides before the end (an ``any``/``all`` that short-circuits) is fed no more; the
build copies on.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import threading
from collections.abc import Iterator
from typing import Any

log = logging.getLogger(__name__)

#: The batches handed to the hook that it has not read yet, at most.
QUEUED_BATCHES = 2

_END = object()


class RowCheck:
    """One run of a table's check over one build's rows. ``start`` it, ``feed`` it each batch of
    rows the build writes, and ``end`` it before the swap (which raises the verdict that stops the
    swap); ``abandon`` it when the build ends without one."""

    def __init__(
        self, fn: Any, *, node: str, columns: list[tuple[str, str]], prior_hash: str | None
    ):
        self._fn = fn
        self._node = node
        self._columns = columns
        self._prior_hash = prior_hash
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=QUEUED_BATCHES)
        self._finished = threading.Event()  # the hook has returned or raised
        self._abandoned = threading.Event()
        self._task: asyncio.Future[Any] | None = None
        self._ctx: Any = None

    def start(self) -> None:
        from provisa.events.processor import NodeContext

        self._ctx = NodeContext(
            node=self._node,
            kind="source",
            claimed=[],
            prior_hash=self._prior_hash,
            columns=self._columns,
        )
        self._task = asyncio.ensure_future(asyncio.to_thread(self._run))

    def _rows(self) -> Iterator[dict]:
        while True:
            item = self._queue.get()
            if item is _END or self._abandoned.is_set():
                return
            yield from item

    def _run(self) -> Any:
        from provisa.mv.preflight import run_preflight

        try:
            # Its own event loop: the hook may be async, and it blocks on the queue as it reads.
            return asyncio.run(run_preflight(self._fn, {self._node: self._rows()}, self._ctx))
        finally:
            self._finished.set()

    def _put(self, item: Any) -> None:
        # Waits while the hook has unread batches; gives up once it has returned.
        while not self._finished.is_set():
            try:
                self._queue.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    async def feed(self, rows: list[dict]) -> None:
        """Hand the hook one batch of the rows the build writes."""
        if self._finished.is_set():
            return  # the hook has decided: it reads no more
        await asyncio.to_thread(self._put, rows)

    async def end(self) -> None:
        """The hook's verdict on the whole table, before the swap: returns on CONTINUE (a reason
        is logged as the advisory warn it is), raises ``PreprocessError`` on ABORT or a hook that
        raised, and ``PreflightQuarantine`` on QUARANTINE."""
        from provisa.events.handlers import _apply_preflight

        if self._task is None:
            raise RuntimeError(f"the preflight of {self._node} was ended before it was started")
        await asyncio.to_thread(self._put, _END)
        task = self._task

        async def _verdict(rows: Any, ctx: Any) -> Any:
            del rows, ctx  # the hook already ran over the streamed rows, with this check's ctx
            return await task

        await _apply_preflight(_verdict, [], self._ctx, self._columns)
        for reason in self._ctx.warns:
            log.warning("preflight of %s: %s", self._node, reason)

    def abandon(self) -> None:
        """The build ended without a swap: the hook is fed no more and its verdict is not asked
        for. Its thread returns at the next row it reads."""
        self._abandoned.set()
        try:
            self._queue.put_nowait(_END)
        except queue.Full:
            pass  # the hook reads the abandonment at its next batch


def row_check(
    table: Any, *, node: str, columns: list[tuple[str, str]], prior_hash: str | None
) -> RowCheck | None:
    """The check a table declares (``mv_preprocess``) for one build of its replica; None when it
    declares none."""
    source = getattr(table, "mv_preprocess", None)
    if source is None or not str(source).strip():
        return None
    from provisa.mv.preflight_services import compile_check

    fn = compile_check(source, subject=f"table {node}")
    return RowCheck(fn, node=node, columns=columns, prior_hash=prior_hash)


__all__ = ["QUEUED_BATCHES", "RowCheck", "row_check"]
