# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4b2d7a-1c6f-4a3e-b8d5-6f0a2c9e1b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request's budget holds even while blocking work owns its thread (REQ-1882, amended
2026-09-29): the watchdog cancels the in-flight statement at the deadline."""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from provisa.core import request_deadline
from provisa.core.connection_loop import connection_loop


class _BlockingStatement:
    """Stands in for a driver call: blocks until cancelled or ``duration`` elapses."""

    def __init__(self, duration: float) -> None:
        self.duration = duration
        self._cancelled = threading.Event()

    def cancel(self) -> None:
        self._cancelled.set()

    def execute(self) -> str:
        if self._cancelled.wait(self.duration):
            raise RuntimeError("canceling statement due to user request")
        return "done"


def test_one_second_budget_cancels_a_five_second_blocking_statement() -> None:
    stmt = _BlockingStatement(5.0)

    async def _request() -> str:
        # No await while it blocks: an asyncio timer alone could never fire here.
        with request_deadline.cancel_on_deadline(stmt.cancel):
            return stmt.execute()

    t0 = time.monotonic()
    with connection_loop() as cl, pytest.raises(TimeoutError, match="1s budget"):
        cl.run(_request(), timeout=1.0)
    elapsed = time.monotonic() - t0
    assert 0.9 <= elapsed < 2.0


def test_statement_within_budget_is_not_cancelled() -> None:
    stmt = _BlockingStatement(0.05)

    async def _request() -> str:
        with request_deadline.cancel_on_deadline(stmt.cancel):
            return stmt.execute()

    with connection_loop() as cl:
        assert cl.run(_request(), timeout=2.0) == "done"


def test_remaining_reflects_the_request_budget() -> None:
    async def _request() -> float | None:
        await asyncio.sleep(0)
        return request_deadline.remaining()

    with connection_loop() as cl:
        left = cl.run(_request(), timeout=3.0)
    assert left is not None and 2.5 < left <= 3.0
    assert request_deadline.remaining() is None  # unbound outside a request


def test_statement_started_after_expiry_fails_immediately() -> None:
    with request_deadline.within(0.05) as dl:
        time.sleep(0.15)
        assert dl.fired
        with pytest.raises(TimeoutError):
            with request_deadline.cancel_on_deadline(lambda: None):
                pytest.fail("must not start a statement after the budget expired")


def test_tighter_enclosing_deadline_wins() -> None:
    with request_deadline.within(1.0) as outer:
        with request_deadline.within(30.0) as inner:
            assert inner is outer


def test_a_budget_timeout_at_an_await_point_names_the_budget() -> None:
    """wait_for's own TimeoutError is empty; the request loop re-raises the deadline's message."""

    async def _slow() -> None:
        await asyncio.sleep(5)

    with connection_loop() as cl, pytest.raises(TimeoutError, match=r"exceeded its 0\.2s budget"):
        cl.run(_slow(), timeout=0.2)
