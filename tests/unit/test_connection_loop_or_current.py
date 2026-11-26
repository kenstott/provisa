# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3c1a84-2e9d-4b8f-b6a0-5c3d7e1f9a02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""connection_loop_or_current reuses an already-bound loop instead of raising (REQ-2225).

When an asyncio threadpool thread already has a connection loop bound (e.g. from
spawn_background), and _stood_in() is called on that thread during script validation,
connection_loop() raises RuntimeError("a different connection loop is already bound to
this thread").  connection_loop_or_current() must yield the already-bound loop so that
_stood_in() can run its coroutine without touching the pool.
"""

from __future__ import annotations

import threading

from provisa.core.connection_loop import connection_loop, connection_loop_or_current


def test_connection_loop_or_current_reuses_bound_loop() -> None:
    """When a loop is already bound on this thread, connection_loop_or_current yields it."""
    result: list[object] = []

    def _run() -> None:
        with connection_loop() as outer:
            with connection_loop_or_current() as inner:
                result.append(inner is outer)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=5)
    assert result == [True], f"expected [True], got {result}"


def test_connection_loop_or_current_checks_out_when_unbound() -> None:
    """When no loop is bound, connection_loop_or_current checks one out from the pool."""
    result: list[object] = []

    def _run() -> None:
        with connection_loop_or_current() as cl:
            result.append(cl is not None)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=5)
    assert result == [True], f"expected [True], got {result}"


def test_connection_loop_raises_when_already_bound() -> None:
    """Confirm the existing behavior: connection_loop() raises when a loop is already bound."""
    errors: list[Exception] = []

    def _run() -> None:
        with connection_loop():
            try:
                with connection_loop():
                    pass
            except RuntimeError as exc:
                errors.append(exc)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=5)
    assert len(errors) == 1
    assert "a different connection loop is already bound to this thread" in str(errors[0])
