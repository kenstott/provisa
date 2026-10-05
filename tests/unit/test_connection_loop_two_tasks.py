# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8e2f71-5a4d-4b9e-a1d6-7e0f2c9b4a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882 (amended 2026-09-29): two tasks on one connection loop.

A connection loop's default executor runs ``run_in_executor`` work INLINE on the request's own
thread. That is only sound while the request runs as ONE task: if a sibling task on the same loop
holds a shared resource across an await (an engine connection, a lock) and the running task then
blocks the thread waiting for that resource, the holder can never be scheduled again — the thread
is blocked inside the very loop that would resume it.

These tests reproduce that deadlock with the real ConnectionLoop and a thread-safe bounded pool
shaped like the engine runtimes' (``pg_runtime._AdbcConnectionPool``: ``getconn`` blocks until a
connection is returned), and pin the fix on the request path that ran sibling tasks: a multi-root
GraphQL query now executes its root fields one after another on the request's thread.
"""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from provisa.core.connection_loop import connection_loop


class _OneConnectionPool:
    """A thread-safe pool of one connection whose ``getconn`` blocks until it is returned.

    Bounded wait (``timeout``) only so a reproduced deadlock fails the test instead of hanging it."""

    def __init__(self, timeout: float) -> None:
        self._cond = threading.Condition()
        self._free = True
        self._timeout = timeout

    def getconn(self) -> None:
        with self._cond:
            if not self._cond.wait_for(lambda: self._free, timeout=self._timeout):
                raise TimeoutError("pool exhausted: the connection's holder never ran again")
            self._free = False

    def putconn(self) -> None:
        with self._cond:
            self._free = True
            self._cond.notify()


def _run_on_connection_thread(make_coro) -> Any:
    """Run ``make_coro()`` the way a request runs: its own thread, its own connection loop."""
    outcome: dict[str, object] = {}

    def _thread() -> None:
        with connection_loop() as cl:
            try:
                outcome["result"] = cl.run(make_coro())
            except BaseException as exc:  # handed back to the test thread, which asserts on it
                outcome["error"] = exc

    t = threading.Thread(target=_thread)
    t.start()
    t.join(timeout=30)
    assert not t.is_alive(), "the connection thread never finished"
    if "error" in outcome:
        raise outcome["error"]  # type: ignore[misc]
    return outcome["result"]


def _field_work(pool: _OneConnectionPool, order: list[str]):
    """One root field's execution: borrow the engine connection (blocking call, run via the
    loop's executor), await something while holding it (a control-plane read), give it back."""

    async def _execute(name: str) -> str:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.getconn)
        order.append(f"{name}:acquired")
        await asyncio.sleep(0.05)  # the holder yields while it has the connection
        pool.putconn()
        order.append(f"{name}:released")
        return name

    return _execute


def test_two_tasks_holding_and_awaiting_a_shared_resource_deadlock_on_one_connection_loop():
    """The hazard itself: gathered on one connection loop, field b's inline getconn blocks the
    thread while field a — holding the only connection and suspended at its await — can never
    resume to return it."""
    pool = _OneConnectionPool(timeout=2.0)
    order: list[str] = []
    execute = _field_work(pool, order)

    async def _gathered() -> list[str]:
        return list(await asyncio.gather(execute("a"), execute("b")))

    with pytest.raises(TimeoutError, match="holder never ran again"):
        _run_on_connection_thread(_gathered)
    assert order == ["a:acquired"], order


def test_the_same_work_as_one_task_completes_on_the_connection_loop():
    pool = _OneConnectionPool(timeout=2.0)
    order: list[str] = []
    execute = _field_work(pool, order)

    async def _sequential() -> list[str]:
        return [await execute("a"), await execute("b")]

    assert _run_on_connection_thread(_sequential) == ["a", "b"]
    assert order == ["a:acquired", "a:released", "b:acquired", "b:released"]


def test_multi_root_graphql_query_runs_its_fields_as_one_task_on_the_request_thread():
    """The request path that gathered root fields (provisa/api/data/endpoint.py _handle_query)
    now runs them one after another: the same shared-connection work completes instead of
    deadlocking, every field executes on the request's thread, and no sibling task exists while a
    field runs."""
    from provisa.api.data import endpoint

    pool = _OneConnectionPool(timeout=2.0)
    order: list[str] = []
    execute = _field_work(pool, order)
    field_threads: list[int] = []
    sibling_tasks: list[int] = []

    async def _execute_one_field(compiled, *args, **kwargs):
        del args, kwargs
        field_threads.append(threading.get_ident())
        me = asyncio.current_task()
        sibling_tasks.append(sum(1 for t in asyncio.all_tasks() if t is not me and not t.done()))
        name = await execute(compiled.name)
        return name, [{"v": name}], None, None

    state = SimpleNamespace()
    compiled = [SimpleNamespace(name="a"), SimpleNamespace(name="b")]
    request_thread: list[int] = []

    async def _request():
        import provisa.api.app as app_mod
        from provisa.core.request_context import reset_current_org, set_current_org

        request_thread.append(threading.get_ident())
        # the request's org, bound on its thread as the org-routing middleware binds it (REQ-1266)
        token = set_current_org(app_mod.state.org_id)
        try:
            return await endpoint._handle_query(
                None,
                None,
                state,
                {},
                {},
                "json",
                "analyst",
                cache_ttl=None,
                cache_opt_in=False,
                debug_trace=False,
            )
        finally:
            reset_current_org(token)

    with (
        patch.object(endpoint, "_split_action_fields", lambda _document, _state: ([], ["a", "b"])),
        patch.object(endpoint, "compile_query", lambda _document, _ctx, _variables: compiled),
        patch.object(endpoint, "_execute_one_field", _execute_one_field),
    ):
        response = _run_on_connection_thread(_request)

    assert response.status_code == 200
    assert order == ["a:acquired", "a:released", "b:acquired", "b:released"]
    assert set(field_threads) == set(request_thread), "a field ran off the request's thread"
    assert sibling_tasks == [0, 0], "a field ran alongside a sibling task on the request loop"
