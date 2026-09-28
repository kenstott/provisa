# Copyright (c) 2026 Kenneth Stott
# Canary: 7a4b8c2d-1e6f-4a9b-8c3d-5f2e7a1b9c4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882: `prepare_front_end`'s sqlglot tokenization runs on the event loop's executor, not
in-line on the calling coroutine's own thread — the fix for the governance-serialization defect a
live py-spy dump caught (see docs/arch/requirements.yaml, REQ-1882).

Confirms `loop.run_in_executor` is actually invoked (not just present in source, per this task's
own instruction) by patching the running loop's `run_in_executor` with a spy that still executes
the call for real, and separately confirms the parse literally runs off the calling thread.
"""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.compiler import prepared


class _FakeState(SimpleNamespace):
    pass


def _state(schema_boot_id="boot-1", schema_version=1):
    return _FakeState(
        schema_boot_id=schema_boot_id, schema_version=schema_version, metrics={}, tables=[]
    )


async def _no_localize(tree, role_id, state):  # noqa: ARG001 - matches injected signature
    return False


@pytest.fixture(autouse=True)
def _clear_cache():
    prepared.clear()
    yield
    prepared.clear()


@pytest.mark.asyncio
async def test_cache_miss_parse_uses_run_in_executor():
    state = _state()
    loop = asyncio.get_running_loop()
    with patch.object(loop, "run_in_executor", wraps=loop.run_in_executor) as spy:
        result = await prepared.prepare_front_end(
            "SELECT * FROM perf_bench.orders WHERE order_id = 1", "org_admin", state, _no_localize
        )
    assert spy.call_count >= 1
    assert result.parsed is not None


@pytest.mark.asyncio
async def test_cache_hit_parse_also_uses_run_in_executor():
    state = _state()
    sql = "SELECT * FROM perf_bench.orders WHERE order_id = 1"
    await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)  # populate the cache

    loop = asyncio.get_running_loop()
    with patch.object(loop, "run_in_executor", wraps=loop.run_in_executor) as spy:
        result = await prepared.prepare_front_end(sql, "org_admin", state, _no_localize)
    assert result.cache_hit is True
    assert spy.call_count >= 1  # the cache-hit path re-parses the cached text off-loop too


@pytest.mark.asyncio
async def test_parse_actually_runs_off_the_calling_thread():
    """Not just "run_in_executor was called" — the sqlglot parse itself must execute on a
    worker thread, not the event-loop thread, or the fix does nothing under real load."""
    state = _state()
    calling_thread = threading.current_thread()
    seen_threads: list[threading.Thread] = []

    import sqlglot as _sqlglot

    real_parse_one = _sqlglot.parse_one

    def _tracking_parse_one(*args, **kwargs):
        seen_threads.append(threading.current_thread())
        return real_parse_one(*args, **kwargs)

    with patch.object(_sqlglot, "parse_one", side_effect=_tracking_parse_one):
        await prepared.prepare_front_end(
            "SELECT * FROM perf_bench.orders WHERE order_id = 1", "org_admin", state, _no_localize
        )

    assert seen_threads, "sqlglot.parse_one was never called"
    assert seen_threads[0] is not calling_thread
