# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3e1a7d-4b8f-4e2a-9d6c-1a7e4f2b8c9d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882: `_off_loop` (provisa.pgwire._pipeline) actually dispatches to the running loop's
executor rather than calling the function in-line — the mechanism the Flight-SQL/gRPC governance
serialization fix (REQ-1882) relies on for its blocking sqlglot-parse / SQL-rewrite call sites.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import patch

import pytest

from provisa.pgwire._pipeline import _off_loop


@pytest.mark.asyncio
async def test_off_loop_returns_the_function_result():
    result = await _off_loop(lambda a, b: a + b, 2, 3)
    assert result == 5


@pytest.mark.asyncio
async def test_off_loop_passes_kwargs():
    result = await _off_loop(lambda a, b=None: (a, b), 1, b=9)
    assert result == (1, 9)


@pytest.mark.asyncio
async def test_off_loop_uses_run_in_executor():
    loop = asyncio.get_running_loop()
    with patch.object(loop, "run_in_executor", wraps=loop.run_in_executor) as spy:
        await _off_loop(lambda: 1)
    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_off_loop_runs_the_call_off_the_calling_thread():
    calling_thread = threading.current_thread()
    seen = {}

    def _record():
        seen["thread"] = threading.current_thread()
        return 42

    result = await _off_loop(_record)
    assert result == 42
    assert seen["thread"] is not calling_thread


@pytest.mark.asyncio
async def test_off_loop_propagates_exceptions():
    def _boom():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await _off_loop(_boom)
