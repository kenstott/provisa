# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9a41-5c86-40b3-91f7-3e0a8c6d24b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1150: `CALL <command>(args)` over Bolt/Cypher invokes a registered command through the
single governed executor (invoke_tracked_function), mapping positional args to declared names."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.bolt.session import _maybe_invoke_command_call, _parse_call_arg

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


def _state():
    return SimpleNamespace(
        tracked_functions={
            "random_python_set": {
                "name": "random_python_set",
                "arguments": [{"name": "rows"}, {"name": "seed"}],
            }
        }
    )


async def test_call_command_invokes_executor():
    rows = [{"id": 1, "region": "east"}]
    with patch(
        "provisa.api.data.action_exec.invoke_tracked_function",
        new=AsyncMock(return_value=rows),
    ) as inv:
        result = await _maybe_invoke_command_call(
            "CALL random_python_set(3, 7)", "admin", _state()
        )
    assert result == (["id", "region"], [[1, "east"]])
    # positional args mapped to the command's declared argument names
    inv.assert_awaited_once()
    assert inv.await_args is not None
    assert inv.await_args.args[0] == "random_python_set"
    assert inv.await_args.args[1] == {"rows": 3, "seed": 7}


async def test_non_command_call_falls_through():
    # unknown name → None so normal Cypher parsing proceeds
    assert await _maybe_invoke_command_call("CALL nope()", "admin", _state()) is None
    # a plain MATCH is not a CALL
    assert await _maybe_invoke_command_call("MATCH (n) RETURN n", "admin", _state()) is None


async def test_yield_clause_tolerated():
    with patch(
        "provisa.api.data.action_exec.invoke_tracked_function",
        new=AsyncMock(return_value=[{"id": 1}]),
    ):
        result = await _maybe_invoke_command_call(
            "CALL random_python_set(2) YIELD id RETURN id", "admin", _state()
        )
    assert result == (["id"], [[1]])


async def test_parse_call_arg_types():
    assert _parse_call_arg("3") == 3
    assert _parse_call_arg("3.5") == 3.5
    assert _parse_call_arg("'x'") == "x"
    assert _parse_call_arg("true") is True
    assert _parse_call_arg("null") is None
